//! Explicit, bounded source-reported history selection. No second token ledger:
//! adopted observations use the existing event publisher and query pipeline.
use super::{ccswitch, claude, db, ingest, maintenance::ForgetScope, resolve, types::*, Ledger};
use chrono::{NaiveDate, TimeZone};
use chrono_tz::Tz;
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

const PARSER: &str = "reported-history-v1";
const MAX_ITEMS: usize = 512;
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReportedAdoption {
    Requests {
        observation_ids: Vec<i64>,
    },
    Days {
        root_id: String,
        from: String,
        to_exclusive: String,
        timezone: String,
    },
}
#[derive(Debug, Serialize)]
pub struct AdoptionPreview {
    pub preview_token: String,
    pub consumer_tool: String,
    pub quality: String,
    pub candidates: usize,
    pub already_represented: usize,
    pub conflicting_existing: usize,
    pub new_items: usize,
    pub metrics: Metrics,
    pub applied: bool,
    pub warnings: Vec<String>,
}
fn digest(value: &impl Serialize) -> Result<String> {
    let bytes = serde_json::to_vec(value).map_err(|_| LedgerError::RecordTooLarge)?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}
fn day_bounds(from: &str, to: &str, timezone: &str) -> Result<(i64, i64)> {
    let (a, b) = calendar_bounds(from, to, timezone)?;
    if b > now_ms() {
        return Err(LedgerError::InvalidInput(
            "historical range must be closed and in the past".into(),
        ));
    }
    Ok((a, b))
}
fn calendar_bounds(from: &str, to: &str, timezone: &str) -> Result<(i64, i64)> {
    let tz: Tz = timezone
        .parse()
        .map_err(|_| LedgerError::InvalidInput("invalid historical timezone".into()))?;
    let parse = |v: &str| -> Result<i64> {
        let date = NaiveDate::parse_from_str(v, "%Y-%m-%d")
            .map_err(|_| LedgerError::InvalidInput("invalid date".into()))?;
        if date.to_string() != v {
            return Err(LedgerError::InvalidInput("noncanonical date".into()));
        }
        // Do not silently choose between ambiguous or missing historical midnight.
        tz.from_local_datetime(
            &date
                .and_hms_opt(0, 0, 0)
                .ok_or(LedgerError::IncompleteEvidence)?,
        )
        .single()
        .map(|v| v.timestamp_millis())
        .ok_or(LedgerError::IncompleteEvidence)
    };
    let (a, b) = (parse(from)?, parse(to)?);
    if b <= a {
        return Err(LedgerError::InvalidInput(
            "historical range must be closed and in the past".into(),
        ));
    }
    Ok((a, b))
}
fn load(c: &Connection, id: i64) -> Result<Observation> {
    Ok(c.query_row(
        "SELECT * FROM usage_observations WHERE observation_id=?1",
        [id],
        resolve::load_observation,
    )?)
}
fn projection(c: &Connection, id: i64) -> Result<Value> {
    let value: String = c.query_row(
        "SELECT projection_json FROM durable_evidence WHERE observation_id=?1",
        [id],
        |r| r.get(0),
    )?;
    serde_json::from_str(&value).map_err(|_| LedgerError::CorruptLedger)
}
fn selected(c: &Connection, o: &Observation) -> Result<Option<Observation>> {
    Ok(c.query_row("SELECT o.* FROM usage_events e JOIN usage_observations o ON o.observation_id=e.selected_observation_id WHERE e.identity_family=?1 AND e.namespace_key=?2 AND e.identity_key=COALESCE((SELECT canonical_key FROM identity_alias WHERE identity_family=?1 AND namespace_key=?2 AND alias_key=?3),?3) AND e.state='active'",params![o.identity_family,o.namespace_key,o.identity_key],resolve::load_observation).optional()?)
}
/// A paged diagnostic list. It exposes only bounded usage metadata already saved
/// in usagebook, never reads upstream configuration or imports anything.
impl Ledger {
    pub fn reported_history(&self, tool: &str, after: i64, limit: usize) -> Result<Value> {
        if !matches!(tool, "opencode" | "pi") || !(1..=512).contains(&limit) || after < 0 {
            return Err(LedgerError::InvalidInput("invalid history page".into()));
        }
        let mut stmt=self.conn.prepare("SELECT o.observation_id,d.projection_json,so.root_id FROM usage_observations o JOIN durable_evidence d USING(observation_id) JOIN source_objects so ON so.object_id=o.object_id WHERE o.report_source='ccswitch-rollup' AND o.native_tool=?1 AND o.observation_id>?2 ORDER BY o.observation_id LIMIT ?3")?;
        let mut rows = stmt.query(params![tool, after, limit as i64])?;
        let mut items = Vec::new();
        let mut last = after;
        while let Some(row) = rows.next()? {
            last = row.get(0)?;
            let raw: String = row.get(1)?;
            let value: Value =
                serde_json::from_str(&raw).map_err(|_| LedgerError::CorruptLedger)?;
            if let Some(candidate) = ccswitch::reported_candidate(&value)? {
                items.push(json!({"observation_id":last,"root_id":row.get::<_,String>(2)?,"consumer_tool":candidate.consumer_tool,"grain":candidate.grain,"date":candidate.native_date,"occurred_ms":candidate.occurred_ms,"model":candidate.model_display,"metrics":candidate.metrics,"quality":"source_reported","finality":candidate.finality}));
            }
        }
        Ok(json!({"items":items,"next_after":last,"quality":"source_reported","adopted":false}))
    }
    /// Preview and apply rederive exactly the same manifest inside BEGIN IMMEDIATE.
    /// The bounded operation is atomic, so no long-lived prepared revision can
    /// survive a later forget. Larger histories must be captured in finite scopes.
    pub fn adopt_reported(
        &mut self,
        request: &ReportedAdoption,
        token: Option<&str>,
    ) -> Result<AdoptionPreview> {
        self.require_writer()?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = adopt(&tx, request, token)?;
        if token.is_some() {
            tx.commit()?;
        }
        Ok(result)
    }
}
fn adopt(
    c: &Connection,
    request: &ReportedAdoption,
    token: Option<&str>,
) -> Result<AdoptionPreview> {
    let (ids, range, timezone, kind) = match request {
        ReportedAdoption::Requests { observation_ids } => {
            (observation_ids.clone(), None, None, "request_set")
        }
        ReportedAdoption::Days {
            root_id,
            from,
            to_exclusive,
            timezone,
        } => {
            let range = day_bounds(from, to_exclusive, timezone)?;
            let members = ccswitch::sealed_day_members(c, root_id, "pi", from, to_exclusive)?;
            (
                members.observation_ids,
                Some(range),
                Some(timezone.clone()),
                "day_range",
            )
        }
    };
    if ids.is_empty()
        || ids.len() > MAX_ITEMS
        || ids.iter().any(|id| *id <= 0)
        || ids.iter().collect::<BTreeSet<_>>().len() != ids.len()
    {
        return Err(LedgerError::InvalidInput(
            "select 1..512 distinct saved observations".into(),
        ));
    }
    let tool = if range.is_some() { "pi" } else { "opencode" };
    let live: bool = c.query_row(
        "SELECT EXISTS(SELECT 1 FROM source_roots WHERE report_source=?1 AND route<>'ledger')",
        [tool],
        |r| r.get(0),
    )?;
    if live {
        return Err(LedgerError::InvalidInput(
            "native source must be reconciled into the ledger before historical adoption".into(),
        ));
    }
    let pending_native:bool=c.query_row("SELECT EXISTS(SELECT 1 FROM usage_observations o WHERE o.native_tool=?1 AND o.report_source<>'ccswitch-rollup' AND o.parser_version<>'reported-history-v1' AND o.eligibility='eligible' AND NOT EXISTS(SELECT 1 FROM ingest_runs r WHERE r.identity_family=o.identity_family AND r.namespace_key=o.namespace_key AND r.state='done' AND r.upper_observation_id>=o.observation_id))",[tool],|r|r.get(0))?;
    if pending_native {
        return Err(LedgerError::InvalidInput(
            "native publication must finish before historical adoption".into(),
        ));
    }
    if let Some((a, b)) = range {
        let overlap:bool=c.query_row("SELECT EXISTS(SELECT 1 FROM usage_events WHERE native_tool=?1 AND state='active' AND ((fact_kind='request' AND (occurred_ms IS NULL OR occurred_ms>=?2 AND occurred_ms<?3)) OR fact_kind='day' AND day_start_ms<?3 AND day_end_ms>?2)) OR EXISTS(SELECT 1 FROM adoption_policies WHERE consumer_tool=?1 AND status='active' AND start_ms<?3 AND end_ms>?2)",params![tool,a,b],|r|r.get(0))?;
        if overlap {
            return Err(LedgerError::InvalidInput(
                "range overlaps adopted history; explicit replacement is required".into(),
            ));
        }
    }
    let mut originals = Vec::new();
    let mut additions = Vec::new();
    let mut represented = 0;
    let mut conflicts = 0;
    let mut identities = BTreeSet::new();
    for id in &ids {
        let raw = load(c, *id)?;
        if raw.report_source != "ccswitch-rollup" {
            return Err(LedgerError::InvalidInput(
                "not a saved CC observation".into(),
            ));
        }
        let candidate = ccswitch::reported_candidate(&projection(c, *id)?)?
            .ok_or(LedgerError::IncompleteEvidence)?;
        if candidate.consumer_tool != tool || (range.is_some()) != (candidate.grain == "day") {
            return Err(LedgerError::IncompleteEvidence);
        }
        let mut derived = raw.clone();
        derived.observation_id = 0;
        derived.identity_family = tool.into();
        derived.namespace_key = super::additional_adapters::namespace(tool);
        derived.parser_version = PARSER.into();
        derived.eligibility = "pending_finality".into();
        derived.report_source = tool.into();
        derived.native_tool = tool.into();
        derived.metrics = candidate.metrics;
        derived.model_key = candidate.model_key;
        derived.model_display = candidate.model_display;
        derived.session_key = candidate.session_key;
        derived.native_session_id = candidate.native_session_id;
        derived.native_message_id = candidate.native_message_id;
        derived.native_request_id = None;
        derived.candidate_key = format!("reported:{id}:{}", digest(request)?);
        derived.record_key = format!("reported:{id}");
        derived.cost_state = "unknown".into();
        derived.cost_units = None;
        derived.cost_scale = None;
        derived.cost_currency = None;
        if let Some((a, b)) = range {
            let date = candidate
                .native_date
                .ok_or(LedgerError::IncompleteEvidence)?;
            let next = NaiveDate::parse_from_str(&date, "%Y-%m-%d")
                .map_err(|_| LedgerError::IncompleteEvidence)?
                .succ_opt()
                .ok_or(LedgerError::IncompleteEvidence)?
                .to_string();
            let (s, e) = day_bounds(&date, &next, timezone.as_deref().unwrap())?;
            if s < a || e > b {
                return Err(LedgerError::IncompleteEvidence);
            }
            derived.identity_key = Some(identity_key(
                tool,
                &derived.namespace_key,
                "reported-day",
                &[&raw.record_key, &digest(request)?],
            ));
            derived.native_date = Some(date);
            derived.native_timezone = timezone.clone();
            derived.day_start_ms = Some(s);
            derived.day_end_ms = Some(e);
            derived.occurred_ms = None;
        } else {
            derived.identity_key = candidate.canonical_identity;
            derived.occurred_ms = candidate.occurred_ms;
            if derived.identity_key.is_none() || derived.occurred_ms.is_none() {
                return Err(LedgerError::IncompleteEvidence);
            }
        }
        if !identities.insert(derived.identity_key.clone()) {
            return Err(LedgerError::InvalidInput(
                "multiple versions of the same consumption selected".into(),
            ));
        }
        if ingest::exact_suppressed(c, &derived)? || suppresses_projection(c, &projection(c, *id)?)?
        {
            return Err(LedgerError::InvalidInput(
                "selected history was forgotten".into(),
            ));
        }
        originals.push(raw.clone());
        if let Some(existing) = selected(c, &derived)? {
            represented += 1;
            if existing.metrics != derived.metrics {
                conflicts += 1;
            }
        } else {
            additions.push((raw, derived));
        }
    }
    let epoch: String =
        c.query_row("SELECT epoch FROM ledger_meta WHERE singleton=1", [], |r| {
            r.get(0)
        })?;
    let event_watermark: i64 = c.query_row(
        "SELECT COALESCE(MAX(updated_commit_seq),0) FROM usage_events WHERE native_tool=?1",
        [tool],
        |r| r.get(0),
    )?;
    let controls = crate::ledger::maintenance::adoption_control_digest(c)?;
    let preview_token = digest(&(request, &originals, epoch, event_watermark, controls))?;
    let mut sums: [Option<i64>; 10] = [Some(0); 10];
    for (_, o) in &additions {
        for (sum, v) in sums.iter_mut().zip(o.metrics) {
            *sum = match (*sum, v) {
                (Some(a), Some(b)) => Some(a.checked_add(b).ok_or(LedgerError::NumericOverflow)?),
                _ => None,
            };
        }
    }
    let mut result = AdoptionPreview {
        preview_token: preview_token.clone(),
        consumer_tool: tool.into(),
        quality: "source_reported".into(),
        candidates: ids.len(),
        already_represented: represented,
        conflicting_existing: conflicts,
        new_items: additions.len(),
        metrics: sums,
        applied: false,
        warnings: vec!["Historical finality is not verified; adopted values remain frozen.".into()],
    };
    if range.is_some() {
        result.warnings.push("Historical timezone is explicitly declared; reported counts are not reconstructed individual requests.".into());
    }
    let Some(token) = token else {
        return Ok(result);
    };
    if token != preview_token {
        return Err(LedgerError::StaleObservation);
    }
    if additions.is_empty() {
        result.applied = true;
        return Ok(result);
    }
    let policy_id = digest(&("reported-policy-v1", &preview_token))?;
    c.execute("INSERT INTO adoption_policies(policy_id,consumer_tool,kind,start_ms,end_ms,native_timezone,manifest_hash,created_ms,status) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,'active')",params![policy_id,tool,kind,range.map(|r|r.0),range.map(|r|r.1),timezone,preview_token,now_ms()])?;
    ensure_reported_route(c, tool)?;
    for (raw, mut derived) in additions {
        derived.normalized_hash = claude::normalized_hash(&derived)?;
        ingest::insert_observation(c, &derived)?;
        let id:i64=c.query_row("SELECT observation_id FROM usage_observations WHERE identity_family=?1 AND namespace_key=?2 AND candidate_key=?3 AND normalized_hash=?4",params![derived.identity_family,derived.namespace_key,derived.candidate_key,derived.normalized_hash],|r|r.get(0))?;
        c.execute("INSERT INTO adoption_items(selected_observation_id,source_observation_id,policy_id) VALUES(?1,?2,?3)",params![id,raw.observation_id,policy_id])?;
        let key = resolve::publish_key(&derived).ok_or(LedgerError::IncompleteEvidence)?;
        resolve::rebuild_unit(
            c,
            &derived.identity_family,
            &derived.namespace_key,
            &key,
            id,
            resolve::RebuildCause::Automatic,
        )?;
    }
    db::bump(c)?;
    result.applied = true;
    Ok(result)
}
fn ensure_reported_route(c: &Connection, tool: &str) -> Result<()> {
    let adapter = super::additional_adapters::spec(tool).ok_or(LedgerError::IncompleteEvidence)?;
    c.execute("INSERT INTO source_roots(root_id,identity_family,namespace_key,collector_key,report_source,native_tool,locator,enabled,identity_version,parser_version,route,enumeration_done) SELECT ?1,?2,?3,'reported-history-v1',?2,?2,?4,0,1,?5,'ledger',1 WHERE NOT EXISTS(SELECT 1 FROM source_roots WHERE report_source=?2)",params![format!("reported:{tool}"),tool,super::additional_adapters::namespace(tool),format!("usagebook:reported:{tool}"),adapter.parser_version])?;
    Ok(())
}
/// Applies source-reported pins and interval policy to the publisher's TEMP decisions.
pub(crate) fn guard_selections(c: &Connection, family: &str, namespace: &str) -> Result<()> {
    c.execute("UPDATE ledger_unit_decisions SET selected_id=(SELECT a.selected_observation_id FROM adoption_items a JOIN adoption_policies p USING(policy_id) JOIN usage_observations o ON o.observation_id=a.selected_observation_id WHERE p.status='active' AND o.identity_family=?1 AND o.namespace_key=?2 AND o.identity_key=ledger_unit_decisions.identity_key LIMIT 1),state='active' WHERE EXISTS(SELECT 1 FROM adoption_items a JOIN adoption_policies p USING(policy_id) JOIN usage_observations o ON o.observation_id=a.selected_observation_id WHERE p.status='active' AND o.identity_family=?1 AND o.namespace_key=?2 AND o.identity_key=ledger_unit_decisions.identity_key)",params![family,namespace])?;
    c.execute("UPDATE ledger_unit_decisions SET fact_kind=(SELECT fact_kind FROM usage_observations WHERE observation_id=ledger_unit_decisions.selected_id) WHERE selected_id IS NOT NULL",[])?;
    c.execute("UPDATE ledger_unit_decisions SET selected_id=NULL,state='unresolved',has_conflict=1 WHERE state='active' AND NOT EXISTS(SELECT 1 FROM adoption_items a WHERE a.selected_observation_id=ledger_unit_decisions.selected_id) AND EXISTS(SELECT 1 FROM usage_observations o JOIN adoption_policies p ON p.consumer_tool=o.native_tool WHERE o.observation_id=ledger_unit_decisions.selected_id AND p.status='active' AND p.start_ms IS NOT NULL AND (p.kind='day_range' OR EXISTS(SELECT 1 FROM adoption_items ai JOIN usage_observations ao ON ao.observation_id=ai.selected_observation_id WHERE ai.policy_id=p.policy_id AND ao.identity_family=o.identity_family AND ao.namespace_key=o.namespace_key)) AND ((o.fact_kind='request' AND (o.occurred_ms IS NULL OR (o.occurred_ms>=p.start_ms AND o.occurred_ms<p.end_ms))) OR (o.fact_kind='day' AND (o.day_start_ms IS NULL OR (o.day_start_ms<p.end_ms AND o.day_end_ms>p.start_ms)))))",[])?;
    // Unknown time is evidence to retain, but it cannot prove that this
    // consumption falls outside a forgotten interval. Known overlaps are
    // rejected before saving; this also protects old restored evidence.
    c.execute("UPDATE ledger_unit_decisions SET selected_id=NULL,state='unresolved',has_conflict=1 WHERE state='active' AND EXISTS(SELECT 1 FROM usage_observations o JOIN adoption_forget_ranges f ON f.consumer_tool=o.native_tool WHERE o.observation_id=ledger_unit_decisions.selected_id AND (f.identity_family IS NULL OR (f.identity_family=o.identity_family AND f.namespace_key=o.namespace_key)) AND ((o.fact_kind='request' AND (o.occurred_ms IS NULL OR (o.occurred_ms>=f.start_ms AND o.occurred_ms<f.end_ms))) OR (o.fact_kind='day' AND (o.day_start_ms IS NULL OR (o.day_start_ms<f.end_ms AND o.day_end_ms>f.start_ms)))))",[])?;
    Ok(())
}
pub(crate) fn check(c: &Connection) -> Result<()> {
    let invalid:bool=c.query_row("SELECT EXISTS(SELECT 1 FROM adoption_items a JOIN adoption_policies p USING(policy_id) JOIN usage_observations o ON o.observation_id=a.selected_observation_id WHERE p.status<>'active' OR o.parser_version<>'reported-history-v1' OR o.eligibility<>'pending_finality' OR o.native_tool<>p.consumer_tool)",[],|r|r.get(0))?;
    if invalid {
        Err(LedgerError::CorruptLedger)
    } else {
        Ok(())
    }
}
pub(crate) fn validate_forget(c: &Connection, scope: &ForgetScope) -> Result<()> {
    if let (Some(a), Some(b)) = (scope.start_ms, scope.end_ms) {
        let partial:bool=c.query_row("SELECT EXISTS(SELECT 1 FROM adoption_items i JOIN usage_observations o ON o.observation_id=i.selected_observation_id WHERE o.fact_kind='day' AND (i.selected_observation_id IN(SELECT id FROM delete_ids) OR i.source_observation_id IN(SELECT id FROM delete_ids)) AND (o.day_start_ms<?1 OR o.day_end_ms>?2))",params![a,b],|r|r.get(0))?;
        if partial {
            return Err(LedgerError::InvalidInput(
                "daily adopted history cannot be split; select its complete day".into(),
            ));
        }
    }
    Ok(())
}
pub(crate) fn before_forget(c: &Connection) -> Result<()> {
    c.execute("INSERT OR IGNORE INTO delete_ids SELECT i.selected_observation_id FROM adoption_items i JOIN adoption_policies p USING(policy_id) JOIN usage_observations o ON o.observation_id=i.selected_observation_id WHERE p.status='forgotten' OR EXISTS(SELECT 1 FROM adoption_forget_ranges f WHERE f.consumer_tool=o.native_tool AND (f.identity_family IS NULL OR (f.identity_family=o.identity_family AND f.namespace_key=o.namespace_key)) AND o.day_start_ms<f.end_ms AND o.day_end_ms>f.start_ms)",[])?;
    c.execute("INSERT OR IGNORE INTO delete_ids SELECT selected_observation_id FROM adoption_items WHERE source_observation_id IN(SELECT id FROM delete_ids)",[])?;
    c.execute("INSERT OR IGNORE INTO delete_ids SELECT source_observation_id FROM adoption_items WHERE selected_observation_id IN(SELECT id FROM delete_ids)",[])?;
    c.execute("INSERT OR IGNORE INTO adoption_forget_ranges(consumer_tool,start_ms,end_ms,native_timezone,identity_family,namespace_key) SELECT o.native_tool,o.day_start_ms,o.day_end_ms,o.native_timezone,CASE WHEN p.kind='day_range' THEN NULL ELSE o.identity_family END,CASE WHEN p.kind='day_range' THEN NULL ELSE o.namespace_key END FROM adoption_items i JOIN adoption_policies p USING(policy_id) JOIN usage_observations o ON o.observation_id=i.selected_observation_id WHERE o.fact_kind='day' AND i.selected_observation_id IN(SELECT id FROM delete_ids)",[])?;
    // Mark touched range controls forgotten, even if some other days remain.
    // Retain only dates/tool/timezone, never the forgotten numeric payload.
    c.execute("UPDATE adoption_policies SET status='forgotten' WHERE policy_id IN(SELECT policy_id FROM adoption_items WHERE selected_observation_id IN(SELECT id FROM delete_ids)) AND NOT EXISTS(SELECT 1 FROM adoption_items a WHERE a.policy_id=adoption_policies.policy_id AND a.selected_observation_id NOT IN(SELECT id FROM delete_ids))",[])?;
    c.execute("DELETE FROM adoption_items WHERE selected_observation_id IN(SELECT id FROM delete_ids) OR source_observation_id IN(SELECT id FROM delete_ids)",[])?;
    Ok(())
}
/// All current decisions must exist in the restored ledger, otherwise a newer
/// range could be released by restoring an older file. Never merge token sums.
pub(crate) fn merge_restore(current: &Connection, staging: &Connection) -> Result<()> {
    let mut ranges=current.prepare("SELECT consumer_tool,start_ms,end_ms,native_timezone,identity_family,namespace_key FROM adoption_forget_ranges")?;
    let mut range_rows = ranges.query([])?;
    while let Some(r) = range_rows.next()? {
        staging.execute("INSERT OR IGNORE INTO adoption_forget_ranges(consumer_tool,start_ms,end_ms,native_timezone,identity_family,namespace_key) VALUES(?1,?2,?3,?4,?5,?6)",params![r.get::<_,String>(0)?,r.get::<_,i64>(1)?,r.get::<_,i64>(2)?,r.get::<_,String>(3)?,r.get::<_,Option<String>>(4)?,r.get::<_,Option<String>>(5)?])?;
    }
    let mut stmt=current.prepare("SELECT policy_id,consumer_tool,kind,start_ms,end_ms,native_timezone,manifest_hash,created_ms,status FROM adoption_policies")?;
    let mut rows = stmt.query([])?;
    while let Some(r) = rows.next()? {
        let id: String = r.get(0)?;
        let status: String = r.get(8)?;
        if status == "active" {
            let hash: String = r.get(6)?;
            let valid:bool=staging.query_row("SELECT EXISTS(SELECT 1 FROM adoption_policies WHERE policy_id=?1 AND manifest_hash=?2 AND status='active')",params![id,hash],|r|r.get(0))?;
            if !valid {
                return Err(LedgerError::IncompleteEvidence);
            }
            type AdoptionDependency = (String, String, String, Vec<u8>, Vec<u8>);
            let dependencies:Vec<AdoptionDependency>=current.prepare("SELECT selected.identity_family,selected.namespace_key,selected.candidate_key,selected.normalized_hash,raw.normalized_hash FROM adoption_items i JOIN usage_observations selected ON selected.observation_id=i.selected_observation_id JOIN usage_observations raw ON raw.observation_id=i.source_observation_id WHERE i.policy_id=?1")?.query_map([&id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?)))?.collect::<rusqlite::Result<_>>()?;
            for (family, namespace, candidate, selected_hash, source_hash) in dependencies {
                let present:bool=staging.query_row("SELECT EXISTS(SELECT 1 FROM adoption_items i JOIN usage_observations selected ON selected.observation_id=i.selected_observation_id JOIN usage_observations raw ON raw.observation_id=i.source_observation_id WHERE i.policy_id=?1 AND selected.identity_family=?2 AND selected.namespace_key=?3 AND selected.candidate_key=?4 AND selected.normalized_hash=?5 AND raw.normalized_hash=?6)",params![id,family,namespace,candidate,selected_hash,source_hash],|r|r.get(0))?;
                if !present {
                    return Err(LedgerError::IncompleteEvidence);
                }
            }
        } else {
            staging.execute("INSERT INTO adoption_policies(policy_id,consumer_tool,kind,start_ms,end_ms,native_timezone,manifest_hash,created_ms,status) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,'forgotten') ON CONFLICT(policy_id) DO UPDATE SET status='forgotten'",params![id,r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,Option<i64>>(3)?,r.get::<_,Option<i64>>(4)?,r.get::<_,Option<String>>(5)?,r.get::<_,String>(6)?,r.get::<_,i64>(7)?])?;
        }
    }
    Ok(())
}
/// Suppress CC payload before it can be saved again under its report channel.
pub(crate) fn suppresses_projection(c: &Connection, value: &Value) -> Result<bool> {
    let Some(candidate) = ccswitch::reported_candidate(value)? else {
        return Ok(false);
    };
    if let Some(key) = candidate.canonical_identity {
        let probe = Observation {
            identity_family: candidate.consumer_tool.clone(),
            namespace_key: super::additional_adapters::namespace(&candidate.consumer_tool),
            identity_key: Some(key),
            report_source: candidate.consumer_tool.clone(),
            native_tool: candidate.consumer_tool,
            occurred_ms: candidate.occurred_ms,
            ..Observation::default()
        };
        return ingest::exact_suppressed(c, &probe);
    }
    let Some(date) = candidate.native_date else {
        return Ok(false);
    };
    suppresses_unbound_day(c, &candidate.consumer_tool, &date)
}
pub(crate) fn suppresses_unbound_day(c: &Connection, tool: &str, date: &str) -> Result<bool> {
    let mut stmt=c.prepare("SELECT start_ms,end_ms,native_timezone FROM adoption_forget_ranges WHERE consumer_tool=?1 AND identity_family IS NULL")?;
    let mut rows = stmt.query([tool])?;
    while let Some(r) = rows.next()? {
        let start: i64 = r.get(0)?;
        let end: i64 = r.get(1)?;
        let tz: Option<String> = r.get(2)?;
        let Some(tz) = tz else { continue };
        let next = NaiveDate::parse_from_str(date, "%Y-%m-%d")
            .map_err(|_| LedgerError::IncompleteEvidence)?
            .succ_opt()
            .ok_or(LedgerError::IncompleteEvidence)?
            .to_string();
        let (a, b) = calendar_bounds(date, &next, &tz)?;
        if a < end && b > start {
            return Ok(true);
        }
    }
    Ok(false)
}
