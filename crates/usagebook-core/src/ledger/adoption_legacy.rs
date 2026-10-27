//! Explicit migration of a fixed legacy snapshot into frozen source-reported
//! interval facts. The live reader's interpretation is retained, never promoted
//! into verified request identity or recomputed on later collection passes.
use super::{
    claude, collection, db, ingest,
    legacy::{LegacyEntry, LegacySnapshot},
    metrics, resolve,
    time_allocation::{self, TimeMember},
    types::*,
    Ledger,
};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, Instant},
};

const MAX_ROWS: usize = 100_000;
const MAX_BUCKETS: usize = 512;
const MAX_UNITS: usize = MAX_BUCKETS;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Bucket {
    date: String,
    model: String,
    start_ms: i64,
    end_ms: i64,
    metrics: Metrics,
    source_records: usize,
    known_cost_records: usize,
    cost_units: Option<i64>,
    cost_scale: Option<i64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    members: Vec<TimeMember>,
}
#[derive(Serialize)]
struct NativeEvidenceBucket {
    date: String,
    model: String,
    saved_observations: usize,
    eligibility_counts: BTreeMap<String, usize>,
    selected_events: usize,
}
impl Bucket {
    fn summary(&self) -> Self {
        Self {
            date: self.date.clone(),
            model: self.model.clone(),
            start_ms: self.start_ms,
            end_ms: self.end_ms,
            metrics: self.metrics,
            source_records: self.source_records,
            known_cost_records: self.known_cost_records,
            cost_units: self.cost_units,
            cost_scale: self.cost_scale,
            members: Vec::new(),
        }
    }
    fn new(date: String, model: String, start_ms: i64, end_ms: i64) -> Self {
        Self {
            date,
            model,
            start_ms,
            end_ms,
            metrics: [Some(0); 10],
            source_records: 0,
            known_cost_records: 0,
            cost_units: None,
            cost_scale: None,
            members: Vec::new(),
        }
    }
    fn add(&mut self, fields: &Metrics, cost: Option<(i64, i64)>) -> Result<()> {
        for (total, value) in self.metrics.iter_mut().zip(fields) {
            *total = match (*total, *value) {
                (Some(a), Some(b)) => Some(a.checked_add(b).ok_or(LedgerError::NumericOverflow)?),
                _ => None,
            };
        }
        // Preserve the same known subtotal the legacy query can represent.
        // Missing or over-precision amounts never erase other known amounts;
        // the reported-history parser keeps this subtotal partial after reopen.
        if let Some((b, sb)) = cost {
            if b < 0 || !(0..=18).contains(&sb) {
                return Err(LedgerError::InvalidUsageNumber);
            }
            let (a, sa) = self.cost_units.zip(self.cost_scale).unwrap_or((0, 0));
            let scale = sa.max(sb);
            let a = (a as i128)
                .checked_mul(10_i128.pow((scale - sa) as u32))
                .ok_or(LedgerError::NumericOverflow)?;
            let b = (b as i128)
                .checked_mul(10_i128.pow((scale - sb) as u32))
                .ok_or(LedgerError::NumericOverflow)?;
            let mut value = a.checked_add(b).ok_or(LedgerError::NumericOverflow)?;
            let mut scale = scale;
            while scale > 0 && value % 10 == 0 {
                value /= 10;
                scale -= 1;
            }
            self.cost_units = Some(i64::try_from(value).map_err(|_| LedgerError::NumericOverflow)?);
            self.cost_scale = Some(scale);
        }
        self.source_records += 1;
        self.known_cost_records += usize::from(cost.is_some());
        Ok(())
    }
}
fn invalid(message: &str) -> LedgerError {
    LedgerError::InvalidInput(message.into())
}
fn digest(value: &impl Serialize) -> Result<String> {
    time_allocation::digest(value)
}
fn within_budget(start: Instant) -> Result<()> {
    if start.elapsed() > Duration::from_secs(5) {
        Err(LedgerError::RecordTooLarge)
    } else {
        Ok(())
    }
}
// A preview depends only on this source's controls. Unrelated ledger commits or
// another tool's successful collection cannot silently expire it.
fn controls(
    c: &Connection,
    source: &str,
    family: &str,
    namespace: &str,
    tool: &str,
) -> Result<Value> {
    let mut result = Vec::<Value>::new();
    let mut stmt=c.prepare("SELECT kind,report_source,identity_family,namespace_key,identity_key,session_key,native_tool,start_ms,end_ms,operation_id FROM suppression_rules WHERE (report_source IS NULL OR report_source=?1) AND (native_tool IS NULL OR native_tool=?4) AND (identity_family IS NULL OR identity_family=?2) AND (namespace_key IS NULL OR namespace_key=?3) ORDER BY rule_id")?;
    let mut rows = stmt.query(params![source, family, namespace, tool])?;
    while let Some(r) = rows.next()? {
        let kind: String = r.get(0)?;
        // A legacy aggregate has no trustworthy request/session key with which
        // to exclude a previously forgotten identity. Fail closed, not revive it.
        if matches!(kind.as_str(), "identity" | "identity_link" | "session") {
            return Err(invalid(
                "legacy snapshot cannot prove exclusion of forgotten identities",
            ));
        }
        result.push(json!([
            kind,
            r.get::<_, Option<String>>(1)?,
            r.get::<_, Option<String>>(2)?,
            r.get::<_, Option<String>>(3)?,
            r.get::<_, Option<String>>(4)?,
            r.get::<_, Option<String>>(5)?,
            r.get::<_, Option<String>>(6)?,
            r.get::<_, Option<i64>>(7)?,
            r.get::<_, Option<i64>>(8)?,
            r.get::<_, String>(9)?
        ]));
    }
    let mut stmt=c.prepare("SELECT start_ms,end_ms,native_timezone,identity_family,namespace_key FROM adoption_forget_ranges WHERE consumer_tool=?1 AND (identity_family IS NULL OR identity_family=?2 AND namespace_key=?3) ORDER BY start_ms,end_ms,native_timezone,identity_family,namespace_key")?;
    let rows = stmt
        .query_map(params![tool, family, namespace], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, Option<String>>(3)?,
                r.get::<_, Option<String>>(4)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(json!({"rules":result,"forgotten_ranges":rows}))
}

#[derive(Serialize, Deserialize)]
struct FrozenPreview {
    source: String,
    cutoff: i64,
    lower: Option<i64>,
    buckets: Vec<Bucket>,
    token: String,
}
#[derive(Serialize, Deserialize)]
struct PreviewMember {
    bucket: usize,
    member: TimeMember,
}
#[derive(Serialize)]
struct PreviewMemberRef<'a> {
    bucket: usize,
    member: &'a TimeMember,
}
fn preview_key(source: &str, token: &str) -> Result<String> {
    if token.len() != 64 || !token.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(LedgerError::StaleObservation);
    }
    Ok(format!("legacy-preview/{source}/{token}"))
}
fn save_preview(c: &Connection, preview: &FrozenPreview) -> Result<()> {
    let prefix = preview_key(&preview.source, &preview.token)?;
    // Bounded, white-listed staging only; each chunk is below the runtime row
    // limit. No source database, prompt, session id or API secret is copied.
    let old:Vec<String>=c.prepare("SELECT state_key FROM ledger_runtime_state WHERE state_key LIKE 'legacy-preview/%/meta' ORDER BY json_extract(value_json,'$.created_ms') DESC")?.query_map([],|r|r.get(0))?.collect::<rusqlite::Result<_>>()?;
    for (i, key) in old.iter().enumerate() {
        let meta = collection::runtime_value(c, key)?.ok_or(LedgerError::CorruptLedger)?;
        if i >= 7 || meta["created_ms"].as_i64().unwrap_or(0) < now_ms() - 86_400_000 {
            c.execute(
                "DELETE FROM ledger_runtime_state WHERE state_key LIKE ?1",
                [format!("{}/%", key.trim_end_matches("/meta"))],
            )?;
        }
    }
    // Members are stored independently so a large bucket never exceeds the
    // runtime state's per-row bound. Both parts belong to this preview token.
    let buckets: Vec<_> = preview.buckets.iter().map(Bucket::summary).collect();
    let mut members = Vec::new();
    for (bucket, value) in preview.buckets.iter().enumerate() {
        members.extend(
            value
                .members
                .iter()
                .map(|member| PreviewMemberRef { bucket, member }),
        );
    }
    if members.is_empty() || members.len() > MAX_ROWS {
        return Err(LedgerError::RecordTooLarge);
    }
    let chunks: Vec<_> = buckets.chunks(16).collect();
    for (i, chunk) in chunks.iter().enumerate() {
        collection::put_runtime_value(c, &format!("{prefix}/{i}"), &json!(chunk))?;
    }
    let member_chunks: Vec<_> = members.chunks(64).collect();
    for (i, chunk) in member_chunks.iter().enumerate() {
        collection::put_runtime_value(c, &format!("{prefix}/members/{i}"), &json!(chunk))?;
    }
    collection::put_runtime_value(
        c,
        &format!("{prefix}/meta"),
        &json!({"version":2,"source":preview.source,"cutoff":preview.cutoff,"lower":preview.lower,"token":preview.token,"chunks":chunks.len(),"member_chunks":member_chunks.len(),"member_count":members.len(),"created_ms":now_ms()}),
    )?;
    Ok(())
}
fn load_preview(c: &Connection, source: &str, token: &str) -> Result<FrozenPreview> {
    let prefix = preview_key(source, token)?;
    let meta = collection::runtime_value(c, &format!("{prefix}/meta"))?
        .ok_or(LedgerError::StaleObservation)?;
    if meta["version"].as_u64() != Some(2)
        || meta["source"].as_str() != Some(source)
        || meta["token"].as_str() != Some(token)
        || meta["created_ms"].as_i64().unwrap_or(0) < now_ms() - 86_400_000
    {
        return Err(LedgerError::StaleObservation);
    }
    let chunks = meta["chunks"]
        .as_u64()
        .filter(|n| (1..=32).contains(n))
        .ok_or(LedgerError::CorruptLedger)?;
    let mut buckets = Vec::new();
    for i in 0..chunks {
        let chunk = collection::runtime_value(c, &format!("{prefix}/{i}"))?
            .ok_or(LedgerError::StaleObservation)?;
        buckets.extend(
            serde_json::from_value::<Vec<Bucket>>(chunk).map_err(|_| LedgerError::CorruptLedger)?,
        );
        if buckets.len() > MAX_BUCKETS || buckets.iter().any(|b| !b.members.is_empty()) {
            return Err(LedgerError::CorruptLedger);
        }
    }
    let count = meta["member_count"]
        .as_u64()
        .filter(|n| (1..=MAX_ROWS as u64).contains(n))
        .ok_or(LedgerError::CorruptLedger)? as usize;
    let chunks = meta["member_chunks"]
        .as_u64()
        .filter(|n| *n == count.div_ceil(64) as u64)
        .ok_or(LedgerError::CorruptLedger)?;
    let mut loaded = 0usize;
    for i in 0..chunks {
        let chunk = collection::runtime_value(c, &format!("{prefix}/members/{i}"))?
            .ok_or(LedgerError::StaleObservation)?;
        let chunk: Vec<PreviewMember> =
            serde_json::from_value(chunk).map_err(|_| LedgerError::CorruptLedger)?;
        if chunk.is_empty() || chunk.len() > 64 {
            return Err(LedgerError::CorruptLedger);
        }
        for value in chunk {
            buckets
                .get_mut(value.bucket)
                .ok_or(LedgerError::CorruptLedger)?
                .members
                .push(value.member);
            loaded += 1;
            if loaded > count {
                return Err(LedgerError::CorruptLedger);
            }
        }
    }
    if loaded != count || buckets.iter().any(|b| b.members.len() != b.source_records) {
        return Err(LedgerError::CorruptLedger);
    }
    Ok(FrozenPreview {
        source: source.into(),
        cutoff: meta["cutoff"].as_i64().ok_or(LedgerError::CorruptLedger)?,
        lower: meta["lower"].as_i64(),
        buckets,
        token: token.into(),
    })
}
impl Ledger {
    /// Failed live captures are real source failures, unlike a complete
    /// capture waiting for durable audit. Normal scan scheduling must report
    /// these through the existing wrapper; background slices never retry them.
    pub fn failed_legacy_captures(&self, snapshot: &LegacySnapshot) -> Result<Vec<String>> {
        let selected:Vec<String>=self.connection().prepare("SELECT DISTINCT o.report_source FROM adoption_policies p JOIN adoption_items a USING(policy_id) JOIN usage_observations o ON o.observation_id=a.selected_observation_id WHERE p.kind='legacy_snapshot' AND p.status='active' ORDER BY o.report_source")?.query_map([],|r|r.get(0))?.collect::<rusqlite::Result<_>>()?;
        Ok(selected
            .into_iter()
            .filter(|source| {
                snapshot.sources.get(source).is_none_or(|status| {
                    status
                        .last_scanned_ms
                        .is_none_or(|cutoff| cutoff > now_ms())
                        || status.stale
                        || status.error.is_some()
                        || status.unaccounted_rows != 0
                        || status.unallocated_sessions != 0
                }) || snapshot.has_missing_input_containers(source)
            })
            .collect())
    }

    /// Cheap read-only scheduling probe. It neither loads legacy entries nor
    /// approves adoption: the selected source is checked again transactionally
    /// by extend_legacy_snapshot. Each completed capture can be tried once.
    pub fn ready_legacy_continuations(
        &self,
        snapshot: &LegacySnapshot,
        attempted: &BTreeMap<String, i64>,
    ) -> Result<Vec<String>> {
        let selected:Vec<(String,i64)>=self.connection().prepare("SELECT o.report_source,MAX(p.end_ms) FROM adoption_policies p JOIN adoption_items a USING(policy_id) JOIN usage_observations o ON o.observation_id=a.selected_observation_id WHERE p.kind='legacy_snapshot' AND p.status='active' GROUP BY o.report_source ORDER BY o.report_source")?.query_map([],|r|Ok((r.get(0)?,r.get(1)?)))?.collect::<rusqlite::Result<_>>()?;
        let mut ready = Vec::new();
        for (source, lower) in selected {
            let Some(status) = snapshot.sources.get(&source) else {
                continue;
            };
            let Some(cutoff) = status.last_scanned_ms else {
                continue;
            };
            if cutoff <= lower
                || cutoff > now_ms()
                || attempted.get(&source) == Some(&cutoff)
                || status.stale
                || status.error.is_some()
                || status.unaccounted_rows != 0
                || status.unallocated_sessions != 0
                || snapshot.has_missing_input_containers(&source)
            {
                continue;
            }
            if collection::source_activation_readiness(self.connection(), &source)?.is_none() {
                ready.push(source);
            }
        }
        Ok(ready)
    }

    /// One failed source does not veto another source's live snapshot. An
    /// unfinished operational attempt is persisted before extending its policy.
    pub fn continue_selected_legacy(
        &mut self,
        snapshot: &LegacySnapshot,
        only: Option<&str>,
    ) -> Result<Value> {
        self.require_writer()?;
        if only.is_some_and(|s| !super::DURABLE_EVIDENCE_SOURCES.contains(&s)) {
            return Err(LedgerError::SourceNotProtected);
        }
        let sources:Vec<String>=self.connection().prepare("SELECT DISTINCT o.report_source FROM adoption_policies p JOIN adoption_items a USING(policy_id) JOIN usage_observations o ON o.observation_id=a.selected_observation_id WHERE p.kind='legacy_snapshot' AND p.status='active' ORDER BY o.report_source")?.query_map([],|r|r.get(0))?.collect::<rusqlite::Result<_>>()?;
        let mut output = serde_json::Map::new();
        for source in sources
            .into_iter()
            .filter(|s| only.is_none_or(|only| s == only))
        {
            let key = format!("legacy-continuation-status/{source}");
            let mut previous = collection::runtime_value(self.connection(), &key)?
                .unwrap_or(json!({"state":"idle","last_success_ms":null,"error":null}));
            if previous["state"] == "running" {
                previous["state"] = json!("recovery_unverified");
                previous["error"] = json!("RECOVERY_UNVERIFIED");
            }
            let mut status = previous.clone();
            status["state"] = json!("running");
            status["last_attempt_ms"] = json!(now_ms());
            collection::put_runtime_value(self.connection(), &key, &status)?;
            match self.extend_legacy_snapshot(snapshot, &source) {
                Ok(result) => {
                    if result["applied"] == true || result["reason"] == "NO_NEW_CONTRIBUTIONS" {
                        // A proven empty interval passed source readiness,
                        // domain and control checks. It clears an old failure;
                        // early exits such as NO_NEW_INTERVAL prove no recovery.
                        status["state"] = json!("ready");
                        status["error"] = Value::Null;
                        status["last_success_ms"] = json!(now_ms());
                    } else {
                        status["state"] = previous["state"].clone();
                        status["error"] = previous["error"].clone();
                    }
                    status["result"] = json!({"applied":result["applied"],"reason":result["reason"],"start_ms":result["start_ms"],"end_ms":result["end_ms"],"new_items":result["new_items"],"policy_id":result["policy_id"]});
                }
                Err(error) => {
                    status["state"] = json!("error");
                    status["error"] = json!(error.code());
                    collection::put_runtime_value(self.connection(), &key, &status)?;
                    if matches!(
                        error,
                        LedgerError::DiskFull
                            | LedgerError::PermissionDenied
                            | LedgerError::CorruptLedger
                            | LedgerError::Sql(_)
                            | LedgerError::Io(_)
                    ) {
                        return Err(error);
                    }
                }
            }
            collection::put_runtime_value(self.connection(), &key, &status)?;
            output.insert(source, status);
        }
        Ok(Value::Object(output))
    }

    /// Preview persists only bounded white-listed values. Apply uses those
    /// frozen values even when a later CLI invocation has scanned again.
    pub fn adopt_legacy_snapshot(
        &mut self,
        snapshot: &LegacySnapshot,
        source: &str,
        token: Option<&str>,
    ) -> Result<Value> {
        self.require_writer()?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let frozen = token
            .map(|token| load_preview(&tx, source, token))
            .transpose()?;
        let result = adopt(&tx, snapshot, source, token, frozen.as_ref(), None)?;
        tx.commit()?;
        Ok(result)
    }
    /// Extend a previously explicit legacy-source selection into a new finite
    /// interval. Old intervals are never reopened or recomputed.
    pub fn extend_legacy_snapshot(
        &mut self,
        snapshot: &LegacySnapshot,
        source: &str,
    ) -> Result<Value> {
        self.require_writer()?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let selection = match collection::runtime_value(
            &tx,
            &format!("legacy-continuation/{source}"),
        )? {
            Some(selection) => selection,
            None => {
                // Restore discards operational state; an explicit active policy
                // is the durable authorization and supplies its domain/cutoff.
                let saved:Option<(String,i64,String,String,String)>=tx.query_row("SELECT p.policy_id,p.end_ms,o.identity_family,o.namespace_key,o.native_tool FROM adoption_policies p JOIN adoption_items a USING(policy_id) JOIN usage_observations o ON o.observation_id=a.selected_observation_id WHERE p.kind='legacy_snapshot' AND p.status='active' AND o.report_source=?1 ORDER BY p.end_ms DESC LIMIT 1",[source],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).optional()?;
                let Some((policy, cutoff, family, namespace, tool)) = saved else {
                    return Ok(json!({"applied":false,"reason":"NOT_SELECTED"}));
                };
                json!({"policy_id":policy,"cutoff":cutoff,"family":family,"namespace":namespace,"tool":tool})
            }
        };
        let cutoff = selection["cutoff"]
            .as_i64()
            .ok_or(LedgerError::CorruptLedger)?;
        let policy = selection["policy_id"]
            .as_str()
            .ok_or(LedgerError::CorruptLedger)?;
        let active:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM adoption_policies WHERE policy_id=?1 AND kind='legacy_snapshot' AND status='active' AND end_ms=?2)",params![policy,cutoff],|r|r.get(0))?;
        if !active {
            return Err(LedgerError::IncompleteEvidence);
        }
        collection::put_runtime_value(&tx, &format!("legacy-continuation/{source}"), &selection)?;
        let result = adopt(&tx, snapshot, source, None, None, Some(cutoff))?;
        tx.commit()?;
        Ok(result)
    }
}

fn adopt(
    c: &Connection,
    snapshot: &LegacySnapshot,
    source: &str,
    token: Option<&str>,
    frozen: Option<&FrozenPreview>,
    lower: Option<i64>,
) -> Result<Value> {
    let started = Instant::now();
    // Explicitly choosing a source-reported interval does not require a native
    // request-identity contract. All evidence adapters must still prove the
    // same complete, bounded, single-domain scope below. CC uses its separate
    // consumer-tool/date adoption contract instead.
    if source == "ccswitch-rollup" || !super::DURABLE_EVIDENCE_SOURCES.contains(&source) {
        return Err(LedgerError::SourceNotProtected);
    }
    let cutoff = if let Some(preview) = frozen {
        preview.cutoff
    } else {
        let status = snapshot
            .sources
            .get(source)
            .ok_or(LedgerError::IncompleteEvidence)?;
        status
            .last_scanned_ms
            .filter(|_| {
                !status.stale
                    && status.error.is_none()
                    && status.unaccounted_rows == 0
                    && status.unallocated_sessions == 0
            })
            .ok_or(LedgerError::IncompleteEvidence)?
    };
    if cutoff > now_ms() || (frozen.is_none() && snapshot.has_missing_input_containers(source)) {
        return Err(LedgerError::IncompleteEvidence);
    }
    if lower.is_some_and(|lower| cutoff <= lower) {
        return Ok(json!({"applied":false,"reason":"NO_NEW_INTERVAL"}));
    }
    if let Some(reason) = collection::source_activation_readiness(c, source)? {
        return Err(invalid(&reason));
    }
    let roots:Vec<(String,String,String,String,i64,String)>=c.prepare("SELECT root_id,identity_family,namespace_key,native_tool,identity_version,parser_version FROM source_roots WHERE report_source=?1 AND collector_key<>'reported-history-v1' ORDER BY root_id")?.query_map([source],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?)))?.collect::<rusqlite::Result<_>>()?;
    let first = roots.first().ok_or(LedgerError::IncompleteEvidence)?;
    let (family, namespace, tool) = (&first.1, &first.2, &first.3);
    if roots
        .iter()
        .any(|r| r.1 != *family || r.2 != *namespace || r.3 != *tool || r.4 != first.4)
    {
        return Err(invalid(
            "legacy snapshot spans incompatible native identity domains",
        ));
    }
    if lower.is_some() {
        let selection = collection::runtime_value(c, &format!("legacy-continuation/{source}"))?
            .ok_or(LedgerError::IncompleteEvidence)?;
        if selection["family"].as_str() != Some(family)
            || selection["namespace"].as_str() != Some(namespace)
            || selection["tool"].as_str() != Some(tool)
        {
            return Err(invalid(
                "native identity domain changed after legacy source selection",
            ));
        }
        let overlap:bool=c.query_row("SELECT EXISTS(SELECT 1 FROM adoption_policies WHERE consumer_tool=?1 AND status='active' AND start_ms<?3 AND end_ms>?2)",params![tool,lower,cutoff],|r|r.get(0))?;
        if overlap {
            return Err(invalid(
                "new legacy interval overlaps an existing selection",
            ));
        }
    }
    if frozen.is_none() && lower.is_none() {
        let future:bool=c.query_row("SELECT EXISTS(SELECT 1 FROM usage_observations WHERE report_source=?1 AND eligibility<>'identity_only' AND occurred_ms>=?2)",params![source,cutoff],|r|r.get(0))?;
        if future {
            return Err(invalid(
                "saved evidence is newer than the complete live capture",
            ));
        }
    }
    let prior: bool = c.query_row(
        "SELECT EXISTS(SELECT 1 FROM adoption_policies WHERE consumer_tool=?1 AND status='active')",
        [tool],
        |r| r.get(0),
    )?;
    if prior && lower.is_none() {
        return Err(invalid(
            "source already has an adopted scope; explicit revision is required",
        ));
    }
    let (epoch, timezone, identity_version): (String, String, i64) = c.query_row(
        "SELECT epoch,report_timezone,identity_version FROM ledger_meta WHERE singleton=1",
        [],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )?;
    if lower.is_some() {
        let changed:bool=c.query_row("SELECT EXISTS(SELECT 1 FROM adoption_policies p JOIN adoption_items a USING(policy_id) JOIN usage_observations o ON o.observation_id=a.selected_observation_id WHERE p.kind='legacy_snapshot' AND p.status='active' AND o.report_source=?1 AND (p.native_timezone<>?2 OR o.identity_family<>?3 OR o.namespace_key<>?4))",params![source,timezone,family,namespace],|r|r.get(0))?;
        if changed {
            return Err(invalid(
                "legacy interval identity or timezone requires explicit revision",
            ));
        }
    }
    let control = controls(c, source, family, namespace, tool)?;
    let raw:Vec<Observation>=c.prepare("SELECT * FROM usage_observations WHERE report_source=?1 AND eligibility<>'identity_only' AND parser_version<>'reported-history-v1' AND (occurred_ms IS NULL OR ((?3 IS NULL OR occurred_ms>=?3) AND occurred_ms<?4)) ORDER BY observation_id LIMIT ?2")?.query_map(params![source,MAX_ROWS as i64+1,lower,cutoff],resolve::load_observation)?.collect::<rusqlite::Result<_>>()?;
    if raw.len() > MAX_ROWS {
        return Err(LedgerError::RecordTooLarge);
    }
    let mut buckets = BTreeMap::<(String, String), Bucket>::new();
    let mut live_rows = 0usize;
    let mut member_occurrences = BTreeMap::<String, usize>::new();
    if let Some(preview) = frozen {
        for bucket in &preview.buckets {
            buckets.insert((bucket.date.clone(), bucket.model.clone()), bucket.clone());
            live_rows += bucket.source_records;
        }
    } else {
        for entry in snapshot
            .entries
            .iter()
            .filter(|e| e.report_source() == source)
        {
            within_budget(started)?;
            let LegacyEntry::Request { record, .. } = entry else {
                return Err(invalid(
                "legacy interval adoption requires request-delta history with trustworthy times",
            ));
            };
            if lower.is_some_and(|lower| record.ts_ms < lower) {
                continue;
            }
            if record.caliber != crate::Caliber::RequestDelta
                || record.ts_ms < 0
                || record.ts_ms >= cutoff
            {
                return Err(invalid(
                    "legacy contribution is outside the fixed capture interval",
                ));
            }
            let (start, end, date) = metrics::day_bounds(&timezone, record.ts_ms)?;
            let model = super::legacy::model_key(&record.model);
            if model.is_empty() || model == "u:" {
                return Err(invalid("legacy contribution has no reliable model"));
            }
            let fields = super::legacy::legacy_metrics(record)
                .map_err(|_| LedgerError::InvalidUsageNumber)?;
            if record
                .cost_usd
                .is_some_and(|cost| !cost.is_finite() || cost < 0.0)
            {
                return Err(LedgerError::InvalidUsageNumber);
            }
            // A finite nonnegative amount outside the existing decimal format
            // is a cost gap, exactly as in the live query, not a zero amount.
            let cost = collection::legacy_cost(record.cost_usd);
            let source_record = if record.dedup_key.is_empty() {
                digest(&("legacy-record-projection-v1", record))?
            } else {
                digest(&(
                    "legacy-source-record-v1",
                    family,
                    namespace,
                    source,
                    &record.dedup_key,
                ))?
            };
            let occurrence = member_occurrences.entry(source_record.clone()).or_default();
            let member = TimeMember {
                member_key: time_allocation::digest(&(
                    "legacy-time-member-v1",
                    source_record,
                    *occurrence,
                ))?,
                occurred_ms: record.ts_ms,
                metrics: fields,
                cost_units: cost.map(|v| v.0),
                cost_scale: cost.map(|v| v.1),
            };
            *occurrence += 1;
            let bucket = buckets
                .entry((date.clone(), model.clone()))
                .or_insert_with(|| {
                    Bucket::new(
                        date,
                        model,
                        start.max(lower.unwrap_or(start)),
                        end.min(cutoff),
                    )
                });
            bucket.add(&fields, cost)?;
            bucket.members.push(member);
            live_rows += 1;
            if live_rows > MAX_ROWS || buckets.len() > MAX_BUCKETS {
                return Err(LedgerError::RecordTooLarge);
            }
        }
    }
    if buckets.is_empty() && lower.is_some() && raw.is_empty() {
        return Ok(json!({"applied":false,"reason":"NO_NEW_CONTRIBUTIONS"}));
    }
    if buckets.is_empty() || raw.is_empty() {
        return Err(LedgerError::IncompleteEvidence);
    }
    let start = lower.unwrap_or(
        buckets
            .values()
            .map(|b| b.start_ms)
            .min()
            .ok_or(LedgerError::IncompleteEvidence)?,
    );
    let mut units = BTreeSet::<String>::new();
    let mut raw_hash = Sha256::new();
    let mut native_buckets = BTreeMap::<(String, String), NativeEvidenceBucket>::new();
    for observation in &raw {
        within_budget(started)?;
        if observation.identity_family != *family
            || observation.namespace_key != *namespace
            || observation.native_tool != *tool
            || observation.fact_kind != "request"
            || observation.action != "upsert"
        {
            return Err(invalid("saved history includes contributions that cannot be bounded by this legacy interval"));
        }
        let occurred = observation
            .occurred_ms
            .ok_or(LedgerError::IncompleteEvidence)?;
        if occurred < start || occurred >= cutoff {
            return Err(invalid(
                "saved evidence is outside the fixed live snapshot interval",
            ));
        }
        let date = metrics::day_bounds(&timezone, occurred)?.2;
        // Explicit source selection covers the whole identity domain and time
        // interval. A native parser's unknown or different model is evidence of
        // a different interpretation, not proof that the row is outside it.
        let native = native_buckets
            .entry((date.clone(), observation.model_key.clone()))
            .or_insert_with(|| NativeEvidenceBucket {
                date,
                model: observation.model_key.clone(),
                saved_observations: 0,
                eligibility_counts: BTreeMap::new(),
                selected_events: 0,
            });
        native.saved_observations += 1;
        *native
            .eligibility_counts
            .entry(observation.eligibility.clone())
            .or_default() += 1;
        if native_buckets.len() > MAX_BUCKETS {
            return Err(LedgerError::RecordTooLarge);
        }
        raw_hash.update(observation.observation_id.to_be_bytes());
        raw_hash.update(&observation.normalized_hash);
    }
    let forgotten:bool=c.query_row("SELECT EXISTS(SELECT 1 FROM adoption_forget_ranges WHERE consumer_tool=?1 AND start_ms<?3 AND end_ms>?2 AND (identity_family IS NULL OR identity_family=?4 AND namespace_key=?5))",params![tool,start,cutoff,family,namespace],|r|r.get(0))?;
    if forgotten {
        return Err(invalid("legacy interval overlaps forgotten history"));
    }
    let active:Vec<Observation>=c.prepare("SELECT o.* FROM usage_events e JOIN usage_observations o ON o.observation_id=e.selected_observation_id WHERE e.report_source=?1 AND e.state='active' AND ((o.fact_kind='request' AND (o.occurred_ms IS NULL OR o.occurred_ms>=?3 AND o.occurred_ms<?4)) OR (o.fact_kind<>'request' AND (o.day_start_ms IS NULL OR o.day_start_ms<?4 AND o.day_end_ms>?3))) ORDER BY e.identity_key LIMIT ?2")?.query_map(params![source,MAX_ROWS as i64+1,start,cutoff],resolve::load_observation)?.collect::<rusqlite::Result<_>>()?;
    if active.len() > MAX_ROWS {
        return Err(LedgerError::RecordTooLarge);
    }
    let mut current = BTreeMap::<(String, String), Bucket>::new();
    for observation in &active {
        let occurred = observation
            .occurred_ms
            .ok_or(LedgerError::IncompleteEvidence)?;
        if observation.fact_kind != "request" || occurred < start || occurred >= cutoff {
            return Err(invalid(
                "active ledger contribution falls outside the proposed legacy interval",
            ));
        }
        let (s, e, date) = metrics::day_bounds(&timezone, occurred)?;
        if let Some(native) = native_buckets.get_mut(&(date.clone(), observation.model_key.clone()))
        {
            native.selected_events += 1;
        }
        current
            .entry((date.clone(), observation.model_key.clone()))
            .or_insert_with(|| Bucket::new(date, observation.model_key.clone(), s, e.min(cutoff)))
            .add(
                &observation.metrics,
                observation.cost_units.zip(observation.cost_scale),
            )?;
    }
    // Counts describe saved evidence, which can contain revisions and
    // duplicates. Only `ledger` below describes currently adopted values.
    let native_only: Vec<_> = native_buckets
        .iter()
        .filter(|(key, _)| !buckets.contains_key(*key))
        .map(|(_, bucket)| bucket)
        .collect();
    let live_only: Vec<_> = buckets
        .iter()
        .filter(|(key, _)| !native_buckets.contains_key(*key))
        .map(|(_, bucket)| json!({"date":bucket.date,"model":bucket.model,"source_records":bucket.source_records}))
        .collect();
    let bucket_differences = json!({
        "native_only_bucket_count":native_only.len(),
        "native_only_observations":native_only.iter().map(|b|b.saved_observations).sum::<usize>(),
        "native_only_selected_events":native_only.iter().map(|b|b.selected_events).sum::<usize>(),
        "native_only":native_only,
        "live_only_bucket_count":live_only.len(),
        "live_only":live_only,
    });
    let event_watermark:i64=c.query_row("SELECT COALESCE(MAX(e.updated_commit_seq),0) FROM usage_events e JOIN usage_observations o ON o.observation_id=e.selected_observation_id WHERE e.report_source=?1 AND (o.occurred_ms IS NULL OR o.occurred_ms>=?2 AND o.occurred_ms<?3)",params![source,start,cutoff],|r|r.get(0))?;
    let manifest = format!("{:x}", raw_hash.finalize());
    for bucket in buckets.values_mut() {
        bucket
            .members
            .sort_by(|a, b| a.member_key.cmp(&b.member_key));
        if bucket.members.len() != bucket.source_records {
            return Err(LedgerError::IncompleteEvidence);
        }
    }
    let values: Vec<_> = buckets.into_values().collect();
    let preview_token = digest(&(
        "legacy-interval-v2",
        source,
        cutoff,
        lower,
        &epoch,
        &timezone,
        identity_version,
        &roots,
        event_watermark,
        &manifest,
        &values,
        &control,
    ))?;
    let mut derived = Vec::new();
    for bucket in &values {
        let identity = identity_key(
            family,
            namespace,
            "legacy-interval",
            &[&preview_token, &bucket.date, &bucket.model],
        );
        let mut o = Observation {
            identity_family: family.clone(),
            namespace_key: namespace.clone(),
            identity_key: Some(identity.clone()),
            candidate_key: identity.clone(),
            record_key: identity,
            parser_version: "reported-history-v1".into(),
            identity_version: first.4,
            eligibility: "pending_finality".into(),
            fact_kind: "day".into(),
            report_source: source.into(),
            native_tool: tool.clone(),
            model_key: bucket.model.clone(),
            model_display: Some(
                bucket
                    .model
                    .strip_prefix("n:")
                    .unwrap_or(&bucket.model)
                    .into(),
            ),
            native_date: Some(bucket.date.clone()),
            native_timezone: Some(timezone.clone()),
            day_start_ms: Some(bucket.start_ms),
            day_end_ms: Some(bucket.end_ms),
            metrics: bucket.metrics,
            observed_ms: cutoff,
            content_generation: preview_token.clone(),
            ..Default::default()
        };
        if let (Some(units), Some(scale)) = (bucket.cost_units, bucket.cost_scale) {
            o.cost_state = "known".into();
            o.cost_units = Some(units);
            o.cost_scale = Some(scale);
            o.cost_currency = Some("USD".into());
        }
        if ingest::exact_suppressed(c, &o)? {
            return Err(invalid("legacy interval overlaps forgotten history"));
        }
        o.normalized_hash = claude::normalized_hash(&o)?;
        derived.push(o);
    }
    let known_cost_records = values
        .iter()
        .map(|bucket| bucket.known_cost_records)
        .sum::<usize>();
    let unknown_cost_records = live_rows
        .checked_sub(known_cost_records)
        .ok_or(LedgerError::CorruptLedger)?;
    let cost_coverage = json!({"source_records":live_rows,"known_cost_records":known_cost_records,"unknown_cost_records":unknown_cost_records,"state":if known_cost_records==0{"unknown"}else{"partial"},"amount_basis":"known_subtotal","counts_scope":"preview_only"});
    let mut warnings = vec!["The legacy reader's reported values are frozen; individual request identity and finality remain unverified.","The selected source interpretation replaces the entire fixed interval, including native-only date/model buckets. Saved observation counts can include revisions or duplicates and are not consumption totals.","Native observations inside the fixed interval remain evidence and cannot add to these adopted values."];
    if unknown_cost_records > 0 {
        warnings.push("Costs preserve only the legacy query's representable known subtotal; missing or unrepresentable costs remain gaps. Cost coverage counts describe this preview only and are not retained as permanent per-record counts.");
    }
    let summaries: Vec<_> = values.iter().map(Bucket::summary).collect();
    let mut result = json!({"preview_token":preview_token,"consumer_tool":tool,"source":source,"quality":"source_reported","grain":"interval","time_precision":"captured_request","start_ms":start,"end_ms":cutoff,"timezone":timezone,"candidates":live_rows,"saved_evidence":raw.len(),"new_items":derived.len(),"replaced_verified_events":active.len(),"unallocated_contributions":0,"continuation":true,"live":summaries,"ledger":current.values().collect::<Vec<_>>(),"bucket_differences":bucket_differences,"cost_coverage":cost_coverage,"applied":false,"warnings":warnings});
    if token.is_none() && lower.is_none() {
        save_preview(
            c,
            &FrozenPreview {
                source: source.into(),
                cutoff,
                lower,
                buckets: values,
                token: preview_token.clone(),
            },
        )?;
        return Ok(result);
    }
    if let Some(token) = token {
        if token != preview_token {
            return Err(LedgerError::StaleObservation);
        }
    }
    within_budget(started)?;
    let policy = digest(&("legacy-policy-v1", &preview_token))?;
    c.execute("INSERT INTO adoption_policies(policy_id,consumer_tool,kind,start_ms,end_ms,native_timezone,manifest_hash,created_ms,status) VALUES(?1,?2,'legacy_snapshot',?3,?4,?5,?6,?7,'active')",params![policy,tool,start,cutoff,timezone,preview_token,now_ms()])?;
    // The frozen values have their own internal acquisition object; a native
    // file cleanup must not erase the provenance of an adopted interval.
    let internal_root = digest(&("legacy-root-v1", family, namespace, source))?;
    let locator = format!("usagebook:reported:{tool}");
    c.execute("INSERT OR IGNORE INTO source_roots(root_id,identity_family,namespace_key,collector_key,report_source,native_tool,locator,enabled,identity_version,parser_version,route,enumeration_done) VALUES(?1,?2,?3,'reported-history-v1',?4,?5,?6,0,?7,?8,'ledger',1)",params![internal_root,family,namespace,source,tool,locator,first.4,first.5])?;
    let internal_root:String=c.query_row("SELECT root_id FROM source_roots WHERE identity_family=?1 AND namespace_key=?2 AND collector_key='reported-history-v1' AND locator=?3",params![family,namespace,locator],|r|r.get(0))?;
    c.execute("INSERT INTO source_objects(root_id,object_key,locator,content_generation,byte_length,object_revision,state,parser_version,target_generation,target_end,target_done,last_observed_ms,audit_due_ms) VALUES(?1,?2,?3,?2,0,0,'ready',?4,?2,0,1,?5,?6)",params![internal_root,policy,format!("usagebook:legacy:{policy}"),first.5,cutoff,i64::MAX])?;
    let object_id = c.last_insert_rowid();
    for (o, bucket) in derived.iter_mut().zip(&values) {
        o.object_id = object_id;
        o.normalized_hash = claude::normalized_hash(o)?;
        ingest::insert_observation(c, o)?;
        let id:i64=c.query_row("SELECT observation_id FROM usage_observations WHERE identity_family=?1 AND namespace_key=?2 AND candidate_key=?3 AND normalized_hash=?4",params![o.identity_family,o.namespace_key,o.candidate_key,o.normalized_hash],|r|r.get(0))?;
        c.execute("INSERT INTO adoption_items(selected_observation_id,source_observation_id,policy_id) VALUES(?1,?1,?2)",params![id,policy])?;
        time_allocation::save(c, id, &bucket.members)?;
        units.insert(resolve::publish_key(o).ok_or(LedgerError::IncompleteEvidence)?);
    }
    retire_selected_interval(
        c,
        &active,
        &IntervalReplacement {
            source,
            family,
            namespace,
            timezone: &timezone,
            start,
            end: cutoff,
        },
        started,
    )?;
    if units.len() > MAX_UNITS {
        return Err(LedgerError::RecordTooLarge);
    }
    let upper:i64=c.query_row("SELECT MAX(observation_id) FROM usage_observations WHERE identity_family=?1 AND namespace_key=?2",params![family,namespace],|r|r.get(0))?;
    for key in units {
        within_budget(started)?;
        resolve::rebuild_unit(
            c,
            family,
            namespace,
            &key,
            upper,
            resolve::RebuildCause::Automatic,
        )?;
    }
    c.execute(
        "UPDATE source_roots SET route='ledger' WHERE report_source=?1",
        [source],
    )?;
    db::bump(c)?;
    collection::put_runtime_value(
        c,
        &format!("reconciliation/{source}"),
        &json!({"source":source,"state":"adopted","reason":null,"quality":"source_reported","policy_id":policy,"saved_observation_id":upper,"checked_at_ms":now_ms()}),
    )?;
    collection::put_runtime_value(
        c,
        &format!("legacy-continuation/{source}"),
        &json!({"version":1,"family":family,"namespace":namespace,"tool":tool,"cutoff":cutoff,"policy_id":policy}),
    )?;
    collection::put_runtime_value(
        c,
        &format!("legacy-continuation-status/{source}"),
        &json!({"state":"ready","error":null,"last_attempt_ms":now_ms(),"last_success_ms":now_ms()}),
    )?;
    if let Some(token) = token {
        c.execute(
            "DELETE FROM ledger_runtime_state WHERE state_key LIKE ?1",
            [format!("{}/%", preview_key(source, token)?)],
        )?;
    }
    result["applied"] = json!(true);
    result["policy_id"] = json!(policy);
    Ok(result)
}

/// Retire exactly the already selected request contributions being replaced.
/// This is not a live-minus-ledger residual: every subtracted metric/count is
/// evaluated from the pinned observation that previously created the rollup.
/// Grouping reduces thousands of per-request rebuilds to one write per bucket.
struct IntervalReplacement<'a> {
    source: &'a str,
    family: &'a str,
    namespace: &'a str,
    timezone: &'a str,
    start: i64,
    end: i64,
}
fn retire_selected_interval(
    c: &Connection,
    active: &[Observation],
    scope: &IntervalReplacement<'_>,
    started: Instant,
) -> Result<()> {
    let IntervalReplacement {
        source,
        family,
        namespace,
        timezone,
        start,
        end,
    } = *scope;
    if active.is_empty() {
        return Ok(());
    }
    let mut groups = BTreeMap::<(i64, String), metrics::Aggregate>::new();
    for observation in active {
        within_budget(started)?;
        if observation.fact_kind != "request"
            || observation.identity_family != family
            || observation.namespace_key != namespace
        {
            return Err(LedgerError::IncompleteEvidence);
        }
        let stamp = observation
            .occurred_ms
            .ok_or(LedgerError::IncompleteEvidence)?;
        if stamp < start || stamp >= end {
            return Err(LedgerError::IncompleteEvidence);
        }
        let day = metrics::day_bounds(timezone, stamp)?.0;
        let mut values = metrics::evaluate(&observation.metrics)?;
        if observation.parser_version == "reported-history-v1" {
            for value in &mut values {
                if value.known_value.is_some() {
                    value.state = ValueState::Partial;
                }
            }
        }
        groups
            .entry((day, observation.model_key.clone()))
            .or_default()
            .add(&values, 1)?;
    }
    let (version, sequence): (i64, i64) = c.query_row(
        "SELECT derived_version,commit_seq FROM ledger_meta WHERE singleton=1",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    let sequence = sequence
        .checked_add(1)
        .ok_or(LedgerError::NumericOverflow)?;
    for ((day, model), aggregate) in groups {
        let mut updates = vec![
            "event_count=event_count-?5".to_owned(),
            "bucket_revision=bucket_revision+1".to_owned(),
        ];
        let mut args = vec![
            rusqlite::types::Value::Integer(version),
            day.into(),
            source.to_owned().into(),
            model.into(),
            aggregate.event_count.into(),
        ];
        for (i, name) in METRIC_NAMES.iter().enumerate() {
            for (suffix, value) in [
                ("known_sum", aggregate.known_sum[i]),
                ("known_count", aggregate.known_count[i]),
                ("exact_count", aggregate.exact_count[i]),
            ] {
                args.push(value.into());
                updates.push(format!("{name}_{suffix}={name}_{suffix}-?{}", args.len()));
            }
        }
        let changed=c.execute(&format!("UPDATE usage_rollups SET {} WHERE derived_version=?1 AND grain='day' AND bucket_start_ms=?2 AND report_source=?3 AND model_key=?4",updates.join(",")),rusqlite::params_from_iter(&args))?;
        if changed != 1 {
            return Err(LedgerError::CorruptLedger);
        }
        c.execute("DELETE FROM usage_rollups WHERE derived_version=?1 AND grain='day' AND bucket_start_ms=?2 AND report_source=?3 AND model_key=?4 AND event_count=0",rusqlite::params_from_iter(&args[..4]))?;
    }
    let changed=c.execute("UPDATE usage_events SET selected_observation_id=NULL,state='unresolved',has_conflict=1,event_revision=event_revision+1,updated_commit_seq=?6 WHERE report_source=?1 AND identity_family=?2 AND namespace_key=?3 AND state='active' AND fact_kind='request' AND occurred_ms>=?4 AND occurred_ms<?5",params![source,family,namespace,start,end,sequence])?;
    if changed != active.len() {
        return Err(LedgerError::StaleObservation);
    }
    c.execute(
        "UPDATE ledger_meta SET commit_seq=?1 WHERE singleton=1",
        [sequence],
    )?;
    resolve::invalidate_run_units(c)?;
    Ok(())
}
