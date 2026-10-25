//! Deterministic publication of frozen evidence. All temporary work is disk backed;
//! even a large Claude message group retains only one candidate projection in RAM.
use super::{metrics, types::*, Ledger};
use rusqlite::{params, Connection, OptionalExtension, Row};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

const RESOLVER_VERSION: &str = "ledger-resolver-v3";
const LEGACY_RESOLVER_VERSION: &str = "ledger-resolver-v2";

// Shared by status and coverage. A compatibility replay can have the same
// upper bound as an old completed run, so the ordinary new-evidence test misses it.
pub(crate) const COMPAT_PENDING_SQL: &str = "SELECT EXISTS(SELECT 1 FROM source_roots r WHERE r.report_source=?1 AND r.identity_family='claude' AND r.route='ledger' AND (EXISTS(SELECT 1 FROM ingest_runs u WHERE u.identity_family=r.identity_family AND u.namespace_key=r.namespace_key AND u.state='publishing') OR (EXISTS(SELECT 1 FROM usage_observations o WHERE o.identity_family=r.identity_family AND o.namespace_key=r.namespace_key) AND NOT EXISTS(SELECT 1 FROM ingest_runs u WHERE u.identity_family=r.identity_family AND u.namespace_key=r.namespace_key AND u.state='done' AND u.resolver_version='ledger-resolver-v3'))))";

#[derive(Clone, Copy)]
pub(crate) enum RebuildCause {
    Automatic,
    LegacyAutomatic,
    ExplicitDelete,
}

#[derive(Debug, Default, Serialize)]
pub struct PublishSummary {
    pub runs: u64,
    pub units: u64,
    pub changed_units: u64,
}

/// The column order deliberately matches the schema's explicit observation projection.
pub(crate) fn load_observation(r: &Row<'_>) -> rusqlite::Result<Observation> {
    Ok(Observation {
        observation_id: r.get(0)?,
        identity_family: r.get(1)?,
        namespace_key: r.get(2)?,
        identity_key: r.get(3)?,
        candidate_key: r.get(4)?,
        normalized_hash: r.get(5)?,
        object_id: r.get(6)?,
        content_generation: r.get(7)?,
        record_key: r.get(8)?,
        parser_version: r.get(9)?,
        identity_version: r.get(10)?,
        semantics_version: r.get(11)?,
        observed_ms: r.get(12)?,
        eligibility: r.get(13)?,
        action: r.get(14)?,
        revision_authority: r.get(15)?,
        revision_no: r.get(16)?,
        fact_kind: r.get(17)?,
        report_source: r.get(18)?,
        native_tool: r.get(19)?,
        model_key: r.get(20)?,
        model_display: r.get(21)?,
        session_key: r.get(22)?,
        native_session_id: r.get(23)?,
        native_message_id: r.get(24)?,
        native_request_id: r.get(25)?,
        occurred_ms: r.get(26)?,
        native_date: r.get(27)?,
        native_timezone: r.get(28)?,
        day_start_ms: r.get(29)?,
        day_end_ms: r.get(30)?,
        metrics: [
            r.get(31)?,
            r.get(32)?,
            r.get(33)?,
            r.get(34)?,
            r.get(35)?,
            r.get(36)?,
            r.get(37)?,
            r.get(38)?,
            r.get(39)?,
            r.get(40)?,
        ],
        cost_state: r.get(41)?,
        cost_units: r.get(42)?,
        cost_scale: r.get(43)?,
        cost_currency: r.get(44)?,
    })
}

pub fn publish_key(o: &Observation) -> Option<String> {
    if o.identity_family == "claude" && o.fact_kind == "request" {
        if let Some(m) = &o.native_message_id {
            return Some(message_publish_key(m));
        }
    }
    o.identity_key.as_ref().map(|k| format!("k:{k}"))
}
fn message_publish_key(m: &str) -> String {
    let mut result = String::with_capacity(2 + m.len() * 2);
    result.push_str("m:");
    use std::fmt::Write;
    for b in m.as_bytes() {
        let _ = write!(result, "{b:02x}");
    }
    result
}
fn decode_message(key: &str) -> Result<String> {
    let h = key
        .strip_prefix("m:")
        .ok_or_else(|| LedgerError::InvalidInput("invalid publication key".into()))?;
    if !h.is_ascii() || h.len() % 2 != 0 || h.len() > 8192 {
        return Err(LedgerError::CorruptLedger);
    }
    let bytes = (0..h.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&h[i..i + 2], 16).map_err(|_| LedgerError::CorruptLedger))
        .collect::<Result<Vec<_>>>()?;
    String::from_utf8(bytes).map_err(|_| LedgerError::CorruptLedger)
}

fn create_work_tables(c: &Connection) -> Result<()> {
    c.execute_batch("CREATE TEMP TABLE IF NOT EXISTS ledger_run_cache(run_id INTEGER PRIMARY KEY, data_version INTEGER NOT NULL);
        CREATE TEMP TABLE IF NOT EXISTS ledger_run_units(run_id INTEGER NOT NULL,publish_key TEXT COLLATE BINARY NOT NULL,raw_key TEXT,message_id TEXT,PRIMARY KEY(run_id,publish_key),CHECK((raw_key IS NULL)<>(message_id IS NULL))) WITHOUT ROWID;
        CREATE TEMP TABLE IF NOT EXISTS ledger_unit_evidence(id INTEGER PRIMARY KEY,raw_key TEXT);
        CREATE INDEX IF NOT EXISTS temp.ledger_unit_evidence_raw ON ledger_unit_evidence(raw_key,id);
        CREATE TEMP TABLE IF NOT EXISTS ledger_unit_pairs(session_id TEXT NOT NULL,request_id TEXT NOT NULL,PRIMARY KEY(session_id,request_id)) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS temp.ledger_unit_pairs_request ON ledger_unit_pairs(request_id,session_id);
        CREATE TEMP TABLE IF NOT EXISTS ledger_unit_members(raw_key TEXT PRIMARY KEY,canonical_key TEXT NOT NULL) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS temp.ledger_unit_members_canonical ON ledger_unit_members(canonical_key,raw_key);
        CREATE TEMP TABLE IF NOT EXISTS ledger_unit_old_keys(identity_key TEXT PRIMARY KEY) WITHOUT ROWID;
        CREATE TEMP TABLE IF NOT EXISTS ledger_unit_decisions(identity_key TEXT PRIMARY KEY,selected_id INTEGER,state TEXT NOT NULL,has_conflict INTEGER NOT NULL,fact_kind TEXT NOT NULL) WITHOUT ROWID;
        CREATE TEMP TABLE IF NOT EXISTS ledger_unit_alias(alias_key TEXT PRIMARY KEY,canonical_key TEXT NOT NULL) WITHOUT ROWID;")?;
    Ok(())
}

#[derive(Debug)]
struct FrozenRun {
    id: i64,
    family: String,
    namespace: String,
    lower: i64,
    upper: i64,
    last: Option<String>,
    version: String,
}

/// A committed keyset boundary may outlive its observations after forget.
fn valid_publish_cursor(family: &str, key: &str) -> bool {
    if key.starts_with("m:") {
        family == "claude"
            && decode_message(key).is_ok_and(|m| !m.is_empty() && message_publish_key(&m) == key)
    } else {
        key.strip_prefix("k:").is_some_and(|h| {
            h.len() == 64
                && h.bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        })
    }
}

/// Freeze new evidence, or one complete v3 replay of a legacy Claude namespace.
fn next_run(c: &Connection) -> Result<Option<FrozenRun>> {
    #[allow(clippy::type_complexity)]
    let active:Option<(i64,String,String,i64,i64,Option<String>,String,String,i64)>=c.query_row(
        "SELECT run_id,identity_family,namespace_key,lower_observation_id,upper_observation_id,last_publish_key,epoch,resolver_version,identity_version FROM ingest_runs WHERE state='publishing' ORDER BY run_id LIMIT 1",[],
        |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?,r.get(8)?))).optional()?;
    let (epoch, iv): (String, i64) = c.query_row(
        "SELECT epoch,identity_version FROM ledger_meta WHERE singleton=1",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    if let Some((id, family, namespace, lower, upper, last, re, mut rv, ri)) = active {
        if re != epoch
            || ri != iv
            || lower < 0
            || upper <= lower
            || !matches!(
                rv.as_str(),
                "ledger-resolver-v1" | LEGACY_RESOLVER_VERSION | RESOLVER_VERSION
            )
            || last
                .as_deref()
                .is_some_and(|key| !valid_publish_cursor(&family, key))
        {
            return Err(LedgerError::UpgradeRequiresDrain);
        }
        if rv == "ledger-resolver-v1" {
            // Drain the original window under v2 rules. Its old prefix has not
            // been reviewed by v3, so this task must not become a v3 completion.
            rv = LEGACY_RESOLVER_VERSION.into();
            c.execute(
                "UPDATE ingest_runs SET resolver_version=?1 WHERE run_id=?2 AND state='publishing'",
                params![rv, id],
            )?;
        }
        return Ok(Some(FrozenRun {
            id,
            family,
            namespace,
            lower,
            upper,
            last,
            version: rv,
        }));
    }
    let repair:Option<(String,String,i64,i64)>=c.query_row(
        "SELECT o.identity_family,o.namespace_key,0,MAX(MAX(o.observation_id),COALESCE((SELECT MAX(r.upper_observation_id) FROM ingest_runs r WHERE r.identity_family=o.identity_family AND r.namespace_key=o.namespace_key AND r.state='done'),0)) FROM usage_observations o WHERE o.identity_family='claude' AND NOT EXISTS(SELECT 1 FROM ingest_runs r WHERE r.identity_family=o.identity_family AND r.namespace_key=o.namespace_key AND r.state='done' AND r.resolver_version=?1) GROUP BY o.identity_family,o.namespace_key ORDER BY o.namespace_key LIMIT 1", [RESOLVER_VERSION],
        |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?;
    let next = if repair.is_some() {
        repair
    } else {
        c.query_row(
        "SELECT o.identity_family,o.namespace_key,COALESCE((SELECT MAX(r.upper_observation_id) FROM ingest_runs r WHERE r.identity_family=o.identity_family AND r.namespace_key=o.namespace_key AND r.state='done'),0) AS lower,MAX(o.observation_id) AS upper FROM usage_observations o GROUP BY o.identity_family,o.namespace_key HAVING upper>lower ORDER BY o.identity_family,o.namespace_key LIMIT 1",[],
        |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?
    };
    if let Some((family, namespace, lower, upper)) = next {
        c.execute("INSERT INTO ingest_runs(identity_family,namespace_key,epoch,resolver_version,identity_version,state,lower_observation_id,upper_observation_id,created_ms) VALUES(?1,?2,?3,?4,?5,'publishing',?6,?7,?8)",params![family,namespace,epoch,RESOLVER_VERSION,iv,lower,upper,now_ms()])?;
        let id = c.last_insert_rowid();
        super::db::bump(c)?;
        Ok(Some(FrozenRun {
            id,
            family,
            namespace,
            lower,
            upper,
            last: None,
            version: RESOLVER_VERSION.into(),
        }))
    } else {
        Ok(None)
    }
}

/// Maintenance can run on another connection. data_version invalidates its
/// writer's cached keyset on the next slice; this clears same-connection caches.
pub(crate) fn invalidate_run_units(c: &Connection) -> Result<()> {
    let exists: bool = c.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_temp_master WHERE name='ledger_run_cache')",
        [],
        |r| r.get(0),
    )?;
    if exists {
        c.execute("DELETE FROM ledger_run_cache", [])?;
    }
    Ok(())
}

fn make_run_units(c: &Connection, r: &FrozenRun) -> Result<()> {
    c.execute("DELETE FROM ledger_run_units", [])?;
    // hex() uses UTF-8 bytes, rather than code-point or locale ordering.
    c.execute("INSERT OR IGNORE INTO ledger_run_units SELECT ?1,CASE WHEN identity_family='claude' AND fact_kind='request' AND native_message_id IS NOT NULL THEN 'm:'||lower(hex(native_message_id)) ELSE 'k:'||identity_key END,CASE WHEN identity_family='claude' AND fact_kind='request' AND native_message_id IS NOT NULL THEN NULL ELSE identity_key END,CASE WHEN identity_family='claude' AND fact_kind='request' AND native_message_id IS NOT NULL THEN native_message_id ELSE NULL END FROM usage_observations WHERE identity_family=?2 AND namespace_key=?3 AND observation_id>?4 AND observation_id<=?5 AND (identity_key IS NOT NULL OR (identity_family='claude' AND fact_kind='request' AND native_message_id IS NOT NULL))",params![r.id,r.family,r.namespace,r.lower,r.upper])?;
    Ok(())
}

impl Ledger {
    /// Publish all currently persisted candidates, in one atomic transaction per unit.
    pub fn publish_pending(&mut self) -> Result<PublishSummary> {
        self.publish_some(usize::MAX)
    }

    /// A bounded publication slice. A restart reconstructs TEMP keys from the
    /// persisted frozen window; late observations wait for the following run.
    pub fn publish_some(&mut self, max_units: usize) -> Result<PublishSummary> {
        if max_units == 0 {
            return Err(LedgerError::InvalidInput(
                "max_units must be positive".into(),
            ));
        }
        self.require_writer()?;
        create_work_tables(&self.conn)?;
        let mut summary = PublishSummary::default();
        loop {
            let tx = self
                .conn
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let r = next_run(&tx)?;
            if r.is_some() {
                regression_stop("t3_before_commit");
            }
            tx.commit()?;
            if r.is_some() {
                regression_stop("t3_after_commit");
            }
            let Some(mut run) = r else { break };
            let data_version: i64 = self
                .conn
                .pragma_query_value(None, "data_version", |row| row.get(0))?;
            let cached: bool = self.conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM ledger_run_cache WHERE run_id=?1 AND data_version=?2)",
                params![run.id, data_version],
                |row| row.get(0),
            )?;
            if !cached {
                make_run_units(&self.conn, &run)?;
                self.conn.execute("DELETE FROM ledger_run_cache", [])?;
                self.conn.execute(
                    "INSERT INTO ledger_run_cache VALUES(?1,?2)",
                    params![run.id, data_version],
                )?;
            }
            loop {
                // Separate first-page and resumed SQL so the composite key is a range seek.
                let key: Option<String> = match &run.last {
                    Some(last) => self.conn.query_row("SELECT publish_key FROM ledger_run_units WHERE run_id=?1 AND publish_key>?2 ORDER BY publish_key LIMIT 1",params![run.id,last],|r|r.get(0)).optional()?,
                    None => self.conn.query_row("SELECT publish_key FROM ledger_run_units WHERE run_id=?1 ORDER BY publish_key LIMIT 1",[run.id],|r|r.get(0)).optional()?,
                };
                let Some(key) = key else { break };
                let tx = self
                    .conn
                    .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                let valid:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM ingest_runs r JOIN ledger_meta m ON m.singleton=1 WHERE r.run_id=?1 AND r.state='publishing' AND r.epoch=m.epoch AND r.resolver_version=?2 AND r.identity_version=m.identity_version AND r.last_publish_key IS ?3)",params![run.id,run.version,run.last],|r|r.get(0))?;
                if !valid {
                    return Err(LedgerError::StaleObservation);
                }
                let changed = rebuild_unit(
                    &tx,
                    &run.family,
                    &run.namespace,
                    &key,
                    run.upper,
                    if run.version == RESOLVER_VERSION {
                        RebuildCause::Automatic
                    } else {
                        RebuildCause::LegacyAutomatic
                    },
                )?;
                if !changed {
                    super::db::bump(&tx)?;
                }
                tx.execute(
                    "UPDATE ingest_runs SET last_publish_key=?1 WHERE run_id=?2",
                    params![key, run.id],
                )?;
                regression_stop("t4_before_commit");
                tx.commit()?;
                regression_stop("t4_after_commit");
                summary.units += 1;
                summary.changed_units += u64::from(changed);
                run.last = Some(key);
                if summary.units >= max_units as u64 {
                    return Ok(summary);
                }
            }
            let tx = self
                .conn
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            tx.execute(
                "UPDATE ingest_runs SET state='done',finished_ms=?1 WHERE run_id=?2",
                params![now_ms(), run.id],
            )?;
            tx.execute("DELETE FROM ingest_runs WHERE identity_family=?1 AND namespace_key=?2 AND state='done' AND run_id<>?3",params![run.family,run.namespace,run.id])?;
            super::db::bump(&tx)?;
            regression_stop("t4_done_before_commit");
            tx.commit()?;
            regression_stop("t4_done_after_commit");
            summary.runs += 1;
        }
        Ok(summary)
    }
}

/// Maintenance must use this bound, including identity evidence, for the affected unit.
pub(crate) fn visible_upper(
    c: &Connection,
    family: &str,
    namespace: &str,
    key: &str,
) -> Result<i64> {
    let run:Option<(i64,i64,Option<String>)>=c.query_row("SELECT lower_observation_id,upper_observation_id,last_publish_key FROM ingest_runs WHERE identity_family=?1 AND namespace_key=?2 AND state='publishing'",params![family,namespace],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
    let published: i64 = c.query_row("SELECT COALESCE(MAX(upper_observation_id),0) FROM ingest_runs WHERE identity_family=?1 AND namespace_key=?2 AND state='done'",params![family,namespace],|r|r.get(0))?;
    if let Some((lower, upper, last)) = run {
        return Ok(if last.as_deref().is_some_and(|l| key <= l) {
            upper
        } else {
            lower.max(published)
        });
    }
    Ok(published)
}

fn prepare_unit(
    c: &Connection,
    family: &str,
    namespace: &str,
    key: &str,
    upper: i64,
) -> Result<Option<bool>> {
    c.execute_batch("DELETE FROM ledger_unit_evidence;DELETE FROM ledger_unit_pairs;DELETE FROM ledger_unit_members;DELETE FROM ledger_unit_old_keys;DELETE FROM ledger_unit_decisions;DELETE FROM ledger_unit_alias;")?;
    let message = if key.starts_with("m:") {
        Some(decode_message(key)?)
    } else {
        None
    };
    if let Some(m) = &message {
        if family != "claude" {
            return Err(LedgerError::CorruptLedger);
        }
        c.execute("INSERT INTO ledger_unit_evidence SELECT observation_id,identity_key FROM usage_observations INDEXED BY observations_claude_message_request WHERE identity_family='claude' AND namespace_key=?1 AND native_message_id=?2 AND fact_kind='request' AND observation_id<=?3",params![namespace,m,upper])?;
    } else {
        let raw = key.strip_prefix("k:").ok_or(LedgerError::CorruptLedger)?;
        c.execute("INSERT INTO ledger_unit_evidence SELECT observation_id,identity_key FROM usage_observations INDEXED BY observations_identity WHERE identity_family=?1 AND namespace_key=?2 AND identity_key=?3 AND observation_id<=?4",params![family,namespace,raw,upper])?;
        c.execute(
            "INSERT OR IGNORE INTO ledger_unit_old_keys VALUES(?1)",
            [raw],
        )?;
    }
    c.execute("INSERT OR IGNORE INTO ledger_unit_old_keys SELECT raw_key FROM ledger_unit_evidence WHERE raw_key IS NOT NULL",[])?;
    // Old canonical keys may have become invalid after a late identity observation.
    c.execute("INSERT OR IGNORE INTO ledger_unit_old_keys SELECT a.canonical_key FROM ledger_unit_old_keys k CROSS JOIN identity_alias a ON a.alias_key=k.identity_key WHERE a.identity_family=?1 AND a.namespace_key=?2",params![family,namespace])?;
    c.execute("INSERT OR IGNORE INTO ledger_unit_old_keys SELECT a.alias_key FROM ledger_unit_old_keys k CROSS JOIN identity_alias a ON a.canonical_key=k.identity_key WHERE a.identity_family=?1 AND a.namespace_key=?2",params![family,namespace])?;
    c.execute("INSERT INTO ledger_unit_members SELECT DISTINCT e.raw_key,e.raw_key FROM ledger_unit_evidence e CROSS JOIN usage_observations o ON o.observation_id=e.id WHERE e.raw_key IS NOT NULL AND o.eligibility<>'identity_only'",[])?;
    if message.is_none() {
        return Ok(None);
    }
    // Force the bounded unit keyset to drive lookups. SQLite otherwise may scan
    // the growing permanent table once per unit, making imports quadratic.
    let has_full:bool=c.query_row("SELECT EXISTS(SELECT 1 FROM ledger_unit_evidence e CROSS JOIN usage_observations o ON o.observation_id=e.id WHERE o.native_request_id IS NOT NULL)",[],|r|r.get(0))?;
    if !has_full {
        return Ok(None);
    }
    c.execute("INSERT OR IGNORE INTO ledger_unit_pairs SELECT o.native_session_id,o.native_request_id FROM ledger_unit_evidence e CROSS JOIN usage_observations o ON o.observation_id=e.id WHERE o.native_session_id IS NOT NULL AND o.native_request_id IS NOT NULL",[])?;
    // A fallback with a possible full identity needs bidirectional uniqueness. This
    // proof is statistical only: it never creates permanent deletion authority.
    let mut q=c.prepare("SELECT DISTINCT o.identity_key,o.native_session_id FROM ledger_unit_evidence e CROSS JOIN usage_observations o ON o.observation_id=e.id WHERE o.eligibility<>'identity_only' AND o.native_request_id IS NULL AND o.identity_key IS NOT NULL")?;
    let mut rows = q.query([])?;
    let mut ambiguity: Option<bool> = None;
    while let Some(r) = rows.next()? {
        let raw: String = r.get(0)?;
        let s: Option<String> = r.get(1)?;
        let Some(s) = s else {
            ambiguity = Some(ambiguity.unwrap_or(false));
            continue;
        };
        let (n, request): (i64, Option<String>) = c.query_row(
            "SELECT COUNT(*),MIN(request_id) FROM ledger_unit_pairs WHERE session_id=?1",
            [&s],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        if n != 1 {
            ambiguity = Some(ambiguity.unwrap_or(false) || n > 1);
            continue;
        }
        let request = request.ok_or(LedgerError::CorruptLedger)?;
        let ns: i64 = c.query_row(
            "SELECT COUNT(*) FROM ledger_unit_pairs WHERE request_id=?1",
            [&request],
            |r| r.get(0),
        )?;
        if ns != 1 {
            ambiguity = Some(ambiguity.unwrap_or(false) || ns > 1);
            continue;
        }
        let canonical = identity_key(
            family,
            namespace,
            "request/full",
            &[message.as_deref().unwrap(), &request],
        );
        c.execute(
            "UPDATE ledger_unit_members SET canonical_key=?1 WHERE raw_key=?2",
            params![canonical, raw],
        )?;
    }
    Ok(ambiguity)
}

#[derive(Clone)]
struct Projection {
    fact_kind: String,
    source: String,
    tool: String,
    model: Option<String>,
    session: Option<String>,
    time: Option<i64>,
    date: Option<String>,
    timezone: Option<String>,
    day_start: Option<i64>,
    day_end: Option<i64>,
    metrics: Metrics,
    cost: Option<(i64, i64, String)>,
}
impl Projection {
    fn from(o: &Observation) -> Self {
        Self {
            fact_kind: o.fact_kind.clone(),
            source: o.report_source.clone(),
            tool: o.native_tool.clone(),
            model: if o.model_key.is_empty() {
                None
            } else {
                Some(o.model_key.clone())
            },
            session: o.session_key.clone(),
            time: if o.identity_family == "claude"
                && matches!(o.fact_kind.as_str(), "session_total" | "session_cost")
            {
                None
            } else {
                o.occurred_ms
            },
            date: o.native_date.clone(),
            timezone: o.native_timezone.clone(),
            day_start: o.day_start_ms,
            day_end: o.day_end_ms,
            metrics: o.metrics,
            cost: if o.cost_state == "known" {
                Some((
                    o.cost_units.unwrap(),
                    o.cost_scale.unwrap(),
                    o.cost_currency.clone().unwrap(),
                ))
            } else {
                None
            },
        }
    }
    fn merge(&mut self, p: &Self, monotonic: bool) -> bool {
        if self.fact_kind != p.fact_kind || self.source != p.source || self.tool != p.tool {
            return false;
        }
        if !merge_value(&mut self.model, &p.model)
            || !merge_value(&mut self.session, &p.session)
            || !merge_value(&mut self.time, &p.time)
            || !merge_value(&mut self.date, &p.date)
            || !merge_value(&mut self.timezone, &p.timezone)
            || !merge_value(&mut self.day_start, &p.day_start)
            || !merge_value(&mut self.day_end, &p.day_end)
        {
            return false;
        }
        for i in 0..10 {
            if monotonic {
                self.metrics[i] = match (self.metrics[i], p.metrics[i]) {
                    (Some(a), Some(b)) => Some(a.max(b)),
                    (a, b) => a.or(b),
                };
            } else if !merge_value(&mut self.metrics[i], &p.metrics[i]) {
                return false;
            }
        }
        if monotonic && self.fact_kind == "session_cost" {
            match (&self.cost, &p.cost) {
                (Some(a), Some(b)) => {
                    if a.2 != b.2 {
                        return false;
                    }
                    let scale = a.1.max(b.1);
                    let av = i128::from(a.0) * 10_i128.pow((scale - a.1) as u32);
                    let bv = i128::from(b.0) * 10_i128.pow((scale - b.1) as u32);
                    if bv > av {
                        self.cost = Some(b.clone());
                    }
                }
                (None, Some(b)) => self.cost = Some(b.clone()),
                _ => {}
            }
        } else if !merge_value(&mut self.cost, &p.cost) {
            return false;
        }
        true
    }
    fn covers(&self, p: &Self) -> bool {
        self.fact_kind == p.fact_kind
            && self.source == p.source
            && self.tool == p.tool
            && covers(&self.model, &p.model)
            && covers(&self.session, &p.session)
            && covers(&self.time, &p.time)
            && covers(&self.date, &p.date)
            && covers(&self.timezone, &p.timezone)
            && covers(&self.day_start, &p.day_start)
            && covers(&self.day_end, &p.day_end)
            && covers(&self.cost, &p.cost)
            && self
                .metrics
                .iter()
                .zip(&p.metrics)
                .all(|(a, b)| covers(a, b))
    }
}
fn merge_value<T: Clone + Eq>(a: &mut Option<T>, b: &Option<T>) -> bool {
    match (&*a, b) {
        (Some(x), Some(y)) => x == y,
        (None, Some(_)) => {
            *a = b.clone();
            true
        }
        _ => true,
    }
}
fn covers<T: Eq>(a: &Option<T>, b: &Option<T>) -> bool {
    b.is_none() || a == b
}

/// Only representation time and collection metadata may differ. Canonical
/// request membership was proved by prepare_unit, including fallback aliases.
fn equivalent_claude_blocks(a: &Observation, b: &Observation) -> bool {
    a.identity_family == "claude"
        && b.identity_family == "claude"
        && a.fact_kind == "request"
        && b.fact_kind == "request"
        && a.eligibility == "eligible"
        && b.eligibility == "eligible"
        && a.action == "upsert"
        && b.action == "upsert"
        && a.revision_authority.is_none()
        && b.revision_authority.is_none()
        && a.revision_no.is_none()
        && b.revision_no.is_none()
        && a.occurred_ms.is_some()
        && b.occurred_ms.is_some()
        && a.namespace_key == b.namespace_key
        && a.report_source == b.report_source
        && a.native_tool == b.native_tool
        && a.model_key == b.model_key
        && a.session_key == b.session_key
        && a.native_session_id == b.native_session_id
        && a.native_message_id == b.native_message_id
        && a.metrics == b.metrics
        && a.cost_state == b.cost_state
        && a.cost_units == b.cost_units
        && a.cost_scale == b.cost_scale
        && a.cost_currency == b.cost_currency
        && a.native_date == b.native_date
        && a.native_timezone == b.native_timezone
        && a.day_start_ms == b.day_start_ms
        && a.day_end_ms == b.day_end_ms
        && a.identity_version == b.identity_version
        && a.semantics_version == b.semantics_version
}

fn repeated_claude_block(c: &Connection, canonical: &str) -> Result<Option<Observation>> {
    let mut q = c.prepare("SELECT o.* FROM ledger_unit_members m CROSS JOIN ledger_unit_evidence e ON e.raw_key=m.raw_key CROSS JOIN usage_observations o ON o.observation_id=e.id WHERE m.canonical_key=?1 AND o.eligibility<>'identity_only' ORDER BY o.occurred_ms,o.normalized_hash,o.identity_key,o.observation_id")?;
    let mut rows = q.query([canonical])?;
    let mut first: Option<Observation> = None;
    let mut different_time = false;
    while let Some(row) = rows.next()? {
        let o = load_observation(row)?;
        if let Some(first) = &first {
            if !equivalent_claude_blocks(first, &o) {
                return Ok(None);
            }
            different_time |= first.occurred_ms != o.occurred_ms;
        } else {
            if !equivalent_claude_blocks(&o, &o) {
                return Ok(None);
            }
            first = Some(o);
        }
    }
    Ok(if different_time { first } else { None })
}

struct Decision {
    selected: Option<Observation>,
    conflict: bool,
    fact_kind: String,
    equivalent_blocks: bool,
}
fn decide(c: &Connection, canonical: &str, allow_blocks: bool) -> Result<Decision> {
    if allow_blocks {
        if let Some(selected) = repeated_claude_block(c, canonical)? {
            return Ok(Decision {
                selected: Some(selected),
                conflict: false,
                fact_kind: "request".into(),
                equivalent_blocks: true,
            });
        }
    }
    let sql="SELECT o.* FROM ledger_unit_members m CROSS JOIN ledger_unit_evidence e ON e.raw_key=m.raw_key CROSS JOIN usage_observations o ON o.observation_id=e.id WHERE m.canonical_key=?1 AND o.eligibility<>'identity_only' ORDER BY o.normalized_hash,o.identity_key,o.observation_id";
    // Authorities are adapter-owned. No current Claude request adapter has one.
    let authority_count:i64=c.query_row("SELECT COUNT(DISTINCT o.revision_authority) FROM ledger_unit_members m CROSS JOIN ledger_unit_evidence e ON e.raw_key=m.raw_key CROSS JOIN usage_observations o ON o.observation_id=e.id WHERE m.canonical_key=?1 AND o.eligibility<>'identity_only'",[canonical],|r|r.get(0))?;
    let authority:Option<String>=c.query_row("SELECT MAX(o.revision_authority) FROM ledger_unit_members m CROSS JOIN ledger_unit_evidence e ON e.raw_key=m.raw_key CROSS JOIN usage_observations o ON o.observation_id=e.id WHERE m.canonical_key=?1 AND o.eligibility<>'identity_only'",[canonical],|r|r.get(0))?;
    let revision: Option<i64> = if let Some(a) = &authority {
        c.query_row("SELECT MAX(o.revision_no) FROM ledger_unit_members m CROSS JOIN ledger_unit_evidence e ON e.raw_key=m.raw_key CROSS JOIN usage_observations o ON o.observation_id=e.id WHERE m.canonical_key=?1 AND o.revision_authority=?2",params![canonical,a],|r|r.get(0))?
    } else {
        None
    };
    let applicable = |o: &Observation| {
        authority_count <= 1
            && (authority.is_none()
                || (o.revision_authority == authority && o.revision_no == revision))
    };
    let mut union: Option<Projection> = None;
    let mut conflict = authority_count > 1;
    let mut action: Option<String> = None;
    let mut fact_kind = String::from("request");
    let mut cumulative = false;
    let mut q = c.prepare(sql)?;
    let mut rows = q.query([canonical])?;
    while let Some(r) = rows.next()? {
        let o = load_observation(r)?;
        fact_kind = o.fact_kind.clone();
        if !applicable(&o) {
            continue;
        }
        if o.eligibility == "inconsistent" {
            conflict = true;
        }
        if !merge_value(&mut action, &Some(o.action.clone())) {
            conflict = true;
        }
        let p = Projection::from(&o);
        let mono = o.identity_family == "claude"
            && matches!(o.fact_kind.as_str(), "session_total" | "session_cost");
        cumulative |= mono;
        if let Some(u) = &mut union {
            if !u.merge(&p, mono) {
                conflict = true;
            }
        } else {
            union = Some(p);
        }
    }
    drop(rows);
    if conflict {
        return Ok(Decision {
            selected: None,
            conflict: true,
            fact_kind,
            equivalent_blocks: false,
        });
    }
    let Some(union) = union else {
        return Ok(Decision {
            selected: None,
            conflict: false,
            fact_kind,
            equivalent_blocks: false,
        });
    };
    let mut rows = q.query([canonical])?;
    while let Some(r) = rows.next()? {
        let o = load_observation(r)?;
        if applicable(&o) && o.eligibility == "eligible" && Projection::from(&o).covers(&union) {
            return Ok(Decision {
                selected: Some(o),
                conflict: false,
                fact_kind,
                equivalent_blocks: false,
            });
        }
    }
    Ok(Decision {
        selected: None,
        conflict: cumulative,
        fact_kind,
        equivalent_blocks: false,
    })
}

#[derive(Clone)]
struct OldSelection {
    key: String,
    selected_id: i64,
    state: String,
    conflict: bool,
    fact_kind: String,
}

fn insert_decision(
    c: &Connection,
    key: &str,
    selected_id: Option<i64>,
    state: &str,
    conflict: bool,
    fact_kind: &str,
) -> Result<()> {
    c.execute(
        "INSERT INTO ledger_unit_decisions VALUES(?1,?2,?3,?4,?5)",
        params![key, selected_id, state, i64::from(conflict), fact_kind],
    )?;
    Ok(())
}

fn monotonic_replacement(old: &Observation, new: &Observation) -> bool {
    if old.fact_kind != new.fact_kind
        || !matches!(
            old.fact_kind.as_str(),
            "session_total" | "session_cost" | "day"
        )
        || (old.fact_kind == "day"
            && (old.revision_authority.as_deref() != Some("native-day-cumulative")
                || new.revision_authority != old.revision_authority))
    {
        return false;
    }
    let a = Projection::from(old);
    let b = Projection::from(new);
    if a.fact_kind != b.fact_kind
        || a.source != b.source
        || a.tool != b.tool
        || a.model != b.model
        || a.session != b.session
        || a.time != b.time
        || a.date != b.date
        || a.timezone != b.timezone
        || a.day_start != b.day_start
        || a.day_end != b.day_end
    {
        return false;
    }
    let (Ok(old_metrics), Ok(new_metrics)) =
        (metrics::evaluate(&a.metrics), metrics::evaluate(&b.metrics))
    else {
        return false;
    };
    if old_metrics
        .iter()
        .zip(new_metrics.iter())
        .any(|(old, new)| {
            (old.state == ValueState::Exact && new.state != ValueState::Exact)
                || old
                    .known_value
                    .is_some_and(|value| new.known_value.is_none_or(|next| next < value))
        })
    {
        return false;
    }
    match (a.cost, b.cost) {
        (Some((av, ascale, ac)), Some((bv, bscale, bc))) if ac == bc => {
            let scale = ascale.max(bscale);
            i128::from(bv) * 10_i128.pow((scale - bscale) as u32)
                >= i128::from(av) * 10_i128.pow((scale - ascale) as u32)
        }
        (Some(_), _) => false,
        _ => true,
    }
}

fn cumulative_candidate(
    c: &Connection,
    canonical: &str,
    old: &Observation,
) -> Result<Option<Observation>> {
    if !matches!(
        old.fact_kind.as_str(),
        "session_total" | "session_cost" | "day"
    ) {
        return Ok(None);
    }
    let mut q = c.prepare("SELECT o.* FROM ledger_unit_members m JOIN ledger_unit_evidence e ON e.raw_key=m.raw_key JOIN usage_observations o ON o.observation_id=e.id WHERE m.canonical_key=?1 AND o.eligibility='eligible' AND o.action='upsert' ORDER BY o.observation_id")?;
    let candidates = q
        .query_map([canonical], load_observation)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(q);
    let growing: Vec<_> = candidates
        .into_iter()
        .filter(|o| monotonic_replacement(old, o))
        .collect();
    let best = growing.iter().find(|candidate| {
        growing
            .iter()
            .all(|other| monotonic_replacement(other, candidate))
    });
    Ok(best.cloned())
}

fn cumulative_conflict(c: &Connection, canonical: &str, selected: &Observation) -> Result<bool> {
    let mut q = c.prepare("SELECT o.* FROM ledger_unit_members m JOIN ledger_unit_evidence e ON e.raw_key=m.raw_key JOIN usage_observations o ON o.observation_id=e.id WHERE m.canonical_key=?1 AND o.eligibility<>'identity_only'")?;
    let mut rows = q.query([canonical])?;
    while let Some(row) = rows.next()? {
        let candidate = load_observation(row)?;
        if candidate.eligibility != "eligible"
            || candidate.action != "upsert"
            || !monotonic_replacement(&candidate, selected)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn automatic_decisions(
    c: &Connection,
    family: &str,
    namespace: &str,
    ambiguity: Option<bool>,
    allow_blocks: bool,
) -> Result<()> {
    let mut old = Vec::<OldSelection>::new();
    let mut q = c.prepare("SELECT e.identity_key,e.selected_observation_id,e.state,e.has_conflict,e.fact_kind FROM ledger_unit_old_keys k CROSS JOIN usage_events e WHERE e.identity_family=?1 AND e.namespace_key=?2 AND e.identity_key=k.identity_key AND e.state<>'unresolved' ORDER BY e.identity_key")?;
    let rows = q.query_map(params![family, namespace], |r| {
        Ok(OldSelection {
            key: r.get(0)?,
            selected_id: r.get(1)?,
            state: r.get(2)?,
            conflict: r.get::<_, i64>(3)? != 0,
            fact_kind: r.get(4)?,
        })
    })?;
    for row in rows {
        old.push(row?);
    }
    drop(q);
    if let Some(identity_conflict) = ambiguity {
        // A fallback may point at more than one full request. Keep published
        // selections, but still admit a new full request if no old fallback
        // for its session could already represent that request.
        let mut old_fallback_sessions = BTreeSet::new();
        let mut old_fallback_without_session = false;
        for item in &old {
            let (request, session): (Option<String>, Option<String>) = c.query_row(
                "SELECT native_request_id,native_session_id FROM usage_observations WHERE observation_id=?1",
                [item.selected_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            if request.is_none() {
                if let Some(session) = session {
                    old_fallback_sessions.insert(session);
                } else {
                    old_fallback_without_session = true;
                }
            }
        }
        for item in &old {
            insert_decision(
                c,
                &item.key,
                Some(item.selected_id),
                &item.state,
                item.conflict || identity_conflict,
                &item.fact_kind,
            )?;
        }
        let mut q = c.prepare("SELECT DISTINCT m.canonical_key,o.native_session_id FROM ledger_unit_members m CROSS JOIN ledger_unit_evidence e CROSS JOIN usage_observations o WHERE e.raw_key=m.raw_key AND o.observation_id=e.id AND o.native_request_id IS NOT NULL AND o.eligibility<>'identity_only' ORDER BY m.canonical_key")?;
        let full = q
            .query_map([], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(q);
        for (canonical, session) in full {
            if old_fallback_without_session
                || session
                    .as_ref()
                    .is_some_and(|s| old_fallback_sessions.contains(s))
            {
                continue;
            }
            let exists: bool = c.query_row(
                "SELECT EXISTS(SELECT 1 FROM ledger_unit_decisions WHERE identity_key=?1)",
                [&canonical],
                |r| r.get(0),
            )?;
            if exists {
                continue;
            }
            let decision = decide(c, &canonical, false)?;
            let state = match decision.selected.as_ref() {
                None => "unresolved",
                Some(o) if o.action == "retract" => "retracted",
                Some(_) => "active",
            };
            insert_decision(
                c,
                &canonical,
                decision.selected.as_ref().map(|o| o.observation_id),
                state,
                decision.conflict,
                &decision.fact_kind,
            )?;
        }
        c.execute("INSERT OR IGNORE INTO ledger_unit_decisions SELECT m.raw_key,NULL,'unresolved',?3,MIN(o.fact_kind) FROM ledger_unit_members m JOIN ledger_unit_evidence e ON e.raw_key=m.raw_key JOIN usage_observations o ON o.observation_id=e.id WHERE o.eligibility<>'identity_only' AND m.raw_key NOT IN(SELECT identity_key FROM ledger_unit_decisions) AND NOT EXISTS(SELECT 1 FROM identity_alias a WHERE a.identity_family=?1 AND a.namespace_key=?2 AND a.alias_key=m.raw_key) GROUP BY m.raw_key", params![family,namespace,i64::from(identity_conflict)])?;
        c.execute("INSERT OR IGNORE INTO ledger_unit_alias SELECT a.alias_key,a.canonical_key FROM ledger_unit_old_keys k JOIN identity_alias a ON a.alias_key=k.identity_key WHERE a.identity_family=?1 AND a.namespace_key=?2", params![family,namespace])?;
        return Ok(());
    }
    let mut groups = BTreeMap::<String, Vec<OldSelection>>::new();
    for item in old {
        let group: Option<String> = c
            .query_row(
                "SELECT canonical_key FROM ledger_unit_members WHERE raw_key=?1",
                [&item.key],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(group) = group {
            groups.entry(group).or_default().push(item);
        } else {
            insert_decision(
                c,
                &item.key,
                Some(item.selected_id),
                &item.state,
                item.conflict,
                &item.fact_kind,
            )?;
        }
    }
    let mut blocked = BTreeSet::new();
    for (group, selected) in &groups {
        if selected.len() > 1 {
            blocked.insert(group.clone());
            for item in selected {
                insert_decision(
                    c,
                    &item.key,
                    Some(item.selected_id),
                    &item.state,
                    true,
                    &item.fact_kind,
                )?;
            }
        } else if group != &selected[0].key {
            c.execute(
                "UPDATE ledger_unit_members SET canonical_key=?1 WHERE canonical_key=?2",
                params![selected[0].key, group],
            )?;
        }
    }
    let mut prior = BTreeMap::<String, OldSelection>::new();
    for (group, selected) in groups {
        if !blocked.contains(&group) {
            prior.insert(selected[0].key.clone(), selected[0].clone());
        }
    }
    let mut q =
        c.prepare("SELECT DISTINCT canonical_key FROM ledger_unit_members ORDER BY canonical_key")?;
    let keys = q
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(q);
    for canonical in keys {
        if blocked.contains(&canonical) {
            continue;
        }
        let decision = decide(c, &canonical, allow_blocks)?;
        if let Some(previous) = prior.get(&canonical) {
            let old_observation = c.query_row(
                "SELECT o.* FROM usage_observations o WHERE o.observation_id=?1",
                [previous.selected_id],
                load_observation,
            )?;
            let cumulative = if previous.state == "active" {
                cumulative_candidate(c, &canonical, &old_observation)?
            } else {
                None
            };
            let next = cumulative.as_ref();
            let selected_id = next.map_or(previous.selected_id, |o| o.observation_id);
            let proven_duplicate = previous.state == "active"
                && decision.equivalent_blocks
                && decision
                    .selected
                    .as_ref()
                    .is_some_and(|candidate| equivalent_claude_blocks(&old_observation, candidate));
            let conflict = !proven_duplicate
                && (previous.conflict
                    || if let Some(selected) = next {
                        cumulative_conflict(c, &canonical, selected)?
                    } else {
                        decision.conflict
                            || decision.selected.as_ref().is_none_or(|candidate| {
                                let mut projection = Projection::from(&old_observation);
                                !projection.merge(&Projection::from(candidate), false)
                            })
                    });
            insert_decision(
                c,
                &canonical,
                Some(selected_id),
                &previous.state,
                conflict,
                &previous.fact_kind,
            )?;
        } else {
            let state = match decision.selected.as_ref() {
                None => "unresolved",
                Some(o) if o.action == "retract" => "retracted",
                Some(_) => "active",
            };
            insert_decision(
                c,
                &canonical,
                decision.selected.as_ref().map(|o| o.observation_id),
                state,
                decision.conflict,
                &decision.fact_kind,
            )?;
        }
    }
    if !blocked.is_empty() {
        c.execute("INSERT OR IGNORE INTO ledger_unit_alias SELECT a.alias_key,a.canonical_key FROM ledger_unit_old_keys k JOIN identity_alias a ON a.alias_key=k.identity_key WHERE a.identity_family=?1 AND a.namespace_key=?2 AND a.canonical_key IN(SELECT identity_key FROM ledger_unit_decisions)", params![family,namespace])?;
    }
    c.execute("INSERT OR IGNORE INTO ledger_unit_alias SELECT canonical_key,canonical_key FROM ledger_unit_members WHERE raw_key<>canonical_key AND canonical_key IN(SELECT identity_key FROM ledger_unit_decisions)", [])?;
    c.execute("INSERT OR IGNORE INTO ledger_unit_alias SELECT raw_key,canonical_key FROM ledger_unit_members WHERE raw_key<>canonical_key AND canonical_key IN(SELECT identity_key FROM ledger_unit_decisions)", [])?;
    c.execute("INSERT OR IGNORE INTO ledger_unit_alias SELECT a.alias_key,a.canonical_key FROM ledger_unit_old_keys k JOIN identity_alias a ON a.alias_key=k.identity_key WHERE a.identity_family=?1 AND a.namespace_key=?2 AND a.alias_key NOT IN(SELECT alias_key FROM ledger_unit_alias) AND a.canonical_key IN(SELECT identity_key FROM ledger_unit_decisions)", params![family,namespace])?;
    // Identity-only observations may prove a full key for an already published
    // fallback. They cannot contribute usage, but the alias is needed for forget.
    let mut q = c.prepare("SELECT DISTINCT o.identity_key,o.native_session_id,o.native_request_id FROM ledger_unit_evidence e CROSS JOIN usage_observations o WHERE o.observation_id=e.id AND o.eligibility='identity_only' AND o.native_session_id IS NOT NULL AND o.native_request_id IS NOT NULL")?;
    let identity_rows = q
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(q);
    for (full_key, session, request) in identity_rows {
        let session_pairs: i64 = c.query_row(
            "SELECT COUNT(*) FROM ledger_unit_pairs WHERE session_id=?1",
            [&session],
            |r| r.get(0),
        )?;
        let request_pairs: i64 = c.query_row(
            "SELECT COUNT(*) FROM ledger_unit_pairs WHERE request_id=?1",
            [&request],
            |r| r.get(0),
        )?;
        if session_pairs != 1 || request_pairs != 1 {
            continue;
        }
        let fallback: Option<String> = c.query_row("SELECT d.identity_key FROM ledger_unit_decisions d JOIN usage_observations o ON o.observation_id=d.selected_id WHERE d.state='active' AND o.native_session_id=?1 AND o.native_request_id IS NULL LIMIT 1", [&session], |r| r.get(0)).optional()?;
        if let Some(fallback) = fallback {
            let full_is_decision: bool = c.query_row(
                "SELECT EXISTS(SELECT 1 FROM ledger_unit_decisions WHERE identity_key=?1)",
                [&full_key],
                |r| r.get(0),
            )?;
            if !full_is_decision {
                c.execute(
                    "INSERT OR IGNORE INTO ledger_unit_alias VALUES(?1,?2)",
                    params![fallback, fallback],
                )?;
                c.execute(
                    "INSERT OR IGNORE INTO ledger_unit_alias VALUES(?1,?2)",
                    params![full_key, fallback],
                )?;
            }
        }
    }
    Ok(())
}

/// Caller owns BEGIN IMMEDIATE. No source I/O and no evidence above `upper` is read.
pub(crate) fn rebuild_unit(
    c: &Connection,
    family: &str,
    namespace: &str,
    key: &str,
    upper: i64,
    cause: RebuildCause,
) -> Result<bool> {
    create_work_tables(c)?;
    let ambiguous = prepare_unit(c, family, namespace, key, upper)?;
    if matches!(
        cause,
        RebuildCause::Automatic | RebuildCause::LegacyAutomatic
    ) {
        automatic_decisions(
            c,
            family,
            namespace,
            ambiguous,
            matches!(cause, RebuildCause::Automatic),
        )?;
    } else if let Some(identity_conflict) = ambiguous {
        c.execute("UPDATE ledger_unit_members SET canonical_key=raw_key", [])?;
        c.execute("INSERT INTO ledger_unit_decisions SELECT m.raw_key,NULL,'unresolved',?1,MIN(o.fact_kind) FROM ledger_unit_members m CROSS JOIN ledger_unit_evidence e ON e.raw_key=m.raw_key CROSS JOIN usage_observations o ON o.observation_id=e.id WHERE o.eligibility<>'identity_only' GROUP BY m.raw_key",[i64::from(identity_conflict)])?;
    } else {
        let mut q = c.prepare(
            "SELECT DISTINCT canonical_key FROM ledger_unit_members ORDER BY canonical_key",
        )?;
        let mut rows = q.query([])?;
        while let Some(r) = rows.next()? {
            let canonical: String = r.get(0)?;
            let d = decide(c, &canonical, true)?;
            let state = match d.selected.as_ref() {
                None => "unresolved",
                Some(o) if o.action == "retract" => "retracted",
                Some(_) => "active",
            };
            c.execute(
                "INSERT INTO ledger_unit_decisions VALUES(?1,?2,?3,?4,?5)",
                params![
                    canonical,
                    d.selected.as_ref().map(|o| o.observation_id),
                    state,
                    i64::from(d.conflict),
                    d.fact_kind
                ],
            )?;
        }
        c.execute("INSERT OR IGNORE INTO ledger_unit_alias SELECT canonical_key,canonical_key FROM ledger_unit_members WHERE raw_key<>canonical_key",[])?;
        c.execute("INSERT OR IGNORE INTO ledger_unit_alias SELECT raw_key,canonical_key FROM ledger_unit_members WHERE raw_key<>canonical_key",[])?;
    }
    let event_diff:bool=c.query_row("SELECT EXISTS(SELECT identity_key,selected_id,state,has_conflict,fact_kind FROM ledger_unit_decisions EXCEPT SELECT e.identity_key,e.selected_observation_id,e.state,e.has_conflict,e.fact_kind FROM ledger_unit_old_keys k CROSS JOIN usage_events e ON e.identity_key=k.identity_key WHERE e.identity_family=?1 AND e.namespace_key=?2) OR EXISTS(SELECT e.identity_key,e.selected_observation_id,e.state,e.has_conflict,e.fact_kind FROM ledger_unit_old_keys k CROSS JOIN usage_events e ON e.identity_key=k.identity_key WHERE e.identity_family=?1 AND e.namespace_key=?2 EXCEPT SELECT identity_key,selected_id,state,has_conflict,fact_kind FROM ledger_unit_decisions)",params![family,namespace],|r|r.get(0))?;
    let alias_diff:bool=c.query_row("SELECT EXISTS(SELECT alias_key,canonical_key FROM ledger_unit_alias EXCEPT SELECT a.alias_key,a.canonical_key FROM ledger_unit_old_keys k CROSS JOIN identity_alias a ON a.alias_key=k.identity_key WHERE a.identity_family=?1 AND a.namespace_key=?2) OR EXISTS(SELECT a.alias_key,a.canonical_key FROM ledger_unit_old_keys k CROSS JOIN identity_alias a ON a.alias_key=k.identity_key WHERE a.identity_family=?1 AND a.namespace_key=?2 EXCEPT SELECT alias_key,canonical_key FROM ledger_unit_alias)",params![family,namespace],|r|r.get(0))?;
    if !event_diff && !alias_diff {
        return Ok(false);
    }
    let seq: i64 = c.query_row(
        "SELECT commit_seq FROM ledger_meta WHERE singleton=1",
        [],
        |r| r.get(0),
    )?;
    let seq = seq.checked_add(1).ok_or(LedgerError::NumericOverflow)?;
    let rev:i64=c.query_row("SELECT COALESCE(MAX(e.event_revision),0) FROM ledger_unit_old_keys k CROSS JOIN usage_events e ON e.identity_key=k.identity_key WHERE e.identity_family=?1 AND e.namespace_key=?2",params![family,namespace],|r|r.get(0))?;
    let rev = rev.checked_add(1).ok_or(LedgerError::NumericOverflow)?;
    let mut q=c.prepare("SELECT o.* FROM ledger_unit_old_keys k CROSS JOIN usage_events e ON e.identity_key=k.identity_key CROSS JOIN usage_observations o ON o.observation_id=e.selected_observation_id WHERE e.identity_family=?1 AND e.namespace_key=?2 AND e.state='active' AND e.fact_kind='request'")?;
    let mut rows = q.query(params![family, namespace])?;
    while let Some(r) = rows.next()? {
        change_rollup(c, &load_observation(r)?, -1)?;
    }
    drop(rows);
    drop(q);
    c.execute("DELETE FROM usage_events WHERE identity_family=?1 AND namespace_key=?2 AND identity_key IN(SELECT identity_key FROM ledger_unit_old_keys)",params![family,namespace])?;
    c.execute("DELETE FROM identity_alias WHERE identity_family=?1 AND namespace_key=?2 AND alias_key<>canonical_key AND alias_key IN(SELECT identity_key FROM ledger_unit_old_keys)",params![family,namespace])?;
    c.execute("DELETE FROM identity_alias WHERE identity_family=?1 AND namespace_key=?2 AND alias_key IN(SELECT identity_key FROM ledger_unit_old_keys)",params![family,namespace])?;
    c.execute("INSERT INTO identity_alias SELECT ?1,?2,a.alias_key,a.canonical_key,m.identity_version,?3 FROM ledger_unit_alias a CROSS JOIN ledger_meta m WHERE m.singleton=1 ORDER BY a.alias_key<>a.canonical_key,a.alias_key",params![family,namespace,seq])?;
    c.execute("INSERT INTO usage_events(identity_family,namespace_key,identity_key,selected_observation_id,state,has_conflict,fact_kind,report_source,native_tool,model_key,session_key,occurred_ms,day_start_ms,day_end_ms,event_revision,updated_commit_seq,model_display) SELECT ?1,?2,d.identity_key,d.selected_id,d.state,d.has_conflict,d.fact_kind,o.report_source,o.native_tool,o.model_key,o.session_key,o.occurred_ms,o.day_start_ms,o.day_end_ms,?3,?4,o.model_display FROM ledger_unit_decisions d LEFT JOIN usage_observations o ON o.observation_id=d.selected_id",params![family,namespace,rev,seq])?;
    let mut q=c.prepare("SELECT o.* FROM ledger_unit_decisions d JOIN usage_observations o ON o.observation_id=d.selected_id WHERE d.state='active' AND d.fact_kind='request'")?;
    let mut rows = q.query([])?;
    while let Some(r) = rows.next()? {
        change_rollup(c, &load_observation(r)?, 1)?;
    }
    c.execute(
        "UPDATE ledger_meta SET commit_seq=?1 WHERE singleton=1",
        [seq],
    )?;
    Ok(true)
}

fn change_rollup(c: &Connection, o: &Observation, sign: i64) -> Result<()> {
    let (tz, version): (String, i64) = c.query_row(
        "SELECT report_timezone,derived_version FROM ledger_meta WHERE singleton=1",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    let (start, end, day) =
        metrics::day_bounds(&tz, o.occurred_ms.ok_or(LedgerError::CorruptLedger)?)?;
    let values = metrics::evaluate(&o.metrics)?;
    let mut columns = String::from("event_count,bucket_revision");
    for name in METRIC_NAMES {
        columns.push_str(&format!(
            ",{name}_known_sum,{name}_known_count,{name}_exact_count"
        ));
    }
    let sql=format!("SELECT {columns} FROM usage_rollups WHERE derived_version=?1 AND grain='day' AND bucket_start_ms=?2 AND report_source=?3 AND model_key=?4");
    let old: Option<Vec<i64>> = c
        .query_row(
            &sql,
            params![version, start, o.report_source, o.model_key],
            |r| (0..32).map(|i| r.get(i)).collect(),
        )
        .optional()?;
    let mut sums = old.clone().unwrap_or_else(|| vec![0; 32]);
    sums[0] = sums[0]
        .checked_add(sign)
        .ok_or(LedgerError::NumericOverflow)?;
    sums[1] = sums[1].checked_add(1).ok_or(LedgerError::NumericOverflow)?;
    for (i, v) in values.iter().enumerate() {
        let contribution = [
            v.known_value.unwrap_or(0),
            i64::from(v.known_value.is_some()),
            i64::from(v.state == ValueState::Exact),
        ];
        for (j, value) in contribution.into_iter().enumerate() {
            let delta = value
                .checked_mul(sign)
                .ok_or(LedgerError::NumericOverflow)?;
            sums[2 + i * 3 + j] = sums[2 + i * 3 + j]
                .checked_add(delta)
                .ok_or(LedgerError::NumericOverflow)?;
        }
    }
    if sums.iter().any(|v| *v < 0) {
        return Err(LedgerError::CorruptLedger);
    }
    if sums[0] == 0 {
        c.execute("DELETE FROM usage_rollups WHERE derived_version=?1 AND grain='day' AND bucket_start_ms=?2 AND report_source=?3 AND model_key=?4",params![version,start,o.report_source,o.model_key])?;
        return Ok(());
    }
    let mut names = String::from(
        "derived_version,grain,bucket_start_ms,bucket_end_ms,local_day,report_source,model_key,",
    );
    names.push_str(&columns);
    let mut args: Vec<rusqlite::types::Value> = vec![
        version.into(),
        "day".to_owned().into(),
        start.into(),
        end.into(),
        day.into(),
        o.report_source.clone().into(),
        o.model_key.clone().into(),
    ];
    args.extend(sums.into_iter().map(Into::into));
    let placeholders = (1..=args.len())
        .map(|i| format!("?{i}"))
        .collect::<Vec<_>>()
        .join(",");
    c.execute(
        &format!("INSERT OR REPLACE INTO usage_rollups({names}) VALUES({placeholders})"),
        rusqlite::params_from_iter(args),
    )?;
    Ok(())
}

/// Offline timezone/derived rebuild reads only already selected events, never pending candidates.
pub(crate) fn rebuild_rollups(c: &Connection) -> Result<()> {
    let version: i64 = c.query_row(
        "SELECT derived_version FROM ledger_meta WHERE singleton=1",
        [],
        |r| r.get(0),
    )?;
    c.execute(
        "DELETE FROM usage_rollups WHERE derived_version=?1",
        [version],
    )?;
    let mut q=c.prepare("SELECT o.* FROM usage_events e JOIN usage_observations o ON o.observation_id=e.selected_observation_id WHERE e.state='active' AND e.fact_kind='request' ORDER BY e.occurred_ms")?;
    let mut rows = q.query([])?;
    while let Some(r) = rows.next()? {
        change_rollup(c, &load_observation(r)?, 1)?;
    }
    Ok(())
}

/// Maintenance calls this before deleting observations in the unit. Discovery may
/// inspect all saved identity keys, but only previously active contributions move.
pub(crate) fn remove_unit_events(
    c: &Connection,
    family: &str,
    namespace: &str,
    key: &str,
) -> Result<()> {
    create_work_tables(c)?;
    prepare_unit(c, family, namespace, key, i64::MAX)?;
    let mut q=c.prepare("SELECT identity_key FROM usage_events WHERE identity_family=?1 AND namespace_key=?2 AND identity_key IN(SELECT identity_key FROM ledger_unit_old_keys)")?;
    let mut rows = q.query(params![family, namespace])?;
    // Avoid modifying the event table while walking its cursor: a small disk TEMP
    // keyset already contains every key, including canonical roots.
    while let Some(r) = rows.next()? {
        let k: String = r.get(0)?;
        let o=c.query_row("SELECT o.* FROM usage_events e JOIN usage_observations o ON o.observation_id=e.selected_observation_id WHERE e.identity_family=?1 AND e.namespace_key=?2 AND e.identity_key=?3 AND e.state='active' AND e.fact_kind='request'",params![family,namespace,k],load_observation).optional()?;
        if let Some(o) = o {
            change_rollup(c, &o, -1)?;
        }
    }
    drop(rows);
    drop(q);
    c.execute("DELETE FROM usage_events WHERE identity_family=?1 AND namespace_key=?2 AND identity_key IN(SELECT identity_key FROM ledger_unit_old_keys)",params![family,namespace])?;
    c.execute("DELETE FROM identity_alias WHERE identity_family=?1 AND namespace_key=?2 AND alias_key<>canonical_key AND alias_key IN(SELECT identity_key FROM ledger_unit_old_keys)",params![family,namespace])?;
    c.execute("DELETE FROM identity_alias WHERE identity_family=?1 AND namespace_key=?2 AND alias_key IN(SELECT identity_key FROM ledger_unit_old_keys)",params![family,namespace])?;
    Ok(())
}

pub(crate) fn visible_horizon(
    c: &Connection,
    family: &str,
    namespace: &str,
    key: &str,
) -> Result<i64> {
    visible_upper(c, family, namespace, key)
}
