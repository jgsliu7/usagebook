//! Fair, resumable acquisition for the adapters admitted to permanent storage.
use super::{ccswitch, db, native_adapters, types::now_ms, Ledger, LedgerError, Result};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::PathBuf, time::Duration};

/// Periodic integrity work is separate from ordinary discovery/incremental
/// collection. Deadlines survive process restart and bind to adapter capability.
pub const HISTORY_AUDIT_INTERVAL_MS: i64 = 24 * 60 * 60 * 1000;

pub(crate) fn history_audit_due(
    conn: &Connection,
    root: &super::durable::DurableRoot,
) -> Result<bool> {
    if !root.enabled {
        return Ok(false);
    }
    let Some(state) = runtime_value(conn, &format!("history-audit/{}", root.root_id))? else {
        return Ok(true);
    };
    Ok(state["version"] != 1
        || state["capability_version"] != root.spec.capability_version
        || state["parser_version"] != root.spec.parser_version
        || state["identity_version"] != root.spec.identity_version
        || state["semantics_version"] != root.spec.semantics_version
        || state["next_due_ms"]
            .as_i64()
            .is_none_or(|due| due <= now_ms()))
}

/// Call only after the caller's whole bounded manifest/audit has completed.
/// May share the transaction which commits the final batch/watermark.
pub(crate) fn complete_history_audit(
    conn: &Connection,
    root: &super::durable::DurableRoot,
) -> Result<()> {
    let completed = now_ms();
    let old = runtime_value(conn, &format!("history-audit/{}", root.root_id))?
        .unwrap_or(serde_json::Value::Null);
    let last_epoch = old["started_ms"]
        .as_i64()
        .or_else(|| old["last_epoch"].as_i64());
    put_runtime_value(
        conn,
        &format!("history-audit/{}", root.root_id),
        &serde_json::json!({
            "version":1,"capability_version":root.spec.capability_version,
            "parser_version":root.spec.parser_version,"identity_version":root.spec.identity_version,
            "semantics_version":root.spec.semantics_version,"completed_ms":completed,
            "next_due_ms":completed+HISTORY_AUDIT_INTERVAL_MS,"last_epoch":last_epoch
        }),
    )
}

pub(crate) fn invalidate_history_audit(
    conn: &Connection,
    root: &super::durable::DurableRoot,
) -> Result<()> {
    invalidate_history_audit_key(conn, &root.root_id)
}

pub(crate) fn history_audit_started(
    conn: &Connection,
    root: &super::durable::DurableRoot,
) -> Result<Option<i64>> {
    if !history_audit_due(conn, root)? {
        return Ok(None);
    }
    let key = format!("history-audit/{}", root.root_id);
    let old = runtime_value(conn, &key)?.unwrap_or(serde_json::Value::Null);
    if old["capability_version"] == root.spec.capability_version
        && old["parser_version"] == root.spec.parser_version
        && old["identity_version"] == root.spec.identity_version
        && old["semantics_version"] == root.spec.semantics_version
    {
        if let Some(started) = old["started_ms"].as_i64() {
            return Ok(Some(started));
        }
    }
    let started = now_ms().max(old["last_epoch"].as_i64().unwrap_or(0).saturating_add(1));
    put_runtime_value(
        conn,
        &key,
        &serde_json::json!({"version":1,
        "capability_version":root.spec.capability_version,"parser_version":root.spec.parser_version,
        "identity_version":root.spec.identity_version,"semantics_version":root.spec.semantics_version,
        "started_ms":started,"next_due_ms":0}),
    )?;
    Ok(Some(started))
}

fn invalidate_history_audit_key(conn: &Connection, root: &str) -> Result<()> {
    let key = format!("history-audit/{root}");
    let mut state = runtime_value(conn, &key)?.unwrap_or_else(|| serde_json::json!({"version":1}));
    if let Some(started) = state["started_ms"].as_i64() {
        state["last_epoch"] = serde_json::json!(started);
    }
    state["next_due_ms"] = serde_json::json!(0);
    state["requested_ms"] = serde_json::json!(now_ms());
    state["started_ms"] = serde_json::Value::Null;
    put_runtime_value(conn, &key, &state)
}

/// Operational records contain only bounded internal identifiers and status.
/// They share the ledger transaction when called by an acquisition commit.
pub(crate) fn runtime_value(conn: &Connection, key: &str) -> Result<Option<serde_json::Value>> {
    let value: Option<String> = conn
        .query_row(
            "SELECT value_json FROM ledger_runtime_state WHERE state_key=?1",
            [key],
            |row| row.get(0),
        )
        .optional()?;
    value
        .map(|value| serde_json::from_str(&value).map_err(|_| LedgerError::CorruptLedger))
        .transpose()
}

pub(crate) fn put_runtime_value(
    conn: &Connection,
    key: &str,
    value: &serde_json::Value,
) -> Result<()> {
    let encoded = serde_json::to_string(value).map_err(|_| LedgerError::CorruptLedger)?;
    if key.len() > 512 || encoded.len() > 65536 {
        return Err(LedgerError::RecordTooLarge);
    }
    conn.execute("INSERT INTO ledger_runtime_state(state_key,value_json) VALUES(?1,?2) ON CONFLICT(state_key) DO UPDATE SET value_json=excluded.value_json", params![key, encoded])?;
    Ok(())
}

pub(crate) fn record_acquisition_progress(
    conn: &Connection,
    source: &str,
    root: &str,
) -> Result<()> {
    let key = format!("acquisition-turn/{source}");
    let sequence = runtime_value(conn, &key)?
        .and_then(|v| v["sequence"].as_u64())
        .unwrap_or(0)
        .checked_add(1)
        .ok_or(LedgerError::NumericOverflow)?;
    put_runtime_value(
        conn,
        &key,
        &serde_json::json!({"version":2,"last_root":root,"sequence":sequence}),
    )?;
    put_runtime_value(
        conn,
        &format!("acquisition-root-turn/{source}/{root}"),
        &serde_json::json!({"version":1,"sequence":sequence}),
    )
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PhaseStatus {
    pub state: String,
    pub last_attempt_ms: Option<i64>,
    pub last_success_ms: Option<i64>,
    pub error: Option<String>,
    pub attempt_id: String,
}

impl PhaseStatus {
    fn begin(&mut self) {
        self.state = "running".into();
        self.last_attempt_ms = Some(now_ms());
        self.attempt_id = super::types::new_epoch();
    }
    fn succeeded(&mut self) {
        self.state = "ready".into();
        self.last_success_ms = Some(now_ms());
        self.error = None;
    }
    fn failed(&mut self, code: &str) {
        self.state = "error".into();
        self.error = Some(code.into());
    }
    fn no_work(&mut self, previous: &Self) {
        if previous.error.is_some() || previous.state == "recovery_unverified" {
            *self = previous.clone();
        } else {
            self.state = if previous.last_success_ms.is_some() {
                "ready"
            } else {
                "idle"
            }
            .into();
            self.error = None;
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SourceRuntimeStatus {
    pub source_read: PhaseStatus,
    pub save: PhaseStatus,
    pub publish: PhaseStatus,
    pub session_id: String,
    pub evidence_gap: bool,
}

fn source_runtime(conn: &Connection, source: &str) -> Result<SourceRuntimeStatus> {
    runtime_value(conn, &format!("collection/{source}"))?
        .map(|value| serde_json::from_value(value).map_err(|_| LedgerError::CorruptLedger))
        .transpose()
        .map(|state| state.unwrap_or_default())
}

fn save_source_runtime(conn: &Connection, source: &str, state: &SourceRuntimeStatus) -> Result<()> {
    put_runtime_value(
        conn,
        &format!("collection/{source}"),
        &serde_json::to_value(state).map_err(|_| LedgerError::CorruptLedger)?,
    )
}

/// Read-only status can be used by HTTP and CLI without replaying the collector.
/// Recovery is conservative: an interrupted attempt is not evidence of success.
pub fn runtime_status(conn: &Connection) -> Result<serde_json::Value> {
    let mut sources = BTreeMap::<String, serde_json::Value>::new();
    let mut reconciliation = BTreeMap::<String, serde_json::Value>::new();
    let mut continuation = BTreeMap::<String, serde_json::Value>::new();
    let mut stmt = conn.prepare("SELECT state_key,value_json FROM ledger_runtime_state WHERE state_key LIKE 'collection/%' OR state_key LIKE 'reconciliation/%' OR state_key LIKE 'legacy-continuation-status/%' ORDER BY state_key")?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let key: String = row.get(0)?;
        let value: String = row.get(1)?;
        let value: serde_json::Value =
            serde_json::from_str(&value).map_err(|_| LedgerError::CorruptLedger)?;
        if let Some(source) = key.strip_prefix("collection/") {
            sources.insert(source.into(), value);
        } else if let Some(source) = key.strip_prefix("reconciliation/") {
            reconciliation.insert(source.into(), value);
        } else if let Some(source) = key.strip_prefix("legacy-continuation-status/") {
            continuation.insert(source.into(), value);
        }
    }
    Ok(
        serde_json::json!({"sources":sources,"reconciliation":reconciliation,"continuation":continuation,"session":runtime_value(conn,"collection-session")?}),
    )
}

fn source_progress(conn: &Connection, source: &str) -> Result<(i64, i64, i64, i64)> {
    Ok(conn.query_row("SELECT (SELECT COUNT(*) FROM source_roots WHERE report_source=?1),(SELECT COALESCE(MAX(last_enumerated_ms),0) FROM source_roots WHERE report_source=?1),(SELECT COALESCE(MAX(observation_id),0) FROM usage_observations WHERE report_source=?1),(SELECT COALESCE(SUM(object_revision),0) FROM source_objects WHERE root_id IN(SELECT root_id FROM source_roots WHERE report_source=?1))",[source], |row|Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?)))?)
}

fn positive(summary: &serde_json::Value, fields: &[&str]) -> bool {
    fields
        .iter()
        .any(|field| summary[*field].as_u64().unwrap_or(0) > 0)
}

#[derive(Default)]
pub struct Collector {
    native: native_adapters::Collector,
    cc: ccswitch::Collector,
    additional: super::additional_adapters::Collector,
    reconciled: super::reconciled_adapters::Collector,
    local: super::local_adapters::Collector,
    documents: super::document_adapters::Collector,
    dsh: super::dsh_evidence::Collector,
    session_id: Option<String>,
}

#[derive(Debug, Default, Serialize)]
pub struct PollSummary {
    pub sources: BTreeMap<String, serde_json::Value>,
    pub errors: BTreeMap<String, String>,
    pub published_units: u64,
    pub activated: Vec<String>,
    pub backlog: bool,
    pub stage_status: BTreeMap<String, SourceRuntimeStatus>,
}

/// Discovery is repeated on later scans. Saved roots are also retried so removal
/// is a visible acquisition gap instead of making an old source disappear.
pub fn roots_for(ledger: &Ledger, source: &str) -> Result<Vec<PathBuf>> {
    let mut roots = crate::registered_sources()
        .into_iter()
        .find(|s| s.id() == source)
        .map(|s| s.discover_roots())
        .unwrap_or_default();
    let saved = ledger
        .conn
        .prepare("SELECT locator FROM source_roots WHERE report_source=?1 AND collector_key<>'reported-history-v1'")?
        .query_map([source], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    roots.extend(saved.into_iter().map(PathBuf::from));
    roots.sort();
    roots.dedup();
    Ok(roots)
}

impl Collector {
    fn start(&mut self, ledger: &mut Ledger) -> Result<()> {
        if self.session_id.is_some() {
            return Ok(());
        }
        ledger.require_writer()?;
        let tx = ledger
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let prior: Vec<(String,String)> = tx.prepare("SELECT state_key,value_json FROM ledger_runtime_state WHERE state_key LIKE 'collection/%'")?
            .query_map([], |row|Ok((row.get(0)?,row.get(1)?)))?.collect::<rusqlite::Result<_>>()?;
        for (key, value) in prior {
            let mut state: SourceRuntimeStatus =
                serde_json::from_str(&value).map_err(|_| LedgerError::CorruptLedger)?;
            for phase in [&mut state.source_read, &mut state.save, &mut state.publish] {
                if phase.state == "running" {
                    phase.state = "recovery_unverified".into();
                    // The interrupted attempt might have committed its batch;
                    // preserve the last proven watermark and retry idempotently.
                    phase.error = Some("RECOVERY_UNVERIFIED".into());
                }
            }
            put_runtime_value(
                &tx,
                &key,
                &serde_json::to_value(state).map_err(|_| LedgerError::CorruptLedger)?,
            )?;
        }
        tx.execute("UPDATE ledger_runtime_state SET value_json=json_set(value_json,'$.state','recovery_unverified','$.error','RECOVERY_UNVERIFIED') WHERE state_key LIKE 'legacy-continuation-status/%' AND json_extract(value_json,'$.state')='running'",[])?;
        let id = super::types::new_epoch();
        put_runtime_value(
            &tx,
            "collection-session",
            &serde_json::json!({"version":1,"id":id,"clean":false,"started_ms":now_ms()}),
        )?;
        tx.commit()?;
        self.session_id = Some(id);
        Ok(())
    }

    /// Record an orderly collector shutdown. Unfinished work is not declared
    /// clean, and persisted source errors are never cleared by this operation.
    pub fn finish(&mut self, ledger: &mut Ledger) -> Result<()> {
        let Some(id) = self.session_id.as_deref() else {
            return Ok(());
        };
        ledger.require_writer()?;
        let running: bool=ledger.conn.query_row("SELECT EXISTS(SELECT 1 FROM ledger_runtime_state WHERE state_key LIKE 'collection/%' AND (json_extract(value_json,'$.source_read.state')='running' OR json_extract(value_json,'$.save.state')='running' OR json_extract(value_json,'$.publish.state')='running'))",[],|row|row.get(0))?;
        put_runtime_value(
            ledger.connection(),
            "collection-session",
            &serde_json::json!({"version":1,"id":id,"clean":!running,"finished_ms":now_ms()}),
        )?;
        self.session_id = None;
        Ok(())
    }
    pub fn invalidate_discovery(&mut self) {
        self.native.invalidate_discovery();
        self.additional.invalidate_discovery();
        self.local.invalidate_discovery();
        self.documents.invalidate_discovery();
        self.dsh.invalidate_discovery();
    }
    /// Start one explicit history audit. Backlog slices share this cycle so a
    /// completed CC value snapshot is not recopied while another source drains.
    pub fn begin_audit_cycle(&mut self) {
        self.cc.begin_audit_cycle();
    }
    /// One bounded slice per source. Source failures do not veto healthy work.
    pub fn poll(&mut self, ledger: &mut Ledger, only: Option<&str>) -> Result<PollSummary> {
        self.poll_inner(ledger, only, None)
    }

    /// Isolated fixture roots prevent regression suites from discovering a
    /// developer's installed tools or reading real tool history.
    #[cfg(feature = "regression-hooks")]
    pub fn poll_isolated(
        &mut self,
        ledger: &mut Ledger,
        source: &str,
        roots: &[PathBuf],
    ) -> Result<PollSummary> {
        self.poll_inner(ledger, Some(source), Some(roots))
    }

    fn poll_inner(
        &mut self,
        ledger: &mut Ledger,
        only: Option<&str>,
        fixture_roots: Option<&[PathBuf]>,
    ) -> Result<PollSummary> {
        if only.is_some_and(|s| !super::DURABLE_EVIDENCE_SOURCES.contains(&s)) {
            return Err(LedgerError::SourceNotProtected);
        }
        self.start(ledger)?;
        let mut result = PollSummary::default();
        let mut destination_failed = false;
        for source in super::DURABLE_EVIDENCE_SOURCES {
            if only.is_some_and(|s| s != *source) {
                continue;
            }
            let previous = source_runtime(ledger.connection(), source)?;
            let before = source_progress(ledger.connection(), source)?;
            let mut state = previous.clone();
            state.session_id = self.session_id.clone().unwrap_or_default();
            state.source_read.begin();
            state.save.begin();
            save_source_runtime(ledger.connection(), source, &state)?;
            let acquired = (|| -> Result<serde_json::Value> {
                if *source == "ccswitch-rollup" {
                    return Ok(serde_json::json!(self.cc.poll_cycle(
                        ledger,
                        fixture_roots.unwrap_or(&[]),
                        2048
                    )?));
                }
                let roots = match fixture_roots {
                    Some(roots) => roots.to_vec(),
                    None => roots_for(ledger, source)?,
                };
                if *source == "dsh" {
                    return Ok(serde_json::json!(self
                        .dsh
                        .poll(ledger, source, &roots, 1024)?));
                }
                if *source == "qoder" {
                    let (databases, streams): (Vec<_>, Vec<_>) = roots.into_iter().partition(|p| {
                        p.extension().is_some_and(|e| {
                            matches!(e.to_str(), Some("db" | "sqlite" | "sqlite3"))
                        })
                    });
                    let local = self.local.poll(ledger, source, &databases, 512);
                    let stream = self.native.poll(ledger, source, &streams, 512);
                    let mut combined = serde_json::json!({"inserted":0,"duplicates":0,"pending":0,"errors":0,"channels":{}});
                    for (channel, value) in [
                        ("ide", local.map(|v| serde_json::json!(v))),
                        ("cli", stream.map(|v| serde_json::json!(v))),
                    ] {
                        match value {
                            Ok(value) => {
                                for field in ["inserted", "duplicates", "pending", "errors"] {
                                    combined[field] = serde_json::json!(
                                        combined[field].as_u64().unwrap_or(0)
                                            + value[field].as_u64().unwrap_or(0)
                                    );
                                }
                                combined["channels"][channel] = value;
                            }
                            Err(error) => {
                                combined["errors"] =
                                    serde_json::json!(combined["errors"].as_u64().unwrap_or(0) + 1);
                                combined["channels"][channel] =
                                    serde_json::json!({"error":error.code()});
                            }
                        }
                    }
                    return Ok(combined);
                }
                if super::additional_adapters::spec(source).is_some() {
                    return Ok(serde_json::json!(self
                        .additional
                        .poll(ledger, source, &roots, 1024)?));
                }
                if super::reconciled_adapters::spec(source).is_some() {
                    return Ok(serde_json::json!(self
                        .reconciled
                        .poll(ledger, source, &roots, 1024)?));
                }
                if super::local_adapters::spec(source).is_some() {
                    return Ok(serde_json::json!(self
                        .local
                        .poll(ledger, source, &roots, 1024)?));
                }
                if super::document_adapters::spec(source).is_some()
                    && !super::document_adapters::STREAM_SOURCES.contains(source)
                {
                    return Ok(serde_json::json!(self
                        .documents
                        .poll(ledger, source, &roots, 1024)?));
                }
                if *source == "claude" {
                    let summary =
                        ledger.poll_claude_budgeted(&roots, Duration::from_millis(150))?;
                    ledger.track_claude_imports()?;
                    Ok(serde_json::json!(summary))
                } else {
                    Ok(serde_json::json!(self
                        .native
                        .poll(ledger, source, &roots, 1024)?))
                }
            })();
            match acquired {
                Ok(summary) => {
                    let persisted_read_error:bool=ledger.conn.query_row("SELECT EXISTS(SELECT 1 FROM source_roots WHERE report_source=?1 AND enabled=1 AND error_code IS NOT NULL) OR EXISTS(SELECT 1 FROM source_objects o JOIN source_roots r USING(root_id) WHERE r.report_source=?1 AND r.enabled=1 AND o.error_code IS NOT NULL AND o.error_code NOT IN('NATIVE_RECORD_GAP','SOURCE_EVIDENCE_GAP'))",[source],|row|row.get(0))?;
                    let read_failed =
                        positive(&summary, &["errors", "failed_databases"]) || persisted_read_error;
                    let activity = positive(
                        &summary,
                        &[
                            "inserted",
                            "saved",
                            "duplicates",
                            "suppressed",
                            "read_lines",
                            "read_rows",
                            "parsed_bytes",
                            "verification_bytes",
                            "completed_files",
                            "completed_passes",
                            "audit_completed",
                        ],
                    ) || source_progress(ledger.connection(), source)? != before;
                    if read_failed {
                        state.source_read.failed("SOURCE_READ_ERROR");
                    } else if activity {
                        state.source_read.succeeded();
                    } else {
                        state.source_read.no_work(&previous.source_read);
                    }
                    if activity {
                        state.save.succeeded();
                    } else {
                        state.save.no_work(&previous.save);
                    }
                    state.evidence_gap = positive(&summary, &["rejected_rows","missing_rows","gaps"])
                        || ledger.conn.query_row("SELECT EXISTS(SELECT 1 FROM durable_import_targets t JOIN durable_imports i USING(import_id) JOIN source_roots r USING(root_id) WHERE r.report_source=?1 AND t.state='gap')",[source],|row|row.get(0))?;
                    if [
                        "errors",
                        "failed_databases",
                        "rejected_rows",
                        "missing_rows",
                        "gaps",
                    ]
                    .iter()
                    .any(|key| summary[*key].as_u64().unwrap_or(0) > 0)
                    {
                        result
                            .errors
                            .insert((*source).into(), "SOURCE_EVIDENCE_GAP".into());
                    }
                    result.sources.insert((*source).into(), summary);
                }
                Err(error) => {
                    result.errors.insert((*source).into(), error.code().into());
                    // Adapters isolate source read failures in their summary.
                    // Escaping failures are failed ledger registration/save
                    // operations; never label a readable source as unreadable.
                    state.source_read.no_work(&previous.source_read);
                    state.save.failed(error.code());
                    destination_failed = matches!(
                        error,
                        LedgerError::DiskFull
                            | LedgerError::PermissionDenied
                            | LedgerError::CorruptLedger
                            | LedgerError::Sql(_)
                            | LedgerError::Io(_)
                    );
                }
            }
            if let Err(error) = save_source_runtime(ledger.connection(), source, &state) {
                state.save.failed(error.code());
                result.errors.insert((*source).into(), error.code().into());
                destination_failed = true;
                // The pre-work running attempt remains durable when a disk
                // failure also prevents recording its exact error.
            }
            result.stage_status.insert((*source).into(), state);
            if destination_failed {
                break;
            }
        }
        if !destination_failed {
            let pending_sources: Vec<String> = ledger.conn.prepare("SELECT DISTINCT o.report_source FROM usage_observations o WHERE o.observation_id>COALESCE((SELECT MAX(u.upper_observation_id) FROM ingest_runs u WHERE u.identity_family=o.identity_family AND u.namespace_key=o.namespace_key AND u.state='done'),0) ORDER BY o.report_source")?
                .query_map([],|row|row.get(0))?.collect::<rusqlite::Result<_>>()?;
            for source in &pending_sources {
                let mut state = source_runtime(ledger.connection(), source)?;
                state.publish.begin();
                save_source_runtime(ledger.connection(), source, &state)?;
            }
            let published = ledger.publish_some(200);
            if let Ok(summary) = &published {
                result.published_units = summary.units;
            }
            for source in &pending_sources {
                let mut state = source_runtime(ledger.connection(), source)?;
                match &published {
                    Ok(_) => {
                        let pending:bool=ledger.conn.query_row("SELECT EXISTS(SELECT 1 FROM usage_observations o WHERE o.report_source=?1 AND o.observation_id>COALESCE((SELECT MAX(u.upper_observation_id) FROM ingest_runs u WHERE u.identity_family=o.identity_family AND u.namespace_key=o.namespace_key AND u.state='done'),0))",[source],|row|row.get(0))?;
                        if pending {
                            state.publish.state = "pending".into();
                        } else {
                            state.publish.succeeded();
                        }
                    }
                    Err(error) => {
                        state.publish.failed(error.code());
                        result.errors.insert(source.clone(), error.code().into());
                    }
                }
                if let Err(error) = save_source_runtime(ledger.connection(), source, &state) {
                    state.publish.failed(error.code());
                    result.errors.insert(source.clone(), error.code().into());
                }
                result.stage_status.insert(source.clone(), state);
            }
            if let Err(error) = published {
                result.errors.insert("ledger".into(), error.code().into());
            }
        }
        // Classification is necessary but does not prove that the old live
        // report is covered. Only explicit snapshot reconciliation may switch.
        result.backlog = ledger.acquisition_backlog(only)?;
        Ok(result)
    }
}

impl Ledger {
    /// Explicit maintenance audit; normal refresh never resets this deadline.
    pub fn request_history_audit(&mut self, only: Option<&str>) -> Result<()> {
        self.require_writer()?;
        if only.is_some_and(|s| !super::DURABLE_EVIDENCE_SOURCES.contains(&s)) {
            return Err(LedgerError::SourceNotProtected);
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let roots:Vec<String> = tx.prepare("SELECT root_id FROM source_roots WHERE enabled=1 AND collector_key<>'reported-history-v1' AND (?1 IS NULL OR report_source=?1)")?
            .query_map([only], |r|r.get(0))?.collect::<rusqlite::Result<_>>()?;
        for root in roots {
            invalidate_history_audit_key(&tx, &root)?;
            tx.execute(
                "UPDATE source_objects SET audit_due_ms=0 WHERE root_id=?1",
                [&root],
            )?;
        }
        db::bump(&tx)?;
        tx.commit()?;
        Ok(())
    }

    pub fn request_native_audit(&mut self, only: Option<&str>) -> Result<()> {
        self.require_writer()?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = tx.execute("UPDATE source_objects SET saved_offset=NULL,checkpoint=NULL,checkpoint_format=NULL,target_done=0,state='replay_required',retry_after_ms=NULL,object_revision=object_revision+1 WHERE root_id IN(SELECT root_id FROM source_roots WHERE enabled=1 AND report_source IN('codex','kimi','kimi-work','qwen') AND (?1 IS NULL OR report_source=?1))", [only])?;
        if changed > 0 {
            db::bump(&tx)?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn acquisition_backlog(&self, only: Option<&str>) -> Result<bool> {
        let acquisition: bool = self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM source_objects o JOIN source_roots r USING(root_id) WHERE r.enabled=1 AND (?1 IS NULL OR r.report_source=?1) AND (r.retry_after_ms IS NULL OR r.retry_after_ms<=?2) AND o.target_done=0 AND o.state NOT IN('error','missing') AND (o.retry_after_ms IS NULL OR o.retry_after_ms<=?2))",
            params![only,now_ms()], |r| r.get(0))?;
        let publication: bool = self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM ingest_runs WHERE state='publishing') OR EXISTS(SELECT 1 FROM usage_observations o WHERE o.observation_id>COALESCE((SELECT MAX(u.upper_observation_id) FROM ingest_runs u WHERE u.identity_family=o.identity_family AND u.namespace_key=o.namespace_key AND u.state='done'),0))",
            [], |r| r.get(0))?;
        Ok(acquisition || publication || discovery_pending(&self.conn, only, true)?)
    }

    /// Compatibility entry point: classification alone never authorizes a
    /// source-wide route switch. Call reconcile_and_activate_sources with a
    /// complete live snapshot, or an explicit adopted-history plan.
    pub fn activate_classified_sources(&mut self, only: Option<&str>) -> Result<Vec<String>> {
        self.require_writer()?;
        if only.is_some_and(|source| !super::DURABLE_EVIDENCE_SOURCES.contains(&source)) {
            return Err(LedgerError::SourceNotProtected);
        }
        Ok(Vec::new())
    }

    /// Compare a complete immutable live scan with the same source's adopted
    /// ledger rows. The comparison and source-wide cutover are one transaction;
    /// there is no reusable approval bit which a later scan can silently reuse.
    pub fn reconcile_and_activate_sources(
        &mut self,
        snapshot: &super::legacy::LegacySnapshot,
        only: Option<&str>,
    ) -> Result<Vec<SourceReconciliation>> {
        self.require_writer()?;
        if only.is_some_and(|source| !super::DURABLE_EVIDENCE_SOURCES.contains(&source)) {
            return Err(LedgerError::SourceNotProtected);
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let timezone: String = tx.query_row(
            "SELECT report_timezone FROM ledger_meta WHERE singleton=1",
            [],
            |row| row.get(0),
        )?;
        let mut results = Vec::new();
        let mut changed = false;
        for source in super::DURABLE_SUPPORTED_SOURCES {
            if only.is_some_and(|selected| selected != *source) {
                continue;
            }
            let mut result=SourceReconciliation {
                source:(*source).into(), state:"shadow".into(),reason:None,
                live_generation:snapshot.generation.clone(),
                saved_observation_id:tx.query_row("SELECT COALESCE(MAX(observation_id),0) FROM usage_observations WHERE report_source=?1",[source],|row|row.get(0))?,
                compared_buckets:0,differing_buckets:0,differences:Vec::new(),checked_at_ms:now_ms(),
            };
            let already: bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM source_roots WHERE report_source=?1 AND route='ledger')",[source],|row|row.get(0))?;
            if already {
                result.state = "ledger".into();
                results.push(result);
                continue;
            }
            result.reason = activation_blocker(&tx, source)?;
            let live_ready = snapshot.sources.get(*source).is_some_and(|status| {
                status.last_scanned_ms.is_some()
                    && !status.stale
                    && status.error.is_none()
                    && status.unaccounted_rows == 0
            }) && !snapshot.has_missing_input_containers(source);
            if !live_ready {
                result.reason = Some("LIVE_SNAPSHOT_UNAVAILABLE".into());
            }
            if result.reason.is_none() {
                let live = reconciliation_live(snapshot, source, &timezone)?;
                let ledger = reconciliation_ledger(&tx, source, &timezone)?;
                let keys: std::collections::BTreeSet<_> =
                    live.keys().chain(ledger.keys()).cloned().collect();
                result.compared_buckets = keys.len();
                for key in keys {
                    let a = live.get(&key).cloned().unwrap_or_default();
                    let b = ledger.get(&key).cloned().unwrap_or_default();
                    if !b.covers(&a) {
                        result.differing_buckets += 1;
                        if result.differences.len() < 12 {
                            result.differences.push(ReconciliationDifference {
                                grain: key.0,
                                day: key.1,
                                model: key.2,
                                live: a,
                                ledger: b,
                            });
                        }
                    }
                }
                if result.differing_buckets > 0 {
                    result.reason = Some("RECONCILIATION_DIFFERENCE".into());
                } else {
                    tx.execute("UPDATE source_roots SET route='ledger' WHERE report_source=?1 AND route='live'",[source])?;
                    result.state = "activated".into();
                    changed = true;
                }
            }
            if result.reason.is_some() {
                result.state = "blocked".into();
            }
            put_runtime_value(
                &tx,
                &format!("reconciliation/{source}"),
                &serde_json::to_value(&result).map_err(|_| LedgerError::CorruptLedger)?,
            )?;
            results.push(result);
        }
        if changed {
            db::bump(&tx)?;
        }
        tx.commit()?;
        Ok(results)
    }
}

/// Incremental enumeration can have another durable page even when the last
/// staged page is fully saved. This is acquisition work, not permission to adopt.
fn discovery_pending(conn: &Connection, source: Option<&str>, backoff: bool) -> Result<bool> {
    Ok(conn.query_row("SELECT EXISTS(SELECT 1 FROM source_roots r WHERE r.enabled=1 AND (?1 IS NULL OR r.report_source=?1) AND (?2=0 OR (r.error_code IS NULL AND (r.retry_after_ms IS NULL OR r.retry_after_ms<=?3))) AND (EXISTS(SELECT 1 FROM ledger_runtime_state s WHERE s.state_key LIKE 'sqlite-incremental/'||r.root_id||'/%' AND json_extract(s.value_json,'$.has_more')=1) OR EXISTS(SELECT 1 FROM ledger_runtime_state s WHERE s.state_key='reconciled-more/'||r.root_id AND json_extract(s.value_json,'$')=1) OR EXISTS(SELECT 1 FROM ledger_runtime_state s WHERE s.state_key='history-audit/'||r.root_id AND json_extract(s.value_json,'$.started_ms') IS NOT NULL AND json_extract(s.value_json,'$.next_due_ms')<=?3 AND EXISTS(SELECT 1 FROM source_objects o WHERE o.root_id=r.root_id AND o.target_done=1 AND o.state='ready' AND o.error_code IS NULL AND CASE WHEN json_valid(o.checkpoint) THEN json_extract(o.checkpoint,'$.audit_epoch') END IS NOT json_extract(s.value_json,'$.started_ms')))))",params![source,backoff,now_ms()],|r|r.get(0))?)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceReconciliation {
    pub source: String,
    pub state: String,
    pub reason: Option<String>,
    pub live_generation: String,
    pub saved_observation_id: i64,
    pub compared_buckets: usize,
    pub differing_buckets: usize,
    pub differences: Vec<ReconciliationDifference>,
    pub checked_at_ms: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReconciliationDifference {
    pub grain: String,
    pub day: String,
    pub model: String,
    pub live: ReconciliationBucket,
    pub ledger: ReconciliationBucket,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ReconciliationBucket {
    pub records: i64,
    pub known_sum: [i64; 10],
    pub known_count: [i64; 10],
    pub exact_count: [i64; 10],
    /// Decimal integers at scale 18, never floating point sums.
    pub cost_units: BTreeMap<String, String>,
    pub known_cost_count: i64,
}

impl ReconciliationBucket {
    fn add(
        &mut self,
        fields: &super::types::Metrics,
        known_fields: u16,
        cost: Option<(&str, i64, i64)>,
    ) -> Result<()> {
        let mut values = super::metrics::evaluate(fields)?;
        for (index, value) in values.iter_mut().enumerate() {
            if known_fields & (1 << index) == 0 && value.known_value.is_some() {
                value.state = super::types::ValueState::Partial;
            }
        }
        let mut aggregate = super::metrics::Aggregate {
            event_count: self.records,
            known_sum: self.known_sum,
            known_count: self.known_count,
            exact_count: self.exact_count,
        };
        aggregate.add(&values, 1)?;
        self.records = aggregate.event_count;
        self.known_sum = aggregate.known_sum;
        self.known_count = aggregate.known_count;
        self.exact_count = aggregate.exact_count;
        if let Some((currency, units, scale)) = cost {
            if !(0..=18).contains(&scale) || units < 0 {
                return Err(LedgerError::InvalidUsageNumber);
            }
            let scaled = (units as i128)
                .checked_mul(10_i128.pow((18 - scale) as u32))
                .ok_or(LedgerError::NumericOverflow)?;
            let previous = self
                .cost_units
                .get(currency)
                .map(|value| {
                    value
                        .parse::<i128>()
                        .map_err(|_| LedgerError::CorruptLedger)
                })
                .transpose()?
                .unwrap_or(0);
            self.cost_units.insert(
                currency.into(),
                previous
                    .checked_add(scaled)
                    .ok_or(LedgerError::NumericOverflow)?
                    .to_string(),
            );
            self.known_cost_count = self
                .known_cost_count
                .checked_add(1)
                .ok_or(LedgerError::NumericOverflow)?;
        }
        Ok(())
    }
    fn covers(&self, live: &Self) -> bool {
        self.records == live.records
            && self.known_sum == live.known_sum
            && self.known_count == live.known_count
            && self
                .exact_count
                .iter()
                .zip(live.exact_count)
                .all(|(new, old)| *new >= old)
            && self.cost_units == live.cost_units
            && self.known_cost_count == live.known_cost_count
    }
}

pub(crate) fn source_activation_readiness(
    conn: &Connection,
    source: &str,
) -> Result<Option<String>> {
    let (roots,unready):(i64,i64)=conn.query_row("SELECT COUNT(*),COALESCE(SUM(enabled=0 OR enumeration_done=0 OR error_code IS NOT NULL),0) FROM source_roots WHERE report_source=?1 AND collector_key<>'reported-history-v1'",[source],|row|Ok((row.get(0)?,row.get(1)?)))?;
    if roots == 0 {
        return Ok(Some("SOURCE_NOT_DISCOVERED".into()));
    }
    if unready > 0 {
        return Ok(Some("SOURCE_NOT_READY".into()));
    }
    let initial_pending:bool=conn.query_row("SELECT EXISTS(SELECT 1 FROM source_roots r WHERE r.report_source=?1 AND r.collector_key<>'reported-history-v1' AND NOT EXISTS(SELECT 1 FROM durable_imports i WHERE i.import_id=(SELECT MAX(j.import_id) FROM durable_imports j WHERE j.root_id=r.root_id) AND i.enumeration_done=1 AND i.completed_ms IS NOT NULL))",[source],|row|row.get(0))?;
    if initial_pending {
        return Ok(Some("INITIAL_IMPORT_PENDING".into()));
    }
    if discovery_pending(conn, Some(source), false)? {
        return Ok(Some("INCREMENTAL_DISCOVERY_PENDING".into()));
    }
    let incomplete:bool=conn.query_row("SELECT EXISTS(SELECT 1 FROM source_objects o JOIN source_roots r USING(root_id) WHERE r.report_source=?1 AND (o.target_done=0 OR o.state NOT IN('ready','missing') OR o.error_code IS NOT NULL)) OR EXISTS(SELECT 1 FROM durable_import_targets t JOIN durable_imports i USING(import_id) JOIN source_roots r USING(root_id) WHERE r.report_source=?1 AND t.state<>'saved')",[source],|row|row.get(0))?;
    if incomplete {
        return Ok(Some("HISTORICAL_EVIDENCE_GAP".into()));
    }
    let unpublished:bool=conn.query_row("SELECT EXISTS(SELECT 1 FROM usage_observations o WHERE o.report_source=?1 AND o.observation_id>COALESCE((SELECT MAX(u.upper_observation_id) FROM ingest_runs u WHERE u.identity_family=o.identity_family AND u.namespace_key=o.namespace_key AND u.state='done'),0))",[source],|row|row.get(0))?;
    if unpublished {
        return Ok(Some("PUBLICATION_PENDING".into()));
    }
    Ok(None)
}

fn activation_blocker(conn: &Connection, source: &str) -> Result<Option<String>> {
    if let Some(reason) = source_activation_readiness(conn, source)? {
        return Ok(Some(reason));
    }
    let unresolved:bool=conn.query_row("SELECT EXISTS(SELECT 1 FROM usage_observations o WHERE o.report_source=?1 AND o.eligibility<>'identity_only' AND (o.identity_key IS NULL OR NOT EXISTS(SELECT 1 FROM usage_events e WHERE e.identity_family=o.identity_family AND e.namespace_key=o.namespace_key AND e.identity_key=COALESCE((SELECT a.canonical_key FROM identity_alias a WHERE a.identity_family=o.identity_family AND a.namespace_key=o.namespace_key AND a.alias_key=o.identity_key),o.identity_key) AND e.state='active' AND e.has_conflict=0)))",[source],|row|row.get(0))?;
    if unresolved {
        return Ok(Some("UNADOPTED_EVIDENCE".into()));
    }
    Ok(None)
}

type ReconciliationMap = BTreeMap<(String, String, String), ReconciliationBucket>;

pub(crate) fn legacy_cost(value: Option<f64>) -> Option<(i64, i64)> {
    let value = value.filter(|value| value.is_finite() && *value >= 0.0)?;
    let text = value.to_string();
    let (mantissa, exponent) = text
        .split_once(['e', 'E'])
        .map(|(m, e)| Some((m, e.parse::<i32>().ok()?)))
        .unwrap_or(Some((text.as_str(), 0)))?;
    let fraction = mantissa.split_once('.').map(|(_, f)| f.len()).unwrap_or(0) as i32;
    let mut scale = fraction - exponent;
    let mut units = mantissa.replace('.', "").parse::<i64>().ok()?;
    if scale < 0 {
        units = units.checked_mul(10_i64.checked_pow((-scale) as u32)?)?;
        scale = 0;
    }
    (scale <= 18).then_some((units, scale as i64))
}

fn reconciliation_live(
    snapshot: &super::legacy::LegacySnapshot,
    source: &str,
    timezone: &str,
) -> Result<ReconciliationMap> {
    use super::legacy::LegacyEntry;
    let mut buckets = ReconciliationMap::new();
    for entry in snapshot
        .entries
        .iter()
        .filter(|entry| entry.report_source() == source)
    {
        let (key, fields, known, cost) = match entry {
            LegacyEntry::Request {
                record,
                known_fields,
                ..
            } => (
                (
                    "request".into(),
                    super::metrics::day_bounds(timezone, record.ts_ms)?.2,
                    super::legacy::model_key(&record.model),
                ),
                super::legacy::legacy_metrics(record)
                    .map_err(|_| LedgerError::InvalidUsageNumber)?,
                *known_fields,
                legacy_cost(record.cost_usd),
            ),
            LegacyEntry::SessionTotal {
                record,
                known_fields,
                projection,
                ..
            } => (
                (
                    "session_total".into(),
                    String::new(),
                    super::legacy::model_key(&record.model),
                ),
                projection.unwrap_or(
                    super::legacy::legacy_metrics(record)
                        .map_err(|_| LedgerError::InvalidUsageNumber)?,
                ),
                *known_fields,
                legacy_cost(record.cost_usd),
            ),
            LegacyEntry::Day(day) => (
                (
                    "day".into(),
                    format!(
                        "{}@{}:{}:{}",
                        day.native_date,
                        day.timezone.as_deref().unwrap_or("unknown"),
                        day.start_ms.map(|v| v.to_string()).unwrap_or_default(),
                        day.end_ms.map(|v| v.to_string()).unwrap_or_default()
                    ),
                    day.model_key.clone(),
                ),
                day.projection,
                1023,
                None,
            ),
        };
        buckets.entry(key).or_default().add(
            &fields,
            known,
            cost.map(|(units, scale)| ("USD", units, scale)),
        )?;
        if buckets.len() > 100_000 {
            return Err(LedgerError::RecordTooLarge);
        }
    }
    Ok(buckets)
}

fn reconciliation_ledger(
    conn: &Connection,
    source: &str,
    timezone: &str,
) -> Result<ReconciliationMap> {
    let columns = super::types::METRIC_NAMES
        .iter()
        .map(|name| format!("o.{name}"))
        .collect::<Vec<_>>()
        .join(",");
    let mut stmt=conn.prepare(&format!("SELECT e.fact_kind,e.model_key,e.occurred_ms,o.native_date,o.native_timezone,o.day_start_ms,o.day_end_ms,{columns},o.cost_state,o.cost_units,o.cost_scale,o.cost_currency FROM usage_events e JOIN usage_observations o ON o.observation_id=e.selected_observation_id WHERE e.report_source=?1 AND e.state='active' ORDER BY e.identity_key"))?;
    let mut rows = stmt.query([source])?;
    let mut buckets = ReconciliationMap::new();
    while let Some(row) = rows.next()? {
        let grain: String = row.get(0)?;
        let model: String = row.get(1)?;
        let day = match grain.as_str() {
            "request" => super::metrics::day_bounds(timezone, row.get(2)?)?.2,
            "day" => format!(
                "{}@{}:{}:{}",
                row.get::<_, Option<String>>(3)?.unwrap_or_default(),
                row.get::<_, Option<String>>(4)?
                    .unwrap_or_else(|| "unknown".into()),
                row.get::<_, Option<i64>>(5)?
                    .map(|v| v.to_string())
                    .unwrap_or_default(),
                row.get::<_, Option<i64>>(6)?
                    .map(|v| v.to_string())
                    .unwrap_or_default()
            ),
            _ => String::new(),
        };
        let mut fields = [None; 10];
        for (index, field) in fields.iter_mut().enumerate() {
            *field = row.get(7 + index)?;
        }
        let currency: Option<String> = row.get(20)?;
        let cost = if row.get::<_, String>(17)? == "known" {
            currency
                .as_deref()
                .zip(row.get::<_, Option<i64>>(18)?)
                .zip(row.get::<_, Option<i64>>(19)?)
                .map(|((currency, units), scale)| (currency, units, scale))
        } else {
            None
        };
        buckets
            .entry((grain, day, model))
            .or_default()
            .add(&fields, 1023, cost)?;
        if buckets.len() > 100_000 {
            return Err(LedgerError::RecordTooLarge);
        }
    }
    Ok(buckets)
}
