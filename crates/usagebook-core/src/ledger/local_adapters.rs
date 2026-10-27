//! Bounded native SQLite history. Physical row keys locate evidence; they do not
//! prove independent requests. Only audited, completed ZCode attempts are admitted.
use super::durable::{AdapterSpec, DurableEvidence, DurableRoot, ObjectSpec};
use super::types::{identity_key, now_ms, Observation};
use super::{Ledger, LedgerError, Result};
use rusqlite::{params, Connection, OpenFlags};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::time::Duration;

pub fn spec(source: &str) -> Option<AdapterSpec> {
    let (family, parser) = match source {
        "zcode" => ("zcode", "zcode-ledger-v1"),
        "minimax" => ("minimax", "minimax-ledger-v1"),
        "hermes" => ("hermes", "hermes-ledger-v1"),
        "qoder" => ("qoder", "qoder-ledger-v1"),
        _ => return None,
    };
    Some(AdapterSpec {
        identity_family: family,
        collector_key: "local-sqlite-history-v1",
        report_source: family,
        native_tool: family,
        parser_version: parser,
        capability_version: "local-sqlite-incremental-v2",
        identity_version: 1,
        semantics_version: 1,
    })
}
fn table(source: &str) -> &'static str {
    match source {
        "zcode" => "model_usage",
        "minimax" => "local_runtime_token_usage",
        "hermes" => "sessions",
        "qoder" => "chat_message",
        _ => unreachable!(),
    }
}
fn fields(source: &str) -> &'static [&'static str] {
    match source {
        "zcode" => &[
            "id",
            "logical_request_id",
            "attempt_index",
            "session_id",
            "model_id",
            "status",
            "started_at",
            "completed_at",
            "finish_reason",
            "input_tokens",
            "output_tokens",
            "reasoning_tokens",
            "cache_read_input_tokens",
            "cache_creation_input_tokens",
            "computed_total_tokens",
        ],
        "minimax" => &[
            "id",
            "session_id",
            "turn_id",
            "model",
            "ts",
            "input_tokens",
            "output_tokens",
            "reasoning_tokens",
            "cache_read_tokens",
            "cache_write_tokens",
        ],
        "hermes" => &[
            "id",
            "model",
            "started_at",
            "ended_at",
            "parent_session_id",
            "input_tokens",
            "output_tokens",
            "reasoning_tokens",
            "cache_read_tokens",
            "cache_write_tokens",
            "api_call_count",
        ],
        "qoder" => &[
            "id",
            "session_id",
            "model",
            "started_at",
            "input_tokens",
            "output_tokens",
            "cache_read_tokens",
            "status",
        ],
        _ => unreachable!(),
    }
}
fn digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
fn encode<T: Serialize>(v: &T) -> Result<Vec<u8>> {
    serde_json::to_vec(v).map_err(|_| LedgerError::RecordTooLarge)
}
fn text(v: &Value, k: &str) -> Option<String> {
    v.get(k)?
        .as_str()
        .filter(|s| !s.trim().is_empty() && s.len() <= 1024 && !s.contains('\0'))
        .map(str::to_owned)
}
fn number(v: &Value, k: &str) -> Result<Option<i64>> {
    match v.get(k) {
        None | Some(Value::Null) => Ok(None),
        Some(x) => x
            .as_i64()
            .filter(|n| *n >= 0)
            .map(Some)
            .ok_or(LedgerError::InvalidUsageNumber),
    }
}
fn time(v: &Value, k: &str, seconds: bool) -> Option<i64> {
    let x = v.get(k)?;
    if let Some(s) = x.as_str() {
        return chrono::DateTime::parse_from_rfc3339(s)
            .ok()
            .map(|x| x.timestamp_millis());
    }
    let n = x.as_f64()?;
    let n = if seconds { n * 1000.0 } else { n };
    (n.is_finite() && n > 0.0 && n <= i64::MAX as f64).then_some(n as i64)
}
fn sum(values: &[Option<i64>]) -> Result<Option<i64>> {
    let mut n = 0i64;
    for v in values {
        let Some(v) = v else { return Ok(None) };
        n = n.checked_add(*v).ok_or(LedgerError::NumericOverflow)?;
    }
    Ok(Some(n))
}
fn sub(a: Option<i64>, b: Option<i64>) -> Option<i64> {
    match (a, b) {
        (Some(a), Some(b)) if a >= b => Some(a - b),
        _ => None,
    }
}
fn has_uuid(s: &str) -> bool {
    s.as_bytes().windows(36).any(|x| {
        std::str::from_utf8(x)
            .ok()
            .is_some_and(|s| uuid::Uuid::parse_str(s).is_ok())
    })
}

/// This accepts only a projected source row, never a UsageRecord or conversation body.
pub fn project_row(source: &str, row: &Value) -> Result<DurableEvidence> {
    let adapter = spec(source).ok_or(LedgerError::SourceNotProtected)?;
    let namespace = format!("unknown:{source}");
    let row_id =
        text(row, "id").or_else(|| row.get("id").and_then(Value::as_i64).map(|n| n.to_string()));
    let session = if source == "hermes" {
        row_id.clone()
    } else {
        text(row, "session_id")
    };
    let model = text(
        row,
        if source == "zcode" {
            "model_id"
        } else {
            "model"
        },
    );
    let occurred = time(
        row,
        if source == "minimax" {
            "ts"
        } else {
            "started_at"
        },
        source == "hermes",
    );
    let mut o = Observation {
        identity_family: source.into(),
        namespace_key: namespace.clone(),
        parser_version: adapter.parser_version.into(),
        report_source: source.into(),
        native_tool: source.into(),
        eligibility: "identity_unknown".into(),
        fact_kind: if source == "hermes" {
            "session_total".into()
        } else {
            "request".into()
        },
        model_key: model
            .as_ref()
            .map(|m| format!("n:{}", m.to_lowercase()))
            .unwrap_or_default(),
        model_display: model,
        occurred_ms: occurred,
        native_session_id: session.clone(),
        session_key: session
            .as_ref()
            .map(|s| identity_key(source, &namespace, "session", &[s])),
        ..Observation::default()
    };
    let input = number(row, "input_tokens")?;
    let output = number(row, "output_tokens")?;
    let reasoning = number(row, "reasoning_tokens")?;
    let cr = number(
        row,
        if source == "zcode" {
            "cache_read_input_tokens"
        } else {
            "cache_read_tokens"
        },
    )?;
    let cw = number(
        row,
        if source == "zcode" {
            "cache_creation_input_tokens"
        } else {
            "cache_write_tokens"
        },
    )?;
    let mut reason = if source == "qoder" {
        "NATIVE_MESSAGE_FINALITY_NOT_PROVEN"
    } else if source == "minimax" {
        "NATIVE_REQUEST_ID_NOT_PERSISTED"
    } else {
        "CUMULATIVE_DOMAIN_NOT_PROVEN"
    };
    if source == "zcode" {
        let total = number(row, "computed_total_tokens")?;
        o.metrics = [
            sub(input, sum(&[cr, cw])?),
            cr,
            cw,
            sub(output, reasoning),
            reasoning,
            input,
            output,
            total,
            None,
            None,
        ];
        let native = text(row, "logical_request_id");
        let attempt = number(row, "attempt_index")?;
        if let (Some(logical), Some(attempt)) =
            (native.as_deref().filter(|id| has_uuid(id)), attempt)
        {
            o.identity_key = Some(identity_key(
                source,
                &namespace,
                "logical-attempt",
                &[logical, &attempt.to_string()],
            ));
            o.native_request_id = Some(logical.into());
        } else if let Some(id) = row_id.as_deref().filter(|id| has_uuid(id)) {
            o.identity_key = Some(identity_key(source, &namespace, "usage-attempt", &[id]));
            o.native_request_id = Some(id.into());
        } else if let (Some(id), Some(session)) =
            (&row_id, session.as_deref().filter(|s| has_uuid(s)))
        {
            o.identity_key = Some(identity_key(
                source,
                &namespace,
                "session-attempt",
                &[session, id],
            ));
            o.native_request_id = Some(id.clone());
        }
        if o.identity_key.is_some() {
            o.metrics[9] = Some(1);
        }
        reason = "NATIVE_IDENTITY_UNKNOWN";
        if o.identity_key.is_some() {
            if text(row, "status").as_deref() == Some("completed")
                && time(row, "completed_at", false).is_some()
                && matches!(
                    text(row, "finish_reason").as_deref(),
                    Some(
                        "stop"
                            | "tool-calls"
                            | "length"
                            | "content-filter"
                            | "end_turn"
                            | "tool_use"
                            | "max_tokens"
                    )
                )
            {
                o.eligibility = if occurred.is_some() {
                    "eligible"
                } else {
                    "time_unknown"
                }
                .into();
                reason = "";
            } else {
                o.eligibility = "pending_finality".into();
                reason = "NATIVE_ATTEMPT_NOT_FINAL";
            }
        }
        if cr
            .zip(cw)
            .zip(input)
            .is_some_and(|((a, b), i)| a.checked_add(b).is_none_or(|c| c > i))
            || reasoning.zip(output).is_some_and(|(r, o)| r > o)
            || total
                .zip(sum(&[input, output])?)
                .is_some_and(|(t, s)| t != s)
        {
            o.eligibility = "inconsistent".into();
            reason = "NATIVE_BUCKETS_INCONSISTENT";
        }
    } else if source == "qoder" {
        o.metrics = [
            sub(input, cr),
            cr,
            None,
            None,
            None,
            input,
            output,
            sum(&[input, output])?,
            None,
            None,
        ];
        if text(row, "status").as_deref() == Some("invalid_token_json") {
            reason = "NATIVE_TOKEN_JSON_INVALID";
        }
    } else {
        // Raw output is kept as the reported total; reasoning is a subset in
        // Hermes. MiniMax's lost provider discriminator prevents exact mapping,
        // so these numbers remain pending evidence regardless of their shape.
        o.metrics = [
            input,
            cr,
            cw,
            if source == "hermes" {
                sub(output, reasoning)
            } else {
                None
            },
            reasoning,
            sum(&[input, cr, cw])?,
            output,
            None,
            None,
            if source == "hermes" {
                number(row, "api_call_count")?
            } else {
                None
            },
        ];
        o.metrics[7] = sum(&[input, cr, cw, output])?;
    }
    let projection = json!({"channel":table(source),"pending_reason":reason,"source_record_id":row_id,"source_session_id":session,"source_usage_scope":number(row,"attempt_index")?.map(|n|format!("attempt:{n}")),"turn_id":text(row,"turn_id"),"native_stop_reason":text(row,"finish_reason"),"status":text(row,"status"),"projection_version":1,"raw_input_tokens":input,"raw_output_tokens":output,"raw_reasoning_tokens":reasoning,"raw_cache_read_tokens":cr,"raw_cache_write_tokens":cw,"raw_total_tokens":if source=="zcode"{number(row,"computed_total_tokens")?}else{None}});
    o.candidate_key = identity_key(
        source,
        &namespace,
        "sqlite-evidence",
        &[&digest(&encode(&(&o, &projection))?)],
    );
    o.record_key = row_id_key(row);
    o.normalized_hash = super::claude::normalized_hash(&o)?;
    Ok(DurableEvidence {
        observation: o,
        projection: Some(projection),
    })
}
fn row_id_key(row: &Value) -> String {
    format!(
        "native-row:{}",
        digest(row.get("id").unwrap_or(&Value::Null).to_string().as_bytes())
    )
}

fn open(path: &Path) -> Result<Connection> {
    let c = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    c.busy_timeout(Duration::from_millis(250))?;
    c.execute_batch("PRAGMA query_only=ON; BEGIN DEFERRED;")?;
    Ok(c)
}
fn columns(c: &Connection, source: &str) -> Result<BTreeSet<String>> {
    let mut q = c.prepare(&format!("PRAGMA table_info({})", table(source)))?;
    let all = q
        .query_map([], |r| Ok((r.get::<_, String>(1)?, r.get::<_, i64>(5)?)))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let primary: Vec<_> = all
        .iter()
        .filter(|(_, p)| *p > 0)
        .map(|(n, _)| n.as_str())
        .collect();
    if primary != ["id"] {
        return Err(LedgerError::UnsupportedVersion);
    }
    let cols: BTreeSet<_> = all
        .into_iter()
        .map(|(n, _)| n)
        .filter(|n| {
            fields(source).contains(&n.as_str())
                || source == "qoder"
                    && [
                        "token_info",
                        "model_info",
                        "gmt_create",
                        "role",
                        "related_session_id",
                    ]
                    .contains(&n.as_str())
        })
        .collect();
    let required = if source == "qoder" {
        &["id", "token_info", "role"][..]
    } else {
        &["id", "input_tokens", "output_tokens"][..]
    };
    if !required.iter().all(|n| cols.contains(*n)) {
        return Err(LedgerError::UnsupportedVersion);
    }
    Ok(cols)
}
fn select_fields(source: &str, cols: &BTreeSet<String>) -> String {
    if source == "qoder" {
        let token = |key: &str| {
            format!("CASE WHEN json_valid(token_info) THEN json_extract(token_info,'$.{key}') END")
        };
        let session = if cols.contains("session_id") {
            "session_id"
        } else if cols.contains("related_session_id") {
            "related_session_id"
        } else {
            "NULL"
        };
        let model = if cols.contains("model_info") {
            "CASE WHEN json_valid(model_info) THEN COALESCE(json_extract(model_info,'$.model_key'),json_extract(model_info,'$.model')) END"
        } else {
            "NULL"
        };
        let time = if cols.contains("gmt_create") {
            "gmt_create"
        } else {
            "NULL"
        };
        return [
            "id".into(),
            session.into(),
            model.into(),
            time.into(),
            token("prompt_tokens"),
            token("completion_tokens"),
            token("cached_tokens"),
            "CASE WHEN json_valid(token_info) THEN NULL ELSE 'invalid_token_json' END".into(),
        ]
        .join(",");
    }
    fields(source)
        .iter()
        .map(|f| {
            if cols.contains(*f) {
                f.to_string()
            } else {
                format!("NULL AS {f}")
            }
        })
        .collect::<Vec<_>>()
        .join(",")
}
fn json_row(row: &rusqlite::Row<'_>, source: &str) -> Result<Value> {
    let mut object = serde_json::Map::new();
    for (i, name) in fields(source).iter().enumerate() {
        use rusqlite::types::ValueRef;
        let value = match row.get_ref(i)? {
            ValueRef::Null => Value::Null,
            ValueRef::Integer(n) => json!(n),
            ValueRef::Real(n) => serde_json::Number::from_f64(n)
                .map(Value::Number)
                .ok_or(LedgerError::InvalidUsageNumber)?,
            ValueRef::Text(s) if s.len() <= 1024 => Value::String(
                std::str::from_utf8(s)
                    .map_err(|_| LedgerError::InvalidUsageNumber)?
                    .into(),
            ),
            _ => return Err(LedgerError::RecordTooLarge),
        };
        object.insert((*name).into(), value);
    }
    Ok(Value::Object(object))
}
#[derive(Serialize, Deserialize)]
struct Manifest {
    format: u32,
    source: String,
    keys: Vec<String>,
    columns: BTreeSet<String>,
    gap: bool,
}
#[derive(Default, Serialize)]
pub struct PollSummary {
    pub discovered_databases: usize,
    pub read_rows: u64,
    pub inserted: u64,
    pub duplicates: u64,
    pub suppressed: u64,
    pub pending: u64,
    pub missing_rows: u64,
    pub rejected_rows: u64,
    pub backlog: usize,
    pub errors: u64,
    pub diagnostics: Vec<String>,
}
#[derive(Default)]
pub struct Collector {
    next_pass: HashMap<(String, PathBuf), i64>,
    next_recent: HashMap<(String, PathBuf), i64>,
    next_root: usize,
}
impl Collector {
    pub fn invalidate_discovery(&mut self) {
        self.next_pass.clear();
        self.next_recent.clear();
    }
    pub fn poll(
        &mut self,
        ledger: &mut Ledger,
        source: &str,
        roots: &[PathBuf],
        budget: usize,
    ) -> Result<PollSummary> {
        if budget == 0 || budget > 65536 {
            return Err(LedgerError::InvalidInput(
                "local batch budget must be 1..65536".into(),
            ));
        }
        let adapter = spec(source).ok_or(LedgerError::SourceNotProtected)?;
        let mut summary = PollSummary::default();
        let mut paths = discover_databases(source, roots)?;
        summary.discovered_databases = paths.len();
        let known:Vec<PathBuf>=ledger.connection().prepare("SELECT locator FROM source_roots WHERE identity_family=?1 AND collector_key='local-sqlite-history-v1' AND enabled=1")?.query_map([source],|r|r.get::<_,String>(0).map(PathBuf::from))?.collect::<rusqlite::Result<_>>()?;
        for path in known {
            if !paths.contains(&path) {
                paths.push(path)
            }
        }
        if paths.is_empty() {
            return Ok(summary);
        }
        let mut left = budget;
        let first = self.next_root % paths.len();
        for step in 0..paths.len() {
            if left == 0 {
                break;
            }
            let path = &paths[(first + step) % paths.len()];
            let key = (source.into(), path.clone());
            let root =
                ledger.register_durable_root(&adapter, &format!("unknown:{source}"), path)?;
            if !root.enabled {
                continue;
            }
            super::source_projection::ensure(ledger.connection(), &root)?;
            if !path.is_file() {
                retire_unrecoverable_db_checkpoints(ledger.connection(), &root)?;
            }
            let retry: Option<i64> = ledger.connection().query_row(
                "SELECT retry_after_ms FROM source_roots WHERE root_id=?1",
                [&root.root_id],
                |r| r.get(0),
            )?;
            if retry.is_some_and(|t| t > now_ms()) {
                continue;
            }
            let slice = (left / (paths.len() - step)).max(1);
            let before = summary.read_rows;
            // Keep a live lane while importing the immutable initial key manifest.
            let recent = if slice < 4
                && !super::collection::history_audit_due(ledger.connection(), &root)?
            {
                slice
            } else {
                (slice / 4).min(128)
            };
            if recent > 0
                && path.is_file()
                && self.next_recent.get(&key).is_none_or(|t| *t <= now_ms())
            {
                match save_recent(ledger, &root, path, source, recent, &mut summary) {
                    Ok(more) => {
                        self.next_recent
                            .insert(key.clone(), now_ms() + if more { 0 } else { 5_000 });
                    }
                    Err(e) => record_error(ledger, &root, &e, &mut summary)?,
                }
            }
            let pending = pending_count(ledger, &root)?;
            let mut audit_active = pending > 0;
            if pending == 0
                && super::collection::history_audit_due(ledger.connection(), &root)?
                && self.next_pass.get(&key).is_none_or(|t| *t <= now_ms())
                && path.is_file()
            {
                match stage_manifest(ledger, &root, path, source) {
                    Ok(()) => {
                        audit_active = true;
                        self.next_pass.insert(key.clone(), now_ms() + 30_000);
                    }
                    Err(e) => {
                        record_error(ledger, &root, &e, &mut summary)?;
                        self.next_pass.insert(key.clone(), now_ms() + 60_000);
                        continue;
                    }
                }
            }
            let history = slice.saturating_sub((summary.read_rows - before) as usize);
            if let Err(e) = save_history(ledger, &root, path, source, history, &mut summary) {
                record_error(ledger, &root, &e, &mut summary)?
            }
            left = left.saturating_sub((summary.read_rows - before) as usize);
            let pending = pending_count(ledger, &root)?;
            summary.backlog += pending as usize;
            if audit_active
                && pending == 0
                && super::collection::history_audit_due(ledger.connection(), &root)?
                && current_audit_manifest_complete(ledger.connection(), &root)?
                && summary.errors == 0
                && summary.rejected_rows == 0
                && summary.missing_rows == 0
            {
                super::source_projection::complete(ledger.connection(), &root)?;
                super::collection::complete_history_audit(ledger.connection(), &root)?;
            }
        }
        self.next_root = (first + 1) % paths.len();
        Ok(summary)
    }
}
fn record_error(
    ledger: &mut Ledger,
    root: &DurableRoot,
    e: &LedgerError,
    s: &mut PollSummary,
) -> Result<()> {
    s.errors += 1;
    s.diagnostics.push(e.code().into());
    ledger.connection().execute("UPDATE source_roots SET error_code=?2,failure_count=failure_count+1,retry_after_ms=?3 WHERE root_id=?1",params![root.root_id,e.code(),now_ms()+60_000])?;
    ledger.connection().execute(
        "UPDATE source_objects SET retry_after_ms=?2 WHERE root_id=?1 AND target_done=0",
        params![root.root_id, now_ms() + 60_000],
    )?;
    Ok(())
}
fn pending_count(ledger: &Ledger, root: &DurableRoot) -> Result<i64> {
    Ok(ledger.connection().query_row("SELECT COUNT(*) FROM source_objects WHERE root_id=?1 AND object_key LIKE 'local-manifest-v1:%' AND target_done=0 AND checkpoint IS NOT NULL AND length(checkpoint)>0",params![root.root_id],|r|r.get(0))?)
}
pub fn discover_databases(source: &str, roots: &[PathBuf]) -> Result<Vec<PathBuf>> {
    if spec(source).is_none() {
        return Err(LedgerError::SourceNotProtected);
    }
    let mut out = BTreeSet::new();
    for root in roots {
        let p = if root.is_file() {
            root.clone()
        } else {
            root.join(match source {
                "zcode" => "cli/db/db.sqlite",
                "minimax" => "v2/sqlite/runtime-state.sqlite",
                "hermes" => "state.db",
                "qoder" => "local.db",
                _ => unreachable!(),
            })
        };
        if p.is_file() {
            out.insert(p.canonicalize()?);
        }
        if source == "hermes" && root.join("profiles").is_dir() {
            for entry in std::fs::read_dir(root.join("profiles"))? {
                let p = entry?.path().join("state.db");
                if p.is_file() {
                    out.insert(p.canonicalize()?);
                }
            }
        }
    }
    Ok(out.into_iter().collect())
}
fn stage_manifest(
    ledger: &mut Ledger,
    root: &DurableRoot,
    path: &Path,
    source: &str,
) -> Result<()> {
    let conn = open(path)?;
    let cols = columns(&conn, source)?;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    conn.progress_handler(1000, Some(move || std::time::Instant::now() >= deadline))?;
    ledger.connection().execute(
        "DELETE FROM ledger_runtime_state WHERE state_key=?1",
        [format!("local-audit-manifest/{}", root.root_id)],
    )?;
    super::source_projection::begin(ledger.connection(), root)?;
    super::source_projection::capture_rows(
        ledger.connection(),
        root,
        &conn,
        table(source),
        &[],
        &[],
    )?;
    let mut q = conn.prepare(&format!(
        "SELECT CAST(id AS TEXT) FROM {} {} ORDER BY CAST(id AS TEXT) COLLATE BINARY",
        table(source),
        predicate(source)
    ))?;
    let mut rows = q.query([])?;
    let mut manifests = Vec::new();
    let mut keys = Vec::new();
    let mut size = 0;
    let mut manifest_bytes = 0usize;
    while let Some(row) = rows.next()? {
        let key: String = row.get(0)?;
        if key.is_empty() || key.len() > 1024 {
            return Err(LedgerError::RecordTooLarge);
        }
        manifest_bytes += key.len() + std::mem::size_of::<String>();
        if manifest_bytes > 64 * 1024 * 1024 || std::time::Instant::now() >= deadline {
            return Err(LedgerError::RecordTooLarge);
        }
        if !keys.is_empty() && (keys.len() >= 256 || size + key.len() > 24 * 1024) {
            manifests.push(std::mem::take(&mut keys));
            size = 0
        }
        size += key.len();
        keys.push(key);
    }
    if !keys.is_empty() {
        manifests.push(keys)
    }
    drop(rows);
    drop(q);
    drop(conn);
    let pass = uuid::Uuid::new_v4().to_string();
    let manifest_count = manifests.len();
    for keys in manifests {
        let key = format!("local-manifest-v1:{}", digest(&encode(&keys)?));
        let manifest = Manifest {
            format: 1,
            source: source.into(),
            keys,
            columns: cols.clone(),
            gap: false,
        };
        let checkpoint = encode(&manifest)?;
        ledger.stage_durable_manifest(
            root,
            ObjectSpec {
                object_key: key.clone(),
                locator: format!("{}#{key}", path.display()),
                content_generation: pass.clone(),
                byte_length: manifest.keys.len() as i64,
                mtime_ns: None,
            },
            &checkpoint,
        )?;
    }
    retire_unrecoverable_db_checkpoints(ledger.connection(), root)?;
    // This receipt describes only the fully enumerated current audit. Old
    // manifests can retain genuine historical gaps after a capability upgrade;
    // those must not keep resetting a successfully rebuilt present-day view.
    // Write the receipt last so a crash while staging cannot seal a partial set.
    let epoch: String = ledger.connection().query_row(
        "SELECT epoch FROM ledger_meta WHERE singleton=1",
        [],
        |row| row.get(0),
    )?;
    super::collection::put_runtime_value(
        ledger.connection(),
        &format!("local-audit-manifest/{}", root.root_id),
        &json!({"version":1,"epoch":epoch,
            "capability_version":root.spec.capability_version,
            "parser_version":root.spec.parser_version,
            "pass":pass,"manifest_count":manifest_count}),
    )?;
    ledger.finish_durable_enumeration(root)?;
    Ok(())
}

fn current_audit_manifest_complete(conn: &Connection, root: &DurableRoot) -> Result<bool> {
    let Some(receipt) =
        super::collection::runtime_value(conn, &format!("local-audit-manifest/{}", root.root_id))?
    else {
        return Ok(false);
    };
    let epoch: String = conn.query_row(
        "SELECT epoch FROM ledger_meta WHERE singleton=1",
        [],
        |row| row.get(0),
    )?;
    if receipt["version"] != 1
        || receipt["epoch"] != epoch
        || receipt["capability_version"] != root.spec.capability_version
        || receipt["parser_version"] != root.spec.parser_version
    {
        return Ok(false);
    }
    let (Some(pass), Some(expected)) =
        (receipt["pass"].as_str(), receipt["manifest_count"].as_i64())
    else {
        return Ok(false);
    };
    let (count, incomplete): (i64, i64) = conn.query_row(
        "SELECT COUNT(*),COALESCE(SUM(target_done=0 OR state<>'ready' OR error_code IS NOT NULL),0) FROM source_objects WHERE root_id=?1 AND object_key LIKE 'local-manifest-v1:%' AND content_generation=?2",
        params![root.root_id, pass],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    Ok(expected >= 0 && count == expected && incomplete == 0)
}
fn read_keys(
    ledger: &Ledger,
    root: &DurableRoot,
    path: &Path,
    source: &str,
    manifest: &Manifest,
    start: usize,
    end: usize,
) -> Result<(Vec<DurableEvidence>, u64, u64)> {
    let wanted = &manifest.keys[start..end];
    if wanted.is_empty() {
        return Ok((vec![], 0, 0));
    }
    if !path.is_file() {
        return Ok((vec![], wanted.len() as u64, 0));
    }
    let conn = open(path)?;
    let cols = columns(&conn, source)?;
    if cols != manifest.columns {
        return Err(LedgerError::UnsupportedVersion);
    }
    let sql = format!(
        "SELECT {} FROM {} WHERE id IN ({}) ORDER BY id",
        select_fields(source, &cols),
        table(source),
        vec!["?"; wanted.len()].join(",")
    );
    let mut q = conn.prepare(&sql)?;
    let mut rows = q.query(rusqlite::params_from_iter(wanted.iter()))?;
    let mut found = 0;
    let mut rejected = 0;
    let mut evidence = Vec::new();
    while let Some(row) = rows.next()? {
        found += 1;
        match json_row(row, source).and_then(|v| project_row(source, &v)) {
            Ok(e) => {
                let key = e
                    .projection
                    .as_ref()
                    .and_then(|p| p.get("source_record_id"))
                    .and_then(Value::as_str);
                if !key
                    .map(|key| {
                        super::source_projection::row_suppressed(
                            ledger.connection(),
                            &root.root_id,
                            table(source),
                            key,
                        )
                    })
                    .transpose()?
                    .unwrap_or(false)
                {
                    evidence.push(e);
                }
            }
            Err(
                LedgerError::InvalidUsageNumber
                | LedgerError::RecordTooLarge
                | LedgerError::NumericOverflow,
            ) => rejected += 1,
            Err(e) => return Err(e),
        }
    }
    super::source_projection::capture_rows(
        ledger.connection(),
        root,
        &conn,
        table(source),
        wanted,
        &evidence,
    )?;
    if source == "minimax" {
        let sessions = evidence
            .iter()
            .filter_map(|e| {
                e.projection
                    .as_ref()?
                    .get("source_session_id")?
                    .as_str()
                    .map(str::to_owned)
            })
            .collect::<Vec<_>>();
        super::source_projection::capture_rows(
            ledger.connection(),
            root,
            &conn,
            "local_runtime_sessions",
            &sessions,
            &evidence,
        )?;
    }
    Ok((
        evidence,
        wanted.len().saturating_sub(found) as u64,
        rejected,
    ))
}
fn save_history(
    ledger: &mut Ledger,
    root: &DurableRoot,
    path: &Path,
    source: &str,
    budget: usize,
    s: &mut PollSummary,
) -> Result<()> {
    let mut left = budget;
    for mut object in ledger.next_durable_objects(root, 1024)? {
        if left == 0 {
            break;
        }
        if !object.checkpoint.as_deref().is_some_and(|x| !x.is_empty()) {
            continue;
        }
        let mut m: Manifest = serde_json::from_slice(object.checkpoint.as_deref().unwrap())
            .map_err(|_| LedgerError::CorruptLedger)?;
        if m.format != 1 || m.source != source {
            return Err(LedgerError::UnsupportedVersion);
        }
        let start = usize::try_from(object.saved_offset).map_err(|_| LedgerError::CorruptLedger)?;
        if start > m.keys.len() {
            return Err(LedgerError::CorruptLedger);
        }
        let end = (start + left).min(m.keys.len());
        let (evidence, missing, rejected) = read_keys(ledger, root, path, source, &m, start, end)?;
        m.gap |= missing > 0 || rejected > 0;
        let saved = ledger.save_durable_evidence_batch_with_gap(
            &mut object,
            &evidence,
            end as i64,
            &encode(&m)?,
            end == m.keys.len(),
            m.gap.then_some("LOCAL_SQLITE_EVIDENCE_GAP"),
        )?;
        s.inserted += saved.inserted;
        s.duplicates += saved.duplicates;
        s.suppressed += saved.suppressed;
        s.pending += saved.pending;
        s.missing_rows += missing;
        s.rejected_rows += rejected;
        s.read_rows += (end - start) as u64;
        left -= end - start;
    }
    Ok(())
}
fn save_recent(
    ledger: &mut Ledger,
    root: &DurableRoot,
    path: &Path,
    source: &str,
    budget: usize,
    s: &mut PollSummary,
) -> Result<bool> {
    let conn = open(path)?;
    let Some(batch) = sqlite_incremental_batch(
        ledger,
        root,
        path,
        &conn,
        table(source),
        "id",
        predicate(source).strip_prefix("WHERE ").unwrap_or(""),
        budget,
    )?
    else {
        return Ok(false);
    };
    let more = batch.has_more;
    if !batch.keys.is_empty() {
        let manifest = Manifest {
            format: 1,
            source: source.into(),
            columns: columns(&conn, source)?,
            keys: batch.keys.clone(),
            gap: false,
        };
        let (evidence, missing, rejected) = read_keys(
            ledger,
            root,
            path,
            source,
            &manifest,
            0,
            manifest.keys.len(),
        )?;
        let generation = digest(&encode(
            &evidence
                .iter()
                .map(|e| &e.observation.normalized_hash)
                .collect::<Vec<_>>(),
        )?);
        let mut object = ledger.discover_durable_object(
            root,
            ObjectSpec {
                object_key: "local-recent-v1".into(),
                locator: format!("{}#recent", path.display()),
                content_generation: generation,
                byte_length: manifest.keys.len() as i64,
                mtime_ns: None,
            },
        )?;
        let saved = ledger.save_durable_evidence_batch_with_gap(
            &mut object,
            &evidence,
            manifest.keys.len() as i64,
            b"",
            true,
            (missing + rejected > 0).then_some("LOCAL_SQLITE_EVIDENCE_GAP"),
        )?;
        if missing + rejected > 0 {
            super::collection::invalidate_history_audit(ledger.connection(), root)?;
        }
        s.read_rows += manifest.keys.len() as u64;
        s.missing_rows += missing;
        s.rejected_rows += rejected;
        s.inserted += saved.inserted;
        s.duplicates += saved.duplicates;
        s.suppressed += saved.suppressed;
        s.pending += saved.pending;
    }
    batch.commit(ledger)?;
    s.backlog += usize::from(more);
    Ok(more)
}

fn predicate(source: &str) -> &'static str {
    if source == "qoder" {
        "WHERE role='assistant' AND token_info IS NOT NULL"
    } else {
        ""
    }
}

/// A physical read cursor, never an accounting identity or consumption timestamp.
/// Append discovery is fast; reuse, in-place edits and WITHOUT ROWID tables remain
/// covered by the separately scheduled complete history audit.
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct SqliteIncrementalState {
    version: u32,
    generation: String,
    file_hint: String,
    highwater: i64,
    #[serde(default)]
    has_more: bool,
}
pub(crate) struct SqliteIncrementalBatch {
    pub keys: Vec<String>,
    pub has_more: bool,
    key: String,
    state: SqliteIncrementalState,
}
impl SqliteIncrementalBatch {
    /// Always call after saving the projected rows. A crash between these two
    /// commits repeats rows, rather than advancing past unsaved evidence.
    pub fn commit(self, ledger: &Ledger) -> Result<()> {
        super::collection::put_runtime_value(
            ledger.connection(),
            &self.key,
            &serde_json::to_value(self.state).map_err(|_| LedgerError::CorruptLedger)?,
        )
    }
}
fn sqlite_file_hint(path: &Path) -> Result<(String, String)> {
    let metadata = std::fs::metadata(path)?;
    #[cfg(unix)]
    let identity = {
        use std::os::unix::fs::MetadataExt;
        format!("{}:{}", metadata.dev(), metadata.ino())
    };
    #[cfg(not(unix))]
    let identity = format!("{:?}", metadata.created().ok());
    let mut hint = format!("{}:{:?}", metadata.len(), metadata.modified().ok());
    let wal = PathBuf::from(format!("{}-wal", path.to_string_lossy()));
    if let Ok(wal) = std::fs::metadata(wal) {
        hint.push_str(&format!(":{}:{:?}", wal.len(), wal.modified().ok()));
    }
    Ok((identity, hint))
}
// Keep the physical source and schema-selected SQL literals explicit at call sites.
#[allow(clippy::too_many_arguments)]
pub(crate) fn sqlite_incremental_batch(
    ledger: &Ledger,
    root: &DurableRoot,
    path: &Path,
    _conn: &Connection,
    table: &str,
    key_column: &str,
    predicate: &str,
    budget: usize,
) -> Result<Option<SqliteIncrementalBatch>> {
    // SQL identifiers and predicates are selected by the adapter, not source data.
    if !matches!(
        table,
        "model_usage"
            | "local_runtime_token_usage"
            | "sessions"
            | "chat_message"
            | "message"
            | "session_message"
            | "session"
            | "proxy_request_logs"
            | "usage_ledger"
            | "assistant_usage_events"
            | "transcript_events"
    ) || !matches!(key_column, "id" | "request_id" | "_rowid_")
        || budget == 0
        || budget > 65536
    {
        return Err(LedgerError::UnsupportedVersion);
    }
    let (identity, file_hint) = sqlite_file_hint(path)?;
    let conn = open(path)?;
    let schema: i64 = conn.query_row("PRAGMA schema_version", [], |r| r.get(0))?;
    let generation = digest(
        format!(
            "rowid-v1:{identity}:{schema}:{}:{}:{}:{}",
            root.spec.capability_version,
            root.spec.parser_version,
            root.spec.identity_version,
            root.spec.semantics_version
        )
        .as_bytes(),
    );
    let old_key = format!("sqlite-incremental/{}/{table}", root.root_id);
    if super::collection::runtime_value(ledger.connection(), &old_key)?.is_some() {
        super::collection::invalidate_history_audit(ledger.connection(), root)?;
        ledger.connection().execute(
            "DELETE FROM ledger_runtime_state WHERE state_key=?1",
            [old_key],
        )?;
    }
    let key = format!(
        "sqlite-incremental/{}/{table}/{}",
        root.root_id,
        digest(path.as_os_str().as_encoded_bytes())
    );
    let stored = super::collection::runtime_value(ledger.connection(), &key)?;
    let had_cursor = stored.is_some();
    let prior = stored
        .and_then(|v| serde_json::from_value::<SqliteIncrementalState>(v).ok())
        .filter(|s| s.version == 1 && s.generation == generation);
    if prior.as_ref().is_some_and(|s| s.file_hint == file_hint) {
        return Ok(None);
    }
    if had_cursor && prior.is_none() {
        super::collection::invalidate_history_audit(ledger.connection(), root)?;
    }
    let shadowed: bool = conn
        .prepare(&format!("PRAGMA table_info({table})"))?
        .query_map([], |r| r.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .iter()
        .any(|n| n.eq_ignore_ascii_case("_rowid_"));
    let rowid_available = !shadowed
        && conn
            .prepare(&format!("SELECT _rowid_ FROM {table} LIMIT 0"))
            .is_ok();
    let upper = if rowid_available {
        conn.query_row(
            &format!("SELECT COALESCE(MAX(_rowid_),0) FROM {table}"),
            [],
            |r| r.get::<_, i64>(0),
        )?
        .max(0)
    } else {
        0
    };
    let previous = prior.as_ref().map_or(upper, |s| s.highwater);
    if upper < previous {
        super::collection::invalidate_history_audit(ledger.connection(), root)?;
    }
    let append = rowid_available && prior.is_some() && upper >= previous && upper > previous;
    let condition = if predicate.is_empty() { "1" } else { predicate };
    let sql = if append {
        format!("SELECT CAST({key_column} AS TEXT),_rowid_,({condition}) FROM {table} WHERE _rowid_>?1 AND _rowid_<=?2 ORDER BY _rowid_ LIMIT ?3")
    } else if rowid_available {
        format!("SELECT CAST({key_column} AS TEXT),_rowid_,({condition}) FROM {table} WHERE ?1<=?2 ORDER BY _rowid_ DESC LIMIT ?3")
    } else {
        format!("SELECT CAST({key_column} AS TEXT),0,({condition}) FROM {table} WHERE ?1<=?2 ORDER BY {key_column} DESC LIMIT ?3")
    };
    let mut statement = conn.prepare(&sql)?;
    let mut rows = statement.query(params![
        if append { previous } else { 0 },
        upper,
        budget as i64
    ])?;
    let mut keys = Vec::new();
    let mut last = previous;
    let mut examined = 0;
    while let Some(row) = rows.next()? {
        examined += 1;
        last = row.get(1)?;
        if !row.get::<_, Option<bool>>(2)?.unwrap_or(false) {
            continue;
        }
        let id: String = row.get(0)?;
        if id.is_empty() || id.len() > 4096 || id.contains('\0') {
            return Err(LedgerError::RecordTooLarge);
        }
        keys.push(id);
    }
    let highwater = if append && examined == budget {
        last
    } else {
        upper
    };
    let has_more = append && highwater < upper;
    let unchanged = sqlite_file_hint(path)?.1 == file_hint;
    Ok(Some(SqliteIncrementalBatch {
        keys,
        has_more,
        key,
        state: SqliteIncrementalState {
            version: 1,
            generation,
            file_hint: if has_more || !unchanged {
                String::new()
            } else {
                file_hint
            },
            highwater,
            has_more,
        },
    }))
}

pub(crate) fn retire_unrecoverable_db_checkpoints(
    conn: &Connection,
    root: &DurableRoot,
) -> Result<()> {
    let tx = conn.unchecked_transaction()?;
    tx.execute("UPDATE durable_import_targets SET state='gap',error_code='DATABASE_CAPABILITY_REPLAY_GAP',finished_ms=?2 WHERE state='pending' AND object_id IN(SELECT object_id FROM source_objects WHERE root_id=?1 AND target_done=0 AND (checkpoint IS NULL OR length(checkpoint)=0))",params![root.root_id,now_ms()])?;
    tx.execute("UPDATE source_objects SET target_done=1,state='missing',error_code='DATABASE_CAPABILITY_REPLAY_GAP' WHERE root_id=?1 AND target_done=0 AND (checkpoint IS NULL OR length(checkpoint)=0)",[&root.root_id])?;
    tx.commit()?;
    Ok(())
}
