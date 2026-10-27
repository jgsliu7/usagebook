//! Native evidence from sources with cumulative or overlapping acquisition channels.
//! Counters from session summaries are retained as pending evidence. They are never
//! turned into synthetic requests by subtracting whatever detail happens to remain.
use super::{
    durable::{AdapterSpec, DurableEvidence, DurableObject, DurableRoot, ObjectSpec},
    types::{identity_key, Observation},
    Ledger, LedgerError, Result,
};
use rusqlite::{params, types::ValueRef, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::HashSet,
    fs::File,
    io::{BufRead, BufReader, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
};

const KEY_BATCH: usize = 128;
const MAX_KEYS: usize = 1_000_000;
const MAX_RECORD: u64 = 8 * 1024 * 1024;
const MAX_COLD_EVIDENCE: usize = 100_000;

pub fn spec(source: &str) -> Option<AdapterSpec> {
    let (family, parser) = match source {
        "goose" => ("goose", "goose-ledger-v1"),
        "openclaw" => ("openclaw", "openclaw-ledger-v1"),
        "copilot" => ("copilot", "copilot-ledger-v1"),
        _ => return None,
    };
    Some(AdapterSpec {
        identity_family: family,
        collector_key: "reconciled-history-v1",
        report_source: family,
        native_tool: family,
        parser_version: parser,
        capability_version: "reconciled-incremental-v2",
        identity_version: 1,
        semantics_version: 1,
    })
}
fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn text(v: &Value, name: &str) -> Option<String> {
    v.get(name)?
        .as_str()
        .filter(|s| !s.is_empty() && s.len() <= 512 && !s.contains('\0'))
        .map(str::to_owned)
}
fn number(v: Option<&Value>) -> Result<Option<i64>> {
    match v {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(n)) => n
            .as_i64()
            .filter(|n| *n >= 0)
            .map(Some)
            .ok_or(LedgerError::InvalidUsageNumber),
        _ => Err(LedgerError::InvalidUsageNumber),
    }
}
fn millis(v: Option<&Value>) -> Option<i64> {
    let v = v?;
    if let Some(s) = v.as_str() {
        chrono::DateTime::parse_from_rfc3339(s)
            .ok()
            .map(|t| t.timestamp_millis())
            .or_else(|| {
                chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                    .ok()
                    .map(|t| t.and_utc().timestamp_millis())
            })
            .or_else(|| {
                s.parse::<i64>().ok().filter(|n| *n >= 0).and_then(|n| {
                    if n < 100_000_000_000 {
                        n.checked_mul(1000)
                    } else {
                        Some(n)
                    }
                })
            })
    } else {
        v.as_i64().filter(|n| *n >= 0).and_then(|n| {
            if n < 100_000_000_000 {
                n.checked_mul(1000)
            } else {
                Some(n)
            }
        })
    }
}
fn plus(values: &[Option<i64>]) -> Result<Option<i64>> {
    let mut n = 0i64;
    for v in values {
        let Some(v) = v else { return Ok(None) };
        n = n.checked_add(*v).ok_or(LedgerError::NumericOverflow)?;
    }
    Ok(Some(n))
}
fn subtract(a: Option<i64>, b: Option<i64>) -> Result<Option<i64>> {
    match (a, b) {
        (Some(a), Some(b)) => a
            .checked_sub(b)
            .filter(|n| *n >= 0)
            .map(Some)
            .ok_or(LedgerError::InvalidUsageNumber),
        _ => Ok(None),
    }
}
fn usage(o: &mut Observation, u: &Value, inclusive: bool) -> Result<()> {
    let input = number(u.get("input"))?;
    let output = number(u.get("output"))?;
    let read = number(u.get("read"))?;
    let write = number(u.get("write"))?;
    let reason = number(u.get("reasoning"))?;
    let declared_total = number(u.get("total"))?;
    let input_total = if inclusive {
        input
    } else {
        plus(&[input, read, write])?
    };
    let uncached = if inclusive {
        subtract(subtract(input, read)?, write)?
    } else {
        input
    };
    o.metrics = [
        uncached,
        read,
        write,
        subtract(output, reason)?,
        reason,
        input_total,
        output,
        declared_total.or(plus(&[input_total, output])?),
        None,
        Some(1),
    ];
    super::metrics::evaluate(&o.metrics)?;
    Ok(())
}
fn base(
    source: &str,
    session: Option<String>,
    id: Option<String>,
    model: Option<String>,
    time: Option<i64>,
) -> Observation {
    let ns = format!("unknown:{source}");
    Observation {
        identity_family: source.into(),
        namespace_key: ns.clone(),
        parser_version: spec(source).unwrap().parser_version.into(),
        report_source: source.into(),
        native_tool: source.into(),
        model_key: model
            .as_ref()
            .map(|m| format!("n:{}", m.to_lowercase()))
            .unwrap_or_default(),
        model_display: model,
        session_key: session
            .as_ref()
            .map(|s| identity_key(source, &ns, "session", &[s])),
        native_session_id: session,
        native_request_id: id,
        occurred_ms: time,
        ..Observation::default()
    }
}
fn complete(
    mut o: Observation,
    mut p: Value,
    raw: &Value,
    native_proven: bool,
    finality: bool,
) -> Result<DurableEvidence> {
    // Native UUIDs are portable across copies; file paths and row positions are not identities.
    if native_proven {
        if o.identity_family == "openclaw" {
            // Forks retain global entry UUIDs but may change the container's
            // session. Preserve native session as evidence; don't guess which
            // copied container owns the one provider request.
            o.session_key = None;
        }
        o.identity_key = Some(identity_key(
            &o.identity_family,
            &o.namespace_key,
            "request",
            &[
                if o.identity_family == "openclaw" {
                    "global-entry-uuid"
                } else {
                    o.native_session_id.as_deref().unwrap_or("")
                },
                o.native_request_id.as_deref().unwrap_or(""),
            ],
        ));
    }
    if o.eligibility == "eligible" {
        o.eligibility = if !native_proven {
            "identity_unknown"
        } else if !finality {
            "pending_finality"
        } else if o.occurred_ms.is_none() {
            "time_unknown"
        } else {
            "eligible"
        }
        .into();
    }
    p["raw_usage_hash"] = json!(hash(
        &serde_json::to_vec(raw).map_err(|_| LedgerError::IncompleteEvidence)?
    ));
    p["projection_version"] = json!(1);
    for (field, raw_field) in [
        ("raw_input_tokens", "input"),
        ("raw_output_tokens", "output"),
        ("raw_cache_read_tokens", "read"),
        ("raw_cache_write_tokens", "write"),
        ("raw_reasoning_tokens", "reasoning"),
        ("raw_total_tokens", "total"),
    ] {
        p[field] = raw
            .get(raw_field)
            .filter(|v| v.is_number())
            .cloned()
            .unwrap_or(Value::Null);
    }
    let mut projection = o.clone();
    projection.record_key.clear();
    projection.normalized_hash.clear();
    o.candidate_key = identity_key(
        &o.identity_family,
        &o.namespace_key,
        "evidence",
        &[&hash(
            &serde_json::to_vec(&(&projection, &p)).map_err(|_| LedgerError::IncompleteEvidence)?,
        )],
    );
    o.record_key = o.candidate_key.clone();
    o.normalized_hash = super::claude::normalized_hash(&o)?;
    Ok(DurableEvidence {
        observation: o,
        projection: Some(p),
    })
}
fn uuid(s: Option<&String>) -> bool {
    s.is_some_and(|s| uuid::Uuid::parse_str(s).is_ok())
}
fn final_stop(s: Option<&str>) -> bool {
    matches!(
        s,
        Some(
            "stop"
                | "length"
                | "toolUse"
                | "tool_calls"
                | "end_turn"
                | "max_tokens"
                | "error"
                | "aborted"
                | "content_filter"
        )
    )
}

/// Project a finalized OpenClaw message. The caller must supply the authoritative
/// native session ID from the DB key or the JSONL session header, never a filename.
pub fn project_openclaw(
    v: &Value,
    session: Option<&str>,
    channel: &str,
) -> Result<Option<DurableEvidence>> {
    if v.get("type").and_then(Value::as_str) != Some("message") {
        return Ok(None);
    }
    let Some(m) = v.get("message") else {
        return Ok(None);
    };
    if m.get("role").and_then(Value::as_str) != Some("assistant") {
        return Ok(None);
    }
    let Some(u) = m.get("usage").filter(|u| u.is_object()) else {
        return Ok(None);
    };
    let alias = |names: &[&str]| {
        names
            .iter()
            .find_map(|n| u.get(*n).filter(|v| !v.is_null()))
            .cloned()
            .unwrap_or(Value::Null)
    };
    let raw = json!({"input":alias(&["input","inputTokens","input_tokens"]),"output":alias(&["output","outputTokens","output_tokens"]),
        "read":alias(&["cacheRead","cache_read_input_tokens","cachedTokens"]),"write":alias(&["cacheWrite","cache_creation_input_tokens","cacheCreationTokens"]),
        "reasoning":alias(&["reasoningTokens","reasoning_tokens","reasoning"]),"total":alias(&["totalTokens","total_tokens","total"])});
    let mut o = base(
        "openclaw",
        session.map(str::to_owned),
        text(v, "id"),
        text(m, "model").or_else(|| text(m, "modelId")),
        millis(v.get("timestamp")).or_else(|| millis(m.get("timestamp"))),
    );
    if usage(&mut o, &raw, false).is_err() {
        o.eligibility = "inconsistent".into();
        o.metrics = [None; 10];
    }
    // Optional reasoning is not zero. Known gross output remains usable.
    let strong = uuid(o.native_session_id.as_ref()) && uuid(o.native_request_id.as_ref());
    let stop = text(m, "stopReason");
    let p = json!({"channel":channel,"source_session_id":o.native_session_id,"source_entry_id":o.native_request_id,
        "upstream_finish":stop,"source_usage_scope":"request","pending_reason":if strong {""} else {"NATIVE_ID_UNPROVEN"}});
    complete(o, p, &raw, strong, final_stop(stop.as_deref())).map(Some)
}

/// Goose's native session ID is date + a database-local sequence; ledger.id is
/// also database-local. Preserve these rows without claiming portable identity.
pub fn project_goose(v: &Value, channel: &str) -> Result<DurableEvidence> {
    let raw = json!({"input":v.get("input"),"output":v.get("output"),"read":v.get("read"),"write":v.get("write"),"reasoning":null,"total":v.get("total")});
    let mut o = base(
        "goose",
        text(v, "session"),
        text(v, "id"),
        text(v, "model"),
        millis(v.get("time")),
    );
    if usage(&mut o, &raw, true).is_err() {
        o.eligibility = "inconsistent".into();
        o.metrics = [None; 10];
    }
    let scope = if channel == "sessions"
        || v.get("cost_source").and_then(Value::as_str) == Some("carried_forward")
    {
        "session_total"
    } else {
        "request"
    };
    if scope == "session_total" {
        o.fact_kind = "session_total".into();
        o.metrics[9] = None;
    }
    let p = json!({"channel":channel,"source_session_id":o.native_session_id,"source_record_id":o.native_request_id,
        "source_usage_scope":scope,"pending_reason":if scope=="session_total" {"CUMULATIVE_COVERAGE_UNPROVEN"} else {"LOGICAL_DATABASE_ID_REQUIRED"}});
    complete(o, p, &raw, false, true)
}

pub fn project_copilot_row(v: &Value) -> Result<DurableEvidence> {
    let raw = json!({"input":v.get("input"),"output":v.get("output"),"read":v.get("read"),"write":v.get("write"),"reasoning":v.get("reasoning"),"total":v.get("total")});
    let mut o = base(
        "copilot",
        text(v, "session"),
        text(v, "id"),
        text(v, "model"),
        millis(v.get("time")),
    );
    if usage(&mut o, &raw, true).is_err() {
        o.eligibility = "inconsistent".into();
        o.metrics = [None; 10];
    }
    let strong = uuid(o.native_session_id.as_ref()) && uuid(o.native_request_id.as_ref());
    // assistant_usage_events is written as a completed request record. A finish
    // reason strengthens provenance but older schemas omit the optional column.
    let p = json!({"channel":"assistant_usage_events","source_session_id":o.native_session_id,
        "source_record_id":o.native_request_id,"source_agent_id":v.get("agent"),"upstream_finish":v.get("finish"),
        "source_usage_scope":"request","pending_reason":if strong {""} else {"NATIVE_ID_UNPROVEN"}});
    complete(o, p, &raw, strong, true)
}
pub fn project_copilot_event(v: &Value, session: Option<&str>) -> Result<Vec<DurableEvidence>> {
    if v.get("type").and_then(Value::as_str) != Some("session.shutdown") {
        return Ok(Vec::new());
    }
    let Some(models) = v.pointer("/data/modelMetrics").and_then(Value::as_object) else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for (model, metrics) in models {
        if model.len() > 512 {
            return Err(LedgerError::RecordTooLarge);
        }
        let Some(u) = metrics.get("usage") else {
            continue;
        };
        let raw = json!({"input":u.get("inputTokens"),"output":u.get("outputTokens"),"read":u.get("cacheReadTokens"),"write":u.get("cacheWriteTokens"),"reasoning":u.get("reasoningTokens"),"total":u.get("totalTokens")});
        let mut o = base(
            "copilot",
            session.map(str::to_owned),
            text(v, "id"),
            Some(model.clone()),
            millis(v.get("timestamp")),
        );
        if usage(&mut o, &raw, true).is_err() {
            o.eligibility = "inconsistent".into();
            o.metrics = [None; 10];
        }
        o.fact_kind = "session_total".into();
        o.metrics[9] = None;
        let p = json!({"channel":"session.shutdown","source_session_id":o.native_session_id,"source_record_id":o.native_request_id,
            "source_usage_scope":"session_total","pending_reason":"CUMULATIVE_COVERAGE_UNPROVEN"});
        out.push(complete(o, p, &raw, false, true)?);
    }
    Ok(out)
}

/// OTel spans and logs can describe the same request as the SQLite store and
/// shutdown summary. Preserve their typed counters without guessing coverage.
pub fn project_copilot_otel(v: &Value) -> Result<Option<DurableEvidence>> {
    let Some(a) = v.get("attributes").filter(|a| a.is_object()) else {
        return Ok(None);
    };
    if !a
        .as_object()
        .unwrap()
        .keys()
        .any(|k| k.starts_with("gen_ai.usage."))
    {
        return Ok(None);
    }
    let session = [
        "gen_ai.conversation.id",
        "copilot_chat.session_id",
        "copilot_chat.chat_session_id",
        "session.id",
    ]
    .iter()
    .find_map(|n| text(a, n));
    let raw = json!({"input":a.get("gen_ai.usage.input_tokens"),"output":a.get("gen_ai.usage.output_tokens"),
        "read":a.get("gen_ai.usage.cache_read.input_tokens"),"write":a.get("gen_ai.usage.cache_write.input_tokens").or_else(||a.get("gen_ai.usage.cache_creation.input_tokens")),
        "reasoning":a.get("gen_ai.usage.reasoning.output_tokens").or_else(||a.get("gen_ai.usage.reasoning_tokens"))});
    let mut o = base(
        "copilot",
        session,
        text(v, "spanId").or_else(|| text(a, "gen_ai.response.id")),
        text(a, "gen_ai.response.model").or_else(|| text(a, "gen_ai.request.model")),
        millis(v.get("endTime")).or_else(|| millis(v.get("timestamp"))),
    );
    if usage(&mut o, &raw, true).is_err() {
        o.eligibility = "inconsistent".into();
        o.metrics = [None; 10];
    }
    let p = json!({"channel":"otel","source_session_id":o.native_session_id,"source_record_id":o.native_request_id,
        "source_usage_scope":"unverified_operation","pending_reason":"CHANNEL_COVERAGE_UNPROVEN"});
    complete(o, p, &raw, false, true).map(Some)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Work {
    path: PathBuf,
    channel: String,
    keys: Vec<Vec<String>>,
    agent: Option<String>,
    session: Option<String>,
    #[serde(default)]
    gap: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    seal: Option<super::native_adapters::PrefixSeal>,
    #[serde(skip)]
    verification_bytes: u64,
}
fn encoded(work: &Work) -> Result<Vec<u8>> {
    serde_json::to_vec(work).map_err(|_| LedgerError::IncompleteEvidence)
}
fn stamp(path: &Path) -> Result<String> {
    let mut bytes = Vec::new();
    for p in [
        path.to_owned(),
        PathBuf::from(format!("{}-wal", path.display())),
    ] {
        match std::fs::metadata(&p) {
            Ok(m) => bytes.extend_from_slice(format!("{}:{:?}", m.len(), m.modified()?).as_bytes()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound && p != path => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(hash(&bytes))
}
fn table(conn: &Connection, name: &str) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1)",
        [name],
        |r| r.get(0),
    )?)
}
fn columns(conn: &Connection, table: &str) -> Result<HashSet<String>> {
    // Table names are internal constants, never source input.
    Ok(conn
        .prepare(&format!("PRAGMA table_info({table})"))?
        .query_map([], |r| r.get(1))?
        .collect::<rusqlite::Result<_>>()?)
}
fn sql_value(v: ValueRef<'_>) -> Result<Value> {
    Ok(match v {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(n) => json!(n),
        ValueRef::Real(n) => serde_json::Number::from_f64(n)
            .map(Value::Number)
            .ok_or(LedgerError::InvalidUsageNumber)?,
        ValueRef::Text(s) => {
            if s.len() > 4096 {
                return Err(LedgerError::RecordTooLarge);
            }
            Value::String(
                std::str::from_utf8(s)
                    .map_err(|_| LedgerError::IncompleteEvidence)?
                    .to_owned(),
            )
        }
        ValueRef::Blob(_) => return Err(LedgerError::IncompleteEvidence),
    })
}
fn rows(conn: &Connection, sql: &str, fields: &[&str], key: &str) -> Result<Option<Value>> {
    let mut stmt = conn.prepare(sql)?;
    let mut q = stmt.query([key])?;
    let Some(row) = q.next()? else {
        return Ok(None);
    };
    let mut v = serde_json::Map::new();
    for (i, k) in fields.iter().enumerate() {
        v.insert((*k).into(), sql_value(row.get_ref(i)?)?);
    }
    if q.next()?.is_some() {
        return Err(LedgerError::IncompleteEvidence);
    }
    Ok(Some(Value::Object(v)))
}
fn optional_column(cols: &HashSet<String>, name: &'static str) -> &'static str {
    if cols.contains(name) {
        name
    } else {
        "NULL"
    }
}

#[derive(Default)]
pub struct Collector {
    next_root: usize,
}
#[derive(Default, Debug, Serialize)]
pub struct PollSummary {
    pub objects: u64,
    pub saved: u64,
    pub duplicates: u64,
    pub pending: u64,
    pub gaps: u64,
    pub errors: u64,
    pub backlog: bool,
    pub prefix_verification_bytes: u64,
}
impl Collector {
    pub fn poll(
        &mut self,
        ledger: &mut Ledger,
        source: &str,
        roots: &[PathBuf],
        max_items: usize,
    ) -> Result<PollSummary> {
        let spec = spec(source).ok_or(LedgerError::SourceNotProtected)?;
        let mut summary = PollSummary::default();
        let mut remaining = max_items.clamp(1, 4096);
        let root_offset = self.next_root;
        self.next_root = self.next_root.wrapping_add(1);
        for index in 0..roots.len() {
            let path = &roots[(root_offset + index) % roots.len()];
            let root = ledger.register_durable_root(&spec, &format!("unknown:{source}"), path)?;
            if !root.enabled {
                continue;
            }
            let retry: Option<i64> = ledger.connection().query_row(
                "SELECT retry_after_ms FROM source_roots WHERE root_id=?1",
                [&root.root_id],
                |r| r.get(0),
            )?;
            if retry.is_some_and(|retry| retry > super::types::now_ms()) {
                continue;
            }
            let unfinished:bool=ledger.conn.query_row("SELECT EXISTS(SELECT 1 FROM source_objects WHERE root_id=?1 AND target_done=0 AND state NOT IN('missing','error') AND checkpoint IS NOT NULL AND length(checkpoint)>0)",[&root.root_id],|r|r.get(0))?;
            let needs_rebuild:bool=ledger.conn.query_row("SELECT EXISTS(SELECT 1 FROM source_objects WHERE root_id=?1 AND target_done=0 AND state NOT IN('missing','error') AND (checkpoint IS NULL OR length(checkpoint)=0))",[&root.root_id],|r|r.get(0))?;
            if !unfinished && needs_rebuild {
                super::collection::invalidate_history_audit(ledger.connection(), &root)?;
            }
            let audit = if !unfinished
                && super::collection::history_audit_due(ledger.connection(), &root)?
            {
                super::collection::history_audit_started(ledger.connection(), &root)?
            } else {
                None
            };
            if !unfinished {
                if let Err(e) = discover(ledger, &root, path, source, audit) {
                    ledger.fail_durable_enumeration(&root, e.code())?;
                    summary.errors += 1;
                    continue;
                }
                // Capability upgrades deliberately retire missing old checkpoints;
                // the completed discovery above recreated every still-live object.
                super::local_adapters::retire_unrecoverable_db_checkpoints(
                    ledger.connection(),
                    &root,
                )?;
            }
            if remaining == 0 {
                summary.backlog = true;
                continue;
            }
            for mut object in ledger.next_durable_objects(&root, 16)? {
                if remaining == 0 {
                    summary.backlog = true;
                    break;
                }
                if object.complete {
                    continue;
                }
                match acquire(&mut object, source, remaining) {
                    Ok((evidence, next, work, visited)) => {
                        let done = next == object.byte_length;
                        let gap = work.gap.then_some("SOURCE_EVIDENCE_GAP");
                        let saved = ledger.save_durable_evidence_batch_with_gap(
                            &mut object,
                            &evidence,
                            next,
                            &encoded(&work)?,
                            done,
                            gap,
                        )?;
                        remaining = remaining.saturating_sub(visited.max(1));
                        summary.objects += u64::from(done);
                        summary.prefix_verification_bytes += work.verification_bytes;
                        summary.saved += saved.inserted;
                        summary.duplicates += saved.duplicates;
                        summary.pending += saved.pending;
                        summary.gaps += u64::from(work.gap);
                        if !done {
                            summary.backlog = true
                        }
                    }
                    Err(LedgerError::CheckpointReplayRequired) => {
                        let mut work: Work = serde_json::from_slice(
                            object
                                .checkpoint
                                .as_deref()
                                .ok_or(LedgerError::CheckpointReplayRequired)?,
                        )
                        .map_err(|_| LedgerError::CheckpointReplayRequired)?;
                        work.keys.clear();
                        work.session = None;
                        work.gap = false;
                        work.seal = None;
                        let key: String = ledger.connection().query_row(
                            "SELECT object_key FROM source_objects WHERE object_id=?1",
                            [object.object_id],
                            |r| r.get(0),
                        )?;
                        let length = i64::try_from(work.path.metadata()?.len())
                            .map_err(|_| LedgerError::RecordTooLarge)?;
                        ledger.stage_durable_manifest(
                            &root,
                            ObjectSpec {
                                object_key: key,
                                locator: work.path.to_string_lossy().into_owned(),
                                content_generation: uuid::Uuid::new_v4().to_string(),
                                byte_length: length,
                                mtime_ns: None,
                            },
                            &encoded(&work)?,
                        )?;
                        summary.backlog = true;
                    }
                    Err(e) => {
                        ledger.mark_durable_gap(&object, e.code())?;
                        summary.errors += 1;
                    }
                }
            }
            let (pending, gaps):(i64,i64)=ledger.connection().query_row("SELECT COALESCE(SUM(target_done=0 AND state NOT IN('missing','error')),0),COALESCE(SUM(error_code IS NOT NULL),0) FROM source_objects WHERE root_id=?1",[&root.root_id],|r|Ok((r.get(0)?,r.get(1)?)))?;
            let audit_state = super::collection::runtime_value(
                ledger.connection(),
                &format!("history-audit/{}", root.root_id),
            )?
            .unwrap_or(Value::Null);
            if pending == 0
                && gaps == 0
                && summary.errors == 0
                && audit_state["started_ms"].is_i64()
                && super::collection::history_audit_due(ledger.connection(), &root)?
            {
                super::collection::complete_history_audit(ledger.connection(), &root)?;
            }
            if pending == 0 && gaps > 0 && audit_state["started_ms"].is_i64() {
                super::collection::invalidate_history_audit(ledger.connection(), &root)?;
                ledger.fail_durable_enumeration(&root, "SOURCE_EVIDENCE_GAP")?;
            }
            let incremental_pending = super::collection::runtime_value(
                ledger.connection(),
                &format!("reconciled-more/{}", root.root_id),
            )?
            .is_some_and(|value| value == true);
            summary.backlog |= pending > 0 || incremental_pending;
        }
        Ok(summary)
    }
}
fn stage(
    ledger: &mut Ledger,
    root: &DurableRoot,
    work: Work,
    length: i64,
    generation: &str,
) -> Result<()> {
    let descriptor = encoded(&work)?;
    if descriptor.len() > 60000 {
        return Err(LedgerError::RecordTooLarge);
    }
    // Acquisition keys may contain physical paths. Consumption identities above never do.
    let object_key = format!("{}:{}", work.channel, hash(&descriptor));
    let generation = if work.channel.ends_with("jsonl") {
        let physical = file_identity(&work.path.metadata()?);
        let current_stamp = stamp(&work.path)?;
        let previous:Option<(String,i64,Option<Vec<u8>>)>=ledger.connection().query_row("SELECT content_generation,byte_length,checkpoint FROM source_objects WHERE root_id=?1 AND object_key=?2",params![root.root_id,object_key],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
        if generation == "incremental" {
            previous
                .and_then(|(generation, old_length, checkpoint)| {
                    let old: Work = serde_json::from_slice(checkpoint.as_deref()?).ok()?;
                    (old.keys.first().and_then(|key| key.get(1)) == Some(&physical)
                        && length >= old_length
                        && (!old.gap || length == old_length))
                        .then_some(generation)
                })
                .unwrap_or_else(|| hash(format!("{physical}:{current_stamp}").as_bytes()))
        } else {
            hash(format!("{physical}:{generation}").as_bytes())
        }
    } else {
        generation.to_owned()
    };
    ledger.stage_durable_manifest(
        root,
        ObjectSpec {
            object_key,
            locator: work.path.to_string_lossy().into_owned(),
            content_generation: generation,
            byte_length: length,
            mtime_ns: None,
        },
        &descriptor,
    )?;
    Ok(())
}
#[allow(clippy::too_many_arguments)] // Source projection fields are explicit at the capture boundary.
fn key_manifests(
    ledger: &mut Ledger,
    root: &DurableRoot,
    path: &Path,
    channel: &str,
    conn: &Connection,
    sql: &str,
    width: usize,
    agent: Option<String>,
    audit: Option<i64>,
) -> Result<()> {
    if audit.is_none() {
        return incremental_keys(ledger, root, path, channel, conn, width, agent);
    }
    let generation = format!("{}:audit:{}", stamp(path)?, audit.unwrap());
    let mut stmt = conn.prepare(sql)?;
    let mut query = stmt.query([])?;
    let mut keys = Vec::new();
    let mut count = 0usize;
    while let Some(row) = query.next()? {
        let mut key = Vec::new();
        for i in 0..width {
            let v = sql_value(row.get_ref(i)?)?;
            let s = if let Some(s) = v.as_str() {
                s.to_owned()
            } else if let Some(n) = v.as_i64() {
                n.to_string()
            } else {
                return Err(LedgerError::IncompleteEvidence);
            };
            if s.len() > 512 {
                return Err(LedgerError::RecordTooLarge);
            }
            key.push(s);
        }
        keys.push(key);
        count += 1;
        if count > MAX_KEYS {
            return Err(LedgerError::RecordTooLarge);
        }
        if keys.len() == KEY_BATCH {
            let len = keys.len() as i64;
            stage(
                ledger,
                root,
                Work {
                    path: path.to_owned(),
                    channel: channel.into(),
                    keys: std::mem::take(&mut keys),
                    agent: agent.clone(),
                    session: None,
                    gap: false,
                    seal: None,
                    verification_bytes: 0,
                },
                len,
                &generation,
            )?;
        }
    }
    if !keys.is_empty() {
        let len = keys.len() as i64;
        stage(
            ledger,
            root,
            Work {
                path: path.to_owned(),
                channel: channel.into(),
                keys,
                agent,
                session: None,
                gap: false,
                seal: None,
                verification_bytes: 0,
            },
            len,
            &generation,
        )?;
    }
    Ok(())
}
fn incremental_keys(
    ledger: &mut Ledger,
    root: &DurableRoot,
    path: &Path,
    channel: &str,
    conn: &Connection,
    width: usize,
    agent: Option<String>,
) -> Result<()> {
    let key_column = if width == 1 { "id" } else { "_rowid_" };
    let rowid_shadowed = if width != 1 {
        conn.prepare("PRAGMA table_info(transcript_events)")?
            .query_map([], |r| r.get::<_, String>(1))?
            .collect::<rusqlite::Result<Vec<_>>>()?
            .iter()
            .any(|name| name.eq_ignore_ascii_case("_rowid_"))
    } else {
        false
    };
    if width != 1
        && (rowid_shadowed
            || conn
                .prepare("SELECT _rowid_ FROM transcript_events LIMIT 0")
                .is_err())
    {
        // WITHOUT ROWID tables have no append proof; their scheduled audit owns coverage.
        return Ok(());
    }
    let Some(batch) = super::local_adapters::sqlite_incremental_batch(
        ledger, root, path, conn, channel, key_column, "", KEY_BATCH,
    )?
    else {
        return Ok(());
    };
    let fresh = crate::common::open_sqlite_readonly(path)?;
    fresh.execute_batch("BEGIN")?;
    let mut keys = Vec::new();
    for key in &batch.keys {
        if width == 1 {
            keys.push(vec![key.clone()]);
        } else {
            let pair: Option<(String, i64)> = fresh
                .query_row(
                    "SELECT session_id,seq FROM transcript_events WHERE _rowid_=?1",
                    [key],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;
            let Some((session, seq)) = pair else {
                return Err(LedgerError::IncompleteEvidence);
            };
            keys.push(vec![session, seq.to_string()]);
        }
    }
    if !keys.is_empty() {
        let count = keys.len() as i64;
        stage(
            ledger,
            root,
            Work {
                path: path.to_owned(),
                channel: channel.into(),
                keys,
                agent,
                session: None,
                gap: false,
                seal: None,
                verification_bytes: 0,
            },
            count,
            &stamp(path)?,
        )?;
    }
    // The fixed key manifest is durable before the hint advances. Acquisition
    // resumes that queue after a crash; no skipped key depends on RAM state.
    if batch.has_more {
        super::collection::put_runtime_value(
            ledger.connection(),
            &format!("reconciled-more/{}", root.root_id),
            &json!(true),
        )?;
    }
    batch.commit(ledger)
}
fn files(
    ledger: &mut Ledger,
    root: &DurableRoot,
    path: &Path,
    channel: &str,
    agent: Option<String>,
    audit: Option<i64>,
) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let _ = crate::common::take_walk_error();
    let list = crate::common::walk_with(path, None, &|p| {
        p.file_name().is_some_and(|n| {
            let n = n.to_string_lossy();
            n.ends_with(".jsonl") || n.contains(".jsonl.reset.") || n.contains(".jsonl.deleted.")
        })
    });
    if crate::common::take_walk_error() {
        return Err(LedgerError::IncompleteEvidence);
    }
    for file in list {
        let generation =
            audit.map_or_else(|| "incremental".into(), |epoch| format!("audit:{epoch}"));
        let length =
            i64::try_from(file.metadata()?.len()).map_err(|_| LedgerError::RecordTooLarge)?;
        stage(
            ledger,
            root,
            Work {
                path: file,
                channel: channel.into(),
                keys: Vec::new(),
                agent: agent.clone(),
                session: None,
                gap: false,
                seal: None,
                verification_bytes: 0,
            },
            length,
            &generation,
        )?;
    }
    Ok(())
}
fn discover(
    ledger: &mut Ledger,
    root: &DurableRoot,
    path: &Path,
    source: &str,
    audit: Option<i64>,
) -> Result<()> {
    // Missing previously registered roots are an error, never successful empty history.
    std::fs::metadata(path)?;
    super::collection::put_runtime_value(
        ledger.connection(),
        &format!("reconciled-more/{}", root.root_id),
        &json!(false),
    )?;
    match source {
        "goose" => {
            let db = if path.is_file() {
                path.to_owned()
            } else {
                path.join("sessions.db")
            };
            let conn = crate::common::open_sqlite_readonly(&db)?;
            conn.execute_batch("BEGIN")?;
            if table(&conn, "usage_ledger")? {
                key_manifests(
                    ledger,
                    root,
                    &db,
                    "usage_ledger",
                    &conn,
                    "SELECT id FROM usage_ledger ORDER BY id",
                    1,
                    None,
                    audit,
                )?;
            }
            if table(&conn, "sessions")? {
                key_manifests(
                    ledger,
                    root,
                    &db,
                    "sessions",
                    &conn,
                    "SELECT id FROM sessions ORDER BY id",
                    1,
                    None,
                    audit,
                )?;
            } else {
                return Err(LedgerError::IncompleteEvidence);
            }
        }
        "copilot" => {
            let db = path.join("session-store.db");
            if db.exists() {
                let conn = crate::common::open_sqlite_readonly(&db)?;
                conn.execute_batch("BEGIN")?;
                if table(&conn, "assistant_usage_events")? {
                    key_manifests(
                        ledger,
                        root,
                        &db,
                        "assistant_usage_events",
                        &conn,
                        "SELECT id FROM assistant_usage_events ORDER BY id",
                        1,
                        None,
                        audit,
                    )?;
                }
            }
            files(
                ledger,
                root,
                &path.join("session-state"),
                "copilot-jsonl",
                None,
                audit,
            )?;
            if let Some(explicit) = std::env::var_os("COPILOT_OTEL_FILE_EXPORTER_PATH") {
                let file = PathBuf::from(explicit);
                let generation =
                    audit.map_or_else(|| "incremental".into(), |epoch| format!("audit:{epoch}"));
                let length = i64::try_from(file.metadata()?.len())
                    .map_err(|_| LedgerError::RecordTooLarge)?;
                stage(
                    ledger,
                    root,
                    Work {
                        path: file,
                        channel: "copilot-otel-jsonl".into(),
                        keys: Vec::new(),
                        agent: None,
                        session: None,
                        gap: false,
                        seal: None,
                        verification_bytes: 0,
                    },
                    length,
                    &generation,
                )?;
            }
        }
        "openclaw" => {
            let agents = path.join("agents");
            if agents.exists() {
                for entry in std::fs::read_dir(agents)? {
                    let entry = entry?;
                    let agent_path = entry.path();
                    if !agent_path.is_dir() {
                        continue;
                    }
                    let agent = entry.file_name().to_string_lossy().into_owned();
                    let db = agent_path.join("agent/openclaw-agent.sqlite");
                    if db.exists() {
                        let conn = crate::common::open_sqlite_readonly(&db)?;
                        conn.execute_batch("BEGIN")?;
                        key_manifests(
                            ledger,
                            root,
                            &db,
                            "transcript_events",
                            &conn,
                            "SELECT session_id,seq FROM transcript_events ORDER BY session_id,seq",
                            2,
                            Some(agent.clone()),
                            audit,
                        )?;
                        let refs = crate::sources::openclaw::cold::references(&conn)
                            .map_err(|_| LedgerError::IncompleteEvidence)?;
                        for a in refs {
                            stage(
                                ledger,
                                root,
                                Work {
                                    path: db.clone(),
                                    channel: "openclaw-cold".into(),
                                    keys: vec![vec![a.session_id, a.archive_sha256.clone()]],
                                    agent: Some(agent.clone()),
                                    session: None,
                                    gap: false,
                                    seal: None,
                                    verification_bytes: 0,
                                },
                                1,
                                &a.archive_sha256,
                            )?;
                        }
                    }
                    // Both representations are evidence; strong native event identity removes overlap.
                    files(
                        ledger,
                        root,
                        &agent_path.join("sessions"),
                        "openclaw-jsonl",
                        Some(agent),
                        audit,
                    )?;
                }
            }
        }
        _ => return Err(LedgerError::SourceNotProtected),
    }
    ledger.finish_durable_enumeration(root)
}
fn acquire(
    object: &mut DurableObject,
    source: &str,
    budget: usize,
) -> Result<(Vec<DurableEvidence>, i64, Work, usize)> {
    let mut work: Work = serde_json::from_slice(
        object
            .checkpoint
            .as_deref()
            .ok_or(LedgerError::CheckpointReplayRequired)?,
    )
    .map_err(|_| LedgerError::CheckpointReplayRequired)?;
    if work.channel.ends_with("jsonl") {
        return read_file(object, source, work, budget);
    }
    let conn = crate::common::open_sqlite_readonly(&work.path)?;
    conn.execute_batch("BEGIN")?;
    if work.channel == "openclaw-cold" {
        let reference = crate::sources::openclaw::cold::references(&conn)
            .map_err(|_| LedgerError::IncompleteEvidence)?
            .into_iter()
            .find(|a| a.session_id == work.keys[0][0] && a.archive_sha256 == work.keys[0][1])
            .ok_or(LedgerError::IncompleteEvidence)?;
        let cold = work
            .path
            .parent()
            .and_then(Path::parent)
            .ok_or(LedgerError::IncompleteEvidence)?
            .join("sessions/cold");
        let mut evidence = Vec::new();
        crate::sources::openclaw::cold::scan(
            &conn,
            &reference,
            &cold,
            &mut Vec::new(),
            |_, line| {
                let v: Value = serde_json::from_str(line).map_err(|_| "invalid event")?;
                if let Some(o) = project_openclaw(&v, Some(&reference.session_id), "cold")
                    .map_err(|_| "invalid usage")?
                {
                    evidence.push(o)
                }
                if evidence.len() > MAX_COLD_EVIDENCE {
                    return Err("too many projected events");
                }
                Ok(())
            },
        )
        .map_err(|_| LedgerError::IncompleteEvidence)?;
        // No observation is committed until the complete archive passes checksum,
        // envelope, count, sequence and decompression bounds.
        return Ok((evidence, 1, work, 1));
    }
    let start = usize::try_from(object.saved_offset).map_err(|_| LedgerError::CorruptLedger)?;
    let end = (start + budget).min(work.keys.len());
    let mut evidence = Vec::new();
    for key in &work.keys[start..end] {
        let mut existing_nonusage = false;
        let parsed = (|| -> Result<Option<DurableEvidence>> {
            Ok(match work.channel.as_str() {
                "usage_ledger" | "sessions" => {
                    let cols = columns(&conn, &work.channel)?;
                    let fields = [
                        "id",
                        "session",
                        "model",
                        "time",
                        "input",
                        "output",
                        "read",
                        "write",
                        "total",
                        "cost_source",
                    ];
                    let sql = if work.channel == "usage_ledger" {
                        format!("SELECT CAST(id AS TEXT),session_id,model,created_timestamp,input_tokens,output_tokens,cache_read_tokens,cache_write_tokens,{},{} FROM usage_ledger WHERE id=?1",optional_column(&cols,"total_tokens"),optional_column(&cols,"cost_source"))
                    } else {
                        format!(
                            "SELECT id,id,NULL,{}, {}, {}, {}, {},{},NULL FROM sessions WHERE id=?1",
                            optional_column(&cols, "updated_at"),
                            optional_column(&cols, "accumulated_input_tokens"),
                            optional_column(&cols, "accumulated_output_tokens"),
                            optional_column(&cols, "accumulated_cache_read_tokens"),
                            optional_column(&cols, "accumulated_cache_write_tokens"),
                        optional_column(&cols, "accumulated_total_tokens")
                        )
                    };
                    rows(&conn, &sql, &fields, &key[0])?
                        .map(|v| project_goose(&v, &work.channel))
                        .transpose()?
                }
                "assistant_usage_events" => {
                    let cols = columns(&conn, &work.channel)?;
                    let sql=format!("SELECT CAST(id AS TEXT),session_id,model,created_at,input_tokens,output_tokens,cache_read_tokens,cache_write_tokens,{},{},{} FROM assistant_usage_events WHERE id=?1",optional_column(&cols,"reasoning_tokens"),optional_column(&cols,"agent_id"),optional_column(&cols,"finish_reason"));
                    rows(
                        &conn,
                        &sql,
                        &[
                            "id",
                            "session",
                            "model",
                            "time",
                            "input",
                            "output",
                            "read",
                            "write",
                            "reasoning",
                            "agent",
                            "finish",
                        ],
                        &key[0],
                    )?
                    .map(|v| project_copilot_row(&v))
                    .transpose()?
                }
                "transcript_events" => {
                    let row:Option<(Option<String>,Option<Vec<u8>>)>=conn.query_row("SELECT CASE WHEN length(CAST(event_json AS BLOB))<=8388608 THEN event_json END,CASE WHEN length(event_zstd)<=8388608 THEN event_zstd END FROM transcript_events WHERE session_id=?1 AND seq=?2",params![key[0],key[1]],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
                    match row {
                        None => None,
                        Some((plain, packed)) => {
                            let raw = match (plain, packed) {
                                (Some(s), None) => s,
                                (None, Some(bytes)) => {
                                    let mut d = zstd::stream::read::Decoder::new(bytes.as_slice())?;
                                    d.window_log_max(26)?;
                                    let mut s = String::new();
                                    d.take(MAX_RECORD + 1).read_to_string(&mut s)?;
                                    if s.len() as u64 > MAX_RECORD {
                                        return Err(LedgerError::RecordTooLarge);
                                    }
                                    s
                                }
                                _ => return Err(LedgerError::IncompleteEvidence),
                            };
                            let v: Value = serde_json::from_str(&raw)
                                .map_err(|_| LedgerError::IncompleteEvidence)?;
                            // An existing non-usage transcript is still an acquired native key.
                            existing_nonusage = true;
                            project_openclaw(&v, Some(&key[0]), "hot")?
                        }
                    }
                }
                _ => return Err(LedgerError::UnsupportedVersion),
            })
        })();
        match parsed {
            Ok(Some(o)) => evidence.push(o),
            Ok(None) if existing_nonusage => {}
            Ok(None) | Err(_) => work.gap = true,
        }
    }
    Ok((evidence, end as i64, work, end - start))
}
fn read_file(
    object: &DurableObject,
    source: &str,
    mut work: Work,
    budget: usize,
) -> Result<(Vec<DurableEvidence>, i64, Work, usize)> {
    let mut file = File::open(&work.path)?;
    if file.metadata()?.len() < object.byte_length as u64 {
        return Err(LedgerError::CheckpointReplayRequired);
    }
    let native_file_id = file_identity(&file.metadata()?);
    let mut prefix = if let Some(seal) = &work.seal {
        if seal.offset != object.saved_offset as u64 {
            return Err(LedgerError::CheckpointReplayRequired);
        }
        let (hash, bytes) = seal
            .restore(&work.path, false)
            .map_err(|_| LedgerError::CheckpointReplayRequired)?;
        work.verification_bytes = bytes;
        hash
    } else {
        let mut chain = super::native_adapters::PrefixHash::default();
        let mut old = Sha256::new();
        let mut left = object.saved_offset as u64;
        work.verification_bytes = left;
        let mut chunk = [0u8; 65536];
        while left > 0 {
            let n = file.read(&mut chunk[..left.min(65536) as usize])?;
            if n == 0 {
                return Err(LedgerError::CheckpointReplayRequired);
            }
            old.update(&chunk[..n]);
            chain.update(&chunk[..n]);
            left -= n as u64;
        }
        if let Some(cp) = work.keys.first() {
            if cp.first() != Some(&format!("{:x}", old.finalize()))
                || cp.get(1) != Some(&native_file_id)
            {
                return Err(LedgerError::CheckpointReplayRequired);
            }
        }
        chain
    };
    file.seek(SeekFrom::Start(object.saved_offset as u64))?;
    let mut reader = BufReader::new(file.take((object.byte_length - object.saved_offset) as u64));
    let mut next = object.saved_offset;
    let mut read_hash = Sha256::new();
    let mut visited = 0;
    let mut out = Vec::new();
    while visited < budget && next < object.byte_length {
        let mut line = Vec::new();
        let count = reader
            .by_ref()
            .take(MAX_RECORD + 1)
            .read_until(b'\n', &mut line)?;
        if count == 0 {
            return Err(LedgerError::IncompleteEvidence);
        }
        if count as u64 > MAX_RECORD {
            return Err(LedgerError::RecordTooLarge);
        }
        // A live trailing fragment is a gap in this fixed snapshot; it will be
        // retried from the next changed generation, never admitted as final usage.
        prefix.update(&line);
        read_hash.update(&line);
        next += count as i64;
        visited += 1;
        if line.last() != Some(&b'\n') {
            work.gap = true;
            continue;
        }
        let v: Value = match serde_json::from_slice(&line) {
            Ok(v) => v,
            Err(_) => {
                work.gap = true;
                continue;
            }
        };
        if source == "openclaw" {
            if v.get("type").and_then(Value::as_str) == Some("session") {
                work.session = text(&v, "id");
                continue;
            }
            if let Some(o) = project_openclaw(&v, work.session.as_deref(), "jsonl")? {
                out.push(o)
            }
        } else if work.channel == "copilot-otel-jsonl" {
            if let Some(o) = project_copilot_otel(&v)? {
                out.push(o)
            }
        } else {
            if v.get("type").and_then(Value::as_str) == Some("session.start") {
                work.session = v.get("data").and_then(|d| text(d, "sessionId"));
                continue;
            }
            out.extend(project_copilot_event(&v, work.session.as_deref())?);
        }
    }
    #[cfg(feature = "regression-hooks")]
    replace_stream_for_regression(&work.path)?;
    // The reader owns an open descriptor while the seal opens the pathname.
    // Prove they still refer to the same physical file and the same newly read
    // bytes; otherwise old-descriptor usage could be paired with a new seal.
    let after = work.path.metadata()?;
    if file_identity(&after) != native_file_id || after.len() < object.byte_length as u64 {
        return Err(LedgerError::CheckpointReplayRequired);
    }
    let mut verify_file = File::open(&work.path)?;
    verify_file.seek(SeekFrom::Start(object.saved_offset as u64))?;
    let mut verify = verify_file.take((next - object.saved_offset) as u64);
    let mut verified_hash = Sha256::new();
    let mut bytes = 0;
    let mut buffer = [0; 65536];
    loop {
        let n = verify.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        verified_hash.update(&buffer[..n]);
        bytes += n as u64;
    }
    work.verification_bytes += bytes;
    if bytes != (next - object.saved_offset) as u64
        || verified_hash.finalize() != read_hash.finalize()
    {
        return Err(LedgerError::CheckpointReplayRequired);
    }
    let seal = super::native_adapters::PrefixSeal::capture(&work.path, next as u64, &prefix)?;
    let sealed = work.path.metadata()?;
    if file_identity(&sealed) != native_file_id
        || sealed.len() != after.len()
        || sealed.modified()? != after.modified()?
    {
        return Err(LedgerError::CheckpointReplayRequired);
    }
    work.keys = vec![vec!["sealed-v2".into(), native_file_id]];
    work.seal = Some(seal);
    Ok((out, next, work, visited))
}

#[cfg(feature = "regression-hooks")]
thread_local! {
    static STREAM_REPLACEMENT: std::cell::RefCell<Option<(PathBuf, PathBuf)>> = const { std::cell::RefCell::new(None) };
}
#[cfg(feature = "regression-hooks")]
pub fn replace_stream_before_validation_for_regression(path: PathBuf, replacement: PathBuf) {
    STREAM_REPLACEMENT.with(|pending| *pending.borrow_mut() = Some((path, replacement)));
}
#[cfg(feature = "regression-hooks")]
fn replace_stream_for_regression(path: &Path) -> Result<()> {
    STREAM_REPLACEMENT.with(|pending| {
        let mut pending = pending.borrow_mut();
        if pending
            .as_ref()
            .is_some_and(|(selected, _)| selected == path)
        {
            let (_, replacement) = pending.take().unwrap();
            std::fs::rename(replacement, path)?;
        }
        Ok(())
    })
}
fn file_identity(metadata: &std::fs::Metadata) -> String {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        format!("{}:{}", metadata.dev(), metadata.ino())
    }
    #[cfg(not(unix))]
    {
        format!("{:?}", metadata.created().ok())
    }
}
