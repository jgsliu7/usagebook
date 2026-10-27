//! Native, privacy-bounded projections for durable ingestion.
//!
//! Logical request keys use upstream response/step identifiers. A path or byte
//! offset is only an acquisition locator. Unidentified evidence is saved but
//! never promoted to a request by this module.
use super::durable::{AdapterSpec, DurableEvidence};
use super::durable::{DurableObject, DurableRoot, ObjectSpec};
use super::types::{identity_key, Observation};
use super::Ledger;
use super::{LedgerError, Result};
use rusqlite::{params, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

pub const MAX_NATIVE_FILE_BYTES: u64 = 256 * 1024 * 1024;
const MAX_LINE_BYTES: usize = super::claude::MAX_LINE_BYTES;

#[derive(Debug)]
pub struct NativeFile {
    pub object_key: String,
    pub locator: String,
    pub content_generation: String,
    pub byte_length: i64,
    pub safe_offset: i64,
    pub mtime_ns: Option<i64>,
    pub complete: bool,
    pub evidence: Vec<DurableEvidence>,
    pub diagnostics: Vec<String>,
}

pub fn spec(source: &str) -> Option<AdapterSpec> {
    let (family, parser) = match source {
        "codex" => ("codex", "codex-ledger-v1"),
        "kimi" => ("kimi", "kimi-ledger-v1"),
        "kimi-work" => ("kimi-work", "kimi-work-ledger-v1"),
        "qwen" => ("qwen", "qwen-ledger-v1"),
        _ if super::document_adapters::STREAM_SOURCES.contains(&source) => {
            return super::document_adapters::spec(source)
        }
        _ => return None,
    };
    Some(AdapterSpec {
        identity_family: family,
        collector_key: "native-history-v1",
        report_source: family,
        native_tool: family,
        parser_version: parser,
        capability_version: "native-history-v1",
        identity_version: 1,
        semantics_version: 1,
    })
}

pub fn namespace(source: &str) -> String {
    format!("unknown:{source}")
}

/// All discovered current and archived objects, with no report-date cutoff.
/// Discovery failure is not a successful empty enumeration.
pub fn discover_files(source: &str, roots: &[PathBuf]) -> Result<Vec<PathBuf>> {
    if spec(source).is_none() {
        return Err(LedgerError::SourceNotProtected);
    }
    let mut result = Vec::new();
    let mut seen = HashSet::new();
    for root in roots {
        let scopes = match source {
            "codex" => vec![root.join("sessions"), root.join("archived_sessions")],
            "qwen" => vec![root.join("usage"), root.join("projects")],
            "workbuddy" | "codebuddy" => vec![root.join("projects")],
            "grok" => vec![root.join("sessions")],
            "qoder" => vec![root.clone()],
            _ => vec![if root.join("sessions").is_dir() {
                root.join("sessions")
            } else {
                root.clone()
            }],
        };
        for scope in scopes {
            match std::fs::symlink_metadata(&scope) {
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
                Ok(_) if !scope.is_dir() => {
                    return Err(LedgerError::InvalidInput(
                        "native source container is not a directory".into(),
                    ));
                }
                _ => {}
            }
            let _ = crate::common::take_walk_error();
            let files = crate::common::walk_with(&scope, None, &|p| {
                p.extension().is_some_and(|x| x == "jsonl")
                    && match source {
                        "kimi" | "kimi-work" => p.file_name().is_some_and(|x| x == "wire.jsonl"),
                        "grok" => p.file_name().is_some_and(|x| x == "updates.jsonl"),
                        "qwen" if scope.file_name().is_some_and(|x| x == "usage") => p
                            .file_name()
                            .is_some_and(|x| x.to_string_lossy().starts_with("token-usage-")),
                        _ => true,
                    }
            });
            if crate::common::take_walk_error() {
                return Err(LedgerError::IncompleteEvidence);
            }
            for path in files {
                let canonical = path.canonicalize()?;
                if seen.insert(canonical.clone()) {
                    result.push(canonical);
                }
            }
        }
    }
    // Monthly evidence is saved before chat when both arrive in the same pass.
    // Cross-pass ownership still belongs to the durable resolver, not ordering.
    result.sort_by_key(|p| (source == "qwen" && !is_monthly(p), p.clone()));
    Ok(result)
}

fn is_monthly(path: &Path) -> bool {
    path.file_name()
        .is_some_and(|n| n.to_string_lossy().starts_with("token-usage-"))
}
fn digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
fn text(v: &Value, pointer: &str) -> Option<String> {
    v.pointer(pointer)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty() && s.len() <= 4096 && !s.contains('\0'))
        .map(str::to_owned)
}
fn integer(v: &Value, pointer: &str) -> Result<Option<i64>> {
    let Some(value) = v.pointer(pointer) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let Value::Number(number) = value else {
        return Err(LedgerError::InvalidUsageNumber);
    };
    let raw = number.to_string();
    if raw.is_empty() || !raw.bytes().all(|b| b.is_ascii_digit()) {
        return Err(LedgerError::InvalidUsageNumber);
    }
    raw.parse()
        .map(Some)
        .map_err(|_| LedgerError::InvalidUsageNumber)
}
fn timestamp(v: &Value, pointer: &str) -> Option<i64> {
    let x = v.pointer(pointer)?;
    if let Some(s) = x.as_str() {
        return chrono::DateTime::parse_from_rfc3339(s)
            .ok()
            .map(|x| x.timestamp_millis());
    }
    let n = x.as_f64()?;
    if !n.is_finite() || n < 0.0 {
        return None;
    }
    let millis = if n < 1e11 { n * 1000.0 } else { n };
    (millis <= i64::MAX as f64).then_some(millis as i64)
}
fn base(source: &str, v: &Value, time: &str, model: Option<String>) -> Observation {
    let spec = spec(source).expect("validated adapter");
    Observation {
        identity_family: source.into(),
        namespace_key: namespace(source),
        parser_version: spec.parser_version.into(),
        report_source: source.into(),
        native_tool: source.into(),
        occurred_ms: timestamp(v, time),
        model_key: model
            .as_ref()
            .map(|m| format!("n:{}", m.to_lowercase()))
            .unwrap_or_default(),
        model_display: model,
        ..Observation::default()
    }
}
fn key(o: &Observation, role: &str, parts: &[&str]) -> String {
    identity_key(&o.identity_family, &o.namespace_key, role, parts)
}
fn finish(mut o: Observation, projection: Value, offset: u64) -> Result<DurableEvidence> {
    if o.identity_key.is_none() && o.eligibility == "eligible" {
        o.eligibility = "identity_unknown".into();
    }
    if o.occurred_ms.is_none() && o.eligibility == "eligible" {
        o.eligibility = "time_unknown".into();
    }
    if o.candidate_key.is_empty() {
        o.candidate_key = match &o.identity_key {
            Some(id) => {
                let raw = serde_json::to_vec(&projection).map_err(|_| {
                    LedgerError::InvalidInput("native projection encoding failed".into())
                })?;
                key(&o, "native-evidence", &[id, &digest(&raw)])
            }
            None => {
                // This is an evidence fingerprint, deliberately not identity_key.
                // It cannot become a billable fact without native identity proof.
                let mut raw = o.clone();
                raw.record_key.clear();
                raw.observed_ms = 0;
                let bytes = serde_json::to_vec(&(&raw, &projection)).map_err(|_| {
                    LedgerError::InvalidInput("native projection encoding failed".into())
                })?;
                key(&o, "unidentified-evidence", &[&digest(&bytes)])
            }
        };
    }
    o.record_key = format!("byte:{offset}");
    o.normalized_hash = super::claude::normalized_hash(&o)?;
    Ok(DurableEvidence {
        observation: o,
        projection: Some(projection),
    })
}
fn checked_sum(values: &[Option<i64>]) -> Result<Option<i64>> {
    let mut result = 0i64;
    for value in values {
        let Some(value) = value else { return Ok(None) };
        result = result
            .checked_add(*value)
            .ok_or(LedgerError::NumericOverflow)?;
    }
    Ok(Some(result))
}
fn input_exclusive(
    total: Option<i64>,
    read: Option<i64>,
    write: Option<i64>,
) -> Result<Option<i64>> {
    match (total, read, write) {
        (Some(total), Some(read), Some(write)) => total
            .checked_sub(read)
            .and_then(|v| v.checked_sub(write))
            .filter(|v| *v >= 0)
            .map(Some)
            .ok_or(LedgerError::InvalidUsageNumber),
        _ => Ok(None),
    }
}
fn codex_metrics(u: &Value) -> Result<[Option<i64>; 10]> {
    let mut m = [None; 10];
    m[5] = integer(u, "/input_tokens")?;
    m[1] = integer(u, "/cached_input_tokens")?;
    // Older Codex did not report cache writes; missing is unknown, not zero.
    m[2] = integer(u, "/cache_write_input_tokens")?;
    m[6] = integer(u, "/output_tokens")?;
    m[4] = integer(u, "/reasoning_output_tokens")?;
    m[7] = integer(u, "/total_tokens")?;
    m[0] = input_exclusive(m[5], m[1], m[2]).unwrap_or(None);
    m[3] = match (m[6], m[4]) {
        (Some(o), Some(r)) => o
            .checked_sub(r)
            .filter(|v| *v >= 0)
            .map(Some)
            .unwrap_or(None),
        _ => None,
    };
    m[9] = Some(1);
    Ok(m)
}
fn codex_consistent(m: &[Option<i64>; 10]) -> bool {
    !m[5]
        .zip(m[1])
        .zip(m[2])
        .is_some_and(|((input, read), write)| {
            i128::from(read) + i128::from(write) > i128::from(input)
        })
        && !m[6]
            .zip(m[4])
            .is_some_and(|(output, reasoning)| reasoning > output)
        && !m[5]
            .zip(m[6])
            .zip(m[7])
            .is_some_and(|((input, output), total)| {
                i128::from(input) + i128::from(output) != i128::from(total)
            })
}
fn kimi_metrics(u: &Value, old: bool) -> Result<[Option<i64>; 10]> {
    let mut m = [None; 10];
    let fields = if old {
        [
            "/input_other",
            "/input_cache_read",
            "/input_cache_creation",
            "/output",
        ]
    } else {
        [
            "/inputOther",
            "/inputCacheRead",
            "/inputCacheCreation",
            "/output",
        ]
    };
    for (i, f) in [0, 1, 2, 6].into_iter().zip(fields) {
        m[i] = integer(u, f)?;
    }
    m[5] = checked_sum(&m[..3])?;
    m[7] = checked_sum(&[m[5], m[6]])?;
    m[9] = Some(1);
    Ok(m)
}

#[derive(Clone)]
struct Row {
    offset: u64,
    value: Value,
}
/// Read a fixed prefix and verify that prefix again. Appending after the frozen
/// boundary is allowed; truncation, replacement and edits within it are not.
/// A partial final line stays outside the committed prefix.
type StableRows = (Vec<Row>, u64, u64, String, i64, Vec<String>);

fn stable_rows(source: &str, path: &Path) -> Result<StableRows> {
    let before = crate::common::file_stamp(path).ok_or(LedgerError::IncompleteEvidence)?;
    if before.size() > MAX_NATIVE_FILE_BYTES {
        return Err(LedgerError::RecordTooLarge);
    }
    let mut reader = BufReader::new(File::open(path)?.take(before.size()));
    let mut line = Vec::new();
    let mut offset = 0u64;
    let mut hash = Sha256::new();
    let mut rows = Vec::new();
    let mut diagnostics = Vec::new();
    loop {
        line.clear();
        let n = reader
            .by_ref()
            .take((MAX_LINE_BYTES + 1) as u64)
            .read_until(b'\n', &mut line)?;
        if n == 0 {
            break;
        }
        if line.len() > MAX_LINE_BYTES {
            return Err(LedgerError::RecordTooLarge);
        }
        if line.last() != Some(&b'\n') {
            diagnostics.push("INCOMPLETE_TAIL".into());
            break;
        }
        let start = offset;
        offset += n as u64;
        hash.update(&line);
        match serde_json::from_slice::<Value>(&line) {
            Ok(v) => match project_value(source, &v) {
                Ok(Some(value)) => rows.push(Row {
                    offset: start,
                    value,
                }),
                Ok(None) => {}
                Err(error) => diagnostics.push(error.code().into()),
            },
            Err(_) => diagnostics.push("INVALID_JSON_RECORD".into()),
        }
    }
    let seal = hash.finalize();
    let after = crate::common::file_stamp(path).ok_or(LedgerError::StaleObservation)?;
    let same_object = before
        .identity_key()
        .split('/')
        .take(2)
        .eq(after.identity_key().split('/').take(2));
    if !same_object || after.size() < offset {
        return Err(LedgerError::StaleObservation);
    }
    let mut verify = File::open(path)?.take(offset);
    let mut second = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    let mut verified = 0u64;
    loop {
        let n = verify.read(&mut buf)?;
        if n == 0 {
            break;
        }
        second.update(&buf[..n]);
        verified += n as u64;
    }
    if verified != offset || second.finalize()[..] != seal[..] {
        return Err(LedgerError::StaleObservation);
    }
    let end = crate::common::file_stamp(path).ok_or(LedgerError::StaleObservation)?;
    if !after
        .identity_key()
        .split('/')
        .take(2)
        .eq(end.identity_key().split('/').take(2))
        || end.size() < offset
    {
        return Err(LedgerError::StaleObservation);
    }
    diagnostics.sort();
    diagnostics.dedup();
    let generation = seal.iter().map(|b| format!("{b:02x}")).collect();
    Ok((
        rows,
        before.size(),
        offset,
        generation,
        before.mtime_ns(),
        diagnostics,
    ))
}

/// Drops all text, tool payloads and credentials immediately after parsing.
fn project_value(source: &str, v: &Value) -> Result<Option<Value>> {
    if super::document_adapters::STREAM_SOURCES.contains(&source) {
        return super::document_adapters::project_stream(source, v);
    }
    Ok(project_native_value(source, v))
}

fn project_native_value(source: &str, v: &Value) -> Option<Value> {
    let typ = v.get("type").and_then(Value::as_str).unwrap_or("");
    let pointers: Vec<&str> = match source {
        "codex" => match typ {
            "session_meta" => vec![
                "/type",
                "/payload/id",
                "/payload/session_id",
                "/payload/forked_from_id",
            ],
            "turn_context" => vec!["/type", "/payload/model", "/payload/turn_id"],
            "token_usage_record" => vec![
                "/type",
                "/timestamp",
                "/payload/thread_id",
                "/payload/session_id",
                "/payload/turn_id",
                "/payload/response_id",
                "/payload/usage",
                "/payload/thread_token_usage",
            ],
            "event_msg"
                if v.pointer("/payload/type").and_then(Value::as_str) == Some("token_count") =>
            {
                vec![
                    "/type",
                    "/timestamp",
                    "/payload/type",
                    "/payload/info/last_token_usage",
                    "/payload/info/total_token_usage",
                ]
            }
            _ => return None,
        },
        "kimi" | "kimi-work" => match typ {
            "metadata" => vec![
                "/type",
                "/protocol_version",
                "/protocolVersion",
                "/created_at",
            ],
            "llm.request" => vec!["/type", "/model", "/turnStep", "/agentId", "/kind"],
            "usage.record" => vec![
                "/type",
                "/time",
                "/usage",
                "/usageScope",
                "/model",
                "/agentId",
            ],
            "context.append_loop_event" => vec![
                "/type",
                "/time",
                "/agentId",
                "/event/type",
                "/event/uuid",
                "/event/stepUuid",
                "/event/turnId",
                "/event/step",
                "/event/usage",
                "/event/finishReason",
                "/event/messageId",
            ],
            "step.begin" | "step.end" => vec![
                "/type",
                "/time",
                "/agentId",
                "/uuid",
                "/stepUuid",
                "/turnId",
                "/step",
                "/usage",
                "/finishReason",
                "/messageId",
            ],
            "" if v.pointer("/message/type").and_then(Value::as_str) == Some("StatusUpdate") => {
                vec![
                    "/timestamp",
                    "/message/type",
                    "/message/payload/token_usage",
                ]
            }
            _ => return None,
        },
        "qwen" => vec![
            "/type",
            "/schemaVersion",
            "/id",
            "/uuid",
            "/timestamp",
            "/sessionId",
            "/model",
            "/authType",
            "/source",
            "/inputTokens",
            "/outputTokens",
            "/cachedTokens",
            "/thoughtsTokens",
            "/usageMetadata",
            "/finishReason",
        ],
        _ => return None,
    };
    // Token objects are projected field by field too: a malicious input cannot
    // smuggle arbitrary nested text into the in-memory/native evidence shape.
    let mut out = json!({});
    for pointer in pointers {
        if let Some(value) = v.pointer(pointer) {
            if value.is_object() {
                for name in [
                    "input_tokens",
                    "cached_input_tokens",
                    "cache_write_input_tokens",
                    "output_tokens",
                    "reasoning_output_tokens",
                    "total_tokens",
                    "inputOther",
                    "inputCacheRead",
                    "inputCacheCreation",
                    "output",
                    "input_other",
                    "input_cache_read",
                    "input_cache_creation",
                    "promptTokenCount",
                    "candidatesTokenCount",
                    "thoughtsTokenCount",
                    "cachedContentTokenCount",
                ] {
                    if let Some(number) = value.get(name) {
                        set_pointer(&mut out, &format!("{pointer}/{name}"), scalar(number));
                    }
                }
            } else {
                set_pointer(&mut out, pointer, scalar(value));
            }
        }
    }
    Some(out)
}
fn scalar(v: &Value) -> Value {
    match v {
        Value::String(s) if s.len() <= 4096 => v.clone(),
        Value::Number(_) | Value::Bool(_) | Value::Null => v.clone(),
        _ => Value::Null,
    }
}
fn set_pointer(out: &mut Value, pointer: &str, value: Value) {
    let pieces: Vec<_> = pointer.trim_start_matches('/').split('/').collect();
    let mut cursor = out;
    for part in &pieces[..pieces.len() - 1] {
        if !cursor[*part].is_object() {
            cursor[*part] = json!({});
        }
        cursor = &mut cursor[*part];
    }
    cursor[pieces[pieces.len() - 1]] = value;
}

pub fn project_file(source: &str, path: &Path) -> Result<NativeFile> {
    if spec(source).is_none() {
        return Err(LedgerError::SourceNotProtected);
    }
    let (rows, size, safe, generation, mtime, mut diagnostics) = stable_rows(source, path)?;
    let evidence = match source {
        "codex" => parse_codex(&rows, &mut diagnostics),
        "kimi" | "kimi-work" => parse_kimi(source, &rows, path, &mut diagnostics),
        "qwen" => parse_qwen(&rows, is_monthly(path), &mut diagnostics),
        _ => parse_documents(source, &rows, path, &mut diagnostics),
    };
    let locator = path.canonicalize()?.to_string_lossy().into_owned();
    diagnostics.sort();
    diagnostics.dedup();
    Ok(NativeFile {
        object_key: digest(locator.as_bytes()),
        locator,
        content_generation: generation,
        byte_length: size as i64,
        safe_offset: safe as i64,
        mtime_ns: Some(mtime),
        complete: safe == size && diagnostics.is_empty(),
        evidence,
        diagnostics,
    })
}

fn parse_codex(rows: &[Row], diagnostics: &mut Vec<String>) -> Vec<DurableEvidence> {
    let mut output = Vec::new();
    let mut model = None;
    let mut turn = None;
    let mut session = None;
    let mut signatures = HashSet::new();
    for row in rows {
        let v = &row.value;
        if v["type"] == "token_usage_record" {
            if let (Some(t), Some(u), Some(total)) = (
                text(v, "/payload/turn_id"),
                v.pointer("/payload/usage"),
                v.pointer("/payload/thread_token_usage"),
            ) {
                if let (Ok(a), Ok(b)) = (codex_metrics(u), codex_metrics(total)) {
                    signatures.insert((t, a, b));
                }
            }
        }
    }
    for row in rows {
        let v = &row.value;
        match v["type"].as_str().unwrap_or("") {
            "session_meta" => {
                session = text(v, "/payload/session_id").or_else(|| text(v, "/payload/id"));
            }
            "turn_context" => {
                model = text(v, "/payload/model");
                turn = text(v, "/payload/turn_id");
            }
            "token_usage_record" => {
                let Some(u) = v.pointer("/payload/usage") else {
                    continue;
                };
                let result = (|| -> Result<DurableEvidence> {
                    let response_turn = text(v, "/payload/turn_id");
                    let response_model = if response_turn.is_some() && response_turn == turn {
                        model.clone()
                    } else {
                        None
                    };
                    let mut o = base("codex", v, "/timestamp", response_model);
                    o.metrics = codex_metrics(u)?;
                    if !codex_consistent(&o.metrics) {
                        o.eligibility = "inconsistent".into();
                    }
                    o.native_session_id = text(v, "/payload/thread_id");
                    o.native_request_id = text(v, "/payload/response_id");
                    if let (Some(s), Some(r)) = (&o.native_session_id, &o.native_request_id) {
                        o.identity_key = Some(key(&o, "request/response", &[s, r]));
                        o.session_key = Some(key(&o, "session", &[s]));
                    }
                    finish(
                        o,
                        json!({"channel":"response","turn_id":text(v,"/payload/turn_id"),"projection_version":1}),
                        row.offset,
                    )
                })();
                match result {
                    Ok(e) => output.push(e),
                    Err(_) => diagnostics.push("INVALID_USAGE_RECORD".into()),
                }
            }
            "event_msg" => {
                let Some(info) = v.pointer("/payload/info") else {
                    continue;
                };
                let Some(u) = info.get("last_token_usage").filter(|v| v.is_object()) else {
                    continue;
                };
                let result = (|| -> Result<Option<DurableEvidence>> {
                    let metrics = codex_metrics(u)?;
                    let total = info
                        .get("total_token_usage")
                        .filter(|v| v.is_object())
                        .map(codex_metrics)
                        .transpose()?;
                    if let (Some(t), Some(total)) = (&turn, total) {
                        if signatures.contains(&(t.clone(), metrics, total)) {
                            return Ok(None);
                        }
                    }
                    let mut o = base("codex", v, "/timestamp", model.clone());
                    o.metrics = metrics;
                    o.native_session_id = session.clone();
                    o.eligibility = if codex_consistent(&metrics) {
                        "identity_unknown"
                    } else {
                        "inconsistent"
                    }
                    .into();
                    Ok(Some(finish(
                        o,
                        json!({"channel":"counter","turn_id":turn,"source_session_id":session,"pending_reason":"COUNTER_REQUEST_IDENTITY_UNKNOWN","projection_version":1}),
                        row.offset,
                    )?))
                })();
                match result {
                    Ok(Some(e)) => output.push(e),
                    Ok(None) => {}
                    Err(_) => diagnostics.push("INVALID_USAGE_RECORD".into()),
                }
            }
            _ => {}
        }
    }
    output
}

#[derive(Default)]
struct StepContext {
    uuid: Option<String>,
    model: Option<String>,
    turn: Option<String>,
    completed: bool,
}
fn parse_kimi(
    source: &str,
    rows: &[Row],
    path: &Path,
    diagnostics: &mut Vec<String>,
) -> Vec<DurableEvidence> {
    let mut output = Vec::new();
    let mut contexts: HashMap<String, StepContext> = HashMap::new();
    let mut ends = HashMap::new();
    let mut protocol = None;
    let session_hint = path
        .parent()
        .and_then(|p| {
            if p.parent()?.file_name()? == "agents" {
                p.parent()?.parent()?.file_name()
            } else {
                p.file_name()
            }
        })
        .map(|p| p.to_string_lossy().into_owned());
    for row in rows {
        let v = &row.value;
        let e = if v["type"] == "context.append_loop_event" {
            &v["event"]
        } else {
            v
        };
        if e["type"] == "step.end" {
            if let (Some(uuid), Some(u)) = (text(e, "/uuid"), e.get("usage")) {
                if let Ok(m) = kimi_metrics(u, false) {
                    ends.insert(uuid, m);
                }
            }
        }
    }
    for row in rows {
        let v = &row.value;
        let agent = text(v, "/agentId").unwrap_or_else(|| "main".into());
        let context = contexts.entry(agent.clone()).or_default();
        match v["type"].as_str().unwrap_or("") {
            "metadata" => {
                protocol = text(v, "/protocol_version").or_else(|| text(v, "/protocolVersion"));
            }
            "llm.request" => {
                if context.completed {
                    context.uuid = None;
                    context.completed = false;
                }
                context.model = text(v, "/model");
            }
            "context.append_loop_event" | "step.begin" | "step.end" => {
                let e = if v["type"] == "context.append_loop_event" {
                    &v["event"]
                } else {
                    v
                };
                if e["type"] == "step.begin" {
                    context.uuid = text(e, "/uuid");
                    context.model = None;
                    context.completed = false;
                    context.turn = text(e, "/turnId");
                    continue;
                }
                if e["type"] != "step.end" {
                    continue;
                }
                let Some(u) = e.get("usage") else { continue };
                let result = (|| -> Result<DurableEvidence> {
                    let mut o = base(source, v, "/time", context.model.clone());
                    o.metrics = kimi_metrics(u, false)?;
                    o.native_request_id = text(e, "/uuid");
                    // Step UUID is generated once by the agent loop and remains
                    // unchanged in fork/replay copies. Path session attribution
                    // is not authoritative for copied steps, so keep it unknown.
                    if let Some(id) = &o.native_request_id {
                        o.identity_key = Some(key(&o, "request/step", &[id]));
                    }
                    if text(e, "/finishReason").is_none() {
                        o.eligibility = "pending_finality".into();
                    }
                    finish(
                        o,
                        json!({"channel":"step","protocol_version":protocol,"step_uuid":text(e,"/uuid"),"turn_id":text(e,"/turnId"),"source_session_id":session_hint,"source_agent_id":agent,"native_stop_reason":text(e,"/finishReason"),"projection_version":1}),
                        row.offset,
                    )
                })();
                match result {
                    Ok(e) => output.push(e),
                    Err(_) => diagnostics.push("INVALID_USAGE_RECORD".into()),
                }
                context.uuid = text(e, "/uuid");
                context.completed = true;
            }
            "usage.record" => {
                let Some(u) = v.get("usage") else { continue };
                let result = (|| -> Result<Option<DurableEvidence>> {
                    let metrics = kimi_metrics(u, false)?;
                    if v["usageScope"] == "turn"
                        && context
                            .uuid
                            .as_ref()
                            .is_some_and(|id| ends.get(id) == Some(&metrics))
                    {
                        return Ok(None);
                    }
                    let mut o = base(
                        source,
                        v,
                        "/time",
                        context.model.clone().or_else(|| text(v, "/model")),
                    );
                    o.metrics = metrics;
                    o.eligibility = "identity_unknown".into();
                    // A final step may arrive after this scan. Preserve its
                    // native step identity now so the later terminal record
                    // resolves this candidate without leaving a permanent gap.
                    if v["usageScope"] == "turn" {
                        if let Some(id) = &context.uuid {
                            o.native_request_id = Some(id.clone());
                            o.identity_key = Some(key(&o, "request/step", &[id]));
                            o.eligibility = "pending_finality".into();
                        }
                    }
                    Ok(Some(finish(
                        o,
                        json!({"channel":"usage","protocol_version":protocol,"source_session_id":session_hint,"source_agent_id":agent,"source_usage_scope":text(v,"/usageScope"),"step_uuid":context.uuid,"pending_reason":"USAGE_WITHOUT_MATCHING_FINAL_STEP","projection_version":1}),
                        row.offset,
                    )?))
                })();
                match result {
                    Ok(Some(e)) => output.push(e),
                    Ok(None) => {}
                    Err(_) => diagnostics.push("INVALID_USAGE_RECORD".into()),
                }
            }
            "" if v.pointer("/message/type").and_then(Value::as_str) == Some("StatusUpdate") => {
                let Some(u) = v
                    .pointer("/message/payload/token_usage")
                    .filter(|v| v.is_object())
                else {
                    continue;
                };
                let result = (|| -> Result<DurableEvidence> {
                    let mut o = base(source, v, "/timestamp", None);
                    o.metrics = kimi_metrics(u, true)?;
                    o.eligibility = "identity_unknown".into();
                    finish(
                        o,
                        json!({"channel":"legacy-status","source_session_id":session_hint,"pending_reason":"LEGACY_REQUEST_IDENTITY_UNKNOWN","projection_version":1}),
                        row.offset,
                    )
                })();
                match result {
                    Ok(e) => output.push(e),
                    Err(_) => diagnostics.push("INVALID_USAGE_RECORD".into()),
                }
            }
            _ => {}
        }
    }
    output
}

fn parse_qwen(rows: &[Row], monthly: bool, diagnostics: &mut Vec<String>) -> Vec<DurableEvidence> {
    let mut output = Vec::new();
    for row in rows {
        let v = &row.value;
        if !monthly && v["type"] != "assistant" {
            continue;
        }
        let result = (|| -> Result<Option<DurableEvidence>> {
            let mut o = base("qwen", v, "/timestamp", text(v, "/model"));
            o.native_session_id = text(v, "/sessionId");
            let channel = if monthly { "monthly" } else { "chat" };
            o.native_request_id = if monthly {
                text(v, "/id")
            } else {
                text(v, "/uuid")
            };
            if let Some(s) = &o.native_session_id {
                o.session_key = Some(key(&o, "session", &[s]));
            }
            if let (Some(s), Some(id)) = (&o.native_session_id, &o.native_request_id) {
                o.identity_key = Some(key(&o, &format!("request/{channel}"), &[s, id]));
            }
            let (input, cache, output_tokens, thoughts) = if monthly {
                (
                    integer(v, "/inputTokens")?,
                    integer(v, "/cachedTokens")?,
                    integer(v, "/outputTokens")?,
                    integer(v, "/thoughtsTokens")?,
                )
            } else {
                let Some(u) = v.get("usageMetadata") else {
                    return Ok(None);
                };
                (
                    integer(u, "/promptTokenCount")?,
                    integer(u, "/cachedContentTokenCount")?,
                    integer(u, "/candidatesTokenCount")?,
                    integer(u, "/thoughtsTokenCount")?,
                )
            };
            o.metrics[5] = input;
            o.metrics[1] = cache;
            o.metrics[2] = Some(0);
            o.metrics[0] = input_exclusive(input, cache, Some(0))?;
            o.metrics[4] = thoughts;
            let auth = text(v, "/authType").unwrap_or_default();
            match auth.as_str() {
                "gemini" | "vertex-ai" if monthly => {
                    o.metrics[3] = output_tokens;
                    o.metrics[6] = checked_sum(&[output_tokens, thoughts])?;
                }
                "" | "openai" | "openai-responses" | "qwen-oauth" => {
                    o.metrics[6] = output_tokens;
                    o.metrics[3] = match (output_tokens, thoughts) {
                        (Some(o), Some(t)) => o
                            .checked_sub(t)
                            .filter(|v| *v >= 0)
                            .map(Some)
                            .ok_or(LedgerError::InvalidUsageNumber)?,
                        _ => None,
                    };
                }
                _ if thoughts == Some(0) => {
                    o.metrics[3] = output_tokens;
                    o.metrics[6] = output_tokens;
                }
                _ => {
                    o.eligibility = "inconsistent".into();
                }
            }
            o.metrics[7] = checked_sum(&[o.metrics[5], o.metrics[6]])?;
            o.metrics[9] = Some(1);
            // Transcript UUIDs identify fragments, but the protocol permits
            // later replacements. Only the monthly completion record has an
            // audited request-finality contract in this adapter generation.
            if !monthly {
                o.eligibility = "pending_finality".into();
            }
            let reason = if monthly {
                None
            } else {
                Some("CHAT_FINALITY_AND_CHANNEL_COVERAGE_UNCONFIRMED")
            };
            Ok(Some(finish(
                o,
                json!({"channel":channel,"source_session_id":text(v,"/sessionId"),"source_record_id":if monthly{text(v,"/id")}else{text(v,"/uuid")},"pending_reason":reason,"projection_version":1}),
                row.offset,
            )?))
        })();
        match result {
            Ok(Some(e)) => output.push(e),
            Ok(None) => {}
            Err(_) => diagnostics.push("INVALID_USAGE_RECORD".into()),
        }
    }
    output
}

/// Only bounded, allowlisted protocol context enters a checkpoint. Text-bearing
/// source records have already been discarded by project_value.
#[derive(Clone, Default, Serialize, Deserialize)]
struct ParserContext {
    codex_meta: Option<Value>,
    codex_turn: Option<Value>,
    codex_response: Option<Value>,
    kimi_metadata: Option<Value>,
    kimi_steps: BTreeMap<String, Value>,
    kimi_models: BTreeMap<String, Value>,
}
impl ParserContext {
    fn prefix(&self, source: &str) -> Vec<Row> {
        let mut values = Vec::new();
        if source == "codex" {
            values.extend(
                [
                    self.codex_meta.clone(),
                    self.codex_turn.clone(),
                    self.codex_response.clone(),
                ]
                .into_iter()
                .flatten(),
            );
        }
        if matches!(source, "kimi" | "kimi-work") {
            values.extend(self.kimi_metadata.clone());
            for (agent, step) in &self.kimi_steps {
                let terminal = step["type"] == "step.end"
                    || step.pointer("/event/type").and_then(Value::as_str) == Some("step.end");
                if terminal {
                    if let Some(model) = self.kimi_models.get(agent) {
                        values.push(model.clone());
                    }
                    values.push(step.clone());
                } else {
                    values.push(step.clone());
                    if let Some(model) = self.kimi_models.get(agent) {
                        values.push(model.clone());
                    }
                }
            }
            for (agent, model) in &self.kimi_models {
                if !self.kimi_steps.contains_key(agent) {
                    values.push(model.clone());
                }
            }
        }
        values
            .into_iter()
            .map(|value| Row {
                offset: u64::MAX,
                value,
            })
            .collect()
    }
    fn consume(&mut self, source: &str, v: &Value) {
        if source == "codex" {
            match v["type"].as_str().unwrap_or("") {
                "session_meta" => self.codex_meta = Some(v.clone()),
                "turn_context" => self.codex_turn = Some(v.clone()),
                "token_usage_record" => self.codex_response = Some(v.clone()),
                _ => {}
            }
        }
        if matches!(source, "kimi" | "kimi-work") {
            let agent = text(v, "/agentId").unwrap_or_else(|| "main".into());
            if v["type"] == "metadata" {
                self.kimi_metadata = Some(v.clone());
            }
            if v["type"] == "llm.request" {
                if self.kimi_steps.get(&agent).is_some_and(|step| {
                    step["type"] == "step.end"
                        || step.pointer("/event/type").and_then(Value::as_str) == Some("step.end")
                }) {
                    self.kimi_steps.remove(&agent);
                }
                self.kimi_models.insert(agent.clone(), v.clone());
            }
            let e = if v["type"] == "context.append_loop_event" {
                &v["event"]
            } else {
                v
            };
            if e["type"] == "step.begin" {
                self.kimi_steps.insert(agent.clone(), v.clone());
                self.kimi_models.remove(&agent);
            }
            if e["type"] == "step.end" {
                self.kimi_steps.insert(agent, v.clone());
            }
        }
    }
}
#[derive(Clone, Serialize, Deserialize)]
struct NativeCheckpoint {
    version: u32,
    source: String,
    offset: u64,
    prefix_sha256: String,
    context: ParserContext,
    gap: bool,
    #[serde(default)]
    block_chain: [u8; 32],
    #[serde(default)]
    file_identity: String,
    #[serde(default)]
    head_sha256: String,
    #[serde(default)]
    tail_sha256: String,
    #[serde(default)]
    audit_epoch: Option<i64>,
}

const PREFIX_BLOCK: usize = 64 * 1024;
const PREFIX_ANCHOR: u64 = 4096;

/// A fixed-block digest chain can resume without serializing source text or
/// opaque library internals. Only the last partial block is reread on resume.
#[derive(Clone, Default)]
pub(crate) struct PrefixHash {
    chain: [u8; 32],
    partial: Vec<u8>,
}
impl PrefixHash {
    pub(crate) fn update(&mut self, mut bytes: &[u8]) {
        while !bytes.is_empty() {
            let n = bytes.len().min(PREFIX_BLOCK - self.partial.len());
            self.partial.extend_from_slice(&bytes[..n]);
            bytes = &bytes[n..];
            if self.partial.len() == PREFIX_BLOCK {
                let mut h = Sha256::new();
                h.update(b"usagebook-prefix-block-v2\0");
                h.update(self.chain);
                h.update(&self.partial);
                self.chain = h.finalize().into();
                self.partial.clear();
            }
        }
    }
    pub(crate) fn digest(&self) -> String {
        let mut h = Sha256::new();
        h.update(b"usagebook-prefix-final-v2\0");
        h.update(self.chain);
        h.update((self.partial.len() as u64).to_be_bytes());
        h.update(&self.partial);
        hash_hex(&h)
    }
}

/// Portable, bounded stream proof shared by collectors. No raw source bytes
/// are serialized. Normal resumes check file identity and prefix boundaries;
/// scheduled audits additionally verify every saved byte.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct PrefixSeal {
    version: u32,
    pub offset: u64,
    chain: [u8; 32],
    pub hash: String,
    identity: String,
    head: String,
    tail: String,
}
impl PrefixSeal {
    pub(crate) fn capture(path: &Path, offset: u64, hash: &PrefixHash) -> Result<Self> {
        let stamp = crate::common::file_stamp(path).ok_or(LedgerError::StaleObservation)?;
        if stamp.size() < offset {
            return Err(LedgerError::StaleObservation);
        }
        let ((head, tail), _) = prefix_anchors(path, offset)?;
        Ok(Self {
            version: 2,
            offset,
            chain: hash.chain,
            hash: hash.digest(),
            identity: file_identity(&stamp),
            head,
            tail,
        })
    }
    pub(crate) fn restore(&self, path: &Path, full: bool) -> Result<(PrefixHash, u64)> {
        let stamp = crate::common::file_stamp(path).ok_or(LedgerError::StaleObservation)?;
        if self.version != 2 || stamp.size() < self.offset || file_identity(&stamp) != self.identity
        {
            return Err(LedgerError::StaleObservation);
        }
        let mut hash = PrefixHash::default();
        let mut file = File::open(path)?;
        let mut left = self.offset;
        if !full {
            hash.chain = self.chain;
            left = self.offset % PREFIX_BLOCK as u64;
            file.seek(SeekFrom::Start(self.offset - left))?;
        }
        let mut bytes = 0;
        let mut buffer = [0; PREFIX_BLOCK];
        while left > 0 {
            let cap = left.min(PREFIX_BLOCK as u64) as usize;
            let n = file.read(&mut buffer[..cap])?;
            if n == 0 {
                return Err(LedgerError::StaleObservation);
            }
            hash.update(&buffer[..n]);
            left -= n as u64;
            bytes += n as u64;
        }
        let ((head, tail), n) = prefix_anchors(path, self.offset)?;
        bytes += n;
        if hash.digest() != self.hash || head != self.head || tail != self.tail {
            return Err(LedgerError::StaleObservation);
        }
        Ok((hash, bytes))
    }
}

fn file_identity(stamp: &crate::common::FileStamp) -> String {
    stamp
        .identity_key()
        .split('/')
        .take(2)
        .collect::<Vec<_>>()
        .join("/")
}

fn prefix_anchors(path: &Path, offset: u64) -> Result<((String, String), u64)> {
    let n = offset.min(PREFIX_ANCHOR) as usize;
    let mut file = File::open(path)?;
    let mut bytes = vec![0; n];
    file.read_exact(&mut bytes)?;
    let head = digest(&bytes);
    file.seek(SeekFrom::Start(offset.saturating_sub(n as u64)))?;
    file.read_exact(&mut bytes)?;
    Ok(((head, digest(&bytes)), (n * 2) as u64))
}

pub struct NativeBatch {
    pub evidence: Vec<DurableEvidence>,
    pub safe_offset: i64,
    pub checkpoint: Vec<u8>,
    pub complete: bool,
    pub at_target: bool,
    pub scanned_lines: u64,
    pub validation_bytes: u64,
    pub diagnostics: Vec<String>,
}
/// A fixed read target; an append beyond target_length is left for the next pass.
/// Checkpoints contain native protocol context and a digest of every committed
/// byte. Incremental collection checks identity and bounded boundary anchors;
/// a low-frequency audit verifies old interior bytes. Truncation, replacement
/// or a detected rewrite causes replay without rewriting adopted history.
pub struct NativeReader {
    source: String,
    path: PathBuf,
    reader: BufReader<File>,
    target_length: u64,
    offset: u64,
    hasher: PrefixHash,
    context: ParserContext,
    stamp: crate::common::FileStamp,
    gap: bool,
    incremental: bool,
    anchors: (String, String),
    audit_epoch: Option<i64>,
    validation_bytes: u64,
}
impl NativeReader {
    pub fn open(
        source: &str,
        path: &Path,
        target_length: u64,
        checkpoint: Option<&[u8]>,
    ) -> Result<Self> {
        Self::open_mode(source, path, target_length, checkpoint, false, None)
    }

    fn open_incremental(
        source: &str,
        path: &Path,
        target_length: u64,
        checkpoint: Option<&[u8]>,
        audit_epoch: Option<i64>,
    ) -> Result<Self> {
        Self::open_mode(source, path, target_length, checkpoint, true, audit_epoch)
    }

    fn open_mode(
        source: &str,
        path: &Path,
        target_length: u64,
        checkpoint: Option<&[u8]>,
        incremental: bool,
        audit_epoch: Option<i64>,
    ) -> Result<Self> {
        if spec(source).is_none() {
            return Err(LedgerError::SourceNotProtected);
        }
        let stamp = crate::common::file_stamp(path).ok_or(LedgerError::IncompleteEvidence)?;
        if stamp.size() < target_length {
            return Err(LedgerError::StaleObservation);
        }
        let restored = checkpoint
            .filter(|c| !c.is_empty())
            .map(|c| {
                serde_json::from_slice::<NativeCheckpoint>(c)
                    .map_err(|_| LedgerError::CheckpointReplayRequired)
            })
            .transpose()?;
        let mut reader = BufReader::new(File::open(path)?);
        let mut hasher = PrefixHash::default();
        let mut offset = 0;
        let mut context = ParserContext::default();
        let mut gap = false;
        let mut validation_bytes = 0;
        if let Some(saved) = restored {
            if !matches!(saved.version, 1 | 2)
                || saved.source != source
                || saved.offset > target_length
            {
                return Err(LedgerError::CheckpointReplayRequired);
            }
            if saved.version == 2 && saved.file_identity != file_identity(&stamp) {
                return Err(LedgerError::StaleObservation);
            }
            let bounded = incremental
                && saved.version == 2
                && !saved.file_identity.is_empty()
                && audit_epoch.is_none_or(|epoch| saved.audit_epoch == Some(epoch));
            let mut remaining = if bounded {
                hasher.chain = saved.block_chain;
                let remainder = saved.offset % PREFIX_BLOCK as u64;
                reader.seek(SeekFrom::Start(saved.offset - remainder))?;
                remainder
            } else {
                saved.offset
            };
            let mut legacy = Sha256::new();
            let mut buf = [0u8; 64 * 1024];
            while remaining > 0 {
                let count = remaining.min(buf.len() as u64) as usize;
                let n = reader.read(&mut buf[..count])?;
                if n == 0 {
                    return Err(LedgerError::StaleObservation);
                }
                hasher.update(&buf[..n]);
                if saved.version == 1 {
                    legacy.update(&buf[..n]);
                }
                validation_bytes += n as u64;
                remaining -= n as u64;
            }
            if (if saved.version == 1 {
                hash_hex(&legacy)
            } else {
                hasher.digest()
            }) != saved.prefix_sha256
            {
                return Err(LedgerError::StaleObservation);
            }
            if bounded {
                let (anchors, bytes) = prefix_anchors(path, saved.offset)?;
                validation_bytes += bytes;
                if anchors != (saved.head_sha256, saved.tail_sha256) {
                    return Err(LedgerError::StaleObservation);
                }
            }
            offset = saved.offset;
            context = saved.context;
            gap = saved.gap;
        }
        let (anchors, bytes) = prefix_anchors(path, offset)?;
        validation_bytes += bytes;
        Ok(Self {
            source: source.into(),
            path: path.to_path_buf(),
            reader,
            target_length,
            offset,
            hasher,
            context,
            stamp,
            gap,
            incremental,
            anchors,
            audit_epoch,
            validation_bytes,
        })
    }
    pub fn read_batch(&mut self, limit: usize) -> Result<NativeBatch> {
        if limit == 0 || limit > 2048 {
            return Err(LedgerError::InvalidInput(
                "native batch size must be 1..2048".into(),
            ));
        }
        let before = crate::common::file_stamp(&self.path).ok_or(LedgerError::StaleObservation)?;
        if !same_file(&self.stamp, &before) || before.size() < self.target_length {
            return Err(LedgerError::StaleObservation);
        }
        let start = self.offset;
        let start_hash = self.hasher.clone();
        let mut rows = Vec::new();
        let mut diagnostics = Vec::new();
        let mut line = Vec::new();
        let mut lines = 0u64;
        let mut candidates = 0usize;
        let mut tail = false;
        while self.offset < self.target_length
            && candidates < limit
            && self.offset - start < 4 * 1024 * 1024
        {
            line.clear();
            let left = self.target_length - self.offset;
            let n = self
                .reader
                .by_ref()
                .take(left.min((MAX_LINE_BYTES + 1) as u64))
                .read_until(b'\n', &mut line)?;
            if n == 0 {
                return Err(LedgerError::StaleObservation);
            }
            if line.len() > MAX_LINE_BYTES {
                return Err(LedgerError::RecordTooLarge);
            }
            if line.last() != Some(&b'\n') {
                self.reader.seek(SeekFrom::Start(self.offset))?;
                tail = true;
                diagnostics.push("INCOMPLETE_TAIL".into());
                break;
            }
            let offset = self.offset;
            self.offset += n as u64;
            self.hasher.update(&line);
            lines += 1;
            match serde_json::from_slice::<Value>(&line) {
                Ok(value) => match project_value(&self.source, &value) {
                    Ok(Some(value)) => {
                        if is_usage_record(&self.source, &value) {
                            candidates += 1;
                        }
                        rows.push(Row { offset, value });
                    }
                    Ok(None) => {}
                    Err(error) => {
                        self.gap = true;
                        diagnostics.push(error.code().into());
                    }
                },
                Err(_) => {
                    self.gap = true;
                    diagnostics.push("INVALID_JSON_RECORD".into());
                }
            }
        }
        let after = crate::common::file_stamp(&self.path).ok_or(LedgerError::StaleObservation)?;
        if !same_file(&before, &after) || after.size() < self.target_length {
            return Err(LedgerError::StaleObservation);
        }
        // Incremental reads validate the committed boundary and newly read
        // bytes. Old interior changes are detected by the persisted full audit
        // deadline; an ordinary append does not reread the entire prefix.
        if self.stamp != after {
            let from = if self.incremental { start } else { 0 };
            let mut file = File::open(&self.path)?;
            file.seek(SeekFrom::Start(from))?;
            let mut verify = file.take(self.offset - from);
            let mut hash = if self.incremental {
                start_hash
            } else {
                PrefixHash::default()
            };
            if self.incremental {
                let (anchors, bytes) = prefix_anchors(&self.path, start)?;
                self.validation_bytes += bytes;
                if anchors != self.anchors {
                    return Err(LedgerError::StaleObservation);
                }
            }
            let mut buf = [0u8; 64 * 1024];
            let mut read = 0;
            loop {
                let n = verify.read(&mut buf)?;
                if n == 0 {
                    break;
                }
                hash.update(&buf[..n]);
                read += n as u64;
            }
            self.validation_bytes += read;
            if read != self.offset - from || hash.digest() != self.hasher.digest() {
                return Err(LedgerError::StaleObservation);
            }
        }
        self.stamp = after;
        let (anchors, bytes) = prefix_anchors(&self.path, self.offset)?;
        self.validation_bytes += bytes;
        self.anchors = anchors;
        let mut contextual = self.context.prefix(&self.source);
        contextual.extend(rows.iter().cloned());
        let mut evidence = match self.source.as_str() {
            "codex" => parse_codex(&contextual, &mut diagnostics),
            "kimi" | "kimi-work" => {
                parse_kimi(&self.source, &contextual, &self.path, &mut diagnostics)
            }
            "qwen" => parse_qwen(&rows, is_monthly(&self.path), &mut diagnostics),
            _ => parse_documents(&self.source, &rows, &self.path, &mut diagnostics),
        };
        evidence.retain(|e| e.observation.record_key != format!("byte:{}", u64::MAX));
        for row in rows {
            self.context.consume(&self.source, &row.value);
        }
        self.gap |= diagnostics.iter().any(|d| d != "INCOMPLETE_TAIL");
        if self.gap {
            diagnostics.push("NATIVE_RECORD_GAP".into());
        }
        let checkpoint = serde_json::to_vec(&NativeCheckpoint {
            version: 2,
            source: self.source.clone(),
            offset: self.offset,
            prefix_sha256: self.hasher.digest(),
            context: self.context.clone(),
            gap: self.gap,
            block_chain: self.hasher.chain,
            file_identity: file_identity(&self.stamp),
            head_sha256: self.anchors.0.clone(),
            tail_sha256: self.anchors.1.clone(),
            audit_epoch: self.audit_epoch,
        })
        .map_err(|_| LedgerError::InvalidInput("native checkpoint encoding failed".into()))?;
        if checkpoint.len() > 65536 {
            return Err(LedgerError::RecordTooLarge);
        }
        diagnostics.sort();
        diagnostics.dedup();
        Ok(NativeBatch {
            evidence,
            safe_offset: self.offset as i64,
            checkpoint,
            complete: self.offset == self.target_length && !self.gap,
            at_target: self.offset == self.target_length || tail,
            scanned_lines: lines,
            validation_bytes: std::mem::take(&mut self.validation_bytes),
            diagnostics,
        })
    }
}
fn same_file(a: &crate::common::FileStamp, b: &crate::common::FileStamp) -> bool {
    a.identity_key()
        .split('/')
        .take(2)
        .eq(b.identity_key().split('/').take(2))
}
fn hash_hex(hash: &Sha256) -> String {
    hash.clone()
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
fn is_usage_record(source: &str, v: &Value) -> bool {
    match source {
        "codex" => matches!(v["type"].as_str(), Some("token_usage_record" | "event_msg")),
        "kimi" | "kimi-work" => {
            v["type"] == "usage.record"
                || v["type"] == "step.end"
                || v.pointer("/event/type").and_then(Value::as_str) == Some("step.end")
                || v.pointer("/message/type").and_then(Value::as_str) == Some("StatusUpdate")
        }
        "qwen" => true,
        _ => super::document_adapters::STREAM_SOURCES.contains(&source),
    }
}

fn parse_documents(
    source: &str,
    rows: &[Row],
    path: &Path,
    diagnostics: &mut Vec<String>,
) -> Vec<DurableEvidence> {
    let mut out = Vec::new();
    for row in rows {
        match super::document_adapters::from_stream(source, &row.value, row.offset) {
            Ok(mut evidence) => {
                for e in &mut evidence {
                    // This is an acquisition candidate key, never a consumption ID.
                    e.observation.candidate_key = digest(
                        json!([
                            path.to_string_lossy(),
                            row.offset,
                            e.observation.candidate_key
                        ])
                        .to_string()
                        .as_bytes(),
                    );
                }
                out.extend(evidence);
            }
            Err(e) => diagnostics.push(e.code().into()),
        }
    }
    out
}

struct FileImport {
    reader: NativeReader,
    object: DurableObject,
    pending: Option<NativeBatch>,
}
#[derive(Default)]
pub struct Collector {
    sessions: HashMap<(String, PathBuf), FileImport>,
    scopes: HashMap<String, DiscoveryScope>,
}
struct DiscoveryScope {
    locators: Vec<PathBuf>,
    roots: Vec<DurableRoot>,
    discovered_files: usize,
    next_discovery_ms: i64,
    audits: std::collections::VecDeque<(PathBuf, DurableObject)>,
    audit_epochs: HashMap<String, Option<i64>>,
}
#[derive(Debug, Default, Serialize)]
pub struct PollSummary {
    pub discovered_files: usize,
    pub read_lines: u64,
    pub validation_bytes: u64,
    pub inserted: u64,
    pub duplicates: u64,
    pub suppressed: u64,
    pub pending: u64,
    pub completed_files: u64,
    pub active_files: usize,
    pub backlog: usize,
    pub errors: u64,
    /// Successfully read but unusable records are persistent coverage gaps,
    /// not a retryable failure to read the next healthy batch.
    pub gaps: u64,
    pub diagnostics: Vec<String>,
}
impl Collector {
    /// Manual refresh requests a new discovery pass without discarding active
    /// readers or an uncommitted destination batch.
    pub fn invalidate_discovery(&mut self) {
        for scope in self.scopes.values_mut() {
            scope.next_discovery_ms = 0;
        }
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
                "native poll budget must be 1..65536".into(),
            ));
        }
        let adapter = spec(source).ok_or(LedgerError::SourceNotProtected)?;
        let now = super::types::now_ms();
        let mut summary = PollSummary::default();
        let mut discover = self
            .scopes
            .get(source)
            .is_none_or(|s| s.locators != roots || s.next_discovery_ms <= now);
        // A persisted deadline or explicit request can become due while this
        // process still holds a discovery cache. Start exactly one new epoch;
        // do not mark a cached, unexamined root complete at the end of the poll.
        if let Some(scope) = self.scopes.get(source) {
            for root in &scope.roots {
                let epoch = super::collection::history_audit_started(ledger.connection(), root)?;
                if epoch.is_some() && scope.audit_epochs.get(&root.root_id) != Some(&epoch) {
                    discover = true;
                }
            }
        }
        if discover {
            let mut scope = DiscoveryScope {
                locators: roots.to_vec(),
                roots: Vec::new(),
                discovered_files: 0,
                next_discovery_ms: now + 30_000,
                audits: std::collections::VecDeque::new(),
                audit_epochs: HashMap::new(),
            };
            for locator in roots {
                let root = ledger.register_durable_root(&adapter, &namespace(source), locator)?;
                scope.roots.push(root.clone());
                if !root.enabled {
                    self.sessions
                        .retain(|_, s| s.object.root.root_id != root.root_id);
                    continue;
                }
                let audit_epoch =
                    super::collection::history_audit_started(ledger.connection(), &root)?;
                scope.audit_epochs.insert(root.root_id.clone(), audit_epoch);
                let paths = match discover_files(source, std::slice::from_ref(locator)) {
                    Ok(paths) => paths,
                    Err(error) => {
                        ledger.fail_durable_enumeration(&root, error.code())?;
                        summary.errors += 1;
                        summary.diagnostics.push(error.code().into());
                        continue;
                    }
                };
                scope.discovered_files += paths.len();
                let seen: HashSet<PathBuf> = paths.iter().cloned().collect();
                self.sessions.retain(|(family, path), s| {
                    family != source || s.object.root.root_id != root.root_id || seen.contains(path)
                });
                let seen_keys: Vec<String> = paths
                    .iter()
                    .map(|p| digest(p.to_string_lossy().as_bytes()))
                    .collect();
                for path in paths {
                    let state_key = (source.to_owned(), path.clone());
                    // An active reader owns a frozen target. Appends are discovered
                    // after that target commits, not allowed to extend it forever.
                    if self.sessions.contains_key(&state_key) {
                        continue;
                    }
                    let Some(stamp) = crate::common::file_stamp(&path) else {
                        summary.errors += 1;
                        continue;
                    };
                    let object_key = digest(path.to_string_lossy().as_bytes());
                    type SavedNativeState = (String, i64, Option<i64>, bool, String, Option<i64>);
                    let old:Option<SavedNativeState>=ledger.conn.query_row("SELECT content_generation,byte_length,mtime_ns,target_done,state,CASE WHEN json_valid(checkpoint) THEN json_extract(checkpoint,'$.audit_epoch') ELSE NULL END FROM source_objects WHERE root_id=?1 AND object_key=?2",params![root.root_id,object_key],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?))).optional()?;
                    let unchanged = old.as_ref().is_some_and(|o| {
                        o.1 == stamp.size() as i64 && o.2 == Some(stamp.mtime_ns())
                    });
                    if unchanged
                        && old.as_ref().is_some_and(|o| {
                            o.3 && o.4 == "ready"
                                && audit_epoch.is_none_or(|started| o.5 == Some(started))
                        })
                    {
                        continue;
                    }
                    let rewritten = old
                        .as_ref()
                        .is_some_and(|o| o.1 == stamp.size() as i64 && !unchanged);
                    let generation = old
                        .filter(|o| o.1 <= stamp.size() as i64 && !rewritten)
                        .map(|o| o.0)
                        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
                    let object = ledger.discover_durable_object(
                        &root,
                        ObjectSpec {
                            object_key,
                            locator: path.to_string_lossy().into_owned(),
                            content_generation: generation,
                            byte_length: stamp.size() as i64,
                            mtime_ns: Some(stamp.mtime_ns()),
                        },
                    )?;
                    if object.complete {
                        scope.audits.push_back((path, object));
                    }
                }
                ledger.mark_durable_missing(&root, &seen_keys)?;
                ledger.finish_durable_enumeration(&root)?;
            }
            self.scopes.insert(source.into(), scope);
        }
        let scope = self
            .scopes
            .get_mut(source)
            .ok_or(LedgerError::IncompleteEvidence)?;
        summary.discovered_files = scope.discovered_files;
        let mut work = Vec::new();
        // Serve the least recently committed root first. Audits advance only
        // their own turn; they cannot repeatedly reset another root's position.
        let mut fair_roots = Vec::new();
        for root in scope.roots.iter().filter(|root| root.enabled) {
            let turn = super::collection::runtime_value(
                ledger.connection(),
                &format!("acquisition-root-turn/{source}/{}", root.root_id),
            )?
            .and_then(|value| value["sequence"].as_u64())
            .unwrap_or(0);
            fair_roots.push((turn, root));
        }
        fair_roots.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.root_id.cmp(&b.1.root_id)));
        let slots = budget.min(4);
        let audit_key = format!("native-audit-turn/{source}");
        let audit_turn = super::collection::runtime_value(ledger.connection(), &audit_key)?
            .and_then(|value| value["next_audit"].as_bool())
            .unwrap_or(false);
        // With a one-record budget alternate audit and pending work. With a
        // larger budget an audit may take only one slot, never the whole slice.
        if (slots > 1 || audit_turn) && !scope.audits.is_empty() {
            if let Some(audit) = scope.audits.pop_front() {
                work.push(audit);
            }
        }
        for round in 0..slots {
            for (_, root) in &fair_roots {
                if work.len() >= slots {
                    break;
                }
                // Obtain enough distinct objects for a second pass over a
                // small root set; each root receives its first slot first.
                for object in ledger.next_durable_objects(root, round + 1)? {
                    let path: String = ledger.conn.query_row(
                        "SELECT locator FROM source_objects WHERE object_id=?1",
                        [object.object_id],
                        |r| r.get(0),
                    )?;
                    if !work.iter().any(|(_, old): &(PathBuf, DurableObject)| {
                        old.object_id == object.object_id
                    }) {
                        work.push((PathBuf::from(path), object));
                        break;
                    }
                }
            }
            if work.len() >= slots {
                break;
            }
        }
        if slots == 1 {
            super::collection::put_runtime_value(
                ledger.connection(),
                &audit_key,
                &serde_json::json!({"version":1,"next_audit":!audit_turn}),
            )?;
        }
        let mut remaining = budget;
        let mut remaining_work = work.len();
        for (path, mut object) in work {
            if remaining == 0 {
                break;
            }
            let quantum = (remaining / remaining_work.max(1)).clamp(1, 512);
            remaining_work = remaining_work.saturating_sub(1);
            let state_key = (source.to_owned(), path.clone());
            if !self.sessions.contains_key(&state_key) {
                let audit_epoch =
                    super::collection::history_audit_started(ledger.connection(), &object.root)?;
                let mut opened = NativeReader::open_incremental(
                    source,
                    &path,
                    object.byte_length as u64,
                    object.checkpoint.as_deref(),
                    audit_epoch,
                );
                if matches!(
                    opened,
                    Err(LedgerError::StaleObservation | LedgerError::CheckpointReplayRequired)
                ) {
                    let Some(stamp) = crate::common::file_stamp(&path) else {
                        ledger.mark_durable_object_missing(&object)?;
                        summary.errors += 1;
                        continue;
                    };
                    object = ledger.discover_durable_object(
                        &object.root,
                        ObjectSpec {
                            object_key: digest(path.to_string_lossy().as_bytes()),
                            locator: path.to_string_lossy().into_owned(),
                            content_generation: uuid::Uuid::new_v4().to_string(),
                            byte_length: stamp.size() as i64,
                            mtime_ns: Some(stamp.mtime_ns()),
                        },
                    )?;
                    opened = NativeReader::open_incremental(
                        source,
                        &path,
                        object.byte_length as u64,
                        None,
                        audit_epoch,
                    );
                }
                match opened {
                    Ok(reader) => {
                        self.sessions.insert(
                            state_key.clone(),
                            FileImport {
                                reader,
                                object: object.clone(),
                                pending: None,
                            },
                        );
                    }
                    Err(error) => {
                        ledger.fail_durable_object(&object, error.code())?;
                        super::collection::record_acquisition_progress(
                            ledger.connection(),
                            source,
                            &object.root.root_id,
                        )?;
                        summary.errors += 1;
                        summary.diagnostics.push(error.code().into());
                        continue;
                    }
                }
            }
            let current = self
                .sessions
                .get_mut(&state_key)
                .ok_or(LedgerError::IncompleteEvidence)?;
            if current.pending.is_none() {
                match current.reader.read_batch(quantum) {
                    Ok(batch) => current.pending = Some(batch),
                    Err(error) => {
                        ledger.fail_durable_object(&current.object, error.code())?;
                        super::collection::record_acquisition_progress(
                            ledger.connection(),
                            source,
                            &current.object.root.root_id,
                        )?;
                        self.sessions.remove(&state_key);
                        summary.errors += 1;
                        summary.diagnostics.push(error.code().into());
                        continue;
                    }
                }
            }
            let batch = current
                .pending
                .as_ref()
                .ok_or(LedgerError::IncompleteEvidence)?;
            // Destination failure retains this exact batch. A process restart
            // resumes from the last committed checkpoint and repeats it safely.
            let has_error = batch.diagnostics.iter().any(|d| d != "INCOMPLETE_TAIL");
            let saved = ledger.save_durable_evidence_batch_with_gap(
                &mut current.object,
                &batch.evidence,
                batch.safe_offset,
                &batch.checkpoint,
                batch.complete,
                has_error.then_some("NATIVE_RECORD_GAP"),
            )?;
            summary.inserted += saved.inserted;
            summary.duplicates += saved.duplicates;
            summary.suppressed += saved.suppressed;
            summary.pending += saved.pending;
            summary.read_lines += batch.scanned_lines;
            summary.validation_bytes += batch.validation_bytes;
            remaining = remaining.saturating_sub(batch.evidence.len().max(1));
            summary.diagnostics.extend(batch.diagnostics.clone());
            let at_target = batch.at_target;
            let complete = batch.complete;
            let tail = batch.diagnostics.iter().any(|d| d == "INCOMPLETE_TAIL");
            current.pending = None;
            if has_error {
                // save_durable_evidence_batch_with_gap already records the
                // coverage gap. Do not turn a healthy unfinished suffix into
                // a new retryable read failure on every subsequent batch.
                summary.gaps += 1;
                if at_target {
                    ledger.conn.execute("UPDATE source_objects SET retry_after_ms=?2 WHERE object_id=?1 AND object_revision=?3",params![current.object.object_id,now+super::collection::HISTORY_AUDIT_INTERVAL_MS,current.object.revision])?;
                }
            } else if tail {
                // A legal unfinished append is waiting work, not a failed read.
                ledger.conn.execute("UPDATE source_objects SET retry_after_ms=?2 WHERE object_id=?1 AND object_revision=?3",params![current.object.object_id,now+30_000,current.object.revision])?;
            }
            if at_target {
                if complete {
                    ledger.conn.execute("UPDATE source_objects SET audit_completed_ms=?2,audit_due_ms=?3 WHERE object_id=?1 AND object_revision=?4",params![current.object.object_id,now,now+super::collection::HISTORY_AUDIT_INTERVAL_MS,current.object.revision])?;
                    if let Some(stamp) = crate::common::file_stamp(&path) {
                        if stamp.size() != current.object.byte_length as u64 {
                            // Appended bytes are outside this fixed pass. Do not
                            // mark the longer current file as fully validated.
                            scope.next_discovery_ms = scope.next_discovery_ms.min(now + 1_000);
                        }
                    }
                    summary.completed_files += 1;
                }
                self.sessions.remove(&state_key);
            }
        }
        for root in &scope.roots {
            if super::collection::history_audit_due(ledger.connection(), root)? {
                let complete:bool = ledger.connection().query_row("SELECT enumeration_done=1 AND error_code IS NULL AND NOT EXISTS(SELECT 1 FROM source_objects o WHERE o.root_id=r.root_id AND (o.target_done=0 OR o.error_code IS NOT NULL)) FROM source_roots r WHERE r.root_id=?1",[&root.root_id],|r|r.get(0))?;
                if complete
                    && !scope
                        .audits
                        .iter()
                        .any(|(_, o)| o.root.root_id == root.root_id)
                {
                    super::collection::complete_history_audit(ledger.connection(), root)?;
                }
            }
        }
        summary.active_files = self
            .sessions
            .keys()
            .filter(|(family, _)| family == source)
            .count();
        let queued:i64=ledger.conn.query_row("SELECT COUNT(*) FROM source_objects o JOIN source_roots r USING(root_id) WHERE r.report_source=?1 AND r.enabled=1 AND o.target_done=0 AND o.state='ready' AND (o.retry_after_ms IS NULL OR o.retry_after_ms<=?2)",params![source,now],|r|r.get(0))?;
        summary.backlog =
            usize::try_from(queued).map_err(|_| LedgerError::NumericOverflow)? + scope.audits.len();
        summary.diagnostics.sort();
        summary.diagnostics.dedup();
        Ok(summary)
    }
}
