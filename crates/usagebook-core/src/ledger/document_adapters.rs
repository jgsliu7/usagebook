//! Evidence-only formats whose finality or cross-channel identity is unproved.
//! Save the native numeric vocabulary, not a fabricated request or snapshot sum.
use super::{
    durable::{AdapterSpec, DurableEvidence, DurableObject, ObjectSpec},
    types::{now_ms, Observation},
    Ledger, LedgerError, Result,
};
use rusqlite::{params, OptionalExtension};
use serde::Serialize;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, VecDeque},
    fs,
    path::{Path, PathBuf},
};

pub const SOURCES: &[&str] = &[
    "workbuddy",
    "codebuddy",
    "codebuddy-ide",
    "cline",
    "roocode",
    "grok",
    "codebuff",
    "droid",
    "qoder",
    "amp",
    "gemini",
];
pub const STREAM_SOURCES: &[&str] = &["workbuddy", "codebuddy", "grok", "qoder"];
pub const PARSER: &str = "document-evidence-v1";
const MAX_DOCUMENT: u64 = 64 * 1024 * 1024;
const MAX_CANDIDATES: usize = 20_000;
pub fn spec(source: &str) -> Option<AdapterSpec> {
    let source = SOURCES.iter().copied().find(|s| *s == source)?;
    Some(AdapterSpec {
        identity_family: source,
        collector_key: "document-evidence",
        report_source: source,
        native_tool: source,
        parser_version: PARSER,
        capability_version: "native-usage-fields-v1",
        identity_version: 1,
        semantics_version: 1,
    })
}
pub fn projection_fields() -> &'static [&'static str] {
    &[
        "channel",
        "evidence_kind",
        "pending_reason",
        "raw_usage",
        "projection_version",
    ]
}
fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn text(v: &Value, paths: &[&str]) -> Option<String> {
    paths.iter().find_map(|p| {
        v.pointer(p)
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty() && s.len() <= 4096 && !s.contains('\0'))
            .map(str::to_owned)
    })
}
fn time(v: &Value) -> Option<i64> {
    v.as_str().and_then(crate::common::iso_to_ms).or_else(|| {
        v.as_i64().and_then(|n| {
            if n <= 0 {
                None
            } else if n < 1_000_000_000_000 {
                n.checked_mul(1000)
            } else {
                Some(n)
            }
        })
    })
}
// Values at these explicit token paths are the only raw usage data retained.
// Cost and credits are intentionally not interpreted as tokens.
const NUMERIC: &[&str] = &[
    "input_tokens",
    "output_tokens",
    "total_tokens",
    "prompt_tokens",
    "completion_tokens",
    "cache_read_input_tokens",
    "cache_creation_input_tokens",
    "prompt_cache_hit_tokens",
    "prompt_cache_write_tokens",
    "completion_thinking_tokens",
    "inputTokens",
    "outputTokens",
    "totalTokens",
    "cacheReadTokens",
    "cacheWriteTokens",
    "cacheCreationTokens",
    "cacheCreationInputTokens",
    "cacheReadInputTokens",
    "cachedInputTokens",
    "cacheMissTokens",
    "thinkingTokens",
    "reasoningTokenCount",
    "cachedReadTokens",
    "tokensIn",
    "tokensOut",
    "cacheReads",
    "cacheWrites",
    "input",
    "output",
    "cached",
    "thoughts",
    "tool",
    "total",
    "cacheReadInputTokens",
    "cacheCreationInputTokens",
    "reasoningOutputTokens",
    "input_tokens_cache_read",
    "input_tokens_cache_write",
    "promptTokens",
    "completionTokens",
    "lastStepInputTokens",
    "lastStepOutputTokens",
    "lastStepCachedInputTokens",
    "inputTokensDetails/0/cached_tokens",
    "outputTokensDetails/0/reasoning_tokens",
    "prompt_tokens_details/cached_tokens",
    "completion_tokens_details/reasoning_tokens",
    "promptTokensDetails/cachedTokens",
    "completionTokensDetails/reasoningTokens",
    "tokens/input",
    "tokens/output",
    "tokens/cacheReadInputTokens",
    "tokens/cacheCreationInputTokens",
    "tokens/reasoningOutputTokens",
];
pub fn validate_raw_usage(value: &Value) -> bool {
    value
        .as_str()
        .filter(|s| s.len() <= 12_000)
        .and_then(|s| serde_json::from_str::<Value>(s).ok())
        .and_then(|v| {
            v.as_object().map(|m| {
                !m.is_empty()
                    && m.iter().all(|(k, v)| {
                        NUMERIC.contains(&k.as_str()) && (v.is_number() || v.is_null())
                    })
            })
        })
        .unwrap_or(false)
}
fn numeric(v: &Value) -> Value {
    let mut out = Map::new();
    for path in NUMERIC {
        if let Some(value) = v.pointer(&format!("/{path}")) {
            // Preserve malformed numeric values as a bounded null marker. They
            // remain uncertain evidence, never silently become zero tokens.
            out.insert(
                (*path).into(),
                if value.is_number() || value.is_null() {
                    value.clone()
                } else {
                    Value::Null
                },
            );
        }
    }
    Value::Object(out)
}

fn evidence(
    source: &str,
    row: &Value,
    usage: &Value,
    channel: &str,
    kind: &str,
    ordinal: usize,
) -> Option<DurableEvidence> {
    let raw = numeric(usage);
    if raw.as_object()?.is_empty() {
        return None;
    }
    let adapter = spec(source)?;
    let namespace = format!("unknown:{source}");
    let id = text(
        row,
        &[
            "/id",
            "/messageId",
            "/providerData/messageId",
            "/providerData/traceId",
            "/params/_meta/eventId",
            "/toMessageId",
        ],
    );
    let session = text(row, &["/sessionId", "/session_id", "/params/sessionId"]);
    let model = text(
        row,
        &[
            "/model",
            "/providerData/model",
            "/message/model",
            "/modelId",
            "/modelID",
        ],
    );
    let occurred = [
        "/timestamp",
        "/createdAt",
        "/ts",
        "/params/_meta/agentTimestampMs",
    ]
    .iter()
    .find_map(|p| row.pointer(p).and_then(time));
    let projection = json!({"channel":channel,"evidence_kind":kind,"pending_reason":"NATIVE_IDENTITY_OR_FINALITY_UNPROVED","raw_usage":raw.to_string(),"projection_version":1});
    let key = hash(
        json!([source, channel, id, session, model, occurred, raw, ordinal])
            .to_string()
            .as_bytes(),
    );
    Some(DurableEvidence {
        observation: Observation {
            identity_family: source.into(),
            namespace_key: namespace,
            identity_key: None,
            candidate_key: key,
            normalized_hash: vec![0; 32],
            record_key: format!("candidate:{ordinal}"),
            parser_version: adapter.parser_version.into(),
            identity_version: 1,
            semantics_version: 1,
            eligibility: "identity_unknown".into(),
            report_source: source.into(),
            native_tool: source.into(),
            native_message_id: id,
            native_session_id: session,
            model_key: model
                .as_ref()
                .map(|m| format!("n:{}", m.to_lowercase()))
                .unwrap_or_default(),
            model_display: model,
            occurred_ms: occurred,
            metrics: [None; 10],
            ..Observation::default()
        },
        projection: Some(projection),
    })
}

/// Whitelist a stream row before it enters a reader's working set/checkpoint.
pub fn project_stream(source: &str, v: &Value) -> Result<Option<Value>> {
    let evidence = project_line(source, v)?;
    if evidence.is_empty() {
        return Ok(None);
    }
    // Only serialised, already-whitelisted evidence passes through the shared
    // stream reader. Original message content never enters its parser context.
    Ok(Some(
        json!({"document_evidence":evidence.into_iter().map(|e|json!({
        "candidate_key":e.observation.candidate_key,"native_message_id":e.observation.native_message_id,
        "native_session_id":e.observation.native_session_id,"model":e.observation.model_display,
        "occurred_ms":e.observation.occurred_ms,"projection":e.projection
    })).collect::<Vec<_>>()}),
    ))
}
pub fn from_stream(source: &str, v: &Value, offset: u64) -> Result<Vec<DurableEvidence>> {
    let mut out = Vec::new();
    for (i, row) in v["document_evidence"]
        .as_array()
        .into_iter()
        .flatten()
        .enumerate()
    {
        let adapter = spec(source).ok_or(LedgerError::SourceNotProtected)?;
        out.push(DurableEvidence {
            observation: Observation {
                identity_family: source.into(),
                namespace_key: format!("unknown:{source}"),
                identity_key: None,
                candidate_key: row["candidate_key"]
                    .as_str()
                    .ok_or(LedgerError::IncompleteEvidence)?
                    .into(),
                normalized_hash: vec![0; 32],
                record_key: format!("byte:{offset}:part:{i}"),
                parser_version: adapter.parser_version.into(),
                identity_version: 1,
                semantics_version: 1,
                eligibility: "identity_unknown".into(),
                report_source: source.into(),
                native_tool: source.into(),
                native_message_id: row["native_message_id"].as_str().map(str::to_owned),
                native_session_id: row["native_session_id"].as_str().map(str::to_owned),
                model_key: row["model"]
                    .as_str()
                    .map(|m| format!("n:{}", m.to_lowercase()))
                    .unwrap_or_default(),
                model_display: row["model"].as_str().map(str::to_owned),
                occurred_ms: row["occurred_ms"].as_i64(),
                metrics: [None; 10],
                ..Observation::default()
            },
            projection: Some(row["projection"].clone()),
        });
    }
    Ok(out)
}
pub fn project_line(source: &str, v: &Value) -> Result<Vec<DurableEvidence>> {
    let mut out = Vec::new();
    let mut overflow = false;
    let mut add = |row: &Value, u: &Value, ch: &str, kind: &str| {
        if out.len() >= MAX_CANDIDATES {
            overflow = true;
            return;
        }
        if let Some(e) = evidence(source, row, u, ch, kind, out.len()) {
            out.push(e);
        }
    };
    match source {
        "workbuddy" | "codebuddy"
            if (v["type"] == "message" && v["role"] == "assistant")
                || v["type"] == "function_call" =>
        {
            for path in [
                "/providerData/rawUsage",
                "/providerData/usage",
                "/message/usage",
            ] {
                if let Some(u) = v.pointer(path) {
                    add(v, u, path, "message_usage");
                }
            }
        }
        "grok"
            if v.pointer("/params/update/sessionUpdate")
                .and_then(Value::as_str)
                == Some("turn_completed") =>
        {
            if let Some(u) = v.pointer("/params/update/usage") {
                if let Some(models) = u.get("modelUsage").and_then(Value::as_object) {
                    let mut row = v.clone();
                    for (model, usage) in models {
                        row["model"] = json!(model);
                        add(&row, usage, "modelUsage", "turn_usage");
                    }
                } else {
                    add(v, u, "usage", "turn_usage");
                }
            }
        }
        "qoder" if v["type"] == "assistant" => {
            if let Some(u) = v.pointer("/message/usage") {
                add(v, u, "cli", "message_usage");
            }
        }
        "gemini" if v["type"] == "gemini" => {
            if let Some(u) = v.get("tokens") {
                add(v, u, "message", "message_usage");
            }
        }
        _ => {}
    }
    if overflow {
        return Err(LedgerError::RecordTooLarge);
    }
    Ok(out)
}
pub fn project_document(source: &str, v: &Value) -> Result<Vec<DurableEvidence>> {
    let mut out = Vec::new();
    let mut overflow = false;
    let mut add = |row: &Value, u: &Value, ch: &str, kind: &str| {
        if out.len() >= MAX_CANDIDATES {
            overflow = true;
            return;
        }
        if let Some(e) = evidence(source, row, u, ch, kind, out.len()) {
            out.push(e);
        }
    };
    match source {
        "droid" => {
            if let Some(u) = v.get("tokenUsage") {
                add(v, u, "settings", "session_snapshot");
            }
        }
        "cline" => {
            for m in v["messages"].as_array().into_iter().flatten() {
                if let Some(u) = m.get("metrics") {
                    let mut row = m.clone();
                    row["sessionId"] = v["sessionId"].clone();
                    add(&row, u, "metrics", "message_usage");
                }
            }
        }
        "roocode" => {
            for m in v.as_array().into_iter().flatten() {
                if matches!(
                    m["say"].as_str(),
                    Some("api_req_started" | "api_req_deleted")
                ) {
                    if let Some(u) = m["text"]
                        .as_str()
                        .and_then(|s| serde_json::from_str::<Value>(s).ok())
                    {
                        add(m, &u, "ui_usage", "message_usage");
                    }
                }
            }
        }
        "codebuddy-ide" if v["role"] == "assistant" => {
            let extra = match &v["extra"] {
                Value::String(s) => serde_json::from_str(s).unwrap_or(Value::Null),
                other => other.clone(),
            };
            if let Some(u) = extra.get("statsSnapshot") {
                add(v, u, "statsSnapshot", "loop_snapshot");
            } else {
                add(v, &extra, "lastStep", "last_step");
            }
        }
        "codebuff" => {
            for m in v.as_array().into_iter().flatten() {
                if matches!(
                    m["role"].as_str().or(m["variant"].as_str()),
                    Some("assistant" | "agent" | "ai")
                ) {
                    for path in ["/metadata/usage", "/metadata/codebuff/usage"] {
                        if let Some(u) = m.pointer(path) {
                            add(m, u, path, "message_usage");
                        }
                    }
                    if let Some(history) = m
                        .pointer("/metadata/runState/sessionState/mainAgentState/messageHistory")
                        .and_then(Value::as_array)
                    {
                        for h in history {
                            if h["role"] == "assistant" {
                                for path in
                                    ["/providerOptions/usage", "/providerOptions/codebuff/usage"]
                                {
                                    if let Some(u) = h.pointer(path) {
                                        add(h, u, path, "message_usage");
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        "amp" => {
            for row in v
                .pointer("/usageLedger/events")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                add(row, row, "usageLedger", "usage_event");
            }
        }
        "gemini" => {
            for row in v["messages"].as_array().into_iter().flatten() {
                if let Some(u) = row.get("tokens") {
                    add(row, u, "messages", "message_usage");
                }
            }
            if let Some(models) = v.pointer("/stats/models").and_then(Value::as_object) {
                for (model, u) in models {
                    let row = json!({"model":model,"id":v["id"]});
                    add(
                        &row,
                        u.get("tokens").unwrap_or(u),
                        "stats.models",
                        "session_snapshot",
                    );
                }
            }
        }
        _ => {}
    }
    if overflow {
        return Err(LedgerError::RecordTooLarge);
    }
    Ok(out)
}

fn accepted(source: &str, p: &Path) -> bool {
    let name = p.file_name().and_then(|s| s.to_str()).unwrap_or("");
    match source {
        "cline" => name.ends_with(".messages.json"),
        "roocode" => name == "ui_messages.json",
        "codebuddy-ide" => {
            p.parent()
                .and_then(Path::file_name)
                .is_some_and(|s| s == "messages")
                && name.ends_with(".json")
        }
        "codebuff" => name == "chat-messages.json",
        "droid" => name.ends_with(".settings.json"),
        "amp" => name.ends_with(".json"),
        "gemini" => name.ends_with(".json") || name.ends_with(".jsonl"),
        _ => false,
    }
}

#[derive(Default)]
pub struct Collector {
    active: BTreeMap<(String, PathBuf), Image>,
    last: BTreeMap<(String, PathBuf), i64>,
    next_root: usize,
}
struct Image {
    object: DurableObject,
    rows: VecDeque<DurableEvidence>,
    index: usize,
    hash: String,
    safe_end: i64,
    gap: bool,
    incomplete_tail: bool,
}
#[derive(Default, Debug, Serialize)]
pub struct PollSummary {
    pub inserted: u64,
    pub duplicates: u64,
    pub pending: u64,
    pub errors: u64,
    pub backlog: bool,
    pub validation_bytes: u64,
}

fn bounded_read(path: &Path) -> Result<Vec<u8>> {
    use std::io::Read;
    let file = fs::File::open(path)?;
    let mut bytes = Vec::new();
    file.take(MAX_DOCUMENT + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_DOCUMENT {
        return Err(LedgerError::RecordTooLarge);
    }
    Ok(bytes)
}
fn reset_image(
    ledger: &mut Ledger,
    root: &super::durable::DurableRoot,
    old: &DurableObject,
    path: &Path,
) -> Result<DurableObject> {
    let stamp = crate::common::file_stamp(path).ok_or(LedgerError::IncompleteEvidence)?;
    let key: String = ledger.connection().query_row(
        "SELECT object_key FROM source_objects WHERE object_id=?1",
        [old.object_id],
        |r| r.get(0),
    )?;
    ledger.discover_durable_object(
        root,
        ObjectSpec {
            object_key: key,
            locator: path.to_string_lossy().into_owned(),
            content_generation: uuid::Uuid::new_v4().to_string(),
            byte_length: stamp.size() as i64,
            mtime_ns: Some(stamp.mtime_ns()),
        },
    )
}
fn image(source: &str, path: &Path, object: &DurableObject) -> Result<Image> {
    if object.byte_length as u64 > MAX_DOCUMENT {
        return Err(LedgerError::RecordTooLarge);
    }
    let before = crate::common::file_stamp(path).ok_or(LedgerError::StaleObservation)?;
    if before.size() != object.byte_length as u64 || Some(before.mtime_ns()) != object.mtime_ns {
        return Err(LedgerError::StaleObservation);
    }
    let bytes = bounded_read(path)?;
    let digest = hash(&bytes);
    let mut safe_end = object.byte_length;
    let mut gap = false;
    let mut incomplete_tail = false;
    let mut rows = if path.extension().is_some_and(|s| s == "jsonl") {
        let mut rows = Vec::new();
        safe_end = 0;
        for line in bytes.split_inclusive(|b| *b == b'\n') {
            if line.last() != Some(&b'\n') {
                incomplete_tail = true;
                break;
            }
            safe_end += line.len() as i64;
            if line.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            let v: Value = match serde_json::from_slice(line) {
                Ok(v) => v,
                Err(_) => {
                    gap = true;
                    continue;
                }
            };
            match project_line(source, &v) {
                Ok(projected) => {
                    if rows.len() + projected.len() > MAX_CANDIDATES {
                        return Err(LedgerError::RecordTooLarge);
                    }
                    rows.extend(projected);
                }
                Err(_) => gap = true,
            }
        }
        rows
    } else {
        let v: Value =
            serde_json::from_slice(&bytes).map_err(|_| LedgerError::IncompleteEvidence)?;
        project_document(source, &v)?
    };
    if bytes.len() != object.byte_length as usize || hash(&bounded_read(path)?) != digest {
        return Err(LedgerError::StaleObservation);
    }
    let mut index = 0;
    if let Some(cp) = object.checkpoint.as_ref().filter(|b| !b.is_empty()) {
        let cp: Value =
            serde_json::from_slice(cp).map_err(|_| LedgerError::CheckpointReplayRequired)?;
        if cp["hash"] != digest {
            return Err(LedgerError::CheckpointReplayRequired);
        }
        index = usize::try_from(
            cp["row"]
                .as_u64()
                .ok_or(LedgerError::CheckpointReplayRequired)?,
        )
        .map_err(|_| LedgerError::CheckpointReplayRequired)?;
    }
    if index > rows.len() {
        return Err(LedgerError::CheckpointReplayRequired);
    }
    for (i, e) in rows.iter_mut().enumerate() {
        e.observation.record_key = format!("record:{i}");
        if e.observation.native_message_id.is_none() {
            e.observation.candidate_key = hash(
                json!([
                    source,
                    path.to_string_lossy(),
                    i,
                    e.observation.candidate_key
                ])
                .to_string()
                .as_bytes(),
            );
        }
    }
    Ok(Image {
        object: object.clone(),
        rows: rows.into_iter().skip(index).collect(),
        index,
        hash: digest,
        safe_end,
        gap,
        incomplete_tail,
    })
}
impl Collector {
    pub fn invalidate_discovery(&mut self) {
        self.last.clear();
    }
    pub fn poll(
        &mut self,
        ledger: &mut Ledger,
        source: &str,
        roots: &[PathBuf],
        budget: usize,
    ) -> Result<PollSummary> {
        let spec = spec(source).ok_or(LedgerError::SourceNotProtected)?;
        let mut summary = PollSummary::default();
        let mut remaining = budget.clamp(1, 4096);
        let turn = self.next_root;
        self.next_root = self.next_root.wrapping_add(1);
        for offset in 0..roots.len() {
            let locator = &roots[(turn + offset) % roots.len()];
            let root =
                ledger.register_durable_root(&spec, &format!("unknown:{source}"), locator)?;
            if !root.enabled {
                self.active.retain(|(s, _), _| s != source);
                continue;
            }
            let key = (source.to_owned(), locator.clone());
            if self.last.get(&key).is_none_or(|t| now_ms() - *t >= 30_000) {
                if let Err(error) = fs::metadata(locator) {
                    if error.kind() == std::io::ErrorKind::NotFound {
                        // The whole registered scope is demonstrably absent.
                        // Resolve previously enumerated targets as gaps rather
                        // than leave the first import waiting forever. Other
                        // I/O failures must never certify source deletion.
                        ledger.mark_durable_missing(&root, &[])?;
                        ledger.finish_durable_enumeration(&root)?;
                        ledger.fail_durable_enumeration(&root, "SOURCE_DISAPPEARED_BEFORE_SAVE")?;
                        self.active
                            .retain(|_, image| image.object.root.root_id != root.root_id);
                    } else {
                        ledger.fail_durable_enumeration(&root, LedgerError::from(error).code())?;
                    }
                    self.last.insert(key, now_ms());
                    summary.errors += 1;
                    continue;
                }
                let discovered = (|| -> Result<()> {
                    // The walker treats missing optional directories as empty;
                    // a registered root is mandatory and must be checked first.
                    fs::metadata(locator)?;
                    let _ = crate::common::take_walk_error();
                    let scope = if source == "amp" && locator.join("threads").is_dir() {
                        locator.join("threads")
                    } else {
                        locator.clone()
                    };
                    let files = crate::common::walk_with(&scope, None, &|p| accepted(source, p));
                    if crate::common::take_walk_error() {
                        return Err(LedgerError::IncompleteEvidence);
                    }
                    let mut seen = Vec::new();
                    for path in files {
                        let path = path.canonicalize()?;
                        let object_key = hash(path.to_string_lossy().as_bytes());
                        seen.push(object_key.clone());
                        let stamp = crate::common::file_stamp(&path)
                            .ok_or(LedgerError::IncompleteEvidence)?;
                        type SavedDocumentState =
                            (String, i64, Option<i64>, bool, Option<Vec<u8>>, Option<i64>);
                        let old:Option<SavedDocumentState>=ledger.connection().query_row("SELECT content_generation,byte_length,mtime_ns,target_done,checkpoint,audit_due_ms FROM source_objects WHERE root_id=?1 AND object_key=?2",params![root.root_id,object_key],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?))).optional()?;
                        let same = old.as_ref().is_some_and(|(_, len, mtime, _, _, _)| {
                            *len == stamp.size() as i64 && *mtime == Some(stamp.mtime_ns())
                        });
                        let mut generation = old
                            .as_ref()
                            .filter(|_| same)
                            .map(|x| x.0.clone())
                            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
                        if let Some((_, _, _, true, cp, due)) = old.as_ref().filter(|_| same) {
                            if due.is_none_or(|t| t <= now_ms()) {
                                let bytes = bounded_read(&path)?;
                                summary.validation_bytes += bytes.len() as u64;
                                let digest = hash(&bytes);
                                let stored = cp
                                    .as_ref()
                                    .and_then(|b| serde_json::from_slice::<Value>(b).ok())
                                    .and_then(|v| v["hash"].as_str().map(str::to_owned));
                                if stored.as_deref() != Some(digest.as_str()) {
                                    generation = uuid::Uuid::new_v4().to_string()
                                } else {
                                    ledger.connection().execute("UPDATE source_objects SET audit_completed_ms=?3,audit_due_ms=?4 WHERE root_id=?1 AND object_key=?2",params![root.root_id,object_key,now_ms(),now_ms()+super::collection::HISTORY_AUDIT_INTERVAL_MS])?;
                                }
                            }
                        }
                        ledger.discover_durable_object(
                            &root,
                            ObjectSpec {
                                object_key,
                                locator: path.to_string_lossy().into_owned(),
                                content_generation: generation,
                                byte_length: stamp.size() as i64,
                                mtime_ns: Some(stamp.mtime_ns()),
                            },
                        )?;
                    }
                    ledger.mark_durable_missing(&root, &seen)?;
                    ledger.finish_durable_enumeration(&root)?;
                    Ok(())
                })();
                if let Err(e) = discovered {
                    ledger.fail_durable_enumeration(&root, e.code())?;
                    self.last.insert(key, now_ms());
                    summary.errors += 1;
                    continue;
                }
                self.last.insert(key, now_ms());
            }
            for mut object in ledger.next_durable_objects(&root, 4)? {
                if remaining == 0 {
                    summary.backlog = true;
                    break;
                }
                let path: PathBuf = ledger
                    .connection()
                    .query_row(
                        "SELECT locator FROM source_objects WHERE object_id=?1",
                        [object.object_id],
                        |r| r.get::<_, String>(0),
                    )?
                    .into();
                let key = (source.to_owned(), path.clone());
                if self.active.get(&key).is_some_and(|i| {
                    i.object.revision != object.revision
                        || i.object.epoch != object.epoch
                        || i.object.generation != object.generation
                }) {
                    self.active.remove(&key);
                }
                let changed = if let Some(i) = self.active.get(&key) {
                    match bounded_read(&path) {
                        Ok(b) => hash(&b) != i.hash,
                        Err(_) => true,
                    }
                } else {
                    false
                };
                if changed {
                    self.active.remove(&key);
                    match reset_image(ledger, &root, &object, &path) {
                        Ok(next) => object = next,
                        Err(e) => {
                            ledger.mark_durable_gap(&object, e.code())?;
                            summary.errors += 1;
                            continue;
                        }
                    }
                }
                if !self.active.contains_key(&key) {
                    let mut opened = image(source, &path, &object);
                    if matches!(
                        opened,
                        Err(LedgerError::CheckpointReplayRequired | LedgerError::StaleObservation)
                    ) {
                        opened = match reset_image(ledger, &root, &object, &path) {
                            Ok(next) => {
                                object = next;
                                image(source, &path, &object)
                            }
                            Err(e) => Err(e),
                        };
                    }
                    match opened {
                        Ok(image) => {
                            self.active.insert(key.clone(), image);
                        }
                        Err(e) => {
                            ledger.mark_durable_gap(&object, e.code())?;
                            summary.errors += 1;
                            continue;
                        }
                    }
                }
                let image = self.active.get_mut(&key).unwrap();
                let n = remaining.min(image.rows.len()).min(512);
                let batch: Vec<_> = image.rows.iter().take(n).cloned().collect();
                let drained = n == image.rows.len();
                let complete = drained && !image.incomplete_tail;
                let checkpoint =
                    serde_json::to_vec(&json!({"format":2,"hash":image.hash,"row":image.index+n}))
                        .unwrap();
                let end = if drained {
                    image.safe_end
                } else {
                    image.object.saved_offset
                };
                let saved = ledger.save_durable_evidence_batch_with_gap(
                    &mut image.object,
                    &batch,
                    end,
                    &checkpoint,
                    complete,
                    image.gap.then_some("SOURCE_RECORD_GAP"),
                )?;
                image.rows.drain(..n);
                image.index += n;
                remaining = remaining.saturating_sub(n.max(1));
                summary.inserted += saved.inserted;
                summary.duplicates += saved.duplicates;
                summary.pending += saved.pending;
                if drained {
                    if image.incomplete_tail {
                        // A partial append is not malformed evidence. Keep the
                        // saved complete prefix and retry only after a delay
                        // or a newly discovered source generation.
                        ledger.connection().execute(
                            "UPDATE source_objects SET retry_after_ms=?2 WHERE object_id=?1 AND object_revision=?3",
                            params![image.object.object_id, now_ms()+30_000, image.object.revision],
                        )?;
                    } else {
                        ledger.connection().execute("UPDATE source_objects SET audit_completed_ms=?2,audit_due_ms=?3 WHERE object_id=?1",params![image.object.object_id,now_ms(),now_ms()+super::collection::HISTORY_AUDIT_INTERVAL_MS])?;
                    }
                    self.active.remove(&key);
                }
            }
        }
        summary.backlog |= self.active.keys().any(|(s, _)| s == source);
        Ok(summary)
    }
}
