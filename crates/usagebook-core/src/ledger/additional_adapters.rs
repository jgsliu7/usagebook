//! Native evidence for Pi, OMP and OpenCode. Conversation bodies are never retained.
//! Eligible requests require native identities and explicit terminal usage; ambiguous
//! forks, in-progress messages and cumulative fallback rows remain saved candidates.
use super::durable::{AdapterSpec, DurableEvidence, DurableObject, DurableRoot, ObjectSpec};
use super::types::{identity_key, Observation};
use super::{Ledger, LedgerError, Result};
use rusqlite::{params, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
const MAX_LINE_BYTES: usize = super::claude::MAX_LINE_BYTES;
pub fn spec(source: &str) -> Option<AdapterSpec> {
    let (family, parser) = match source {
        "pi" => ("pi", "pi-ledger-v1"),
        "omp" => ("omp", "omp-ledger-v1"),
        "opencode" => ("opencode", "opencode-ledger-v1"),
        "kilo" => ("kilo", "kilo-ledger-v1"),
        _ => return None,
    };
    Some(AdapterSpec {
        identity_family: family,
        collector_key: "additional-native-v1",
        report_source: family,
        native_tool: family,
        parser_version: parser,
        capability_version: if matches!(source, "opencode" | "kilo") {
            "native-sqlite-incremental-v2"
        } else {
            "native-terminal-history-v1"
        },
        identity_version: 1,
        semantics_version: 1,
    })
}
pub fn namespace(source: &str) -> String {
    format!("unknown:{source}")
}
fn digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
fn text(v: &Value, p: &str) -> Option<String> {
    v.pointer(p)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty() && s.len() <= 4096 && !s.contains('\0'))
        .map(str::to_owned)
}
fn integer(v: &Value, p: &str) -> Result<Option<i64>> {
    match v.pointer(p) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(n)) => n
            .as_u64()
            .and_then(|n| i64::try_from(n).ok())
            .map(Some)
            .ok_or(LedgerError::InvalidUsageNumber),
        _ => Err(LedgerError::InvalidUsageNumber),
    }
}
fn timestamp(v: &Value, p: &str) -> Option<i64> {
    let v = v.pointer(p)?;
    if let Some(s) = v.as_str() {
        return chrono::DateTime::parse_from_rfc3339(s)
            .ok()
            .map(|t| t.timestamp_millis());
    }
    let n = v.as_i64().filter(|n| *n >= 0)?;
    if n < 1_000_000_000_000 {
        n.checked_mul(1000)
    } else {
        Some(n)
    }
}
fn base(source: &str, model: Option<String>) -> Observation {
    Observation {
        identity_family: source.into(),
        namespace_key: namespace(source),
        parser_version: spec(source).expect("adapter").parser_version.into(),
        report_source: source.into(),
        native_tool: source.into(),
        model_key: super::legacy::model_key(model.as_deref().unwrap_or("")),
        model_display: model,
        ..Observation::default()
    }
}
fn finish(mut o: Observation, projection: Value, offset: u64) -> Result<DurableEvidence> {
    if o.identity_key.is_none() && o.eligibility == "eligible" {
        o.eligibility = "identity_unknown".into();
    }
    if o.occurred_ms.is_none() && o.eligibility == "eligible" {
        o.eligibility = "time_unknown".into();
    }
    let bytes = serde_json::to_vec(&(&o, &projection)).map_err(|_| LedgerError::RecordTooLarge)?;
    o.candidate_key = identity_key(
        &o.identity_family,
        &o.namespace_key,
        "evidence",
        &[&digest(&bytes)],
    );
    o.record_key = format!("byte:{offset}");
    o.normalized_hash = super::claude::normalized_hash(&o)?;
    Ok(DurableEvidence {
        observation: o,
        projection: Some(projection),
    })
}
#[derive(Clone, Default, Serialize, Deserialize)]
struct SessionContext {
    session: Option<String>,
    parent_session: Option<String>,
    version: Option<i64>,
    header_conflict: bool,
}
impl SessionContext {
    fn consume(&mut self, v: &Value) {
        if v["type"] == "session" {
            let next = text(v, "/id");
            if self.session.is_some() && self.session != next {
                self.header_conflict = true;
            }
            self.session = next;
            self.parent_session = text(v, "/parentSession");
            self.version = v["version"].as_i64();
        }
    }
}
#[derive(Clone)]
struct Row {
    offset: u64,
    value: Value,
}
fn project_value(_source: &str, v: &Value) -> Option<Value> {
    if !matches!(v["type"].as_str(), Some("session" | "message")) {
        return None;
    }
    Some(crate::common::usage_projection(
        v,
        &[
            "/type",
            "/id",
            "/parentSession",
            "/version",
            "/timestamp",
            "/message/role",
            "/message/model",
            "/message/provider",
            "/message/responseId",
            "/message/stopReason",
            "/message/timestamp",
            "/message/usage/input",
            "/message/usage/cacheRead",
            "/message/usage/cacheWrite",
            "/message/usage/cacheWrite1h",
            "/message/usage/output",
            "/message/usage/reasoning",
            "/message/usage/reasoningTokens",
            "/message/usage/totalTokens",
        ],
    ))
}
fn is_usage_record(_source: &str, v: &Value) -> bool {
    v["type"] == "message"
        && v.pointer("/message/role").and_then(Value::as_str) == Some("assistant")
        && v.pointer("/message/usage").is_some()
}
pub fn project_pi_message(
    source: &str,
    session: Option<&str>,
    parent_session: Option<&str>,
    version: Option<i64>,
    v: &Value,
) -> Result<Option<DurableEvidence>> {
    let ctx = SessionContext {
        session: session.map(str::to_owned),
        parent_session: parent_session.map(str::to_owned),
        version,
        header_conflict: false,
    };
    parse_pi(source, &ctx, v, 0)
}
fn parse_pi(
    source: &str,
    ctx: &SessionContext,
    v: &Value,
    offset: u64,
) -> Result<Option<DurableEvidence>> {
    if !is_usage_record(source, v) {
        return Ok(None);
    }
    let entry = text(v, "/id");
    let response = text(v, "/message/responseId");
    let provider = text(v, "/message/provider");
    let stop = text(v, "/message/stopReason");
    let mut o = base(source, text(v, "/message/model"));
    o.occurred_ms = timestamp(v, "/message/timestamp").or_else(|| timestamp(v, "/timestamp"));
    o.native_session_id = ctx.session.clone();
    o.native_message_id = entry.clone();
    o.native_request_id = response.clone();
    o.session_key = ctx
        .session
        .as_ref()
        .map(|s| identity_key(source, &namespace(source), "session", &[s]));
    let u = v
        .pointer("/message/usage")
        .ok_or(LedgerError::IncompleteEvidence)?;
    for (i, p) in [
        (0, "/input"),
        (1, "/cacheRead"),
        (2, "/cacheWrite"),
        (6, "/output"),
        (8, "/cacheWrite1h"),
        (7, "/totalTokens"),
    ] {
        o.metrics[i] = integer(u, p)?;
    }
    o.metrics[4] = integer(u, "/reasoning")?.or(integer(u, "/reasoningTokens")?);
    if let (Some(output), Some(reasoning)) = (o.metrics[6], o.metrics[4]) {
        o.metrics[3] = Some(
            output
                .checked_sub(reasoning)
                .filter(|n| *n >= 0)
                .ok_or(LedgerError::MetricConflict)?,
        );
    }
    o.metrics[9] = Some(1);
    super::metrics::evaluate(&o.metrics)?;
    if o.metrics[..9].iter().flatten().all(|n| *n == 0) {
        return Ok(None);
    }
    let terminal = stop.as_deref().is_some_and(|s| {
        matches!(
            s,
            "stop"
                | "length"
                | "toolUse"
                | "tool_use"
                | "error"
                | "aborted"
                | "end_turn"
                | "max_tokens"
        )
    });
    let mut reason = None;
    if ctx.header_conflict || ctx.version != Some(3) {
        reason = Some("SESSION_PROTOCOL_UNKNOWN");
    } else if !terminal {
        reason = Some("MESSAGE_FINALITY_UNKNOWN");
    } else if let (Some(response), Some(provider)) = (&response, &provider) {
        // A provider request copied into a fork has one consumption identity but
        // multiple session containers. Preserve containers as evidence, without
        // inventing an authoritative session for the published request.
        o.session_key = None;
        // This is a provider request identity. The local entry ID belongs to
        // its container and is retained in source_entry_id, not the full tuple.
        o.native_message_id = None;
        o.identity_key = Some(identity_key(
            source,
            &namespace(source),
            "response",
            &[provider, response],
        ));
    } else if ctx.parent_session.is_some() {
        reason = Some("FORK_OWNERSHIP_UNKNOWN");
    } else if let (Some(session), Some(entry)) = (&ctx.session, &entry) {
        o.identity_key = Some(identity_key(
            source,
            &namespace(source),
            "entry",
            &[session, entry],
        ));
    } else {
        reason = Some("NATIVE_IDENTITY_UNKNOWN");
    }
    if reason.is_some() {
        o.eligibility = "identity_unknown".into();
    }
    let projection = json!({"channel":"session-message","source_session_id":ctx.session,"source_parent_session":ctx.parent_session,"source_entry_id":entry,"source_record_id":response,"source_agent_id":provider,"upstream_finish":stop,"protocol_version":ctx.version,"pending_reason":reason,"raw_usage_hash":digest(serde_json::to_string(u).map_err(|_|LedgerError::RecordTooLarge)?.as_bytes())});
    Ok(Some(finish(o, projection, offset)?))
}
fn discover_files(source: &str, roots: &[PathBuf]) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    let mut seen = HashSet::new();
    for root in roots {
        if source == "pi" {
            let omp = crate::common::home().join(".omp");
            let omp = omp.canonicalize().unwrap_or(omp);
            let physical = root.canonicalize().unwrap_or_else(|_| root.clone());
            if physical.starts_with(omp) {
                continue;
            }
        }
        let scope = if root.join("sessions").is_dir() {
            root.join("sessions")
        } else {
            root.clone()
        };
        if !scope.is_dir() {
            if scope.exists() {
                return Err(LedgerError::InvalidInput(
                    "source is not a directory".into(),
                ));
            }
            continue;
        }
        let _ = crate::common::take_walk_error();
        let found = crate::common::walk_with(&scope, None, &|p| {
            p.extension().is_some_and(|e| e == "jsonl")
        });
        if crate::common::take_walk_error() {
            return Err(LedgerError::IncompleteEvidence);
        }
        for p in found {
            let p = p.canonicalize()?;
            if seen.insert(p.clone()) {
                paths.push(p);
            }
        }
    }
    if !matches!(source, "pi" | "omp") {
        return Err(LedgerError::SourceNotProtected);
    }
    paths.sort();
    Ok(paths)
}

#[derive(Clone, Serialize, Deserialize)]
struct TranscriptCheckpoint {
    version: u32,
    source: String,
    offset: u64,
    prefix_sha256: String,
    context: SessionContext,
    gap: bool,
}

pub struct TranscriptBatch {
    pub evidence: Vec<DurableEvidence>,
    pub safe_offset: i64,
    pub checkpoint: Vec<u8>,
    pub complete: bool,
    pub at_target: bool,
    pub scanned_lines: u64,
    pub diagnostics: Vec<String>,
}
/// A fixed read target; an append beyond target_length is left for the next pass.
/// Checkpoints contain native protocol context and a digest of every committed
/// byte, so truncation/rewrite causes a replay rather than guessed continuation.
pub struct TranscriptReader {
    source: String,
    path: PathBuf,
    reader: BufReader<File>,
    target_length: u64,
    offset: u64,
    hasher: Sha256,
    context: SessionContext,
    stamp: crate::common::FileStamp,
    gap: bool,
}
impl TranscriptReader {
    pub fn open(
        source: &str,
        path: &Path,
        target_length: u64,
        checkpoint: Option<&[u8]>,
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
                serde_json::from_slice::<TranscriptCheckpoint>(c)
                    .map_err(|_| LedgerError::CheckpointReplayRequired)
            })
            .transpose()?;
        let mut reader = BufReader::new(File::open(path)?);
        let mut hasher = Sha256::new();
        let mut offset = 0;
        let mut context = SessionContext::default();
        let mut gap = false;
        if let Some(saved) = restored {
            if saved.version != 1 || saved.source != source || saved.offset > target_length {
                return Err(LedgerError::CheckpointReplayRequired);
            }
            let mut remaining = saved.offset;
            let mut buf = [0u8; 64 * 1024];
            while remaining > 0 {
                let count = remaining.min(buf.len() as u64) as usize;
                let n = reader.read(&mut buf[..count])?;
                if n == 0 {
                    return Err(LedgerError::StaleObservation);
                }
                hasher.update(&buf[..n]);
                remaining -= n as u64;
            }
            if hash_hex(&hasher) != saved.prefix_sha256 {
                return Err(LedgerError::StaleObservation);
            }
            offset = saved.offset;
            context = saved.context;
            gap = saved.gap;
        }
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
        })
    }
    pub fn read_batch(&mut self, limit: usize) -> Result<TranscriptBatch> {
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
                Ok(value) => {
                    if let Some(value) = project_value(&self.source, &value) {
                        if is_usage_record(&self.source, &value) {
                            candidates += 1;
                        }
                        rows.push(Row { offset, value });
                    }
                }
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
        // The normal immutable historical file avoids repeated prefix I/O. A
        // concurrent append changes the stamp, so verify all committed bytes;
        // a matching prefix is safe even when the tail continues to grow.
        if self.stamp != after {
            let mut verify = File::open(&self.path)?.take(self.offset);
            let mut hash = Sha256::new();
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
            if read != self.offset || hash_hex(&hash) != hash_hex(&self.hasher) {
                return Err(LedgerError::StaleObservation);
            }
        }
        self.stamp = after;
        let mut evidence = Vec::new();
        for row in rows {
            if row.value["type"] == "session" {
                self.context.consume(&row.value);
                continue;
            }
            match parse_pi(&self.source, &self.context, &row.value, row.offset) {
                Ok(Some(value)) => evidence.push(value),
                Ok(None) => {}
                Err(error) => diagnostics.push(error.code().into()),
            }
        }
        self.gap |= diagnostics.iter().any(|d| d != "INCOMPLETE_TAIL");
        if self.gap {
            diagnostics.push("NATIVE_RECORD_GAP".into());
        }
        let checkpoint = serde_json::to_vec(&TranscriptCheckpoint {
            version: 1,
            source: self.source.clone(),
            offset: self.offset,
            prefix_sha256: hash_hex(&self.hasher),
            context: self.context.clone(),
            gap: self.gap,
        })
        .map_err(|_| LedgerError::InvalidInput("native checkpoint encoding failed".into()))?;
        if checkpoint.len() > 65536 {
            return Err(LedgerError::RecordTooLarge);
        }
        diagnostics.sort();
        diagnostics.dedup();
        Ok(TranscriptBatch {
            evidence,
            safe_offset: self.offset as i64,
            checkpoint,
            complete: self.offset == self.target_length && !self.gap,
            at_target: self.offset == self.target_length || tail,
            scanned_lines: lines,
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

struct FileImport {
    reader: TranscriptReader,
    object: DurableObject,
    pending: Option<TranscriptBatch>,
}
#[derive(Default)]
pub struct TranscriptCollector {
    sessions: HashMap<(String, PathBuf), FileImport>,
    /// Avoid rehashing unchanged completed files every poll. A restart validates
    /// their persisted prefix again; periodic audits also expire this hint.
    validated: HashMap<(String, PathBuf), (crate::common::FileStamp, i64)>,
    scopes: HashMap<String, DiscoveryScope>,
}
struct DiscoveryScope {
    locators: Vec<PathBuf>,
    roots: Vec<DurableRoot>,
    discovered_files: usize,
    next_discovery_ms: i64,
    audits: std::collections::VecDeque<(PathBuf, DurableObject)>,
}
#[derive(Debug, Default, Serialize)]
pub struct PollSummary {
    pub discovered_files: usize,
    pub read_lines: u64,
    pub inserted: u64,
    pub duplicates: u64,
    pub suppressed: u64,
    pub pending: u64,
    pub completed_files: u64,
    pub active_files: usize,
    pub backlog: usize,
    pub errors: u64,
    pub diagnostics: Vec<String>,
}
impl TranscriptCollector {
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
        let discover = self
            .scopes
            .get(source)
            .is_none_or(|s| s.locators != roots || s.next_discovery_ms <= now);
        if discover {
            let mut scope = DiscoveryScope {
                locators: roots.to_vec(),
                roots: Vec::new(),
                discovered_files: 0,
                next_discovery_ms: now + 30_000,
                audits: std::collections::VecDeque::new(),
            };
            for locator in roots {
                let root = ledger.register_durable_root(&adapter, &namespace(source), locator)?;
                scope.roots.push(root.clone());
                if !root.enabled {
                    self.sessions
                        .retain(|_, s| s.object.root.root_id != root.root_id);
                    continue;
                }
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
                    let recent = self
                        .validated
                        .get(&state_key)
                        .is_some_and(|(old, time)| old == &stamp && now - *time < 3_600_000);
                    if recent {
                        continue;
                    }
                    let object_key = digest(path.to_string_lossy().as_bytes());
                    let old:Option<(String,i64)>=ledger.conn.query_row("SELECT content_generation,byte_length FROM source_objects WHERE root_id=?1 AND object_key=?2",params![root.root_id,object_key],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
                    let generation = old
                        .filter(|(_, len)| *len <= stamp.size() as i64)
                        .map(|(g, _)| g)
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
        // One audit lane plus the core's fresh/oldest alternating history queue.
        if let Some(audit) = scope.audits.pop_front() {
            work.push(audit);
        }
        for root in &scope.roots {
            if work.len() >= 4 {
                break;
            }
            for object in ledger.next_durable_objects(root, 4 - work.len())? {
                let path: String = ledger.conn.query_row(
                    "SELECT locator FROM source_objects WHERE object_id=?1",
                    [object.object_id],
                    |r| r.get(0),
                )?;
                if !work
                    .iter()
                    .any(|(_, old): &(PathBuf, DurableObject)| old.object_id == object.object_id)
                {
                    work.push((PathBuf::from(path), object));
                }
            }
        }
        let mut remaining = budget;
        for (path, mut object) in work {
            if remaining == 0 {
                break;
            }
            let state_key = (source.to_owned(), path.clone());
            if !self.sessions.contains_key(&state_key) {
                let mut opened = TranscriptReader::open(
                    source,
                    &path,
                    object.byte_length as u64,
                    object.checkpoint.as_deref(),
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
                    opened = TranscriptReader::open(source, &path, object.byte_length as u64, None);
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
                match current.reader.read_batch(remaining.min(512)) {
                    Ok(batch) => current.pending = Some(batch),
                    Err(error) => {
                        ledger.fail_durable_object(&current.object, error.code())?;
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
            let saved = ledger.save_durable_evidence_batch_with_gap(
                &mut current.object,
                &batch.evidence,
                batch.safe_offset,
                &batch.checkpoint,
                batch.complete
                    || (batch.at_target
                        && !batch.diagnostics.iter().any(|d| d == "INCOMPLETE_TAIL")),
                batch
                    .diagnostics
                    .iter()
                    .any(|d| d != "INCOMPLETE_TAIL")
                    .then_some("NATIVE_RECORD_GAP"),
            )?;
            summary.inserted += saved.inserted;
            summary.duplicates += saved.duplicates;
            summary.suppressed += saved.suppressed;
            summary.pending += saved.pending;
            summary.read_lines += batch.scanned_lines;
            remaining = remaining.saturating_sub(batch.evidence.len().max(1));
            summary.diagnostics.extend(batch.diagnostics.clone());
            let at_target = batch.at_target;
            let complete = batch.complete;
            let tail = batch.diagnostics.iter().any(|d| d == "INCOMPLETE_TAIL");
            let has_error = batch.diagnostics.iter().any(|d| d != "INCOMPLETE_TAIL");
            current.pending = None;
            if has_error {
                // The gap and evidence cursor were committed together above.
                summary.errors += 1;
            } else if tail {
                // A legal unfinished append is waiting work, not a failed read.
                ledger.conn.execute("UPDATE source_objects SET retry_after_ms=?2 WHERE object_id=?1 AND object_revision=?3",params![current.object.object_id,now+30_000,current.object.revision])?;
            }
            if at_target {
                if complete {
                    if let Some(stamp) = crate::common::file_stamp(&path) {
                        if stamp.size() == current.object.byte_length as u64 {
                            self.validated.insert(state_key.clone(), (stamp, now));
                        } else {
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

// SQLite passes preserve their initial row keys in the destination. Every external
// read transaction ends before a destination write or a scheduler yield.
#[derive(Clone, Serialize, Deserialize)]
struct DbManifest {
    version: u32,
    source: String,
    table: String,
    keys: Vec<String>,
    ambiguous_channel: bool,
    gap: bool,
}
#[derive(Default)]
struct DbCollector {
    next_scan: HashMap<(String, PathBuf), i64>,
    next_root: HashMap<String, usize>,
}
#[derive(Default)]
pub struct Collector {
    transcripts: TranscriptCollector,
    databases: DbCollector,
    json: JsonCollector,
}
impl Collector {
    pub fn invalidate_discovery(&mut self) {
        self.transcripts.invalidate_discovery();
        self.databases.next_scan.clear();
        self.json.next_scan.clear();
    }
    pub fn poll(
        &mut self,
        ledger: &mut Ledger,
        source: &str,
        roots: &[PathBuf],
        budget: usize,
    ) -> Result<PollSummary> {
        match source {
            "pi" | "omp" => self.transcripts.poll(ledger, source, roots, budget),
            "opencode" | "kilo" => {
                let mut summary = self.databases.poll(ledger, source, roots, budget)?;
                let remaining = budget.saturating_sub(summary.read_lines as usize);
                if remaining > 0 {
                    let other = self.json.poll(ledger, source, roots, remaining)?;
                    summary.inserted += other.inserted;
                    summary.duplicates += other.duplicates;
                    summary.suppressed += other.suppressed;
                    summary.pending += other.pending;
                    summary.read_lines += other.read_lines;
                    summary.completed_files += other.completed_files;
                    summary.backlog += other.backlog;
                    summary.errors += other.errors;
                    summary.diagnostics.extend(other.diagnostics);
                }
                Ok(summary)
            }
            _ => Err(LedgerError::SourceNotProtected),
        }
    }
}
fn database_paths(source: &str, roots: &[PathBuf]) -> Result<Vec<(PathBuf, bool)>> {
    let mut paths = BTreeSet::new();
    for root in roots {
        if root.is_file() {
            paths.insert(root.canonicalize()?);
            continue;
        }
        if !root.exists() {
            continue;
        }
        for entry in std::fs::read_dir(root)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let matches = if source == "kilo" {
                name == "kilo.db"
            } else {
                name == "opencode.db" || (name.starts_with("opencode-") && name.ends_with(".db"))
            };
            if matches && entry.path().is_file() {
                paths.insert(entry.path().canonicalize()?);
            }
        }
    }
    Ok(paths
        .iter()
        .map(|p| {
            let ambiguous = p.file_name().is_some_and(|n| n == "opencode-next.db")
                && paths
                    .iter()
                    .any(|other| other != p && other.parent() == p.parent());
            (p.clone(), ambiguous)
        })
        .collect())
}
// The old OpenCode reader includes its configured directory in the identity
// domain. A DB symlink escaping that directory cannot use the physical parent
// as a substitute. Keep collecting evidence, but fail the compatibility view
// closed until that alias provenance can be represented without changing keys.
fn escaped_opencode_reader_root(path: &Path, roots: &[PathBuf]) -> Result<bool> {
    for root in roots.iter().filter(|root| root.is_dir()) {
        let root = root.canonicalize()?;
        if path.parent() == Some(root.as_path()) {
            continue;
        }
        for entry in std::fs::read_dir(&root)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if (name == "opencode.db" || (name.starts_with("opencode-") && name.ends_with(".db")))
                && entry
                    .path()
                    .canonicalize()
                    .is_ok_and(|candidate| candidate == path)
            {
                return Ok(true);
            }
        }
    }
    Ok(false)
}
fn open_database(path: &Path) -> Result<rusqlite::Connection> {
    if !path.is_file() {
        return Err(LedgerError::IncompleteEvidence);
    }
    let conn = rusqlite::Connection::open_with_flags(
        path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    conn.busy_timeout(std::time::Duration::from_millis(50))?;
    conn.execute_batch("PRAGMA query_only=ON; BEGIN DEFERRED;")?;
    Ok(conn)
}
fn table_columns(conn: &rusqlite::Connection, table: &str) -> Result<HashSet<String>> {
    // table names are adapter literals, never source-controlled SQL.
    if !matches!(
        table,
        "message" | "session_message" | "session" | "session_v2"
    ) {
        return Err(LedgerError::UnsupportedVersion);
    }
    let mut statement = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let values = statement
        .query_map([], |r| r.get::<_, String>(1))?
        .collect::<std::result::Result<HashSet<_>, _>>()?;
    Ok(values)
}
fn backend(conn: &rusqlite::Connection, source: &str) -> Result<Option<String>> {
    let tables: &[(&str, &[&str])] = if source == "kilo" {
        &[("message", &["id", "session_id", "data"])]
    } else {
        &[
            (
                "session_message",
                &["id", "session_id", "data", "type", "seq", "time_created"],
            ),
            ("message", &["id", "session_id", "data"]),
            (
                "session",
                &[
                    "id",
                    "tokens_input",
                    "tokens_output",
                    "tokens_reasoning",
                    "tokens_cache_read",
                    "tokens_cache_write",
                    "time_created",
                ],
            ),
        ]
    };
    for (table, required) in tables {
        let cols = table_columns(conn, table)?;
        if cols.is_empty() {
            continue;
        }
        if required.iter().any(|c| !cols.contains(*c)) {
            return Err(LedgerError::UnsupportedVersion);
        }
        let mut info = conn.prepare(&format!("PRAGMA table_info({table})"))?;
        let key_columns = info
            .query_map([], |r| Ok((r.get::<_, String>(1)?, r.get::<_, i64>(5)?)))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        if key_columns.iter().filter(|(_, pk)| *pk > 0).count() != 1
            || !key_columns
                .iter()
                .any(|(name, pk)| name == "id" && *pk == 1)
        {
            return Err(LedgerError::UnsupportedVersion);
        }
        let count: i64 = conn.query_row(
            &format!("SELECT EXISTS(SELECT 1 FROM {table} LIMIT 1)"),
            [],
            |r| r.get(0),
        )?;
        if count > 0 {
            return Ok(Some((*table).into()));
        }
    }
    Ok(None)
}
fn stage_database(
    ledger: &mut Ledger,
    root: &DurableRoot,
    path: &Path,
    source: &str,
    ambiguous: bool,
) -> Result<()> {
    let conn = open_database(path)?;
    let selected = backend(&conn, source)?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    conn.progress_handler(1000, Some(move || std::time::Instant::now() >= deadline))?;
    super::source_projection::begin(ledger.connection(), root)?;
    for table in ["session_message", "message", "session", "session_v2"] {
        super::source_projection::capture_rows(ledger.connection(), root, &conn, table, &[], &[])?;
    }
    let mut manifests = Vec::new();
    if let Some(table) = selected {
        let mut stmt = conn.prepare(&format!("SELECT id FROM {table} ORDER BY id"))?;
        let mut rows = stmt.query([])?;
        let mut keys = Vec::new();
        let mut bytes = 0usize;
        let mut manifest_bytes = 0usize;
        while let Some(row) = rows.next()? {
            let id: String = row.get(0)?;
            if id.is_empty() || id.len() > 4096 || id.contains('\0') {
                return Err(LedgerError::IncompleteEvidence);
            }
            manifest_bytes += id.len() + std::mem::size_of::<String>();
            if manifest_bytes > 64 * 1024 * 1024 || std::time::Instant::now() >= deadline {
                return Err(LedgerError::RecordTooLarge);
            }
            if !keys.is_empty() && (keys.len() >= 256 || bytes + id.len() > 24 * 1024) {
                manifests.push(DbManifest {
                    version: 1,
                    source: source.into(),
                    table: table.clone(),
                    keys: std::mem::take(&mut keys),
                    ambiguous_channel: ambiguous,
                    gap: false,
                });
                bytes = 0;
            }
            bytes += id.len();
            keys.push(id);
        }
        if !keys.is_empty() {
            manifests.push(DbManifest {
                version: 1,
                source: source.into(),
                table,
                keys,
                ambiguous_channel: ambiguous,
                gap: false,
            });
        }
    }
    drop(conn);
    let generation = uuid::Uuid::new_v4().to_string();
    for manifest in manifests {
        let keys = serde_json::to_vec(&manifest.keys).map_err(|_| LedgerError::RecordTooLarge)?;
        let checkpoint = serde_json::to_vec(&manifest).map_err(|_| LedgerError::RecordTooLarge)?;
        ledger.stage_durable_manifest(
            root,
            ObjectSpec {
                object_key: format!("native-db-v1:{}:{}", manifest.table, digest(&keys)),
                locator: path.to_string_lossy().into_owned(),
                content_generation: generation.clone(),
                byte_length: manifest.keys.len() as i64,
                mtime_ns: None,
            },
            &checkpoint,
        )?;
    }
    super::local_adapters::retire_unrecoverable_db_checkpoints(ledger.connection(), root)?;
    ledger.finish_durable_enumeration(root)?;
    Ok(())
}
fn pending_database_objects(ledger: &Ledger, root: &DurableRoot) -> Result<i64> {
    Ok(ledger.connection().query_row("SELECT COUNT(*) FROM source_objects WHERE root_id=?1 AND object_key LIKE 'native-db-v1:%' AND target_done=0 AND checkpoint IS NOT NULL AND length(checkpoint)>0",[&root.root_id],|r|r.get(0))?)
}
impl DbCollector {
    fn poll(
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
        let mut summary = PollSummary::default();
        let mut paths = database_paths(source, roots)?;
        // Persisted unfinished roots are also visited when their source was removed.
        let mut stmt = ledger.connection().prepare(
            "SELECT DISTINCT locator FROM source_roots WHERE collector_key=?1 AND report_source=?2",
        )?;
        let saved = stmt
            .query_map(params!["additional-native-v1", source], |r| {
                r.get::<_, String>(0)
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        drop(stmt);
        for path in saved {
            let path = PathBuf::from(path);
            if !paths.iter().any(|(p, _)| *p == path) {
                paths.push((path, false));
            }
        }
        let now = super::types::now_ms();
        let mut left = budget;
        if !paths.is_empty() {
            let start = *self.next_root.get(source).unwrap_or(&0) % paths.len();
            paths.rotate_left(start);
            self.next_root.insert(source.to_owned(), start + 1);
        }
        for (path, ambiguous) in paths {
            let unsupported_reader_domain =
                source == "opencode" && escaped_opencode_reader_root(&path, roots)?;
            let root = ledger.register_durable_root(
                &spec(source).ok_or(LedgerError::SourceNotProtected)?,
                &namespace(source),
                &path,
            )?;
            if !root.enabled {
                continue;
            }
            summary.discovered_files += 1;
            let key = (source.to_owned(), path.clone());
            super::source_projection::ensure(ledger.connection(), &root)?;
            if !path.is_file() {
                super::local_adapters::retire_unrecoverable_db_checkpoints(
                    ledger.connection(),
                    &root,
                )?;
            }
            // New physical rows are independent of their consumption date.
            let fresh_budget =
                if left < 4 && !super::collection::history_audit_due(ledger.connection(), &root)? {
                    left
                } else {
                    (left / 4).min(128)
                };
            if fresh_budget > 0 && path.is_file() {
                match save_database_recent(
                    ledger,
                    &root,
                    &path,
                    source,
                    ambiguous,
                    fresh_budget,
                    &mut summary,
                ) {
                    Ok(n) => left = left.saturating_sub(n),
                    Err(error) => {
                        summary.errors += 1;
                        summary.diagnostics.push(error.code().into());
                    }
                }
            }
            let mut audit_active = pending_database_objects(ledger, &root)? > 0;
            if !audit_active
                && super::collection::history_audit_due(ledger.connection(), &root)?
                && self.next_scan.get(&key).is_none_or(|t| *t <= now)
            {
                match stage_database(ledger, &root, &path, source, ambiguous) {
                    Ok(()) => {
                        audit_active = true;
                        self.next_scan.insert(key.clone(), now + 30_000);
                    }
                    Err(error) => {
                        ledger.fail_durable_enumeration(&root, error.code())?;
                        summary.errors += 1;
                        summary.diagnostics.push(error.code().into());
                        continue;
                    }
                }
            }
            for mut object in ledger.next_durable_objects(&root, 64)? {
                if left == 0 {
                    break;
                }
                let mut manifest: DbManifest = serde_json::from_slice(
                    object
                        .checkpoint
                        .as_deref()
                        .ok_or(LedgerError::IncompleteEvidence)?,
                )
                .map_err(|_| LedgerError::CheckpointReplayRequired)?;
                if manifest.version != 1 || manifest.source != source {
                    return Err(LedgerError::CheckpointReplayRequired);
                }
                let start = usize::try_from(object.saved_offset)
                    .map_err(|_| LedgerError::NumericOverflow)?;
                if start > manifest.keys.len() {
                    return Err(LedgerError::CheckpointReplayRequired);
                }
                let end = (start + left.min(256)).min(manifest.keys.len());
                let mut evidence = Vec::new();
                match read_database_batch(ledger, &root, &path, &manifest, start, end) {
                    Ok((entries, gap)) => {
                        evidence = entries;
                        manifest.gap |= gap;
                    }
                    Err(error) if !path.exists() => {
                        manifest.gap = true;
                        summary.errors += 1;
                        summary.diagnostics.push(error.code().into());
                    }
                    Err(error) => {
                        ledger.fail_durable_object(&object, error.code())?;
                        summary.errors += 1;
                        summary.diagnostics.push(error.code().into());
                        continue;
                    }
                }
                let checkpoint =
                    serde_json::to_vec(&manifest).map_err(|_| LedgerError::RecordTooLarge)?;
                let saved = ledger.save_durable_evidence_batch_with_gap(
                    &mut object,
                    &evidence,
                    end as i64,
                    &checkpoint,
                    end == manifest.keys.len(),
                    manifest.gap.then_some("NATIVE_DATABASE_EVIDENCE_GAP"),
                )?;
                summary.inserted += saved.inserted;
                summary.duplicates += saved.duplicates;
                summary.suppressed += saved.suppressed;
                summary.pending += saved.pending;
                summary.read_lines += (end - start) as u64;
                left = left.saturating_sub((end - start).max(1));
                if manifest.gap {
                    summary.errors += 1;
                    summary
                        .diagnostics
                        .push("NATIVE_DATABASE_EVIDENCE_GAP".into());
                }
                if end == manifest.keys.len() {
                    summary.completed_files += 1;
                }
            }
            let ready:i64=ledger.connection().query_row("SELECT COUNT(*) FROM source_objects WHERE root_id=?1 AND target_done=0 AND state='ready' AND (retry_after_ms IS NULL OR retry_after_ms<=?2)",params![root.root_id,now],|r|r.get(0))?;
            summary.backlog += usize::try_from(ready).map_err(|_| LedgerError::NumericOverflow)?;
            if audit_active && pending_database_objects(ledger, &root)? == 0 && summary.errors == 0
            {
                let gap:bool=ledger.connection().query_row("SELECT EXISTS(SELECT 1 FROM source_objects WHERE root_id=?1 AND object_key LIKE 'native-db-v1:%' AND error_code IS NOT NULL)",[&root.root_id],|r|r.get(0))?;
                if !gap {
                    super::source_projection::complete(ledger.connection(), &root)?;
                    super::collection::complete_history_audit(ledger.connection(), &root)?;
                }
            }
            if unsupported_reader_domain {
                ledger.connection().execute("UPDATE source_projection_scopes SET complete=0,error_code='SOURCE_READER_DOMAIN_UNSUPPORTED' WHERE root_id=?1",[&root.root_id])?;
            }
        }
        summary.active_files = summary.backlog;
        summary.diagnostics.sort();
        summary.diagnostics.dedup();
        Ok(summary)
    }
}

fn save_database_recent(
    ledger: &mut Ledger,
    root: &DurableRoot,
    path: &Path,
    source: &str,
    ambiguous: bool,
    budget: usize,
    summary: &mut PollSummary,
) -> Result<usize> {
    let conn = open_database(path)?;
    let Some(table) = backend(&conn, source)? else {
        return Ok(0);
    };
    let Some(batch) = super::local_adapters::sqlite_incremental_batch(
        ledger, root, path, &conn, &table, "id", "", budget,
    )?
    else {
        return Ok(0);
    };
    let count = batch.keys.len();
    let manifest = DbManifest {
        version: 1,
        source: source.into(),
        table,
        keys: batch.keys.clone(),
        ambiguous_channel: ambiguous,
        gap: false,
    };
    let (entries, gap) = read_database_batch(ledger, root, path, &manifest, 0, count)?;
    let generation = digest(
        &serde_json::to_vec(
            &entries
                .iter()
                .map(|e| &e.observation.normalized_hash)
                .collect::<Vec<_>>(),
        )
        .map_err(|_| LedgerError::RecordTooLarge)?,
    );
    let mut object = ledger.discover_durable_object(
        root,
        ObjectSpec {
            object_key: "native-db-recent-v2".into(),
            locator: path.to_string_lossy().into_owned(),
            content_generation: generation,
            byte_length: count as i64,
            mtime_ns: None,
        },
    )?;
    let saved = ledger.save_durable_evidence_batch_with_gap(
        &mut object,
        &entries,
        count as i64,
        b"",
        true,
        gap.then_some("NATIVE_DATABASE_EVIDENCE_GAP"),
    )?;
    summary.inserted += saved.inserted;
    summary.duplicates += saved.duplicates;
    summary.suppressed += saved.suppressed;
    summary.pending += saved.pending;
    summary.read_lines += count as u64;
    if gap {
        summary.errors += 1;
        super::collection::invalidate_history_audit(ledger.connection(), root)?;
    }
    summary.backlog += usize::from(batch.has_more);
    batch.commit(ledger)?;
    Ok(count)
}
// Explicit scalar JSON projection. Neither the part table nor chat fields are read.
const DB_FIELDS: &[(&str, &str)] = &[
    ("role", "$.role"),
    ("payload_type", "$.type"),
    ("model", "$.modelID"),
    ("model_alt", "$.modelId"),
    ("model_nested", "$.model.id"),
    ("provider", "$.providerID"),
    ("input", "$.tokens.input"),
    ("output", "$.tokens.output"),
    ("reasoning", "$.tokens.reasoning"),
    ("cache_read", "$.tokens.cache.read"),
    ("cache_write", "$.tokens.cache.write"),
    ("total", "$.tokens.total"),
    ("created", "$.time.created"),
    ("completed", "$.time.completed"),
    ("finish", "$.finish"),
];
fn read_database_batch(
    ledger: &Ledger,
    root: &DurableRoot,
    path: &Path,
    manifest: &DbManifest,
    start: usize,
    end: usize,
) -> Result<(Vec<DurableEvidence>, bool)> {
    let conn = open_database(path)?;
    let active_backend = backend(&conn, &manifest.source)?;
    let backend_changed = active_backend
        .as_deref()
        .is_some_and(|t| t != manifest.table.as_str());
    let table = manifest.table.as_str();
    if table_columns(&conn, table)?.is_empty() {
        return Ok((Vec::new(), true));
    }
    let fields = if table == "session" {
        "json_object('role','assistant','input',tokens_input,'output',tokens_output,'reasoning',tokens_reasoning,'cache_read',tokens_cache_read,'cache_write',tokens_cache_write,'created',time_created)".into()
    } else {
        let pairs = DB_FIELDS
            .iter()
            .map(|(field, path)| format!("'{field}',json_extract(data,'{path}')"))
            .collect::<Vec<_>>()
            .join(",");
        format!("CASE WHEN json_valid(data) THEN json_object({pairs}) ELSE NULL END")
    };
    let (session, kind, seq, time) = if table == "session_message" {
        ("session_id", "type", "seq", "time_created")
    } else if table == "message" {
        ("session_id", "NULL", "NULL", "NULL")
    } else {
        ("id", "NULL", "NULL", "time_created")
    };
    let sql = format!("SELECT id,{session},{kind},{seq},{time},{fields} FROM {table} WHERE id=?1");
    let mut statement = conn.prepare(&sql)?;
    let mut evidence = Vec::new();
    let mut gap = false;
    for key in &manifest.keys[start..end] {
        if super::source_projection::row_suppressed(ledger.connection(), &root.root_id, table, key)?
        {
            continue;
        }
        let row = statement
            .query_row([key], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, Option<String>>(2)?,
                    r.get::<_, Option<i64>>(3)?,
                    r.get::<_, Option<i64>>(4)?,
                    r.get::<_, Option<String>>(5)?,
                ))
            })
            .optional();
        let (id, session, kind, seq, time, payload) = match row {
            Ok(Some(row)) => row,
            Ok(None) | Err(_) => {
                gap = true;
                continue;
            }
        };
        let Some(payload) = payload else {
            gap = true;
            continue;
        };
        if payload.len() > 16 * 1024 {
            gap = true;
            continue;
        }
        let mut value: Value = match serde_json::from_str(&payload) {
            Ok(v) => v,
            Err(_) => {
                gap = true;
                continue;
            }
        };
        if table == "session_message" {
            if seq.is_none_or(|n| n < 0) {
                gap = true;
                continue;
            }
            match kind.as_deref() {
                Some("user") => continue,
                Some("assistant") => {}
                _ => {
                    gap = true;
                    continue;
                }
            }
            if ["role", "payload_type"]
                .iter()
                .any(|p| value[*p].as_str().is_some_and(|s| s != "assistant"))
            {
                gap = true;
                continue;
            }
            value["role"] = json!("assistant");
            if value["created"].is_null() {
                value["created"] = json!(time);
            }
        }
        if value["role"] != "assistant" {
            if value["role"] != "user"
                && ["input", "output", "reasoning", "cache_read", "cache_write"]
                    .iter()
                    .any(|k| !value[*k].is_null())
            {
                gap = true;
            }
            continue;
        }
        let fork = if table == "session_message" {
            fork_ownership(&conn, &session, seq.unwrap_or(0))?
        } else {
            ForkEvidence::unknown()
        };
        match project_database_message(
            &manifest.source,
            table,
            &id,
            &session,
            seq,
            &value,
            fork,
            manifest.ambiguous_channel || backend_changed,
        ) {
            Ok(Some(record)) => evidence.push(record),
            Ok(None) => {}
            Err(_) => gap = true,
        }
    }
    super::source_projection::capture_rows(
        ledger.connection(),
        root,
        &conn,
        table,
        &manifest.keys[start..end],
        &evidence,
    )?;
    if table == "session_message" {
        let mut sessions = Vec::new();
        for key in &manifest.keys[start..end] {
            if let Some(id) = conn
                .query_row(
                    "SELECT session_id FROM session_message WHERE id=?1",
                    [key],
                    |r| r.get::<_, String>(0),
                )
                .optional()?
            {
                sessions.push(id);
            }
        }
        super::source_projection::capture_rows(
            ledger.connection(),
            root,
            &conn,
            "session_v2",
            &sessions,
            &evidence,
        )?;
    }
    Ok((evidence, gap))
}
#[derive(Clone, Debug)]
enum ForkProof {
    Owned,
    Inherited,
    Unknown,
}
struct ForkEvidence {
    state: ForkProof,
    parent: Option<String>,
    boundary: Option<String>,
}
impl ForkEvidence {
    fn unknown() -> Self {
        Self {
            state: ForkProof::Unknown,
            parent: None,
            boundary: None,
        }
    }
}
fn fork_ownership(conn: &rusqlite::Connection, session: &str, seq: i64) -> Result<ForkEvidence> {
    let cols = table_columns(conn, "session_v2")?;
    if ["id", "fork_session_id", "fork_boundary"]
        .iter()
        .any(|p| !cols.contains(*p))
    {
        return Ok(ForkEvidence::unknown());
    }
    let row: Option<(Option<String>, Option<String>)> = conn
        .query_row(
            "SELECT fork_session_id,fork_boundary FROM session_v2 WHERE id=?1",
            [session],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    let Some((parent, boundary)) = row else {
        return Ok(ForkEvidence::unknown());
    };
    if parent.is_none() && boundary.is_none() {
        return Ok(ForkEvidence {
            state: ForkProof::Owned,
            parent: None,
            boundary: None,
        });
    }
    let (Some(parent), Some(boundary)) = (parent, boundary) else {
        return Ok(ForkEvidence::unknown());
    };
    if boundary.len() > 8192 || parent.len() > 4096 || parent.is_empty() {
        return Ok(ForkEvidence::unknown());
    }
    let boundary: Value = match serde_json::from_str(&boundary) {
        Ok(v) => v,
        Err(_) => return Ok(ForkEvidence::unknown()),
    };
    let Some(id) = text(&boundary, "/messageID") else {
        return Ok(ForkEvidence::unknown());
    };
    let parent_seq: Option<i64> = conn
        .query_row(
            "SELECT seq FROM session_message WHERE session_id=?1 AND id=?2",
            params![parent, id],
            |r| r.get(0),
        )
        .optional()?;
    let mut proof = ForkEvidence {
        state: ForkProof::Unknown,
        parent: Some(parent),
        boundary: Some(json!({"type":text(&boundary,"/type"),"messageID":id}).to_string()),
    };
    if proof.boundary.as_ref().is_some_and(|b| b.len() > 4096) {
        proof.boundary = None;
        return Ok(proof);
    }
    let Some(parent_seq) = parent_seq.filter(|n| *n >= 0) else {
        return Ok(proof);
    };
    let inherited = match boundary["type"].as_str() {
        Some("through") => seq <= parent_seq,
        Some("before") => seq < parent_seq,
        _ => return Ok(proof),
    };
    proof.state = if inherited {
        ForkProof::Inherited
    } else {
        ForkProof::Owned
    };
    Ok(proof)
}
#[allow(clippy::too_many_arguments)] // Positional fields mirror the bounded source row.
fn project_database_message(
    source: &str,
    table: &str,
    id: &str,
    session: &str,
    seq: Option<i64>,
    v: &Value,
    fork: ForkEvidence,
    ambiguous: bool,
) -> Result<Option<DurableEvidence>> {
    let mut o = base(
        source,
        text(v, "/model")
            .or_else(|| text(v, "/model_alt"))
            .or_else(|| text(v, "/model_nested")),
    );
    let raw_output = integer(v, "/output")?;
    let raw_reasoning = integer(v, "/reasoning")?;
    let raw_total = integer(v, "/total")?;
    o.native_session_id = Some(session.into());
    o.native_message_id = Some(id.into());
    o.session_key = Some(identity_key(
        source,
        &namespace(source),
        "session",
        &[session],
    ));
    o.occurred_ms = timestamp(v, "/created");
    for (i, p) in [(0, "/input"), (1, "/cache_read"), (2, "/cache_write")] {
        o.metrics[i] = integer(v, p)?;
    }
    let ambiguous_meter = source == "kilo" && raw_reasoning != Some(0);
    if !ambiguous_meter {
        o.metrics[3] = raw_output;
        o.metrics[4] = raw_reasoning;
    } else {
        o.metrics[4] = raw_reasoning;
    }
    // tokens.total varies across SDK generations; retain it as raw evidence only.
    o.metrics[9] = if table == "session" { None } else { Some(1) };
    if o.metrics[..9].iter().flatten().all(|n| *n == 0) && raw_output.unwrap_or(0) == 0 {
        return Ok(None);
    }
    super::metrics::evaluate(&o.metrics)?;
    let terminal = timestamp(v, "/completed").is_some_and(|completed| {
        completed > 0 && o.occurred_ms.is_none_or(|start| completed >= start)
    }) && text(v, "/finish").is_some_and(|s| {
        matches!(
            s.as_str(),
            "stop" | "length" | "tool-calls" | "content-filter" | "error"
        )
    });
    let reason = if ambiguous {
        Some("MIGRATION_CHANNEL_AMBIGUOUS")
    } else if table == "session" {
        o.fact_kind = "session_total".into();
        Some("SESSION_TOTAL_UNALLOCATED")
    } else if ambiguous_meter {
        Some("OUTPUT_SEMANTICS_UNKNOWN")
    } else if !matches!(fork.state, ForkProof::Owned) {
        Some(if matches!(fork.state, ForkProof::Inherited) {
            "FORK_INHERITED_COPY"
        } else {
            "FORK_OWNERSHIP_UNKNOWN"
        })
    } else if !terminal {
        Some("MESSAGE_FINALITY_UNKNOWN")
    } else {
        None
    };
    if reason.is_none() {
        o.identity_key = Some(identity_key(
            source,
            &namespace(source),
            "message",
            &[session, id],
        ));
    } else {
        o.eligibility = "identity_unknown".into();
    }
    let projection = json!({"channel":table,"source_session_id":session,"source_record_id":id,"source_sequence":seq,"source_fork_parent":fork.parent,"source_fork_boundary":fork.boundary,"upstream_finish":text(v,"/finish"),"pending_reason":reason,"payload_schema":table,"raw_output_tokens":raw_output,"raw_reasoning_tokens":raw_reasoning,"raw_total_tokens":raw_total,"raw_usage_hash":digest(serde_json::to_string(v).map_err(|_|LedgerError::RecordTooLarge)?.as_bytes())});
    finish(o, projection, 0).map(Some)
}

#[derive(Default)]
struct JsonCollector {
    next_scan: HashMap<PathBuf, i64>,
}
impl JsonCollector {
    fn poll(
        &mut self,
        ledger: &mut Ledger,
        source: &str,
        roots: &[PathBuf],
        budget: usize,
    ) -> Result<PollSummary> {
        let mut out = PollSummary::default();
        let mut left = budget;
        let now = super::types::now_ms();
        let mut visited = HashSet::new();
        for locator in roots {
            if locator.is_file() {
                continue;
            }
            let restored_scope = locator.file_name().is_some_and(|s| s == "message")
                && locator
                    .parent()
                    .and_then(Path::file_name)
                    .is_some_and(|s| s == "storage");
            let data_root = if restored_scope {
                locator.parent().and_then(Path::parent).unwrap_or(locator)
            } else {
                locator.as_path()
            };
            let scope = if restored_scope {
                locator.clone()
            } else {
                locator.join("storage/message")
            };
            let scope = scope.canonicalize().unwrap_or(scope);
            if !visited.insert(scope.clone()) {
                continue;
            }
            let registered:bool=ledger.connection().query_row("SELECT EXISTS(SELECT 1 FROM source_roots WHERE collector_key='additional-json-v1' AND report_source=?1 AND locator=?2)",params![source,scope.to_string_lossy().as_ref()],|r|r.get(0))?;
            if !scope.is_dir() && !registered {
                continue;
            }
            let databases = database_paths(source, &[data_root.to_path_buf()])?;
            let mut empty = true;
            let mut healthy = true;
            for (path, _) in databases {
                match open_database(&path).and_then(|c| backend(&c, source)) {
                    Ok(None) => {}
                    Ok(Some(_)) => empty = false,
                    Err(_) => healthy = false,
                }
            }
            let discover = empty && healthy;
            if !discover && !registered {
                continue;
            }
            let mut adapter = spec(source).ok_or(LedgerError::SourceNotProtected)?;
            adapter.collector_key = "additional-json-v1";
            let root = ledger.register_durable_root(&adapter, &namespace(source), &scope)?;
            if !root.enabled {
                continue;
            }
            if discover && self.next_scan.get(&scope).is_none_or(|t| *t <= now) {
                let _ = crate::common::take_walk_error();
                let paths = if scope.is_dir() {
                    crate::common::walk_with(&scope, None, &|p| {
                        p.extension().is_some_and(|v| v == "json")
                    })
                } else {
                    Vec::new()
                };
                if crate::common::take_walk_error() {
                    ledger.fail_durable_enumeration(&root, "IO_ERROR")?;
                    out.errors += 1;
                    continue;
                }
                let mut keys = Vec::new();
                for path in paths {
                    let path = path.canonicalize()?;
                    let key = digest(path.to_string_lossy().as_bytes());
                    keys.push(key.clone());
                    let Some(stamp) = crate::common::file_stamp(&path) else {
                        out.errors += 1;
                        continue;
                    };
                    let generation = format!(
                        "{}:{}:{}",
                        stamp.identity_key(),
                        stamp.size(),
                        stamp.mtime_ns()
                    );
                    ledger.discover_durable_object(
                        &root,
                        ObjectSpec {
                            object_key: key,
                            locator: path.to_string_lossy().into_owned(),
                            content_generation: generation,
                            byte_length: stamp.size() as i64,
                            mtime_ns: Some(stamp.mtime_ns()),
                        },
                    )?;
                }
                ledger.mark_durable_missing(&root, &keys)?;
                ledger.finish_durable_enumeration(&root)?;
                self.next_scan.insert(scope.clone(), now + 30_000);
            }
            for mut object in ledger.next_durable_objects(&root, left.clamp(1, 512))? {
                if left == 0 {
                    break;
                }
                left -= 1;
                let path: String = ledger.connection().query_row(
                    "SELECT locator FROM source_objects WHERE object_id=?1",
                    [object.object_id],
                    |r| r.get(0),
                )?;
                let result = read_json_document(source, Path::new(&path), object.byte_length);
                match result {
                    Ok(evidence) => {
                        let offset = object.byte_length;
                        let saved = ledger.save_durable_evidence_batch(
                            &mut object,
                            &evidence,
                            offset,
                            b"",
                            true,
                        )?;
                        out.inserted += saved.inserted;
                        out.duplicates += saved.duplicates;
                        out.suppressed += saved.suppressed;
                        out.pending += saved.pending;
                        out.read_lines += 1;
                        out.completed_files += 1;
                    }
                    Err(error) => {
                        ledger.fail_durable_object(&object, error.code())?;
                        out.errors += 1;
                        out.diagnostics.push(error.code().into());
                    }
                }
            }
            let queued:i64=ledger.connection().query_row("SELECT COUNT(*) FROM source_objects WHERE root_id=?1 AND target_done=0 AND state='ready' AND (retry_after_ms IS NULL OR retry_after_ms<=?2)",params![root.root_id,now],|r|r.get(0))?;
            out.backlog += usize::try_from(queued).map_err(|_| LedgerError::NumericOverflow)?;
        }
        Ok(out)
    }
}
fn read_json_document(source: &str, path: &Path, size: i64) -> Result<Vec<DurableEvidence>> {
    if size < 0 || size > MAX_LINE_BYTES as i64 {
        return Err(LedgerError::RecordTooLarge);
    }
    let before = crate::common::file_stamp(path).ok_or(LedgerError::IncompleteEvidence)?;
    let mut bytes = Vec::new();
    File::open(path)?
        .take((MAX_LINE_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_LINE_BYTES {
        return Err(LedgerError::RecordTooLarge);
    }
    if bytes.len() != size as usize || crate::common::file_stamp(path).as_ref() != Some(&before) {
        return Err(LedgerError::StaleObservation);
    }
    let value: Value =
        serde_json::from_slice(&bytes).map_err(|_| LedgerError::IncompleteEvidence)?;
    if value["role"] != "assistant" {
        return Ok(Vec::new());
    }
    let mut flat = serde_json::Map::new();
    for (field, pointer) in DB_FIELDS {
        let pointer = pointer.trim_start_matches('$').replace('.', "/");
        flat.insert(
            (*field).into(),
            value.pointer(&pointer).cloned().unwrap_or(Value::Null),
        );
    }
    let id = text(&value, "/id").unwrap_or_default();
    let session = text(&value, "/sessionID")
        .or_else(|| text(&value, "/sessionId"))
        .unwrap_or_default();
    Ok(project_database_message(
        source,
        "json-message",
        &id,
        &session,
        None,
        &Value::Object(flat),
        ForkEvidence::unknown(),
        false,
    )?
    .into_iter()
    .collect())
}

#[cfg(feature = "regression-hooks")]
pub fn project_saved_json_reader_for_regression(reader: impl std::io::Read) -> Result<String> {
    super::source_projection::project_json_reader(reader, &["role", "tokens.input"])
}
