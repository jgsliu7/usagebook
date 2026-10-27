//! DSH historical evidence, including compressed and torn streams. Settlement
//! replacement/retry semantics require a future versioned lifecycle proof; this
//! module deliberately preserves all legal usage candidates without charging them.
use super::durable::{AdapterSpec, DurableEvidence, DurableObject, DurableRoot, ObjectSpec};
use super::types::{identity_key, now_ms, Observation};
use super::{Ledger, LedgerError, Result};
use rusqlite::{params, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
const MAX_BYTES: usize = 64 * 1024 * 1024;
const MAX_LINE: usize = 16 * 1024 * 1024;
pub fn spec() -> AdapterSpec {
    AdapterSpec {
        identity_family: "dsh",
        collector_key: "dsh-evidence-history-v1",
        report_source: "dsh",
        native_tool: "dsh",
        parser_version: "dsh-ledger-v1",
        capability_version: "dsh-native-usage-evidence-v2",
        identity_version: 1,
        semantics_version: 1,
    }
}
fn digest(v: &[u8]) -> String {
    format!("{:x}", Sha256::digest(v))
}
fn encode<T: Serialize>(v: &T) -> Result<Vec<u8>> {
    serde_json::to_vec(v).map_err(|_| LedgerError::RecordTooLarge)
}
fn text(v: &Value, p: &str) -> Option<String> {
    v.pointer(p)?
        .as_str()
        .filter(|s| !s.is_empty() && s.len() <= 1024 && !s.contains('\0'))
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
fn sum(v: &[Option<i64>]) -> Result<Option<i64>> {
    let mut n = 0i64;
    for x in v {
        let Some(x) = x else { return Ok(None) };
        n = n.checked_add(*x).ok_or(LedgerError::NumericOverflow)?;
    }
    Ok(Some(n))
}
pub fn project_line(v: &Value, session: Option<&str>) -> Result<Option<DurableEvidence>> {
    let kind = v.get("type").and_then(Value::as_str).unwrap_or_default();
    if !matches!(
        kind,
        "assistant/message" | "assistant/attempt" | "compaction/summary"
    ) {
        return Ok(None);
    }
    let usage = v
        .pointer("/data/usage")
        .or_else(|| v.get("usage"))
        .or_else(|| {
            v.pointer("/data/stream")
                .and_then(Value::as_array)
                .and_then(|s| {
                    s.iter().rev().find_map(|c| {
                        if c.pointer("/chunk/type").and_then(Value::as_str) == Some("usage") {
                            c.pointer("/chunk/usage")
                        } else if c.get("type").and_then(Value::as_str) == Some("usage") {
                            c.get("usage")
                        } else {
                            None
                        }
                    })
                })
        });
    let Some(usage) = usage else { return Ok(None) };
    let (input, output, cr, cw, reasoning, total) = (
        number(usage, "inputTokens")?,
        number(usage, "outputTokens")?,
        number(usage, "cacheReadTokens")?,
        number(usage, "cacheWriteTokens")?,
        number(usage, "reasoningTokens")?,
        number(usage, "totalTokens")?,
    );
    if [input, output, cr, cw, reasoning, total]
        .iter()
        .all(Option::is_none)
    {
        return Ok(None);
    }
    let native = text(v, "/data/message/id")
        .or_else(|| text(v, "/data/attemptId"))
        .or_else(|| text(v, "/data/retryId"))
        .or_else(|| text(v, "/data/compactionId"));
    let model = text(v, "/data/message/source/replayState/response/responseModel")
        .or_else(|| text(v, "/data/message/source/model"));
    let occurred = v
        .get("time")
        .and_then(Value::as_i64)
        .filter(|n| *n > 0)
        .and_then(|n| {
            if n < 100_000_000_000 {
                n.checked_mul(1000)
            } else {
                Some(n)
            }
        });
    let mut o = Observation {
        identity_family: "dsh".into(),
        namespace_key: "unknown:dsh".into(),
        parser_version: "dsh-ledger-v1".into(),
        report_source: "dsh".into(),
        native_tool: "dsh".into(),
        eligibility: "identity_unknown".into(),
        native_message_id: native.clone(),
        model_key: model
            .as_ref()
            .map(|m| format!("n:{}", m.to_lowercase()))
            .unwrap_or_default(),
        model_display: model,
        occurred_ms: occurred,
        ..Observation::default()
    };
    o.metrics = [
        input,
        cr,
        cw,
        output
            .zip(reasoning)
            .and_then(|(a, b)| a.checked_sub(b).filter(|x| *x >= 0)),
        reasoning,
        sum(&[input, cr, cw])?,
        output,
        total,
        None,
        None,
    ];
    let projection = json!({"channel":kind,"pending_reason":"DSH_SETTLEMENT_LIFECYCLE_UNPROVED","source_record_id":native,"source_session_id":session,"turn_id":v.pointer("/data/turn").and_then(Value::as_i64).map(|n|n.to_string()),"native_stop_reason":text(v,"/data/message/source/replayState/response/stopReason"),"raw_input_tokens":input,"raw_output_tokens":output,"raw_cache_read_tokens":cr,"raw_cache_write_tokens":cw,"raw_reasoning_tokens":reasoning,"raw_total_tokens":total,"projection_version":1});
    // Parent session attribution in forked copies is a hint, never an accounting
    // identity. Different hints may produce separate immutable pending evidence.
    o.candidate_key = identity_key(
        "dsh",
        "unknown:dsh",
        "native-usage-evidence",
        &[&digest(&encode(&(&o, &projection))?)],
    );
    o.normalized_hash = super::claude::normalized_hash(&o)?;
    Ok(Some(DurableEvidence {
        observation: o,
        projection: Some(projection),
    }))
}
fn valid_name(path: &Path) -> bool {
    let Some(n) = path.file_name().and_then(|x| x.to_str()) else {
        return false;
    };
    let n = n.strip_suffix(".zstd").unwrap_or(n);
    let Some(stem) = n.strip_suffix(".jsonl") else {
        return false;
    };
    stem == "session"
        || stem
            .strip_prefix("session.v")
            .is_some_and(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()))
}
pub fn discover_files(roots: &[PathBuf]) -> Result<Vec<PathBuf>> {
    let mut out = BTreeSet::new();
    for root in roots {
        if root.is_file() {
            if valid_name(root) {
                out.insert(root.canonicalize()?);
            }
            continue;
        }
        let scope = if root.join("sessions").is_dir() {
            root.join("sessions")
        } else {
            root.clone()
        };
        if !scope.exists() {
            continue;
        }
        let _ = crate::common::take_walk_error();
        let files = crate::common::walk_with(&scope, None, &valid_name);
        if crate::common::take_walk_error() {
            return Err(LedgerError::IncompleteEvidence);
        }
        for p in files {
            out.insert(p.canonicalize()?);
        }
    }
    Ok(out.into_iter().collect())
}
#[derive(Clone, Serialize, Deserialize)]
struct Checkpoint {
    format: u32,
    hash: String,
    next_row: usize,
    gap: Option<String>,
    #[serde(default)]
    base: Option<super::native_adapters::PrefixSeal>,
    #[serde(default)]
    audit_epoch: Option<i64>,
    #[serde(default)]
    session: Option<String>,
}
struct Snapshot {
    evidence: Vec<DurableEvidence>,
    checkpoint: Checkpoint,
    object: DurableObject,
    lines: u64,
    next: Option<Checkpoint>,
    end_offset: i64,
    at_target: bool,
    validation_bytes: u64,
}
fn read_prefix(path: &Path, target: u64) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    File::open(path)?
        .take(target.min(MAX_BYTES as u64))
        .read_to_end(&mut bytes)?;
    Ok(bytes)
}
fn snapshot(path: &Path, object: DurableObject, audit_epoch: Option<i64>) -> Result<Snapshot> {
    let mut magic = [0; 4];
    let mut f = File::open(path)?;
    let n = f.read(&mut magic)?;
    if n != 4 || magic != [0x28, 0xb5, 0x2f, 0xfd] {
        return plain_snapshot(path, object, audit_epoch);
    }
    let target = u64::try_from(object.byte_length).map_err(|_| LedgerError::CorruptLedger)?;
    let raw = read_prefix(path, target)?;
    let hash = digest(&raw);
    let mut gap = if target > MAX_BYTES as u64 {
        Some("DSH_COMPRESSED_LIMIT".into())
    } else if raw.len() as u64 != target {
        Some("DSH_SOURCE_TRUNCATED".into())
    } else {
        None
    };
    // Verify the frozen prefix again, allowing ordinary appends after the target.
    if digest(&read_prefix(path, target)?) != hash {
        return Err(LedgerError::StaleObservation);
    }
    let old = object
        .checkpoint
        .as_deref()
        .filter(|v| !v.is_empty())
        .map(serde_json::from_slice::<Checkpoint>)
        .transpose()
        .map_err(|_| LedgerError::CorruptLedger)?;
    if old
        .as_ref()
        .is_some_and(|c| c.format != 1 || c.hash != hash)
    {
        return Err(LedgerError::StaleObservation);
    }
    let mut decoded = Vec::new();
    if raw.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]) {
        match zstd::stream::read::Decoder::new(raw.as_slice()) {
            Ok(mut decoder) => {
                decoder.window_log_max(26)?;
                let mut buffer = [0u8; 65536];
                loop {
                    let left = (MAX_BYTES + 1).saturating_sub(decoded.len());
                    if left == 0 {
                        gap = Some("DSH_DECODED_LIMIT".into());
                        break;
                    }
                    let take = left.min(buffer.len());
                    match decoder.read(&mut buffer[..take]) {
                        Ok(0) => break,
                        Ok(n) => decoded.extend_from_slice(&buffer[..n]),
                        Err(_) => {
                            gap = Some("DSH_COMPRESSED_TAIL".into());
                            break;
                        }
                    }
                }
            }
            Err(_) => gap = Some("DSH_COMPRESSED_INVALID".into()),
        }
    } else {
        decoded = raw.clone()
    }
    if decoded.len() > MAX_BYTES {
        decoded.truncate(MAX_BYTES);
        gap = Some("DSH_DECODED_LIMIT".into())
    }
    let mut evidence = Vec::new();
    let mut session = None;
    let mut lines = 0;
    let mut offset = 0;
    for part in decoded.split_inclusive(|b| *b == b'\n') {
        if part.last() != Some(&b'\n') {
            gap.get_or_insert_with(|| "DSH_INCOMPLETE_LINE".into());
            break;
        }
        lines += 1;
        if part.len() > MAX_LINE {
            gap.get_or_insert_with(|| "DSH_LINE_TOO_LARGE".into());
            offset += part.len();
            continue;
        }
        match serde_json::from_slice::<Value>(part) {
            Ok(v) => {
                if v.get("type").and_then(Value::as_str) == Some("session") {
                    session = text(&v, "/id");
                }
                match project_line(&v, session.as_deref()) {
                    Ok(Some(mut e)) => {
                        e.observation.record_key = format!("decoded-byte:{offset}");
                        evidence.push(e)
                    }
                    Ok(None) => {}
                    Err(_) => {
                        gap.get_or_insert_with(|| "DSH_INVALID_USAGE".into());
                    }
                }
            }
            Err(_) => {
                gap.get_or_insert_with(|| "DSH_INVALID_LINE".into());
            }
        }
        offset += part.len();
    }
    let next_row = old.as_ref().map(|c| c.next_row).unwrap_or(0);
    if next_row > evidence.len() {
        return Err(LedgerError::StaleObservation);
    }
    let gap = gap.or_else(|| old.and_then(|c| c.gap));
    Ok(Snapshot {
        evidence,
        checkpoint: Checkpoint {
            format: 1,
            hash,
            next_row,
            gap,
            base: None,
            audit_epoch,
            session: None,
        },
        next: None,
        end_offset: object.byte_length,
        at_target: true,
        validation_bytes: target.saturating_mul(2),
        object,
        lines,
    })
}
fn plain_snapshot(
    path: &Path,
    object: DurableObject,
    audit_epoch: Option<i64>,
) -> Result<Snapshot> {
    use super::native_adapters::{PrefixHash, PrefixSeal};
    let target = u64::try_from(object.byte_length).map_err(|_| LedgerError::CorruptLedger)?;
    if target > 64 * 1024 * 1024 * 1024 {
        return Err(LedgerError::RecordTooLarge);
    }
    let old: Option<Checkpoint> = object
        .checkpoint
        .as_deref()
        .filter(|v| !v.is_empty())
        .map(serde_json::from_slice)
        .transpose()
        .map_err(|_| LedgerError::CheckpointReplayRequired)?;
    if old.as_ref().is_some_and(|c| c.format != 2) {
        return Err(LedgerError::CheckpointReplayRequired);
    }
    let start = object.saved_offset as u64;
    let (mut hash, mut validation_bytes) =
        if let Some(base) = old.as_ref().and_then(|c| c.base.as_ref()) {
            if base.offset != start {
                return Err(LedgerError::CheckpointReplayRequired);
            }
            base.restore(
                path,
                audit_epoch
                    .is_some_and(|epoch| old.as_ref().is_none_or(|c| c.audit_epoch != Some(epoch))),
            )?
        } else if start == 0 {
            (PrefixHash::default(), 0)
        } else {
            return Err(LedgerError::CheckpointReplayRequired);
        };
    let before = crate::common::file_stamp(path).ok_or(LedgerError::StaleObservation)?;
    let start_hash = hash.clone();
    let mut file = File::open(path)?;
    if file.metadata()?.len() < target {
        return Err(LedgerError::StaleObservation);
    }
    file.seek(SeekFrom::Start(start))?;
    let mut reader = BufReader::new(file.take(target - start));
    let mut offset = start;
    let mut session = old.as_ref().and_then(|c| c.session.clone());
    let mut gap = old.as_ref().and_then(|c| c.gap.clone());
    let mut evidence = Vec::new();
    let mut lines = 0;
    while offset < target && offset - start < 4 * 1024 * 1024 {
        let mut line = Vec::new();
        let n = reader
            .by_ref()
            .take(MAX_LINE as u64 + 1)
            .read_until(b'\n', &mut line)?;
        if n == 0 {
            return Err(LedgerError::StaleObservation);
        }
        if n > MAX_LINE {
            return Err(LedgerError::RecordTooLarge);
        }
        hash.update(&line);
        let record_offset = offset;
        offset += n as u64;
        if line.last() != Some(&b'\n') {
            gap.get_or_insert_with(|| "DSH_INCOMPLETE_LINE".into());
            break;
        }
        lines += 1;
        match serde_json::from_slice::<Value>(&line) {
            Ok(v) => {
                if v["type"] == "session" {
                    session = text(&v, "/id");
                }
                match project_line(&v, session.as_deref()) {
                    Ok(Some(mut e)) => {
                        e.observation.record_key = format!("decoded-byte:{record_offset}");
                        evidence.push(e)
                    }
                    Ok(None) => {}
                    Err(_) => {
                        gap.get_or_insert_with(|| "DSH_INVALID_USAGE".into());
                    }
                }
            }
            Err(_) => {
                gap.get_or_insert_with(|| "DSH_INVALID_LINE".into());
            }
        }
    }
    let after = crate::common::file_stamp(path).ok_or(LedgerError::StaleObservation)?;
    if before
        .identity_key()
        .split('/')
        .take(2)
        .ne(after.identity_key().split('/').take(2))
        || after.size() < target
    {
        return Err(LedgerError::StaleObservation);
    }
    if before != after {
        let mut verify = start_hash;
        let mut file = File::open(path)?;
        file.seek(SeekFrom::Start(start))?;
        let mut input = file.take(offset - start);
        let mut buffer = [0; 65536];
        let mut read = 0;
        loop {
            let n = input.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            verify.update(&buffer[..n]);
            read += n as u64;
        }
        validation_bytes += read;
        if read != offset - start || verify.digest() != hash.digest() {
            return Err(LedgerError::StaleObservation);
        }
    }
    let end = PrefixSeal::capture(path, offset, &hash)?;
    let next_row = old.as_ref().map_or(0, |c| c.next_row);
    if next_row > evidence.len()
        || old
            .as_ref()
            .is_some_and(|c| c.next_row > 0 && c.hash != end.hash)
    {
        return Err(LedgerError::StaleObservation);
    }
    let next = Checkpoint {
        format: 2,
        hash: end.hash.clone(),
        next_row: 0,
        gap: gap.clone(),
        base: Some(end.clone()),
        audit_epoch: audit_epoch.or_else(|| old.as_ref().and_then(|c| c.audit_epoch)),
        session: session.clone(),
    };
    let checkpoint = Checkpoint {
        format: 2,
        hash: end.hash.clone(),
        next_row,
        gap,
        base: old.as_ref().and_then(|c| c.base.clone()),
        audit_epoch: audit_epoch.or_else(|| old.as_ref().and_then(|c| c.audit_epoch)),
        session: old.and_then(|c| c.session),
    };
    Ok(Snapshot {
        evidence,
        checkpoint,
        object,
        lines,
        next: Some(next),
        end_offset: offset as i64,
        at_target: offset == target,
        validation_bytes,
    })
}
#[derive(Default, Serialize)]
pub struct PollSummary {
    pub discovered_files: usize,
    pub read_lines: u64,
    pub validation_bytes: u64,
    pub inserted: u64,
    pub duplicates: u64,
    pub suppressed: u64,
    pub pending: u64,
    pub backlog: usize,
    pub errors: u64,
    pub diagnostics: Vec<String>,
}
#[derive(Default)]
pub struct Collector {
    roots: Vec<PathBuf>,
    registered: Vec<DurableRoot>,
    next_discovery: i64,
    active: HashMap<i64, Snapshot>,
    audits: VecDeque<DurableObject>,
    audit_epochs: HashMap<String, Option<i64>>,
}
impl Collector {
    pub fn invalidate_discovery(&mut self) {
        self.next_discovery = 0;
    }
    pub fn poll(
        &mut self,
        ledger: &mut Ledger,
        source: &str,
        roots: &[PathBuf],
        budget: usize,
    ) -> Result<PollSummary> {
        if source != "dsh" {
            return Err(LedgerError::SourceNotProtected);
        }
        if budget == 0 || budget > 65536 {
            return Err(LedgerError::InvalidInput(
                "DSH budget must be 1..65536".into(),
            ));
        }
        let mut s = PollSummary::default();
        let enabled: BTreeSet<String> = ledger
            .connection()
            .prepare("SELECT root_id FROM source_roots WHERE identity_family='dsh' AND enabled=1")?
            .query_map([], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        self.active
            .retain(|_, snapshot| enabled.contains(&snapshot.object.root.root_id));
        self.audits.retain(|o| enabled.contains(&o.root.root_id));
        let mut due_discovery = false;
        for root in &self.registered {
            let epoch = super::collection::history_audit_started(ledger.connection(), root)?;
            if epoch.is_some() && self.audit_epochs.get(&root.root_id) != Some(&epoch) {
                due_discovery = true;
            }
        }
        if self.roots != roots || self.next_discovery <= now_ms() || due_discovery {
            self.registered.clear();
            self.audits.clear();
            for locator in roots {
                let root = ledger.register_durable_root(&spec(), "unknown:dsh", locator)?;
                if !root.enabled {
                    continue;
                }
                let audit_epoch =
                    super::collection::history_audit_started(ledger.connection(), &root)?;
                self.audit_epochs.insert(root.root_id.clone(), audit_epoch);
                let paths = match discover_files(std::slice::from_ref(locator)) {
                    Ok(p) => p,
                    Err(e) => {
                        ledger.fail_durable_enumeration(&root, e.code())?;
                        s.errors += 1;
                        s.diagnostics.push(e.code().into());
                        continue;
                    }
                };
                let mut seen = Vec::new();
                for path in paths {
                    s.discovered_files += 1;
                    let key = digest(path.to_string_lossy().as_bytes());
                    seen.push(key.clone());
                    let Some(stamp) = crate::common::file_stamp(&path) else {
                        continue;
                    };
                    type SavedDshState = (
                        i64,
                        String,
                        i64,
                        Option<i64>,
                        bool,
                        String,
                        i64,
                        bool,
                        Option<i64>,
                    );
                    let old:Option<SavedDshState>=ledger.connection().query_row("SELECT object_id,content_generation,byte_length,mtime_ns,target_done,state,audit_due_ms,CASE WHEN json_valid(checkpoint) THEN json_extract(checkpoint,'$.format')=2 AND json_extract(checkpoint,'$.gap') IS NULL ELSE 0 END,CASE WHEN json_valid(checkpoint) THEN json_extract(checkpoint,'$.audit_epoch') END FROM source_objects WHERE root_id=?1 AND object_key=?2",params![root.root_id,key],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?,r.get(8)?))).optional()?;
                    if old
                        .as_ref()
                        .is_some_and(|(id, _, _, _, _, _, _, _, _)| self.active.contains_key(id))
                    {
                        continue;
                    }
                    let unchanged =
                        old.as_ref()
                            .is_some_and(|(_, _, len, mtime, _, _, _, _, _)| {
                                *len == stamp.size() as i64 && *mtime == Some(stamp.mtime_ns())
                            });
                    if unchanged
                        && old
                            .as_ref()
                            .is_some_and(|(_, _, _, _, complete, _, _, _, epoch)| {
                                *complete
                                    && audit_epoch.is_none_or(|started| *epoch == Some(started))
                            })
                    {
                        continue;
                    }
                    let pending_target =
                        old.as_ref()
                            .filter(|(_, _, _, _, complete, state, _, _, _)| {
                                !*complete && state == "ready"
                            });
                    let target_length = pending_target
                        .map(|(_, _, len, _, _, _, _, _, _)| *len)
                        .unwrap_or(stamp.size() as i64);
                    let target_mtime = pending_target
                        .and_then(|(_, _, _, mtime, _, _, _, _, _)| *mtime)
                        .or(Some(stamp.mtime_ns()));
                    let generation = old
                        .as_ref()
                        .filter(|(_, _, len, _, _, _, _, plain, _)| {
                            unchanged
                                || pending_target.is_some()
                                || (*plain && stamp.size() as i64 > *len)
                        })
                        .map(|(_, g, _, _, _, _, _, _, _)| g.clone())
                        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
                    let object = ledger.discover_durable_object(
                        &root,
                        ObjectSpec {
                            object_key: key,
                            locator: path.to_string_lossy().into_owned(),
                            content_generation: generation,
                            byte_length: target_length,
                            mtime_ns: target_mtime,
                        },
                    )?;
                    if object.complete {
                        self.audits.push_back(object)
                    }
                }
                ledger.mark_durable_missing(&root, &seen)?;
                ledger.finish_durable_enumeration(&root)?;
                self.registered.push(root);
            }
            self.roots = roots.to_vec();
            self.next_discovery = now_ms() + 30_000;
        }
        // Frozen projected evidence already read remains saveable if the source
        // is removed between polls. It is not invalidated by physical absence.
        let mut work: Vec<_> = self
            .active
            .values()
            .take(3)
            .map(|s| s.object.clone())
            .collect();
        if let Some(audit) = self.audits.pop_front() {
            work.push(audit)
        }
        for root in &self.registered {
            if work.len() >= 4 {
                break;
            }
            for object in ledger.next_durable_objects(root, 4 - work.len())? {
                if !work.iter().any(|o| o.object_id == object.object_id) {
                    work.push(object)
                }
            }
        }
        let mut left = budget;
        for mut object in work {
            if left == 0 {
                break;
            }
            let id = object.object_id;
            if let std::collections::hash_map::Entry::Vacant(entry) = self.active.entry(id) {
                let path: PathBuf = ledger.connection().query_row(
                    "SELECT locator FROM source_objects WHERE object_id=?1",
                    [id],
                    |r| r.get::<_, String>(0).map(PathBuf::from),
                )?;
                let audit_epoch =
                    super::collection::history_audit_started(ledger.connection(), &object.root)?;
                let mut opened = snapshot(&path, object.clone(), audit_epoch);
                if matches!(
                    opened,
                    Err(LedgerError::StaleObservation | LedgerError::CheckpointReplayRequired)
                ) {
                    if let Some(stamp) = crate::common::file_stamp(&path) {
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
                        opened = snapshot(&path, object.clone(), audit_epoch);
                    }
                }
                match opened {
                    Ok(snapshot) => {
                        s.read_lines += snapshot.lines;
                        s.validation_bytes += snapshot.validation_bytes;
                        entry.insert(snapshot);
                    }
                    Err(e) => {
                        ledger.mark_durable_gap(&object, e.code())?;
                        s.errors += 1;
                        s.diagnostics.push(e.code().into());
                        continue;
                    }
                }
            }
            let current = self
                .active
                .get_mut(&id)
                .ok_or(LedgerError::IncompleteEvidence)?;
            let start = current.checkpoint.next_row;
            let end = (start + left.min(512)).min(current.evidence.len());
            let drained = end == current.evidence.len();
            let complete = drained && current.at_target;
            let mut checkpoint = current.checkpoint.clone();
            checkpoint.next_row = end;
            if drained {
                if let Some(next) = &current.next {
                    checkpoint = next.clone();
                }
            }
            let offset = if drained {
                current.end_offset
            } else {
                current.object.saved_offset
            };
            // Advance only after the same transaction saves every selected candidate.
            let saved = ledger.save_durable_evidence_batch_with_gap(
                &mut current.object,
                &current.evidence[start..end],
                offset,
                &encode(&checkpoint)?,
                complete,
                if drained {
                    checkpoint.gap.as_deref()
                } else {
                    None
                },
            )?;
            current.checkpoint = checkpoint;
            s.inserted += saved.inserted;
            s.duplicates += saved.duplicates;
            s.suppressed += saved.suppressed;
            s.pending += saved.pending;
            left = left.saturating_sub((end - start).max(1));
            if drained {
                if let Some(code) = &current.checkpoint.gap {
                    s.errors += 1;
                    s.diagnostics.push(code.clone());
                }
                if complete {
                    ledger.connection().execute("UPDATE source_objects SET audit_completed_ms=CASE WHEN audit_due_ms<=?2 THEN ?2 ELSE audit_completed_ms END,audit_due_ms=CASE WHEN audit_due_ms<=?2 THEN ?3 ELSE audit_due_ms END WHERE object_id=?1",params![id,now_ms(),now_ms()+super::collection::HISTORY_AUDIT_INTERVAL_MS])?;
                }
                self.active.remove(&id);
            }
        }
        for root in &self.registered {
            s.backlog+=ledger.connection().query_row("SELECT COUNT(*) FROM source_objects WHERE root_id=?1 AND target_done=0 AND state='ready' AND (retry_after_ms IS NULL OR retry_after_ms<=?2)",params![root.root_id,now_ms()],|r|r.get::<_,i64>(0))? as usize;
        }
        for root in &self.registered {
            if super::collection::history_audit_due(ledger.connection(), root)? {
                let complete:bool=ledger.connection().query_row("SELECT enumeration_done=1 AND error_code IS NULL AND NOT EXISTS(SELECT 1 FROM source_objects o WHERE o.root_id=r.root_id AND (target_done=0 OR error_code IS NOT NULL)) FROM source_roots r WHERE root_id=?1",[&root.root_id],|r|r.get(0))?;
                if complete
                    && !self.audits.iter().any(|o| o.root.root_id == root.root_id)
                    && !self
                        .active
                        .values()
                        .any(|s| s.object.root.root_id == root.root_id)
                {
                    super::collection::complete_history_audit(ledger.connection(), root)?;
                }
            }
        }
        s.backlog += self.audits.len();
        Ok(s)
    }
}
