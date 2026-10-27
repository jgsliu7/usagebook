//! Bounded, restartable model-only enrichment for saved MiniMax usage rows.
use super::{norm_ms, ScanStats, SessionModels};
use rusqlite::{params, Connection, OptionalExtension};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{BufRead, BufReader, Read, Seek, SeekFrom},
    path::Path,
};
const AUDIT_MS: i64 = 86_400_000;
const SLICE_BYTES: u64 = 4 * 1024 * 1024;
// A model-bearing record may also contain a large tool result or transcript.
// Match the existing native record ceiling: serde skips those body fields and
// only the model whitelist is retained. A slice may finish one bounded line.
const MAX_LINE: u64 = 16 * 1024 * 1024;

#[derive(Deserialize)]
struct Manifest {
    #[serde(rename = "sessionId")]
    session_id: String,
}
#[derive(Deserialize)]
struct Message {
    role: Option<String>,
    model: Option<String>,
    timestamp: Option<i64>,
}
#[derive(Deserialize)]
struct Line {
    turn_id: Option<String>,
    message: Option<Message>,
}
fn identity(metadata: &std::fs::Metadata) -> String {
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
fn modified(metadata: &std::fs::Metadata) -> String {
    metadata
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_nanos().to_string())
        .unwrap_or_default()
}
fn anchor(file: &mut File, offset: u64) -> std::io::Result<Vec<u8>> {
    let start = offset.saturating_sub(4096);
    file.seek(SeekFrom::Start(start))?;
    let mut bytes = vec![0; (offset - start) as usize];
    file.read_exact(&mut bytes)?;
    Ok(Sha256::digest(bytes).to_vec())
}

pub(super) fn load(
    ledger: &Connection,
    root: &Path,
    needed: &HashSet<String>,
    stats: &mut ScanStats,
) -> HashMap<String, SessionModels> {
    match load_inner(ledger, root, needed, stats) {
        Ok(models) => models,
        Err(_) => {
            stats.failed = true;
            stats
                .notes
                .push("MiniMax 模型索引尚未完整，保留上次完整结果".into());
            HashMap::new()
        }
    }
}
fn load_inner(
    ledger: &Connection,
    root: &Path,
    needed: &HashSet<String>,
    stats: &mut ScanStats,
) -> Result<HashMap<String, SessionModels>, Box<dyn std::error::Error>> {
    let mut models = HashMap::<String, SessionModels>::new();
    if needed.is_empty() {
        return Ok(models);
    }
    let epoch: String = ledger.query_row("SELECT epoch FROM ledger_meta", [], |r| r.get(0))?;
    let root_path = root.to_string_lossy();
    let now = crate::ledger::types::now_ms();
    for path in crate::common::walk_jsonl(root, None) {
        if path.file_name().is_none_or(|n| n != "messages.jsonl") {
            continue;
        }
        let key = path.to_string_lossy().into_owned();
        let metadata = std::fs::metadata(&path)?;
        let id = identity(&metadata);
        let mtime = modified(&metadata);
        type SavedCursor = (String, String, String, u64, String, u64, Vec<u8>, i64);
        let old:Option<SavedCursor>=ledger.query_row(
            "SELECT epoch,session_id,identity,byte_length,mtime_ns,saved_offset,anchor,audit_ms FROM minimax_model_files WHERE path=?1",[&key],
            |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,positive(r,3)?,r.get(4)?,positive(r,5)?,r.get(6)?,r.get(7)?))).optional()?;
        let manifest = path.parent().ok_or("model path")?.join("manifest.json");
        // Bound manifest reads; an absent archived manifest retains previously proved ownership.
        let sid = File::open(manifest)
            .ok()
            .and_then(|f| {
                let mut bytes = Vec::new();
                f.take(65537).read_to_end(&mut bytes).ok()?;
                (bytes.len() <= 65536).then_some(bytes)
            })
            .and_then(|bytes| serde_json::from_slice::<Manifest>(&bytes).ok())
            .map(|m| m.session_id)
            .or_else(|| {
                old.as_ref()
                    .filter(|o| o.0 == epoch && o.2 == id)
                    .map(|o| o.1.clone())
            });
        let Some(sid) = sid.filter(|s| !s.is_empty() && s.len() <= 4096) else {
            stats.failed = true;
            continue;
        };
        if !needed.contains(&sid) {
            continue;
        }
        let mut file = File::open(&path)?;
        let mut offset = 0;
        let mut audit = now;
        let mut replay = true;
        if let Some(old) = &old {
            let audit_due = now.saturating_sub(old.7) >= AUDIT_MS || now < old.7;
            if old.0 == epoch
                && old.1 == sid
                && old.2 == id
                && metadata.len() >= old.5
                && !audit_due
            {
                if metadata.len() == old.3 && mtime == old.4 && old.5 == old.3 {
                    continue;
                }
                if (metadata.len() > old.3
                    || (old.5 < old.3 && metadata.len() == old.3 && mtime == old.4))
                    && anchor(&mut file, old.5)? == old.6
                {
                    offset = old.5;
                    audit = old.7;
                    replay = false;
                }
            }
        }
        let begin = offset;
        let limit = metadata.len().min(offset.saturating_add(SLICE_BYTES));
        file.seek(SeekFrom::Start(offset))?;
        let mut reader = BufReader::new(file.try_clone()?);
        let mut accepted = Vec::new();
        let mut complete = true;
        while offset < limit {
            let mut line = Vec::new();
            let n = reader
                .by_ref()
                .take(MAX_LINE + 1)
                .read_until(b'\n', &mut line)?;
            if n == 0 {
                break;
            }
            if n as u64 > MAX_LINE {
                return Err("model line too large".into());
            }
            if line.last() != Some(&b'\n') {
                complete = false;
                break;
            }
            let line_start = offset;
            let next_offset = offset.checked_add(n as u64).ok_or("model offset")?;
            match serde_json::from_slice::<Line>(&line) {
                Ok(row) => {
                    if let Some(message) = row.message {
                        if message.role.as_deref() == Some("assistant") {
                            if let Some(model) =
                                message.model.filter(|m| !m.is_empty() && m.len() <= 1024)
                            {
                                let turn = row.turn_id.filter(|t| t.len() <= 4096);
                                accepted.push((
                                    line_start,
                                    turn,
                                    message.timestamp.map(norm_ms),
                                    model,
                                ));
                            }
                        }
                    }
                }
                Err(_) => {
                    complete = false;
                    break;
                }
            }
            offset = next_offset;
        }
        let after = std::fs::metadata(&path)?;
        if identity(&after) != id
            || after.len() < metadata.len()
            || (after.len() == metadata.len() && modified(&after) != mtime)
        {
            return Err("model input changed".into());
        }
        let digest = anchor(&mut file, offset)?;
        let tx = ledger.unchecked_transaction()?;
        if replay {
            tx.execute("DELETE FROM minimax_model_rows WHERE path=?1", [&key])?;
        }
        tx.execute("INSERT INTO minimax_model_files VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10) ON CONFLICT(path) DO UPDATE SET root_path=excluded.root_path,epoch=excluded.epoch,session_id=excluded.session_id,identity=excluded.identity,byte_length=excluded.byte_length,mtime_ns=excluded.mtime_ns,saved_offset=excluded.saved_offset,anchor=excluded.anchor,audit_ms=excluded.audit_ms",
            params![key,root_path,epoch,sid,id,i64::try_from(metadata.len())?,mtime,i64::try_from(offset)?,digest,audit])?;
        for (start, turn, time, model) in accepted {
            tx.execute(
                "INSERT OR REPLACE INTO minimax_model_rows VALUES(?1,?2,?3,?4,?5)",
                params![key, i64::try_from(start)?, turn, time, model],
            )?;
        }
        tx.commit()?;
        stats.bytes_read += offset.saturating_sub(begin);
        stats.files_read += 1;
        if !complete || offset < metadata.len() {
            stats.failed = true;
        }
    }
    // Missing files are still useful model attribution. Never erase their cache.
    // The usage collector independently reports missing authoritative input.
    let mut stmt=ledger.prepare("SELECT f.session_id,r.turn_id,r.timestamp_ms,r.model FROM minimax_model_rows r JOIN minimax_model_files f USING(path) WHERE f.root_path=?1 AND f.epoch=?2 ORDER BY f.path,r.line_offset")?;
    let mut rows = stmt.query(params![root_path, epoch])?;
    while let Some(row) = rows.next()? {
        let sid: String = row.get(0)?;
        if !needed.contains(&sid) {
            continue;
        }
        let entry = models.entry(sid).or_insert_with(|| SessionModels {
            by_turn: HashMap::new(),
            by_ts: HashMap::new(),
            freq: HashMap::new(),
        });
        let model: String = row.get(3)?;
        if let Some(turn) = row.get::<_, Option<String>>(1)? {
            entry.by_turn.insert(turn, model.clone());
        }
        if let Some(time) = row.get::<_, Option<i64>>(2)? {
            entry.by_ts.insert(time, model.clone());
        }
        *entry.freq.entry(model).or_default() += 1;
    }
    Ok(models)
}

fn positive(row: &rusqlite::Row<'_>, column: usize) -> rusqlite::Result<u64> {
    let value: i64 = row.get(column)?;
    u64::try_from(value).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(column, value))
}
