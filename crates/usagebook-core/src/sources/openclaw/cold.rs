//! Read-only projection of OpenClaw schema-20 cold transcript envelopes.
//! Only reference metadata and usage projections survive a scan; content and
//! auxiliary search/index rows never enter the source cache.
use crate::common::{file_stamp, FileStamp};
use rusqlite::Connection;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    io::{BufRead, BufReader, Read},
    path::{Path, PathBuf},
};

// Match the upstream decoder's 64 MiB limit. Apply it independently to the
// compressed input, decoded envelope and individual event before allocating.
pub(super) const MAX_BYTES: u64 = 64 * 1024 * 1024;
const MAX_REFERENCES: usize = 100_000;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub(crate) struct Archive {
    pub session_id: String,
    pub generation: String,
    pub archive_name: String,
    pub archive_sha256: String,
    pub event_count: u64,
    pub raw_bytes: u64,
    pub archive_bytes: u64,
    pub last_seq: i64,
    pub archived_at: i64,
    pub storage: String,
    blob_bytes: Option<u64>,
}

fn unsigned(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<u64> {
    let value: i64 = row.get(index)?;
    u64::try_from(value).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(index, value))
}

pub(crate) fn references(conn: &Connection) -> Result<Vec<Archive>, &'static str> {
    // Fixed upstream bd04f4c, state/openclaw-agent-db-contract.ts: current 24;
    // schema 20 introduced cold transcripts. Do not guess future compatibility.
    // Existing unversioned/legacy projections retain their column-shape checks.
    let version: i64 = conn
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(|_| "数据库协议版本不可读")?;
    if !(0..=24).contains(&version) {
        return Err("尚不支持的 OpenClaw 数据库协议版本");
    }
    let present: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='session_transcript_cold_archives')",
        [], |row| row.get(0),
    ).map_err(|_| "冷归档表目录不可读")?;
    if !present {
        if version >= 20 {
            return Err("冷归档版本缺少必要 schema");
        }
        return Ok(Vec::new());
    }
    let mut stmt = conn.prepare(
        "SELECT session_id,generation,archive_name,archive_sha256,event_count,raw_bytes,archive_bytes,last_seq,archived_at,storage,length(archive_blob) FROM session_transcript_cold_archives ORDER BY session_id"
    ).map_err(|_| "不支持的冷归档 schema")?;
    let mut rows = stmt.query([]).map_err(|_| "冷归档引用读取失败")?;
    let mut result = Vec::new();
    while let Some(row) = rows.next().map_err(|_| "冷归档引用读取中断")? {
        if result.len() >= MAX_REFERENCES {
            return Err("冷归档引用数量超限");
        }
        let a = (|| -> rusqlite::Result<Archive> {
            Ok(Archive {
                session_id: row.get(0)?,
                generation: row.get(1)?,
                archive_name: row.get(2)?,
                archive_sha256: row.get(3)?,
                event_count: unsigned(row, 4)?,
                raw_bytes: unsigned(row, 5)?,
                archive_bytes: unsigned(row, 6)?,
                last_seq: row.get(7)?,
                archived_at: row.get(8)?,
                storage: row.get(9)?,
                blob_bytes: row
                    .get::<_, Option<i64>>(10)?
                    .map(u64::try_from)
                    .transpose()
                    .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(10, -1))?,
            })
        })()
        .map_err(|_| "冷归档引用字段无效")?;
        let hex = |s: &str| {
            s.len() == 64
                && s.bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        };
        let safe_name = a.archive_name.strip_suffix(".jsonl.zst").is_some_and(hex);
        if a.session_id.is_empty()
            || a.generation.is_empty()
            || !safe_name
            || !hex(&a.archive_sha256)
            || a.archive_bytes == 0
            || a.archive_bytes > MAX_BYTES
            || a.raw_bytes > MAX_BYTES
            || a.event_count == 0
            || a.event_count > MAX_BYTES
            || a.last_seq < 0
            || !matches!(a.storage.as_str(), "file" | "sqlite")
            || (a.storage == "sqlite" && a.blob_bytes != Some(a.archive_bytes))
            || result
                .last()
                .is_some_and(|prev: &Archive| prev.session_id == a.session_id)
        {
            return Err("冷归档引用无效或超限");
        }
        result.push(a);
    }
    Ok(result)
}

pub(super) fn reference_marker(db: &Path, archives: &[Archive]) -> PathBuf {
    let bytes = serde_json::to_vec(archives).expect("archive metadata is serializable");
    db.join(format!(".usagebook-cold-{:x}", Sha256::digest(bytes)))
}

#[derive(Deserialize)]
#[serde(tag = "kind")]
enum Envelope {
    #[serde(rename = "header")]
    Header {
        version: u64,
        #[serde(rename = "sessionId")]
        session_id: String,
        generation: String,
    },
    #[serde(rename = "event")]
    Event { row: Event },
    // Validate the envelope tag while skipping all non-usage auxiliary data.
    // Unknown tags are rejected, and no FTS text or conversation is retained.
    #[serde(rename = "identity")]
    Identity { row: Identity },
    #[serde(rename = "active")]
    Active { row: Active },
    #[serde(rename = "index")]
    Index { row: Index },
    #[serde(rename = "fts")]
    Fts { row: Fts },
}
// Auxiliary rows are validated, but strings are consumed without allocating
// retained fields. In particular, FTS text is never part of our projection.
struct DiscardString;
impl<'de> Deserialize<'de> for DiscardString {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl serde::de::Visitor<'_> for Visitor {
            type Value = DiscardString;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a string")
            }
            fn visit_str<E: serde::de::Error>(self, _: &str) -> Result<Self::Value, E> {
                Ok(DiscardString)
            }
        }
        deserializer.deserialize_str(Visitor)
    }
}
#[allow(dead_code)]
#[derive(Deserialize)]
struct Identity {
    event_id: DiscardString,
    seq: i64,
    event_type: Option<DiscardString>,
    parent_id: Option<DiscardString>,
    message_idempotency_key: Option<DiscardString>,
    created_at: i64,
}
#[allow(dead_code)]
#[derive(Deserialize)]
struct Active {
    active_position: i64,
    event_seq: i64,
    message_position: Option<i64>,
    context_eligible: Option<i64>,
}
#[allow(dead_code)]
#[derive(Deserialize)]
struct Index {
    indexed_seq: i64,
    leaf_event_id: Option<DiscardString>,
    needs_rebuild: i64,
    active_event_count: i64,
    active_message_count: i64,
    updated_at: i64,
}
#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(untagged)]
enum Timestamp {
    Text(DiscardString),
    Number(serde_json::Number),
}
#[allow(dead_code)]
#[derive(Deserialize)]
struct Fts {
    text: Option<DiscardString>,
    message_id: Option<DiscardString>,
    role: Option<DiscardString>,
    timestamp: Option<Timestamp>,
}

#[derive(Deserialize)]
struct Event {
    seq: i64,
    event_json: String,
    created_at: i64,
}

pub(crate) fn scan(
    conn: &Connection,
    archive: &Archive,
    cold_dir: &Path,
    files: &mut Vec<(PathBuf, FileStamp)>,
    mut event: impl FnMut(i64, &str) -> Result<(), &'static str>,
) -> Result<(), &'static str> {
    let path = cold_dir.join(&archive.archive_name);
    let before: Option<FileStamp>;
    let bytes = if archive.storage == "file" {
        // Resolve only the validated single filename. No glob and no orphan
        // archive traversal, even when cold storage has since been disabled.
        let root = std::fs::canonicalize(cold_dir).map_err(|_| "冷归档目录不可读")?;
        let actual = std::fs::canonicalize(&path).map_err(|_| "冷归档文件缺失或不可读")?;
        if actual.parent() != Some(root.as_path()) {
            return Err("冷归档链接越出归档目录");
        }
        before = file_stamp(&path);
        if before.as_ref().map(FileStamp::size) != Some(archive.archive_bytes) {
            return Err("冷归档文件大小不符");
        }
        let f = std::fs::File::open(&path).map_err(|_| "冷归档文件不可读")?;
        let mut bytes = Vec::new();
        f.take(MAX_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| "冷归档文件读取中断")?;
        bytes
    } else {
        before = None;
        conn.query_row(
            "SELECT CASE WHEN length(archive_blob)<=?2 THEN archive_blob ELSE NULL END FROM session_transcript_cold_archives WHERE session_id=?1",
            rusqlite::params![archive.session_id, MAX_BYTES as i64], |row| row.get::<_, Vec<u8>>(0),
        ).map_err(|_| "冷归档内嵌数据不可读或超限")?
    };
    if bytes.len() as u64 != archive.archive_bytes
        || format!("{:x}", Sha256::digest(&bytes)) != archive.archive_sha256
    {
        return Err("冷归档大小或 SHA256 不符");
    }
    let mut decoder =
        zstd::stream::read::Decoder::new(bytes.as_slice()).map_err(|_| "冷归档压缩格式无效")?;
    // Bound zstd's internal window too, not only the decoded output allocation.
    decoder
        .window_log_max(26)
        .map_err(|_| "冷归档解压窗口配置失败")?;
    let mut reader = BufReader::new(decoder.take(MAX_BYTES + 1));
    let mut line = Vec::new();
    let (mut envelope_bytes, mut event_count, mut raw_bytes, mut last_seq) =
        (0_u64, 0_u64, 0_u64, None);
    let mut first = true;
    loop {
        line.clear();
        let read = reader
            .read_until(b'\n', &mut line)
            .map_err(|_| "冷归档解压失败")?;
        if read == 0 {
            break;
        }
        envelope_bytes += read as u64;
        if envelope_bytes > MAX_BYTES {
            return Err("冷归档解压后超限");
        }
        let row: Envelope = serde_json::from_slice(&line).map_err(|_| "冷归档行格式无效")?;
        match row {
            Envelope::Header {
                version,
                session_id,
                generation,
            } if first => {
                if version != 1
                    || session_id != archive.session_id
                    || generation != archive.generation
                {
                    return Err("冷归档版本或会话身份不符");
                }
            }
            Envelope::Header { .. } => return Err("冷归档重复 header"),
            _ if first => return Err("冷归档缺少 header"),
            Envelope::Event { row } => {
                if row.seq < 0 || last_seq.is_some_and(|last| row.seq <= last) {
                    return Err("冷归档事件顺序或身份冲突");
                }
                let _ = row.created_at;
                event_count += 1;
                raw_bytes += row.event_json.len() as u64;
                if event_count > archive.event_count || raw_bytes > archive.raw_bytes {
                    return Err("冷归档事件数量或字节数不符");
                }
                last_seq = Some(row.seq);
                event(row.seq, &row.event_json)?;
            }
            Envelope::Identity { row } => {
                let _ = row;
            }
            Envelope::Active { row } => {
                let _ = row;
            }
            Envelope::Index { row } => {
                let _ = row;
            }
            Envelope::Fts { row } => {
                let _ = row;
            }
        }
        first = false;
    }
    if first
        || event_count != archive.event_count
        || last_seq != Some(archive.last_seq)
        || raw_bytes + event_count.saturating_sub(1) != archive.raw_bytes
    {
        return Err("冷归档事件数量、末序号或原始字节数不符");
    }
    if archive.storage == "file" {
        if before != file_stamp(&path) {
            return Err("扫描期间冷归档发生变化");
        }
        files.push((path, before.ok_or("冷归档状态不可读")?));
    }
    Ok(())
}
