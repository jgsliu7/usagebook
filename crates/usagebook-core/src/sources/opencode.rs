//! OpenCode 适配器 —— 依据 docs/数据源档案/28-opencode.md(Community,本机未实证)。
//!
//! 根(§1):OPENCODE_DATA_DIR(多根)或 $XDG_DATA_HOME/opencode → ~/.local/share/opencode,
//!     macOS 候选 ~/Library/Application Support/opencode
//! 存储(§2):主库与独立 channel 库逐一读取,物理别名合并,next 迁移歧义显式失败;
//!     表内 v1 message / v2 session_message 并存逐一探测;无 db 时 JSON storage/message 兜底
//! payload(§3):tokens 五桶互斥直取;模型 modelID>model.id>modelId;time.created 秒/毫秒自适应
//! 去重键(§4):发现根+物理库域+会话/请求身份;v2 fork 按边界剔除复制前缀

use crate::common::{env_roots, home, json_u64, walk_with};
use crate::model::{Caliber, ScanStats, UsageRecord, UsageSource};
use rusqlite::types::Value as SqlValue;
use rusqlite::{Connection, OptionalExtension};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

pub struct OpencodeSource;

/// SQLite 值 → 文本。
fn v_text(v: &SqlValue) -> Option<String> {
    match v {
        SqlValue::Text(s) => Some(s.clone()),
        SqlValue::Integer(i) => Some(i.to_string()),
        _ => None,
    }
}

/// payload data JSON → 五桶 + 模型 + 时间 + 成本(§3)。role/type 判定由调用方注入。
struct Payload {
    input: u64,
    cache_read: u64,
    cache_write: u64,
    output: u64,
    reasoning: u64,
    model: String,
    ts_ms: i64,
    cost_usd: Option<f64>,
}

/// 选择性反序列化(内存审计):只声明被消费的字段,serde 跳过未知键——
/// 尤其 parts/content 等大体积字段完全不建 DOM(实测峰值 582MB 的主因)。
#[derive(serde::Deserialize)]
struct OcMsg<'a> {
    role: Option<&'a str>,
    #[serde(rename = "type")]
    kind: Option<&'a str>,
    tokens: Option<OcTokens>,
    #[serde(rename = "modelID", alias = "modelId")]
    model_id: Option<&'a str>,
    model: Option<OcModel<'a>>,
    time: Option<OcTime>,
    cost: Option<f64>,
}
#[derive(serde::Deserialize)]
struct OcRole<'a> {
    role: Option<&'a str>,
    #[serde(rename = "type")]
    kind: Option<&'a str>,
}
#[derive(serde::Deserialize)]
struct OcTokens {
    input: Option<u64>,
    output: Option<u64>,
    reasoning: Option<u64>,
    cache: Option<OcCache>,
}
#[derive(serde::Deserialize)]
struct OcCache {
    read: Option<u64>,
    write: Option<u64>,
}
#[derive(serde::Deserialize)]
struct OcModel<'a> {
    id: Option<&'a str>,
}
#[derive(serde::Deserialize)]
struct OcTime {
    created: Option<i64>,
}

fn parse_msg(m: &OcMsg) -> Option<Payload> {
    let tk = m.tokens.as_ref()?;
    let (i, o, rs) = (
        tk.input.unwrap_or(0),
        tk.output.unwrap_or(0),
        tk.reasoning.unwrap_or(0),
    );
    let (cr, cw) = match tk.cache.as_ref() {
        Some(c) => (c.read.unwrap_or(0), c.write.unwrap_or(0)),
        None => (0, 0),
    };
    if i == 0 && o == 0 && cr == 0 && cw == 0 && rs == 0 {
        return None; // 五桶全 0 不入账(§3)
    }
    let model = m
        .model_id
        .or(m.model.as_ref().and_then(|x| x.id))
        .unwrap_or("unknown")
        .to_string();
    let ts_ms = m
        .time
        .as_ref()
        .and_then(|t| t.created)
        .map(crate::common::secs_or_ms)
        .unwrap_or(0);
    Some(Payload {
        input: i,
        cache_read: cr,
        cache_write: cw,
        output: o,
        reasoning: rs,
        model,
        ts_ms,
        cost_usd: m.cost,
    })
}

fn parse_payload(doc: &serde_json::Value) -> Option<Payload> {
    let tk = doc.get("tokens")?;
    let (i, o, rs) = (
        json_u64(tk.get("input")),
        json_u64(tk.get("output")),
        json_u64(tk.get("reasoning")),
    );
    let (cr, cw) = match tk.get("cache") {
        Some(c) if c.is_object() => (json_u64(c.get("read")), json_u64(c.get("write"))),
        _ => (0, 0),
    };
    if i == 0 && o == 0 && cr == 0 && cw == 0 && rs == 0 {
        return None; // 五桶全 0 不入账;不按 total 估算(§3)
    }
    let model = ["modelID", "modelId"]
        .iter()
        .find_map(|k| doc.get(*k).and_then(|x| x.as_str()))
        .or_else(|| doc.pointer("/model/id").and_then(|x| x.as_str()))
        .unwrap_or("unknown")
        .to_string();
    let ts_ms = doc
        .pointer("/time/created")
        .and_then(|x| x.as_i64())
        .map(crate::common::secs_or_ms)
        .unwrap_or(0); // 缺失不再静默丢行:落 0 由调用方计数上报(2026-10-17,对齐 claude 处理)
    Some(Payload {
        input: i,
        cache_read: cr,
        cache_write: cw,
        output: o,
        reasoning: rs,
        model,
        ts_ms,
        cost_usd: doc.get("cost").and_then(|x| x.as_f64()),
    })
}

fn column_set(conn: &Connection, table: &str) -> Option<HashSet<String>> {
    let sql = match table {
        "session_v2" => "SELECT * FROM session_v2 LIMIT 0",
        "session_message" => "SELECT * FROM session_message LIMIT 0",
        _ => return None,
    };
    let stmt = conn.prepare(sql).ok()?;
    Some(stmt.column_names().into_iter().map(str::to_owned).collect())
}

/// Resolve only proved fork boundaries. Missing or malformed metadata keeps the
/// original rows: dropping usage on a guessed boundary is worse than a warning.
fn copied_prefix_end(
    conn: &Connection,
    parent: &str,
    boundary: &str,
    stats: &mut ScanStats,
) -> Option<i64> {
    let boundary: serde_json::Value = serde_json::from_str(boundary).ok()?;
    let kind = boundary.get("type")?.as_str()?;
    let message = boundary.get("messageID")?.as_str()?;
    let boundary_seq = match conn
        .query_row(
            "SELECT seq FROM session_message WHERE session_id=?1 AND id=?2 LIMIT 1",
            rusqlite::params![parent, message],
            |row| row.get::<_, i64>(0),
        )
        .optional()
    {
        Ok(Some(seq)) => seq,
        Ok(None) => return None,
        Err(error) => {
            stats.failed = true;
            stats
                .notes
                .push(format!("opencode fork 边界读取失败: {error}"));
            return None;
        }
    };
    match kind {
        "through" => Some(boundary_seq),
        "before" => match conn.query_row(
            "SELECT MAX(seq) FROM session_message WHERE session_id=?1 AND seq<?2",
            rusqlite::params![parent, boundary_seq],
            |row| row.get::<_, Option<i64>>(0),
        ) {
            Ok(end) => end,
            Err(error) => {
                stats.failed = true;
                stats
                    .notes
                    .push(format!("opencode fork 前序边界读取失败: {error}"));
                None
            }
        },
        _ => None,
    }
}

fn copied_prefix_boundaries(conn: &Connection, stats: &mut ScanStats) -> HashMap<String, i64> {
    let mut boundaries = HashMap::new();
    let (Some(sessions), Some(messages)) = (
        column_set(conn, "session_v2"),
        column_set(conn, "session_message"),
    ) else {
        return boundaries;
    };
    if !["id", "fork_session_id", "fork_boundary"]
        .iter()
        .all(|column| sessions.contains(*column))
        || !["id", "session_id", "seq"]
            .iter()
            .all(|column| messages.contains(*column))
    {
        return boundaries;
    }
    let mut stmt = match conn.prepare(
        "SELECT id,fork_session_id,fork_boundary FROM session_v2 \
         WHERE fork_session_id IS NOT NULL AND fork_boundary IS NOT NULL",
    ) {
        Ok(stmt) => stmt,
        Err(error) => {
            stats.failed = true;
            stats
                .notes
                .push(format!("opencode fork 元数据查询失败: {error}"));
            return boundaries;
        }
    };
    let mut rows = match stmt.query([]) {
        Ok(rows) => rows,
        Err(error) => {
            stats.failed = true;
            stats
                .notes
                .push(format!("opencode fork 元数据读取失败: {error}"));
            return boundaries;
        }
    };
    let mut unresolved = 0u64;
    while let Some(row) = crate::common::next_sqlite_row(&mut rows, "opencode fork 元数据", stats)
    {
        let (Ok(fork), Ok(parent), Ok(boundary)) = (
            row.get::<_, String>(0),
            row.get::<_, String>(1),
            row.get::<_, String>(2),
        ) else {
            stats.lines_bad_json += 1;
            continue;
        };
        if fork.is_empty() || parent.is_empty() {
            unresolved += 1;
            continue;
        }
        if let Some(end) = copied_prefix_end(conn, &parent, &boundary, stats) {
            boundaries.insert(fork, end);
        } else {
            unresolved += 1;
        }
    }
    if unresolved > 0 {
        stats.notes.push(format!(
            "opencode {unresolved} 个 fork 边界无法确认,保留原记录并提示可能双计"
        ));
    }
    boundaries
}

/// 单个 db 内三通道互斥探测(§2,本机实证):session_message(v2)> message(v1)>
/// session 聚合兜底。聚合 = 明细之和(本机对拍五列全吻合),同取两通道必双计。
fn record_key(domain: &str, session: &str, id: &str) -> String {
    // Delimited JSON prevents ambiguous native IDs or path separators colliding.
    format!("opencode:{}", serde_json::json!([domain, session, id]))
}

fn scan_db(
    db: &Path,
    domain: &str,
    records: &mut Vec<UsageRecord>,
    seen: &mut HashSet<String>,
    stats: &mut ScanStats,
) -> Result<bool, String> {
    let conn = crate::common::open_sqlite_readonly(db).map_err(|_| "DATABASE_OPEN")?;
    conn.execute_batch("BEGIN DEFERRED")
        .map_err(|_| "DATABASE_SNAPSHOT")?;
    let mut recognized = false;
    // Within one database, preserve the existing v2 > v1 > aggregate authority.
    // Old-table residue must not resurrect sessions deleted from the v2 authority.
    for (table, v2) in [("session_message", true), ("message", false)] {
        let exists: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1)",
                [table],
                |r| r.get(0),
            )
            .map_err(|_| "SCHEMA_READ")?;
        if !exists {
            continue;
        }
        let columns = conn
            .prepare(&format!("SELECT * FROM {table} LIMIT 0"))
            .map_err(|_| "SCHEMA_READ")?
            .column_names()
            .into_iter()
            .map(str::to_owned)
            .collect::<HashSet<_>>();
        if !["id", "session_id", "data"]
            .iter()
            .all(|c| columns.contains(*c))
            || (v2
                && !["type", "seq", "time_created"]
                    .iter()
                    .all(|column| columns.contains(*column)))
        {
            return Err("UNSUPPORTED_MESSAGE_SCHEMA".into());
        }
        recognized = true;
        let nonempty: bool = conn
            .query_row(
                &format!("SELECT EXISTS(SELECT 1 FROM {table} LIMIT 1)"),
                [],
                |r| r.get(0),
            )
            .map_err(|_| "MESSAGE_READ")?;
        if !nonempty {
            continue;
        }
        let has_seq = v2 && columns.contains("seq");
        let fork_boundaries = if has_seq {
            copied_prefix_boundaries(&conn, stats)
        } else {
            HashMap::new()
        };
        let seq = if has_seq { "seq" } else { "NULL" };
        let kind = if v2 { "type" } else { "NULL" };
        let created = if v2 && columns.contains("time_created") {
            "time_created"
        } else {
            "NULL"
        };
        let mut stmt = conn
            .prepare(&format!(
                "SELECT id, session_id, data, {seq}, {kind}, {created} FROM {table}"
            ))
            .map_err(|_| "MESSAGE_QUERY")?;
        let mut rows = stmt.query([]).map_err(|_| "MESSAGE_READ")?;
        let mut table_rows = 0u64;
        let mut copied_rows = 0u64;
        while let Some(row) = crate::common::next_sqlite_row(&mut rows, "opencode", stats) {
            table_rows += 1;
            let (Ok(id), Ok(session)) = (row.get::<_, String>(0), row.get::<_, String>(1)) else {
                stats.lines_bad_json += 1;
                continue;
            };
            // v2 ordering is part of the protocol, not an optional hint. An
            // invalid sequence must not silently bypass a proven fork boundary.
            let sequence = if v2 {
                match row.get::<_, i64>(3) {
                    Ok(seq) if seq >= 0 => Some(seq),
                    _ => return Err("INVALID_MESSAGE_SEQUENCE".into()),
                }
            } else {
                None
            };
            let copied = sequence
                .zip(fork_boundaries.get(&session))
                .is_some_and(|(seq, end)| seq <= *end);
            if copied {
                copied_rows += 1;
                continue;
            }
            let data = match row.get_ref(2) {
                Ok(rusqlite::types::ValueRef::Text(t)) => t,
                _ => {
                    stats.lines_bad_json += 1;
                    continue;
                }
            };
            let sql_kind = if v2 {
                match row.get_ref(4) {
                    Ok(rusqlite::types::ValueRef::Text(t)) => match std::str::from_utf8(t) {
                        Ok(s) if !s.is_empty() => Some(s),
                        _ => {
                            stats.lines_bad_json += 1;
                            continue;
                        }
                    },
                    _ => {
                        stats.lines_bad_json += 1;
                        continue;
                    }
                }
            } else {
                None
            };
            if let Some(kind) = sql_kind.filter(|kind| *kind != "assistant") {
                // SQL type is authoritative. Inspect only redundant kind fields;
                // user text or even token-looking properties never become usage.
                match serde_json::from_slice::<OcRole>(data) {
                    Ok(doc)
                        if [doc.kind, doc.role]
                            .into_iter()
                            .flatten()
                            .all(|v| v == kind) => {}
                    _ => {
                        stats.lines_bad_json += 1;
                    }
                }
                continue;
            }
            // Borrow SQLite's JSON bytes; selectively deserialize only usage.
            let doc = match serde_json::from_slice::<OcMsg>(data) {
                Ok(doc) => doc,
                Err(_) => {
                    stats.lines_bad_json += 1;
                    continue;
                }
            };
            if v2 {
                if [doc.kind, doc.role]
                    .into_iter()
                    .flatten()
                    .any(|v| v != "assistant")
                {
                    stats.lines_bad_json += 1;
                    continue;
                }
            } else if doc.role != Some("assistant") {
                continue;
            }
            if v2
                && doc
                    .time
                    .as_ref()
                    .and_then(|t| t.created)
                    .is_some_and(|time| time <= 0)
            {
                stats.lines_bad_json += 1;
                continue;
            }
            let Some(mut p) = parse_msg(&doc) else {
                continue;
            };
            if v2 {
                // Only a missing payload timestamp may use the protocol's SQL
                // fallback; an explicit invalid timestamp cannot be papered over.
                if doc.time.as_ref().and_then(|t| t.created).is_none() {
                    p.ts_ms = row
                        .get::<_, Option<i64>>(5)
                        .ok()
                        .flatten()
                        .filter(|time| *time > 0)
                        .map(crate::common::secs_or_ms)
                        .unwrap_or(0);
                }
                if p.ts_ms <= 0 {
                    stats.lines_bad_json += 1;
                    continue;
                }
            } else if p.ts_ms == 0 {
                stats.lines_unknown_type += 1;
            }
            let key = record_key(domain, &session, &id);
            if seen.insert(key.clone()) {
                records.push(UsageRecord {
                    source: "opencode",
                    session_id: session,
                    agent: None,
                    model: p.model,
                    ts_ms: p.ts_ms,
                    caliber: Caliber::RequestDelta,
                    request_count: 1,
                    input_uncached: p.input,
                    cache_read: p.cache_read,
                    cache_write: p.cache_write,
                    cache_write_1h: 0,
                    output: p.output,
                    reasoning_output: p.reasoning,
                    cost_usd: p.cost_usd,
                    dedup_key: key,
                });
            }
        }
        if copied_rows > 0 {
            stats.notes.push(format!(
                "opencode fork 复制前缀跳过 {copied_rows} 行(按 session_v2 边界去重)"
            ));
        }
        stats.notes.push(format!(
            "{} {} 表 {} 行(v{})",
            db.display(),
            table,
            table_rows,
            if v2 { 2 } else { 1 }
        ));
        stats.files_parsed += 1;
        return Ok(true);
    }
    // 通道三:session 表聚合列兜底(会话级累计,锚 time_created,§2)
    let mut columns = conn
        .prepare("PRAGMA table_info(session)")
        .map_err(|_| "SCHEMA_READ")?;
    let names = columns
        .query_map([], |r| r.get::<_, String>(1))
        .map_err(|_| "SCHEMA_READ")?
        .collect::<Result<HashSet<_>, _>>()
        .map_err(|_| "SCHEMA_READ")?;
    let has_cols = names.contains("tokens_input");
    if has_cols {
        let sql = "SELECT id, model, cost, tokens_input, tokens_output, tokens_reasoning, \
                   tokens_cache_read, tokens_cache_write, time_created FROM session \
                   WHERE tokens_input>0 OR tokens_output>0 OR tokens_reasoning>0 \
                      OR tokens_cache_read>0 OR tokens_cache_write>0";
        let Ok(mut stmt) = conn.prepare(sql) else {
            return Err("SESSION_QUERY".into());
        };
        let Ok(mut rows) = stmt.query([]) else {
            return Err("SESSION_QUERY".into());
        };
        let mut n = 0u64;
        while let Some(row) = crate::common::next_sqlite_row(&mut rows, "opencode", stats) {
            let vals: Vec<SqlValue> = (0..9)
                .filter_map(|i| row.get::<_, SqlValue>(i).ok())
                .collect();
            if vals.len() < 9 {
                continue;
            }
            let Some(id) = v_text(&vals[0]) else { continue };
            let ts_ms = match &vals[8] {
                SqlValue::Integer(t) if *t > 0 => crate::common::secs_or_ms(*t),
                _ => 0,
            };
            if ts_ms <= 0 {
                continue;
            }
            let g = |i: usize| match &vals[i] {
                SqlValue::Integer(v) => (*v).max(0) as u64,
                SqlValue::Real(f) if *f > 0.0 => *f as u64,
                _ => 0,
            };
            let model = v_text(&vals[1])
                .and_then(|s| {
                    serde_json::from_str::<serde_json::Value>(&s)
                        .ok()
                        .and_then(|m| m.get("id").and_then(|x| x.as_str()).map(|s| s.to_string()))
                })
                .filter(|m| !m.is_empty())
                .or_else(|| v_text(&vals[1]).filter(|m| !m.is_empty()))
                .unwrap_or_else(|| "unknown".into());
            let cost_usd = match &vals[2] {
                SqlValue::Real(f) => Some(*f),
                SqlValue::Integer(i) => Some(*i as f64),
                _ => None,
            };
            let key = record_key(domain, &id, "session-total");
            if seen.insert(key.clone()) {
                n += 1;
                records.push(UsageRecord {
                    source: "opencode",
                    session_id: id.clone(),
                    agent: None,
                    model,
                    ts_ms,
                    caliber: Caliber::RequestDelta, // 会话终态一次性入账(§4 同 droid 裁决)
                    request_count: 1,
                    input_uncached: g(3),
                    cache_read: g(6),
                    cache_write: g(7),
                    cache_write_1h: 0,
                    output: g(4),
                    reasoning_output: g(5),
                    cost_usd,
                    dedup_key: key,
                });
            }
        }
        if n > 0 {
            stats.notes.push(format!(
                "{} session 聚合 {} 行(明细表空,兜底通道)",
                db.display(),
                n
            ));
            stats.files_parsed += 1;
            return Ok(true);
        }
    }
    if !recognized && !has_cols {
        return Err("UNSUPPORTED_SCHEMA".into());
    }
    Ok(false)
}

impl UsageSource for OpencodeSource {
    fn id(&self) -> &'static str {
        "opencode"
    }

    fn discover_roots(&self) -> Vec<PathBuf> {
        let mut roots = env_roots("OPENCODE_DATA_DIR");
        if roots.is_empty() {
            if let Ok(x) = std::env::var("XDG_DATA_HOME") {
                let p = PathBuf::from(x).join("opencode");
                if p.is_dir() {
                    roots.push(p);
                }
            }
        }
        if roots.is_empty() {
            let p = home().join(".local/share/opencode");
            if p.is_dir() {
                roots.push(p);
            }
        }
        if roots.is_empty() {
            let p = home().join("Library/Application Support/opencode");
            if p.is_dir() {
                roots.push(p);
            }
        }
        roots
    }

    fn prepare_scan(&self, roots: &[PathBuf]) -> Option<Box<dyn crate::model::PreparedSourceScan>> {
        Some(Box::new(DatabaseInputs::discover(roots)))
    }

    fn scan_roots(
        &self,
        roots: &[PathBuf],
        _max_files: Option<usize>,
    ) -> (Vec<UsageRecord>, ScanStats) {
        use crate::model::PreparedSourceScan;
        DatabaseInputs::discover(roots).scan()
    }
}

struct DatabaseInput {
    path: PathBuf,
    physical: String,
    domain: String,
    next: bool,
}
struct RootInput {
    databases: Vec<DatabaseInput>,
    json: Vec<PathBuf>,
    json_selected: bool,
    incomplete: bool,
}

// Discovery needs only the old fallback gate, not any payload. It prevents an
// unused, unreadable legacy directory from degrading a valid database backend.
fn database_has_rows(path: &Path) -> Result<bool, ()> {
    let conn = crate::common::open_sqlite_readonly(path).map_err(|_| ())?;
    conn.execute_batch("BEGIN DEFERRED").map_err(|_| ())?;
    let mut supported = false;
    for sql in [
        "SELECT EXISTS(SELECT 1 FROM session_message LIMIT 1)",
        "SELECT EXISTS(SELECT 1 FROM message LIMIT 1)",
        "SELECT EXISTS(SELECT 1 FROM session WHERE tokens_input>0 OR tokens_output>0 OR tokens_reasoning>0 OR tokens_cache_read>0 OR tokens_cache_write>0 LIMIT 1)",
    ] {
        if let Ok(found) = conn.query_row(sql, [], |r| r.get::<_, bool>(0)) {
            supported = true;
            if found { return Ok(true); }
        }
    }
    supported.then_some(false).ok_or(())
}
struct DatabaseInputs {
    roots: Vec<RootInput>,
    blocked: HashSet<String>,
    fingerprint: crate::common::SourceFingerprint,
    stats: ScanStats,
}
impl DatabaseInputs {
    fn failure(&mut self, reason: &str) {
        self.stats.failed = true;
        self.stats.notes.push(format!("opencode {reason}"));
    }

    fn stamp(&mut self, path: &Path) -> Option<crate::common::FileStamp> {
        let stamp = crate::common::file_stamp(path);
        if let Some(stamp) = &stamp {
            self.fingerprint.push((path.to_path_buf(), stamp.clone()));
        } else {
            self.failure("INPUT_METADATA");
        }
        stamp
    }

    fn sidecars(&mut self, path: &Path) {
        for suffix in ["-wal", "-shm", "-journal"] {
            let mut name = path.as_os_str().to_owned();
            name.push(suffix);
            let sidecar = PathBuf::from(name);
            match std::fs::symlink_metadata(&sidecar) {
                Ok(_) => {
                    self.stamp(&sidecar);
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => self.failure("SIDECAR_METADATA"),
            }
        }
    }

    fn discover(roots: &[PathBuf]) -> Self {
        let mut inputs = Self {
            roots: Vec::new(),
            blocked: HashSet::new(),
            fingerprint: Vec::new(),
            stats: ScanStats::default(),
        };
        let mut normalized = Vec::new();
        for root in roots {
            match root.canonicalize() {
                Ok(root) => normalized.push(root),
                Err(_) => inputs.failure("ROOT_READ"),
            }
        }
        normalized.sort();
        normalized.dedup();
        for root in normalized {
            inputs
                .fingerprint
                .push((root.clone(), crate::common::FileStamp::membership()));
            let mut discovered = RootInput {
                databases: Vec::new(),
                json: Vec::new(),
                json_selected: false,
                incomplete: false,
            };
            let entries = match std::fs::read_dir(&root)
                .and_then(|entries| entries.collect::<Result<Vec<_>, _>>())
            {
                Ok(entries) => entries,
                Err(_) => {
                    inputs.failure("ROOT_READ");
                    continue;
                }
            };
            let mut candidates = entries
                .into_iter()
                .map(|e| e.path())
                .filter(|path| {
                    path.file_name().and_then(|n| n.to_str()).is_some_and(|n| {
                        n == "opencode.db" || (n.starts_with("opencode-") && n.ends_with(".db"))
                    })
                })
                .collect::<Vec<_>>();
            candidates.sort();
            for alias in candidates {
                let is_next = alias.file_name().is_some_and(|n| n == "opencode-next.db");
                let Some(stamp) = inputs.stamp(&alias) else {
                    discovered.incomplete = true;
                    continue;
                };
                let path = match alias.canonicalize() {
                    Ok(path) if path.is_file() => path,
                    _ => {
                        inputs.failure("DATABASE_PATH");
                        discovered.incomplete = true;
                        continue;
                    }
                };
                // SQLite resolves database symlinks; follow the real sidecars too.
                inputs.sidecars(&alias);
                if path != alias {
                    inputs.stamp(&path);
                    inputs.sidecars(&path);
                }
                let identity = stamp.identity_key();
                let parts = identity.split('/').take(2).collect::<Vec<_>>();
                let physical = if parts.len() == 2 && parts.iter().all(|p| !p.is_empty()) {
                    format!("file:{}:{}", parts[0], parts[1])
                } else {
                    // On platforms without a reliable file ID only canonical
                    // aliases can be proved equal, never equal contents or names.
                    format!("path:{path:?}")
                };
                if let Some(existing) = discovered
                    .databases
                    .iter_mut()
                    .find(|db| db.physical == physical)
                {
                    existing.next |= is_next;
                    continue;
                }
                let mut domain = Sha256::new();
                domain.update(root.as_os_str().as_encoded_bytes());
                domain.update([0]);
                domain.update(physical.as_bytes());
                discovered.databases.push(DatabaseInput {
                    path,
                    physical,
                    domain: format!("{:x}", domain.finalize()),
                    next: is_next,
                });
            }
            if discovered.databases.len() > 1 || discovered.incomplete {
                for db in &discovered.databases {
                    if db.next {
                        inputs.blocked.insert(db.physical.clone());
                        inputs.failure("NEXT_MIGRATION_AMBIGUOUS: 同根迁移库与其他物理库并存");
                        discovered.incomplete = true;
                    }
                }
            }
            discovered.json_selected = !discovered.incomplete
                && discovered
                    .databases
                    .iter()
                    .all(|db| database_has_rows(&db.path) == Ok(false));
            if !discovered.json_selected {
                inputs.roots.push(discovered);
                continue;
            }
            let messages = root.join("storage/message");
            match std::fs::symlink_metadata(&messages) {
                Ok(_) => {
                    // Legacy JSON discovery errors must not look like successful
                    // empty fallback, and every input gets a real target stamp.
                    let _ = crate::common::take_walk_error();
                    discovered.json = walk_with(&messages, None, &|p| {
                        p.extension().is_some_and(|x| x == "json")
                    });
                    if crate::common::take_walk_error() {
                        inputs.failure("JSON_DISCOVERY");
                        discovered.incomplete = true;
                    }
                    discovered.json.sort();
                    for path in &discovered.json {
                        inputs.stamp(path);
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => {
                    inputs.failure("JSON_DISCOVERY");
                    discovered.incomplete = true;
                }
            }
            inputs.roots.push(discovered);
        }
        inputs.fingerprint.sort_by(|a, b| a.0.cmp(&b.0));
        inputs.fingerprint.dedup_by(|a, b| a.0 == b.0);
        inputs
    }
}

impl crate::model::PreparedSourceScan for DatabaseInputs {
    fn fingerprint(&self) -> Option<crate::common::SourceFingerprint> {
        (!self.stats.failed).then(|| self.fingerprint.clone())
    }

    fn scan(&self) -> (Vec<UsageRecord>, ScanStats) {
        let mut stats = self.stats.clone();
        let mut records = Vec::new();
        let mut seen = HashSet::new();
        let mut scanned = HashMap::<String, Result<bool, String>>::new();
        for root in &self.roots {
            let mut has_rows = false;
            let mut failed = root.incomplete;
            for db in &root.databases {
                if self.blocked.contains(&db.physical) {
                    failed = true;
                    continue;
                }
                let result = scanned.entry(db.physical.clone()).or_insert_with(|| {
                    stats.files_seen += 1;
                    scan_db(&db.path, &db.domain, &mut records, &mut seen, &mut stats)
                });
                match result {
                    Ok(found) => has_rows |= *found,
                    Err(reason) => {
                        stats.failed = true;
                        stats.notes.push(format!("opencode {reason}"));
                        failed = true;
                    }
                }
            }
            if has_rows || failed {
                continue;
            }
            if !root.json_selected {
                // The database became empty after discovery. Re-discovery will
                // select the old JSON fallback; this incomplete view is not zero.
                stats.failed = true;
                stats
                    .notes
                    .push("opencode BACKEND_CHANGED: 下轮重新发现用量输入".into());
                continue;
            }
            stats.files_seen += root.json.len();
            let mut parsed = 0u64;
            for path in &root.json {
                let Ok(text) = std::fs::read_to_string(path) else {
                    stats.lines_bad_json += 1;
                    continue;
                };
                let Ok(doc) = serde_json::from_str::<serde_json::Value>(&text) else {
                    stats.lines_bad_json += 1;
                    continue;
                };
                let role = doc.get("role").or_else(|| doc.get("type"));
                if role.and_then(|x| x.as_str()) != Some("assistant") {
                    continue;
                }
                let Some(p) = parse_payload(&doc) else {
                    continue;
                };
                if p.ts_ms == 0 {
                    stats.lines_unknown_type += 1;
                } // 缺时间戳:入账但计数可见(2026-10-17)
                  // 会话 id = storage/message/<sid>/<mid>.json 的上级目录
                let session = path
                    .parent()
                    .and_then(|d| d.file_name())
                    .and_then(|s| s.to_str())
                    .unwrap_or("opencode")
                    .to_string();
                let key = format!("opencode:json:{path:?}");
                if seen.insert(key.clone()) {
                    parsed += 1;
                    records.push(UsageRecord {
                        source: "opencode",
                        session_id: session,
                        agent: None,
                        model: p.model,
                        ts_ms: p.ts_ms,
                        caliber: Caliber::RequestDelta,
                        request_count: 1,
                        input_uncached: p.input,
                        cache_read: p.cache_read,
                        cache_write: p.cache_write,
                        cache_write_1h: 0,
                        output: p.output,
                        reasoning_output: p.reasoning,
                        cost_usd: p.cost_usd,
                        dedup_key: key,
                    });
                }
            }
            stats.files_parsed += root.json.len();
            if parsed > 0 {
                stats
                    .notes
                    .push(format!("JSON 兜底 {} 条(storage/message)", parsed));
            }
        }
        stats.failed |= stats.lines_bad_json > 0;
        (records, stats)
    }
}
