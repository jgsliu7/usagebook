//! OpenClaw 适配器 —— 依据 docs/数据源档案/17-openclaw.md(源码一手取证+实库对账)。
//!
//! 双后端(§7.2,SQLite 正典优先):agents/<agentId>/agent/openclaw-agent.sqlite 的
//!     transcript_events(event_json / event_zstd 二选一)为正典;agents/<agentId>/sessions/*.jsonl
//!     为遗留冷层,仅当该 agent 无 SQLite 时兜底。同 (agentId,sessionId) 不双读。
//! 发现(§7.1):OPENCLAW_STATE_DIR(单根)> 默认 ~/.openclaw(缺失回退 ~/.clawdbot 遗留)。
//! 语义(§7.3):input **exclusive 直取**(官方已归一,勿减 cache);total=in+out+cr+cw 恒等式;
//!     reasoningTokens 是 output 明细 → output 落库拆分为 output−reasoning + reasoning 单列;
//!     cost 分治:totalOrigin==provider-billed 直取,估算不落(model.rs 纪律,2026-10-17 定稿)。
//! 去重(§7.4):openclaw:{agentId}:{session}:{event_id},(session,seq) 兜底。

use crate::common::{env_roots, flex_to_ms, home, json_u64};
use crate::model::{Caliber, ScanStats, UsageRecord, UsageSource};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::io::Read;

pub(crate) mod cold;
use std::path::{Path, PathBuf};

pub struct OpenclawSource;

#[cfg(feature = "regression-hooks")]
thread_local! {
    #[allow(clippy::type_complexity)]
    static AFTER_COLD_ARCHIVE: std::cell::RefCell<Option<(PathBuf, Box<dyn FnOnce()>)>> = const { std::cell::RefCell::new(None) };
    #[allow(clippy::type_complexity)]
    static AFTER_SQLITE_SNAPSHOT: std::cell::RefCell<Option<(PathBuf, Box<dyn FnOnce()>)>> = const { std::cell::RefCell::new(None) };
    static ENTRY_ERROR: std::cell::RefCell<Option<PathBuf>> = const { std::cell::RefCell::new(None) };
    #[allow(clippy::type_complexity)]
    static AFTER_ENTRIES: std::cell::RefCell<Option<(PathBuf, Box<dyn FnOnce()>)>> = const { std::cell::RefCell::new(None) };
}

/// Inject one directory iterator error, which is otherwise difficult to provoke
/// reliably on a local filesystem. Production builds do not contain this hook.
#[cfg(feature = "regression-hooks")]
#[doc(hidden)]
pub fn __fail_directory_entry_for_tests(path: PathBuf) {
    ENTRY_ERROR.with(|slot| *slot.borrow_mut() = Some(path));
}

#[cfg(feature = "regression-hooks")]
#[doc(hidden)]
pub fn __after_directory_entries_for_tests(path: PathBuf, hook: impl FnOnce() + 'static) {
    AFTER_ENTRIES.with(|slot| *slot.borrow_mut() = Some((path, Box::new(hook))));
}

/// Run a mutation through a separate connection after the scan owns its read
/// snapshot. Used only by external regression tests for archive transitions.
#[cfg(feature = "regression-hooks")]
#[doc(hidden)]
pub fn __after_sqlite_snapshot_for_tests(path: PathBuf, hook: impl FnOnce() + 'static) {
    AFTER_SQLITE_SNAPSHOT.with(|slot| *slot.borrow_mut() = Some((path, Box::new(hook))));
}

#[cfg(feature = "regression-hooks")]
#[doc(hidden)]
pub fn __after_cold_archive_for_tests(path: PathBuf, hook: impl FnOnce() + 'static) {
    AFTER_COLD_ARCHIVE.with(|slot| *slot.borrow_mut() = Some((path, Box::new(hook))));
}

/// 只读打开(收编至 common,2026-10-17):此前自持样板且 URI flag 对纯 PathBuf 无意义——
/// SQLITE_OPEN_URI 仅影响 file: URI 解析,传 PathBuf 反而在路径含 `?` 时有解析歧义。
fn open_ro(db: &Path) -> Option<rusqlite::Connection> {
    crate::common::open_sqlite_readonly(db).ok()
}

/// 别名组取值:首个存在且非 null 的键(遗留 JSONL 通道别名宽度,§2)。
fn alias(v: &Value, keys: &[&str]) -> u64 {
    for k in keys {
        if let Some(x) = v.get(*k) {
            if !x.is_null() {
                return json_u64(Some(x));
            }
        }
    }
    0
}

/// 从 assistant message 的 usage 构造五桶(exclusive 语义,§7.3)。
fn buckets_from_usage(usage: &Value) -> Option<(u64, u64, u64, u64, u64)> {
    let i = alias(usage, &["input", "inputTokens", "input_tokens"]);
    let cr = alias(
        usage,
        &["cacheRead", "cache_read_input_tokens", "cachedTokens"],
    );
    let cw = alias(
        usage,
        &[
            "cacheWrite",
            "cache_creation_input_tokens",
            "cacheCreationTokens",
        ],
    );
    let o = alias(usage, &["output", "outputTokens", "output_tokens"]);
    let rs = alias(usage, &["reasoningTokens", "reasoning_tokens", "reasoning"]);
    if i == 0 && cr == 0 && cw == 0 && o == 0 {
        return None; // 全零 usage 不入账(三跳过)
    }
    // output 含 reasoning(明细)→ 拆分,防 token_total 双计(model.rs output 语义)
    let out_net = o.saturating_sub(rs);
    Some((i, cr, cw, out_net, rs))
}

impl UsageSource for OpenclawSource {
    fn id(&self) -> &'static str {
        "openclaw"
    }

    fn discover_roots(&self) -> Vec<PathBuf> {
        let mut roots = env_roots("OPENCLAW_STATE_DIR");
        if roots.is_empty() {
            let oc = home().join(".openclaw");
            if oc.is_dir() {
                roots.push(oc);
            } else {
                // 遗留目录兜底(§7.1:.clawdbot 仍存在;.moltbot 迁移已移除)
                let legacy = home().join(".clawdbot");
                if legacy.is_dir() {
                    roots.push(legacy);
                }
            }
        }
        roots
    }

    fn prepare_scan(&self, roots: &[PathBuf]) -> Option<Box<dyn crate::model::PreparedSourceScan>> {
        Some(Box::new(OpenclawInputs::discover(roots)))
    }

    fn scan_roots(
        &self,
        roots: &[PathBuf],
        _max_files: Option<usize>,
    ) -> (Vec<UsageRecord>, ScanStats) {
        use crate::model::PreparedSourceScan;
        OpenclawInputs::discover(roots).scan()
    }
}

#[derive(Clone)]
enum AgentBackend {
    Sqlite {
        db: PathBuf,
        cold_dir: PathBuf,
        archives: Vec<cold::Archive>,
    },
    Jsonl(Vec<PathBuf>),
}
struct AgentInput {
    id: String,
    backend: AgentBackend,
}
struct OpenclawInputs {
    agents: Vec<AgentInput>,
    fingerprint: crate::common::SourceFingerprint,
    stats: ScanStats,
}
impl OpenclawInputs {
    fn failure(&mut self, path: &Path, reason: impl std::fmt::Display) {
        self.stats.failed = true;
        self.stats
            .notes
            .push(format!("{} 用量输入发现失败: {reason}", path.display()));
    }

    // The discovery tree is shallow (root / agents / logical agent / backend).
    // Track only the current ancestry: two logical aliases of one physical agent
    // must both be retained. canonicalize also rejects OS-level symlink loops.
    fn directory(
        &mut self,
        path: &Path,
        ancestors: &[PathBuf],
        optional: bool,
    ) -> Result<Option<Vec<PathBuf>>, ()> {
        match std::fs::symlink_metadata(path) {
            Err(e) if optional && e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => {
                self.failure(path, e.kind());
                return Err(());
            }
            Ok(_) => {}
        }
        let canonical = match std::fs::canonicalize(path) {
            Ok(p) => p,
            Err(e) => {
                self.failure(path, e.kind());
                return Err(());
            }
        };
        if ancestors.len() >= 8 || ancestors.contains(&canonical) {
            self.failure(path, "目录链接循环或超出发现深度");
            return Err(());
        }
        match std::fs::metadata(path) {
            Ok(md) if md.is_dir() => {}
            Ok(_) => {
                self.failure(path, "预期目录");
                return Err(());
            }
            Err(e) => {
                self.failure(path, e.kind());
                return Err(());
            }
        }
        self.fingerprint
            .push((path.to_path_buf(), crate::common::FileStamp::membership()));
        let mut chain = ancestors.to_vec();
        chain.push(canonical);
        Ok(Some(chain))
    }

    fn entries(&mut self, path: &Path) -> Vec<std::fs::DirEntry> {
        let mut entries = Vec::new();
        match std::fs::read_dir(path) {
            Ok(reader) => {
                #[cfg(feature = "regression-hooks")]
                let reader = {
                    let fault = ENTRY_ERROR.with(|slot| {
                        if slot.borrow().as_deref() == Some(path) {
                            slot.borrow_mut().take();
                            true
                        } else {
                            false
                        }
                    });
                    reader.chain(
                        fault.then(|| Err(std::io::Error::other("injected directory entry error"))),
                    )
                };
                for entry in reader {
                    match entry {
                        Ok(entry) => entries.push(entry),
                        Err(e) => self.failure(path, e.kind()),
                    }
                }
            }
            Err(e) => self.failure(path, e.kind()),
        }
        entries.sort_by_key(|entry| entry.path());
        #[cfg(feature = "regression-hooks")]
        {
            let hook = AFTER_ENTRIES.with(|slot| {
                if slot.borrow().as_ref().is_some_and(|(p, _)| p == path) {
                    slot.borrow_mut().take()
                } else {
                    None
                }
            });
            if let Some((_, hook)) = hook {
                hook();
            }
        }
        entries
    }

    /// Only a genuinely absent optional path is missing. Broken symlinks and
    /// unreadable paths are failures; they must not select the other backend.
    fn file(&mut self, path: &Path, optional: bool) -> Result<bool, ()> {
        match std::fs::symlink_metadata(path) {
            Err(e) if optional && e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(e) => {
                self.failure(path, e.kind());
                return Err(());
            }
            Ok(_) => {}
        }
        match std::fs::metadata(path) {
            Ok(md) if md.is_file() => match crate::common::file_stamp(path) {
                Some(stamp) => {
                    self.fingerprint.push((path.to_path_buf(), stamp));
                    Ok(true)
                }
                None => {
                    self.failure(path, "无法读取文件状态");
                    Err(())
                }
            },
            Ok(_) => {
                self.failure(path, "预期用量文件");
                Err(())
            }
            Err(e) => {
                self.failure(path, e.kind());
                Err(())
            }
        }
    }

    fn discover(roots: &[PathBuf]) -> Self {
        let mut out = Self {
            agents: Vec::new(),
            fingerprint: Vec::new(),
            stats: ScanStats::default(),
        };
        if roots.is_empty() {
            out.stats
                .notes
                .push("未发现 ~/.openclaw 或 OPENCLAW_STATE_DIR".into());
        }
        for root in roots {
            let Ok(Some(root_chain)) = out.directory(root, &[], false) else {
                continue;
            };
            let agents_dir = root.join("agents");
            let Ok(Some(agents_chain)) = out.directory(&agents_dir, &root_chain, true) else {
                continue;
            };
            for entry in out.entries(&agents_dir) {
                let path = entry.path();
                // Regular files under agents are not agent directories. A link
                // must be resolved even if broken, so errors are not hidden.
                match entry.file_type() {
                    Ok(kind) if !kind.is_dir() && !kind.is_symlink() => continue,
                    Err(e) => {
                        out.failure(&path, e.kind());
                        continue;
                    }
                    _ => {}
                }
                let Some(id) = entry.file_name().to_str().map(str::to_owned) else {
                    out.failure(&path, "agent 名称不是有效 UTF-8");
                    continue;
                };
                let Ok(Some(agent_chain)) = out.directory(&path, &agents_chain, false) else {
                    continue;
                };
                let database_dir = path.join("agent");
                let db = database_dir.join("openclaw-agent.sqlite");
                // An inaccessible/broken canonical directory is not proof that
                // the SQLite backend is absent; never silently use JSONL.
                let db_dir = match out.directory(&database_dir, &agent_chain, true) {
                    Ok(directory) => directory,
                    Err(()) => continue,
                };
                match if db_dir.is_some() {
                    out.file(&db, true)
                } else {
                    Ok(false)
                } {
                    Err(()) => continue,
                    Ok(true) => {
                        // SQLite resolves a database-file symlink before finding
                        // WAL/journal companions. Keep logical ownership in the
                        // manifest, but fix the actual opened database path too.
                        let actual_db = match std::fs::canonicalize(&db) {
                            Ok(path) => path,
                            Err(e) => {
                                out.failure(&db, e.kind());
                                continue;
                            }
                        };
                        if actual_db != db && !matches!(out.file(&actual_db, false), Ok(true)) {
                            continue;
                        }
                        for suffix in ["-wal", "-shm", "-journal"] {
                            let mut name = actual_db.as_os_str().to_os_string();
                            name.push(suffix);
                            let _ = out.file(&PathBuf::from(name), true);
                        }
                        let cold_dir = path.join("sessions/cold");
                        let archives = match open_ro(&actual_db)
                            .ok_or("数据库不可读")
                            .and_then(|conn| cold::references(&conn))
                        {
                            Ok(archives) => archives,
                            Err(reason) => {
                                out.failure(&db, reason);
                                continue;
                            }
                        };
                        // A content hash of fixed reference descriptors is metadata,
                        // never transcript or FTS content. Include even the empty set.
                        out.fingerprint.push((
                            cold::reference_marker(&actual_db, &archives),
                            crate::common::FileStamp::membership(),
                        ));
                        for archive in &archives {
                            if archive.storage == "file" {
                                let _ = out.file(&cold_dir.join(&archive.archive_name), false);
                            }
                        }
                        out.agents.push(AgentInput {
                            id,
                            backend: AgentBackend::Sqlite {
                                db: actual_db,
                                cold_dir,
                                archives,
                            },
                        });
                        continue;
                    }
                    Ok(false) => {}
                }
                let sessions = path.join("sessions");
                let mut files = Vec::new();
                if matches!(out.directory(&sessions, &agent_chain, true), Ok(Some(_))) {
                    for entry in out.entries(&sessions) {
                        let file = entry.path();
                        // Keep reset/deleted archives; do not inspect unrelated files.
                        if !file
                            .file_name()
                            .and_then(|n| n.to_str())
                            .is_some_and(|n| n.contains(".jsonl"))
                        {
                            continue;
                        }
                        match entry.file_type() {
                            Ok(kind) if kind.is_dir() => continue,
                            Err(e) => {
                                out.failure(&file, e.kind());
                                continue;
                            }
                            _ => {}
                        }
                        if matches!(out.file(&file, false), Ok(true)) {
                            files.push(file);
                        }
                    }
                }
                out.agents.push(AgentInput {
                    id,
                    backend: AgentBackend::Jsonl(files),
                });
            }
        }
        out.fingerprint.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }
}
impl crate::model::PreparedSourceScan for OpenclawInputs {
    fn fingerprint(&self) -> Option<crate::common::SourceFingerprint> {
        (!self.stats.failed).then(|| self.fingerprint.clone())
    }
    fn scan(&self) -> (Vec<UsageRecord>, ScanStats) {
        let mut stats = self.stats.clone();
        let mut records = Vec::new();
        let mut seen = HashSet::new();
        let mut sqlite_seen = HashMap::new();
        let mut sqlite_guards = Vec::new();
        let mut zstd_decoded = 0;
        let (mut sqlite_agents, mut jsonl_agents) = (0, 0);
        for agent in &self.agents {
            let tag = (agent.id != "main").then_some(agent.id.as_str());
            match &agent.backend {
                AgentBackend::Sqlite {
                    db,
                    cold_dir,
                    archives,
                } => {
                    sqlite_agents += 1;
                    stats.files_seen += 1;
                    stats.files_parsed += 1;
                    if let Some(guard) = scan_sqlite_agent(
                        db,
                        cold_dir,
                        archives,
                        &agent.id,
                        tag,
                        &mut records,
                        &mut sqlite_seen,
                        &mut stats,
                        &mut zstd_decoded,
                    ) {
                        sqlite_guards.push(guard);
                    }
                }
                AgentBackend::Jsonl(files) => {
                    if !files.is_empty() {
                        jsonl_agents += 1;
                    }
                    stats.files_seen += files.len();
                    scan_jsonl_agent(files, &agent.id, tag, &mut records, &mut seen, &mut stats);
                }
            }
        }
        // Validate again after every agent: an early archive/database can be
        // changed while a later archive or agent is still being processed.
        for guard in &sqlite_guards {
            if !guard.unchanged() {
                stats.failed = true;
                stats
                    .notes
                    .push("整源扫描期间 SQLite 或冷归档发生变化，下轮重试".into());
            }
        }
        stats.notes.push(format!(
            "SQLite 正典 agent {sqlite_agents} 个 / JSONL 遗留 agent {jsonl_agents} 个(§7.2)"
        ));
        if zstd_decoded > 0 {
            stats
                .notes
                .push(format!("event_zstd 解压 {zstd_decoded} 行"));
        }
        (records, stats)
    }
}

// Select only the fields needed for accounting. serde skips message content,
// tool arguments and all unrelated envelope fields without materializing them.
#[derive(Deserialize, Serialize)]
struct EventProjection {
    #[serde(rename = "type", default)]
    kind: Option<String>,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    timestamp: Option<Value>,
    #[serde(default)]
    message: Option<MessageProjection>,
}
#[derive(Deserialize, Serialize)]
struct MessageProjection {
    #[serde(default)]
    role: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default, rename = "modelId")]
    model_id: Option<String>,
    #[serde(default)]
    timestamp: Option<Value>,
    #[serde(default)]
    usage: Option<Value>,
}

#[allow(clippy::too_many_arguments)]
fn project_sqlite_event(
    raw: &[u8],
    session: &str,
    seq: i64,
    domain: &str,
    agent_id: &str,
    agent_tag: Option<&str>,
    records: &mut Vec<UsageRecord>,
    seen: &mut HashMap<String, [u8; 32]>,
    stats: &mut ScanStats,
) -> Result<(), &'static str> {
    stats.lines_total += 1;
    let event: EventProjection = serde_json::from_slice(raw).map_err(|_| "事件 JSON 无效")?;
    let identity = event
        .id
        .as_deref()
        .filter(|id| !id.is_empty())
        .map(|id| format!("id:{id}"))
        .unwrap_or_else(|| format!("seq:{seq}"));
    // Length-safe tuple encoding prevents opaque IDs containing separators from
    // colliding. Physical database identity separates independent discovery roots.
    let key = format!(
        "openclaw:{}",
        serde_json::to_string(&(domain, agent_id, session, identity)).expect("identity tuple")
    );
    let digest: [u8; 32] =
        Sha256::digest(serde_json::to_vec(&event).expect("event projection")).into();
    if let Some(previous) = seen.get(&key) {
        return if previous == &digest {
            Ok(())
        } else {
            Err("同一事件身份的用量投影冲突")
        };
    }
    seen.insert(key.clone(), digest);
    if event.kind.as_deref() != Some("message") {
        return Ok(());
    }
    let Some(msg) = event.message else {
        return Ok(());
    };
    if msg.role.as_deref() != Some("assistant") {
        return Ok(());
    }
    let Some(usage) = msg.usage.as_ref().filter(|usage| usage.is_object()) else {
        return Ok(());
    };
    let Some((i, cr, cw, output, reasoning)) = buckets_from_usage(usage) else {
        return Ok(());
    };
    let ts_ms = flex_to_ms(msg.timestamp.as_ref())
        .or_else(|| flex_to_ms(event.timestamp.as_ref()))
        .unwrap_or(0);
    let model = msg
        .model
        .or(msg.model_id)
        .filter(|model| !model.is_empty())
        .unwrap_or_else(|| "unknown".into());
    let cost_usd = (usage.pointer("/cost/totalOrigin").and_then(Value::as_str)
        == Some("provider-billed"))
    .then(|| {
        usage
            .pointer("/cost/total")
            .and_then(Value::as_f64)
            .filter(|cost| *cost > 0.0)
    })
    .flatten();
    records.push(UsageRecord {
        source: "openclaw",
        session_id: session.into(),
        agent: agent_tag.map(str::to_owned),
        model,
        ts_ms,
        caliber: Caliber::RequestDelta,
        request_count: 1,
        input_uncached: i,
        cache_read: cr,
        cache_write: cw,
        cache_write_1h: 0,
        output,
        reasoning_output: reasoning,
        cost_usd,
        dedup_key: key,
    });
    Ok(())
}

struct SqliteReadGuard {
    conn: rusqlite::Connection,
    db: PathBuf,
    version: i64,
    stamp: crate::common::FileStamp,
    files: Vec<(PathBuf, crate::common::FileStamp)>,
}
impl SqliteReadGuard {
    fn unchanged(&self) -> bool {
        self.conn
            .query_row("PRAGMA data_version", [], |row| row.get::<_, i64>(0))
            .ok()
            == Some(self.version)
            && crate::common::file_stamp(&self.db).as_ref() == Some(&self.stamp)
            && self
                .files
                .iter()
                .all(|(path, stamp)| crate::common::file_stamp(path).as_ref() == Some(stamp))
    }
}

#[allow(clippy::too_many_arguments)]
fn scan_sqlite_agent(
    db: &Path,
    cold_dir: &Path,
    expected_archives: &[cold::Archive],
    agent_id: &str,
    agent_tag: Option<&str>,
    records: &mut Vec<UsageRecord>,
    seen: &mut HashMap<String, [u8; 32]>,
    stats: &mut ScanStats,
    zstd_decoded: &mut u64,
) -> Option<SqliteReadGuard> {
    let result = (|| -> Result<SqliteReadGuard, &'static str> {
        let conn = open_ro(db).ok_or("数据库只读打开失败")?;
        let version: i64 = conn
            .query_row("PRAGMA data_version", [], |row| row.get(0))
            .map_err(|_| "数据库版本不可读")?;
        let stamp = crate::common::file_stamp(db).ok_or("数据库状态不可读")?;
        let domain = stamp
            .identity_key()
            .split('/')
            .take(2)
            .collect::<Vec<_>>()
            .join("/");
        let domain = if domain.is_empty() {
            db.to_string_lossy().into_owned()
        } else {
            domain
        };
        conn.execute_batch("BEGIN DEFERRED")
            .map_err(|_| "只读快照建立失败")?;
        let archives = cold::references(&conn)?;
        if archives != expected_archives {
            return Err("扫描前冷归档引用发生变化");
        }
        #[cfg(feature = "regression-hooks")]
        {
            let hook = AFTER_SQLITE_SNAPSHOT.with(|slot| {
                if slot.borrow().as_ref().is_some_and(|(path, _)| path == db) {
                    slot.borrow_mut().take()
                } else {
                    None
                }
            });
            if let Some((_, hook)) = hook {
                hook();
            }
        }
        let has_events: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='transcript_events')", [], |row| row.get(0),
        ).map_err(|_| "SQLite 表目录不可读")?;
        if has_events {
            let mut stmt = conn.prepare(
                "SELECT session_id,seq,CASE WHEN length(CAST(event_json AS BLOB))<=67108864 THEN event_json ELSE NULL END,CASE WHEN length(event_zstd)<=67108864 THEN event_zstd ELSE NULL END FROM transcript_events ORDER BY session_id,seq"
            ).map_err(|_| "transcript_events schema 不支持")?;
            let mut rows = stmt.query([]).map_err(|_| "热事件查询失败")?;
            while let Some(row) = rows.next().map_err(|_| "热事件读取中断")? {
                let session: String = row.get(0).map_err(|_| "热事件 session 无效")?;
                let seq: i64 = row.get(1).map_err(|_| "热事件序号无效")?;
                let json: Option<String> = row.get(2).map_err(|_| "热事件 JSON 列无效")?;
                let blob: Option<Vec<u8>> = row.get(3).map_err(|_| "热事件压缩列无效")?;
                let raw = match (json, blob) {
                    (Some(json), None) => json.into_bytes(),
                    (None, Some(blob)) => {
                        let mut decoder = zstd::stream::read::Decoder::new(blob.as_slice())
                            .map_err(|_| "热事件压缩格式无效")?;
                        decoder
                            .window_log_max(26)
                            .map_err(|_| "热事件解压窗口配置失败")?;
                        let mut raw = Vec::new();
                        decoder
                            .take(cold::MAX_BYTES + 1)
                            .read_to_end(&mut raw)
                            .map_err(|_| "热事件解压失败")?;
                        if raw.len() as u64 > cold::MAX_BYTES {
                            return Err("热事件解压后超限");
                        }
                        *zstd_decoded += 1;
                        raw
                    }
                    _ => return Err("热事件载荷缺失、冲突或超限"),
                };
                project_sqlite_event(
                    &raw, &session, seq, &domain, agent_id, agent_tag, records, seen, stats,
                )?;
            }
        } else {
            // An empty reference set cannot prove an empty transcript if its
            // required hot table is absent (including unversioned projections).
            return Err("数据库缺少必要的热事件 schema");
        }
        let mut files = Vec::new();
        for archive in &archives {
            stats.files_seen += 1;
            stats.files_parsed += 1;
            cold::scan(&conn, archive, cold_dir, &mut files, |seq, raw| {
                project_sqlite_event(
                    raw.as_bytes(),
                    &archive.session_id,
                    seq,
                    &domain,
                    agent_id,
                    agent_tag,
                    records,
                    seen,
                    stats,
                )
            })?;
            #[cfg(feature = "regression-hooks")]
            {
                let path = cold_dir.join(&archive.archive_name);
                let hook = AFTER_COLD_ARCHIVE.with(|slot| {
                    if slot
                        .borrow()
                        .as_ref()
                        .is_some_and(|(target, _)| target == &path)
                    {
                        slot.borrow_mut().take()
                    } else {
                        None
                    }
                });
                if let Some((_, hook)) = hook {
                    hook();
                }
            }
        }
        conn.execute_batch("ROLLBACK")
            .map_err(|_| "只读快照结束失败")?;
        let after: i64 = conn
            .query_row("PRAGMA data_version", [], |row| row.get(0))
            .map_err(|_| "数据库版本不可读")?;
        if version != after || crate::common::file_stamp(db).as_ref() != Some(&stamp) {
            return Err("扫描期间数据库发生变化，下轮重试");
        }
        Ok(SqliteReadGuard {
            conn,
            db: db.to_path_buf(),
            version,
            stamp,
            files,
        })
    })();
    match result {
        Ok(guard) => Some(guard),
        Err(reason) => {
            stats.failed = true;
            stats.notes.push(format!("{}: {reason}", db.display()));
            None
        }
    }
}

/// 遗留 JSONL 通道(同构事件流;exclusive 语义修正,§7.3)。
fn scan_jsonl_agent(
    files: &[PathBuf],
    agent_id: &str,
    agent_tag: Option<&str>,
    records: &mut Vec<UsageRecord>,
    seen: &mut HashSet<String>,
    stats: &mut ScanStats,
) {
    for path in files {
        let session_id = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_string();
        let read_outcome = crate::common::for_each_line(path, |line| {
            let Ok(v) = serde_json::from_str::<Value>(line) else {
                stats.lines_bad_json += 1;
                return;
            };
            if v.get("type").and_then(|x| x.as_str()) != Some("message") {
                return;
            }
            let msg = v.get("message").unwrap_or(&Value::Null);
            if msg.get("role").and_then(|x| x.as_str()) != Some("assistant") {
                return;
            }
            let Some(usage) = msg.get("usage").filter(|u| u.is_object()) else {
                return;
            };
            let Some((i, cr, cw, out_net, rs)) = buckets_from_usage(usage) else {
                return; // 全零(如 429 失败行)
            };
            let ts_ms = flex_to_ms(msg.get("timestamp"))
                .or_else(|| flex_to_ms(v.get("timestamp")))
                .unwrap_or(0);
            let model = msg
                .get("model")
                .or_else(|| msg.get("modelId"))
                .and_then(|x| x.as_str())
                .map(str::to_string)
                .filter(|m| !m.is_empty())
                .unwrap_or_else(|| "unknown".into());
            let event_id = v.get("id").and_then(|x| x.as_str()).unwrap_or("");
            let key = if event_id.is_empty() {
                format!(
                    "openclaw:{agent_id}:{session_id}:{}:{ts_ms}:{i}:{cr}:{cw}:{out_net}:{rs}",
                    msg.get("id").and_then(|x| x.as_str()).unwrap_or("")
                )
            } else {
                format!("openclaw:{agent_id}:{session_id}:{event_id}")
            };
            if !seen.insert(key.clone()) {
                return;
            }
            records.push(UsageRecord {
                source: "openclaw",
                session_id: session_id.clone(),
                agent: agent_tag.map(str::to_string),
                model,
                ts_ms,
                caliber: Caliber::RequestDelta,
                request_count: 1,
                input_uncached: i,
                cache_read: cr,
                cache_write: cw,
                cache_write_1h: 0,
                output: out_net,
                reasoning_output: rs,
                cost_usd: usage // 成本分治同 SQLite 通道(2026-10-17)
                    .pointer("/cost/totalOrigin")
                    .and_then(|x| x.as_str())
                    .and_then(|o| {
                        if o == "provider-billed" {
                            usage
                                .pointer("/cost/total")
                                .and_then(|x| x.as_f64())
                                .filter(|c| *c > 0.0)
                        } else {
                            None
                        }
                    }),
                dedup_key: key,
            });
        });
        read_outcome.apply_to_stats(stats);
        stats.files_parsed += 1;
    }
}
