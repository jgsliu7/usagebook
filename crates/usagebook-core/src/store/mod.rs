//! Version-isolated, disposable scan cache. Source files remain authoritative.
//! SQLite connections hold a process-shared lifecycle lease until they close.
//! Recovery requires an exclusive lease and moves the entire cache directory.
//! File projections are replaced only if their observed revision still matches.

pub mod cursor;
mod lifecycle;

pub use cursor::{judge, judge_reload, seal_at, tail_seal, FileCursor, Verdict, TAIL_SEAL_BYTES};

use crate::model::ScanStats;
use lifecycle::Lease;
use rusqlite::{Connection, OptionalExtension};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

/// 7:kimi-work 源接入(桌面版内嵌运行时)+ kimi 核收编 tools.register_user_tool(§10)。
pub const PARSER_VERSION: &str = "7";
const SCHEMA_VERSION: i32 = 2;
const LOCK_PROTOCOL_VERSION: u32 = 1;

const SCHEMA_SQL: &str = "CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS revision_clock (
  singleton INTEGER PRIMARY KEY CHECK(singleton=1),
  epoch TEXT NOT NULL,
  counter INTEGER NOT NULL CHECK(counter >= 0)
);
INSERT OR IGNORE INTO revision_clock VALUES (1, lower(hex(randomblob(32))), 0);
CREATE TABLE IF NOT EXISTS file_versions (
  source TEXT NOT NULL,
  path TEXT NOT NULL,
  revision TEXT NOT NULL UNIQUE,
  PRIMARY KEY(source, path)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS file_state (
  source TEXT NOT NULL, path TEXT NOT NULL,
  mtime_ns INTEGER NOT NULL, size INTEGER NOT NULL,
  byte_offset INTEGER NOT NULL, tail_seal INTEGER NOT NULL, identity TEXT,
  events BLOB NOT NULL, events_len INTEGER NOT NULL,
  lines_total INTEGER NOT NULL, updated_ms INTEGER NOT NULL,
  PRIMARY KEY(source, path)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS source_state (
  source TEXT PRIMARY KEY, fingerprint BLOB, records BLOB,
  records_len INTEGER NOT NULL DEFAULT 0, updated_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_file_state_source ON file_state(source);";

/// Field drop order is intentional: close SQLite before releasing its lease.
struct ActiveStore {
    conn: Connection,
    _lease: Lease,
}

enum StoreState {
    Disabled,
    Failed,
    FutureSchema,
    Incompatible,
    Enabled(ActiveStore),
}

static STORE: OnceLock<Mutex<StoreState>> = OnceLock::new();

fn store_cell() -> &'static Mutex<StoreState> {
    STORE.get_or_init(|| Mutex::new(init_state()))
}

#[derive(Debug)]
enum OpenError {
    Sql(rusqlite::Error),
    Io(std::io::Error),
    FutureSchema(i32),
    Incompatible,
}

impl From<rusqlite::Error> for OpenError {
    fn from(e: rusqlite::Error) -> Self {
        Self::Sql(e)
    }
}
impl From<std::io::Error> for OpenError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
impl std::error::Error for OpenError {}

impl std::fmt::Display for OpenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sql(e) => write!(f, "{e}"),
            Self::Io(e) => write!(f, "{e}"),
            Self::FutureSchema(v) => write!(f, "future schema {v}"),
            Self::Incompatible => write!(
                f,
                "cache metadata does not match its compatibility namespace"
            ),
        }
    }
}

fn is_corruption(error: &rusqlite::Error) -> bool {
    matches!(
        error.sqlite_error_code(),
        Some(rusqlite::ErrorCode::DatabaseCorrupt | rusqlite::ErrorCode::NotADatabase)
    )
}

fn state_from_error(error: &OpenError) -> StoreState {
    tracing::warn!("状态库不可用({error}),本进程降级直读");
    match error {
        OpenError::FutureSchema(_) => StoreState::FutureSchema,
        OpenError::Incompatible => StoreState::Incompatible,
        _ => StoreState::Failed,
    }
}

fn init_state() -> StoreState {
    if !incremental_enabled() {
        return StoreState::Disabled;
    }
    match open_shared() {
        Ok(active) => StoreState::Enabled(active),
        Err(OpenError::Sql(error)) if is_corruption(&error) => recover_exclusive(),
        Err(error) => state_from_error(&error),
    }
}

fn open_shared() -> Result<ActiveStore, OpenError> {
    let lease = Lease::shared(&state_lock_path())?;
    let conn = open_connection()?;
    Ok(ActiveStore {
        conn,
        _lease: lease,
    })
}

/// The caller must hold either lifecycle lease. This function never rotates.
fn open_connection() -> Result<Connection, OpenError> {
    let path = state_db_path();
    // Read the main-file header before SQLite can touch WAL/SHM. The following
    // connection check also catches a newer version still present only in WAL.
    if let Some(version) = probe_schema_version(&path)? {
        if version > effective_schema_version() {
            return Err(OpenError::FutureSchema(version));
        }
    }
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let conn = Connection::open(&path)?;
    // 库文件收紧到属主读写(2026-10-17 安全审计):库含项目全路径/会话 ID/用量元数据,
    // 多用户机器上 0644 可被其他本地账户读取;WAL/SHM 由 SQLite 以主库权限创建。
    // 与 snapshot-cache.json 0600 惯例一致;失败不阻断(只读文件系统等场景降级)
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
        // sidecar(-wal/-shm)一并收紧(2026-10-17 安全二轮):外部工具或异常时序创建的 0644
        // sidecar 不会自愈,而 checkpoint 前全部载荷在 WAL 中——每次打开顺带修权
        let mut side = path.clone().into_os_string();
        side.push("-wal");
        let _ = std::fs::set_permissions(
            std::path::PathBuf::from(side),
            std::fs::Permissions::from_mode(0o600),
        );
        let mut side = path.clone().into_os_string();
        side.push("-shm");
        let _ = std::fs::set_permissions(
            std::path::PathBuf::from(side),
            std::fs::Permissions::from_mode(0o600),
        );
    }
    conn.busy_timeout(std::time::Duration::from_secs(5))?;
    let version = get_schema_version(&conn)?;
    check_schema(version)?;
    if version != 0 {
        check_parser(&conn)?;
    }
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    if version == 0 {
        init_schema(&conn)?;
    }
    Ok(conn)
}

fn check_schema(version: i32) -> Result<(), OpenError> {
    if version > effective_schema_version() {
        return Err(OpenError::FutureSchema(version));
    }
    if version != 0 && version != effective_schema_version() {
        return Err(OpenError::Incompatible);
    }
    Ok(())
}

fn check_parser(conn: &Connection) -> Result<(), OpenError> {
    let stored: Option<String> = conn
        .query_row("SELECT v FROM kv WHERE k='parser_version'", [], |r| {
            r.get(0)
        })
        .optional()?;
    if stored.as_deref() != Some(PARSER_VERSION) {
        return Err(OpenError::Incompatible);
    }
    Ok(())
}

fn init_schema(conn: &Connection) -> Result<(), OpenError> {
    let tx = rusqlite::Transaction::new_unchecked(conn, rusqlite::TransactionBehavior::Immediate)?;
    // Another same-version process may have initialized while we waited.
    let version = get_schema_version(&tx)?;
    check_schema(version)?;
    if version == 0 {
        tx.execute_batch(SCHEMA_SQL)?;
        tx.execute(
            "INSERT INTO kv(k,v) VALUES ('parser_version',?1)",
            [PARSER_VERSION],
        )?;
        tx.pragma_update(None, "user_version", effective_schema_version())?;
    } else {
        check_parser(&tx)?;
    }
    tx.commit()?;
    Ok(())
}

fn get_schema_version(conn: &Connection) -> Result<i32, rusqlite::Error> {
    conn.query_row("PRAGMA user_version", [], |r| r.get(0))
}

fn probe_schema_version(path: &Path) -> Result<Option<i32>, std::io::Error> {
    use std::io::Read;
    let mut file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let mut header = [0u8; 64];
    match file.read_exact(&mut header) {
        Ok(()) if &header[..16] == b"SQLite format 3\0" => {
            Ok(Some(i32::from_be_bytes(header[60..64].try_into().unwrap())))
        }
        Ok(()) => Ok(None),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
        Err(e) => Err(e),
    }
}

/// USAGEBOOK_STATE_DB is a logical base, not a physical SQLite filename.
/// Append the namespace without interpreting any user-provided suffix.
fn state_dir() -> PathBuf {
    let base = std::env::var_os("USAGEBOOK_STATE_DB")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| crate::common::data_dir().join("state.db"));
    let mut name = base.into_os_string();
    name.push(format!(
        ".cache-p{PARSER_VERSION}-s{}-l{LOCK_PROTOCOL_VERSION}",
        effective_schema_version()
    ));
    PathBuf::from(name)
}

pub fn state_db_path() -> PathBuf {
    state_dir().join("state.db")
}
pub fn state_lock_path() -> PathBuf {
    let mut path = state_dir().into_os_string();
    path.push(".lock");
    PathBuf::from(path)
}

pub fn incremental_enabled() -> bool {
    match std::env::var("USAGEBOOK_INCREMENTAL") {
        Ok(v) => !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "off" | "0" | "false"
        ),
        Err(_) => true,
    }
}

/// Called only after all this process's connections and shared leases closed.
fn recover_exclusive() -> StoreState {
    let lease = match Lease::exclusive(&state_lock_path()) {
        Ok(lease) => lease,
        Err(error) => return state_from_error(&OpenError::Io(error)),
    };
    // Recheck under the exclusive lease; a peer might already have recovered.
    // Even this probe connection MUST close before the directory is renamed.
    let corrupt = match open_connection() {
        Ok(conn) => {
            let result = conn.query_row("PRAGMA quick_check", [], |r| r.get::<_, String>(0));
            drop(conn);
            match result {
                Ok(result) => result != "ok",
                Err(error) if is_corruption(&error) => true,
                Err(error) => return state_from_error(&OpenError::Sql(error)),
            }
        }
        Err(OpenError::Sql(error)) if is_corruption(&error) => true,
        Err(error) => return state_from_error(&error),
    };
    if corrupt {
        if let Err(error) = rotate_directory() {
            return state_from_error(&OpenError::Io(error));
        }
        match open_connection() {
            Ok(conn) => {
                record_rotation(&conn);
                drop(conn);
            }
            Err(error) => return state_from_error(&error),
        }
        prune_bad_backups_locked();
    }
    // Portable transition: close every SQLite handle before releasing exclusive;
    // then obtain shared and reopen/recheck. No open handle exists in the gap.
    drop(lease);
    match open_shared() {
        Ok(active) => StoreState::Enabled(active),
        Err(error) => state_from_error(&error),
    }
}

fn rotate_directory() -> std::io::Result<()> {
    let dir = state_dir();
    let parent = dir
        .parent()
        .ok_or_else(|| std::io::Error::other("cache directory has no parent"))?;
    let name = dir
        .file_name()
        .ok_or_else(|| std::io::Error::other("cache directory has no name"))?;
    let now = chrono::Local::now().timestamp_millis();
    for serial in 0..1000u32 {
        let mut destination = name.to_os_string();
        destination.push(format!(".bad-{now}-{serial}"));
        let destination = parent.join(destination);
        if destination.try_exists()? {
            continue;
        }
        // One rename covers the main database and every sidecar. Any failure is
        // terminal for this recovery attempt; never blindly create at the old name.
        return std::fs::rename(&dir, destination);
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        "cache backup names exhausted",
    ))
}

/// Only this compatibility namespace's backups are eligible for cleanup.
pub fn prune_bad_backups() {
    if !incremental_enabled() {
        return;
    }
    let Ok(_lease) = Lease::exclusive(&state_lock_path()) else {
        return;
    };
    prune_bad_backups_locked();
}

fn prune_bad_backups_locked() {
    let path = state_dir();
    let Some(parent) = path.parent() else { return };
    let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
        return;
    };
    let prefix = format!("{name}.bad-");
    let Ok(entries) = std::fs::read_dir(parent) else {
        return;
    };
    let mut backups = Vec::new();
    for entry in entries.flatten() {
        if !entry.file_type().is_ok_and(|t| t.is_dir()) {
            continue;
        }
        let name = entry.file_name();
        let Some(rest) = name.to_str().and_then(|n| n.strip_prefix(&prefix)) else {
            continue;
        };
        let Some((stamp, serial)) = rest.split_once('-') else {
            continue;
        };
        if let (Ok(stamp), Ok(serial)) = (stamp.parse::<i64>(), serial.parse::<u32>()) {
            backups.push(((stamp, serial), entry.path()));
        }
    }
    backups.sort_by_key(|(key, _)| *key);
    let remove_count = backups.len().saturating_sub(3);
    for (_, path) in backups.into_iter().take(remove_count) {
        if let Err(error) = std::fs::remove_dir_all(&path) {
            tracing::warn!(
                "轮转残片清理失败({error} {})",
                crate::logging::abbrev_home(&path)
            );
        }
    }
}

fn record_rotation(conn: &Connection) {
    let _ = conn.execute("INSERT INTO kv(k,v) VALUES ('rotations','1') ON CONFLICT(k) DO UPDATE SET v=CAST(CAST(v AS INTEGER)+1 AS TEXT)", []);
}

fn bump_kv(key: &str) {
    let _ = with_conn(|conn| {
        conn.execute(
            "INSERT INTO kv(k, v) VALUES (?1, '1')
             ON CONFLICT(k) DO UPDATE SET v = CAST(CAST(v AS INTEGER) + 1 AS TEXT)",
            [key],
        )?;
        Ok::<(), rusqlite::Error>(())
    });
}

fn bump_kv_u64(key: &str, n: u64) {
    let _ = with_conn(|conn| {
        conn.execute(
            "INSERT INTO kv(k, v) VALUES (?1, ?2)
             ON CONFLICT(k) DO UPDATE SET v = CAST(CAST(v AS INTEGER) + ?2 AS TEXT)",
            rusqlite::params![key, n as i64],
        )?;
        Ok::<(), rusqlite::Error>(())
    });
}

/// 单文件载荷超过 64MiB 上限:不持久化、每轮全读,doctor 可见 冻结护栏)。
pub fn note_capped_file() {
    bump_kv("capped_files");
}

/// SQL/codec failures do not imply corruption. Only SQLite's explicit corruption
/// errors may enter recovery; lock/permission/full-disk errors merely bypass.
fn with_conn<T>(f: impl FnOnce(&Connection) -> Result<T, rusqlite::Error>) -> Option<T> {
    let mut guard = store_cell().lock().unwrap_or_else(|e| e.into_inner());
    let result = match &*guard {
        StoreState::Enabled(active) => f(&active.conn),
        _ => return None,
    };
    match result {
        Ok(value) => Some(value),
        Err(error) => {
            tracing::warn!("状态库操作失败({error}),本次降级直读");
            if is_corruption(&error) {
                let old = std::mem::replace(&mut *guard, StoreState::Failed);
                drop(old); // ActiveStore closes connection, then releases shared.
                *guard = recover_exclusive();
            }
            None
        }
    }
}

/// 确认 L2 可用(仅 Enabled)。
fn ready() -> bool {
    let cell = store_cell();
    let guard = cell.lock().unwrap_or_else(|e| e.into_inner());
    matches!(*guard, StoreState::Enabled(_))
}

static TEST_SCHEMA_VERSION: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(0);
fn effective_schema_version() -> i32 {
    let v = TEST_SCHEMA_VERSION.load(std::sync::atomic::Ordering::Relaxed);
    if v > 0 {
        v
    } else {
        SCHEMA_VERSION
    }
}

/// 测试口:临时改写版本比较值(>0 生效;0=未覆盖)。
#[doc(hidden)]
pub fn set_schema_version_for_tests(v: i32) {
    TEST_SCHEMA_VERSION.store(v, std::sync::atomic::Ordering::Relaxed);
}

// —— 载荷编解码(锁外) ——

fn encode<T: serde::Serialize + ?Sized>(value: &T) -> Option<(Vec<u8>, usize)> {
    let bytes = serde_json::to_vec(value).ok()?;
    let len = bytes.len();
    let compressed = zstd::bulk::compress(&bytes, 1).ok()?;
    Some((compressed, len))
}

// —— file_state ——

pub struct FileRow {
    pub cursor: FileCursor,
    /// zstd 压缩的已提交前缀载荷(UsageLines);可直接用于 L1a 回填(零重压缩)。
    pub events: std::sync::Arc<[u8]>,
    pub events_len: usize,
    pub lines_total: i64,
}

/// An observation always has a revision, including a never-cached path. Its
/// registration survives payload GC so an old Fresh result cannot resurrect it.
pub struct FileObservation {
    pub revision: String,
    pub row: Option<FileRow>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileCommit {
    Committed,
    Conflict,
    Unavailable,
}

fn next_revision(conn: &Connection) -> Result<String, rusqlite::Error> {
    let (epoch, counter): (String, i64) = conn.query_row(
        "UPDATE revision_clock SET counter=counter+1
         WHERE singleton=1 AND counter<9223372036854775807 RETURNING epoch,counter",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    // Monotonic inside one database; a fresh 256-bit epoch isolates recovery.
    Ok(format!("{epoch}:{counter}"))
}

pub fn observe_file(source: &str, path: &Path) -> Option<FileObservation> {
    with_conn(|conn| {
        let tx =
            rusqlite::Transaction::new_unchecked(conn, rusqlite::TransactionBehavior::Immediate)?;
        let revision: Option<String> = tx
            .query_row(
                "SELECT revision FROM file_versions WHERE source=?1 AND path=?2",
                rusqlite::params![source, path.to_string_lossy()],
                |r| r.get(0),
            )
            .optional()?;
        let revision = match revision {
            Some(revision) => revision,
            None => {
                let revision = next_revision(&tx)?;
                tx.execute(
                    "INSERT INTO file_versions(source,path,revision) VALUES (?1,?2,?3)",
                    rusqlite::params![source, path.to_string_lossy(), revision],
                )?;
                revision
            }
        };
        let row = read_file_row(&tx, source, path)?;
        tx.commit()?;
        Ok(FileObservation { revision, row })
    })
}

/// Read-only diagnostic helper. Production calculations must use observe_file.
pub fn file_row(source: &str, path: &Path) -> Option<FileRow> {
    with_conn(|conn| read_file_row(conn, source, path)).flatten()
}

fn read_file_row(
    conn: &Connection,
    source: &str,
    path: &Path,
) -> Result<Option<FileRow>, rusqlite::Error> {
    conn.query_row(
        "SELECT mtime_ns,size,byte_offset,tail_seal,identity,events,events_len,lines_total
         FROM file_state WHERE source=?1 AND path=?2",
        rusqlite::params![source, path.to_string_lossy()],
        |r| {
            Ok(FileRow {
                cursor: FileCursor {
                    mtime_ns: r.get(0)?,
                    size: r.get::<_, i64>(1)? as u64,
                    byte_offset: r.get::<_, i64>(2)? as u64,
                    tail_seal: r.get(3)?,
                    identity: r.get(4)?,
                },
                events: r.get::<_, Vec<u8>>(5)?.into_boxed_slice().into(),
                events_len: r.get::<_, i64>(6)? as usize,
                lines_total: r.get(7)?,
            })
        },
    )
    .optional()
}

pub struct FileWrite<'a> {
    pub cursor: &'a FileCursor,
    /// 已提交前缀载荷(zstd 压缩)。
    pub events: &'a [u8],
    pub events_len: usize,
    /// 已提交前缀非空行计数(kimi line_no 续算)。
    pub lines_total: i64,
}

fn insert_file(
    conn: &Connection,
    source: &str,
    path: &Path,
    w: &FileWrite,
    now: i64,
) -> Result<usize, rusqlite::Error> {
    conn.execute(
        "INSERT INTO file_state (source, path, mtime_ns, size, byte_offset, tail_seal,
                                identity, events, events_len, lines_total, updated_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        rusqlite::params![
            source,
            path.to_string_lossy(),
            w.cursor.mtime_ns,
            w.cursor.size as i64,
            w.cursor.byte_offset as i64,
            w.cursor.tail_seal,
            w.cursor.identity,
            w.events,
            w.events_len as i64,
            w.lines_total,
            now
        ],
    )
}

/// Replace an entire projection only while its observed generation still owns
/// this path. Fresh, replay, append and damaged-row repair share this operation.
pub fn replace_file(
    source: &str,
    path: &Path,
    expected_revision: &str,
    w: FileWrite,
) -> FileCommit {
    let now = chrono::Local::now().timestamp_millis();
    with_conn(|conn| {
        let tx =
            rusqlite::Transaction::new_unchecked(conn, rusqlite::TransactionBehavior::Immediate)?;
        let revision = next_revision(&tx)?;
        let changed = tx.execute(
            "UPDATE file_versions SET revision=?1 WHERE source=?2 AND path=?3 AND revision=?4",
            rusqlite::params![revision, source, path.to_string_lossy(), expected_revision],
        )?;
        if changed == 0 {
            tx.rollback()?;
            return Ok(FileCommit::Conflict);
        }
        tx.execute(
            "DELETE FROM file_state WHERE source=?1 AND path=?2",
            rusqlite::params![source, path.to_string_lossy()],
        )?;
        insert_file(&tx, source, path, &w, now)?;
        tx.commit()?;
        Ok(FileCommit::Committed)
    })
    .unwrap_or(FileCommit::Unavailable)
}

pub fn append_file(source: &str, path: &Path, expected_revision: &str, w: FileWrite) -> FileCommit {
    replace_file(source, path, expected_revision, w)
}

/// 死行 GC prune_usage_cache 在非空文件清单轮调用):删该源本轮未见路径的行。
/// 实现:临时表装 live 路径后单语句 NOT IN(SELECT …)——不用 `IN (?,?,…)` 分块
/// (匿名占位与 ?1 混排易错,且分块 NOT IN 语义会误删块外存活行)。
pub fn gc_source(source: &str, live: &[PathBuf]) -> usize {
    let deleted = with_conn(|conn| {
        let tx = conn.unchecked_transaction()?;
        tx.execute_batch(
            "CREATE TEMP TABLE IF NOT EXISTS gc_live(k TEXT PRIMARY KEY) WITHOUT ROWID;
             DELETE FROM gc_live;",
        )?;
        {
            let mut insert = tx.prepare("INSERT OR IGNORE INTO gc_live(k) VALUES (?1)")?;
            for p in live {
                insert.execute(rusqlite::params![p.to_string_lossy()])?;
            }
        }
        // Invalidate both payload rows and absent observations. Tombstones are
        // never physically removed while this compatibility namespace is in use.
        let dead: Vec<String> = {
            let mut query = tx.prepare("SELECT path FROM file_versions WHERE source=?1 AND path NOT IN (SELECT k FROM gc_live)")?;
            let rows = query
                .query_map([source], |r| r.get(0))?
                .collect::<Result<Vec<_>, _>>()?;
            rows
        };
        for path in dead {
            let revision = next_revision(&tx)?;
            tx.execute(
                "UPDATE file_versions SET revision=?1 WHERE source=?2 AND path=?3",
                rusqlite::params![revision, source, path],
            )?;
        }
        let n = tx.execute(
            "DELETE FROM file_state WHERE source = ?1 AND path NOT IN (SELECT k FROM gc_live)",
            [source],
        )?;
        tx.execute_batch("DELETE FROM gc_live;")?;
        tx.commit()?;
        Ok(n)
    });
    let n = deleted.unwrap_or(0);
    if n > 0 {
        bump_kv_u64("gc_files", n as u64);
    }
    n
}

// —— source_state(非 JSONL 大源的源级指纹门; ——

pub struct SourceRow {
    pub fingerprint: std::sync::Arc<[u8]>,
    pub records: std::sync::Arc<[u8]>,
    pub records_len: usize,
}

pub fn source_row(source: &str) -> Option<SourceRow> {
    with_conn(|conn| {
        conn.query_row(
            "SELECT fingerprint, records, records_len FROM source_state WHERE source = ?1",
            [source],
            |r| {
                Ok(SourceRow {
                    fingerprint: r.get::<_, Vec<u8>>(0)?.into_boxed_slice().into(),
                    records: r.get::<_, Vec<u8>>(1)?.into_boxed_slice().into(),
                    records_len: r.get::<_, i64>(2)? as usize,
                })
            },
        )
        .optional()
    })
    .flatten()
}

pub fn replace_source(source: &str, fingerprint: &[u8], records: &[u8], records_len: usize) {
    let now = chrono::Local::now().timestamp_millis();
    let _ = with_conn(|conn| {
        conn.execute(
            "INSERT INTO source_state (source, fingerprint, records, records_len, updated_ms)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(source) DO UPDATE SET fingerprint = ?2, records = ?3,
                                              records_len = ?4, updated_ms = ?5",
            rusqlite::params![source, fingerprint, records, records_len as i64, now],
        )?;
        Ok::<(), rusqlite::Error>(())
    });
}

/// 空记录且扫描成功的源:删除旧行(镜像 L1b 的不冻结语义;。
pub fn delete_source(source: &str) {
    let _ = with_conn(|conn| {
        conn.execute("DELETE FROM source_state WHERE source = ?1", [source])?;
        Ok::<(), rusqlite::Error>(())
    });
}

// —— 编解码公共入口(供缝使用) ——

pub fn encode_lines(lines: &crate::common::UsageLines) -> Option<(Vec<u8>, usize)> {
    encode(lines)
}

pub fn decode_lines(blob: &[u8], len: usize) -> Option<crate::common::UsageLines> {
    zstd::bulk::decompress(blob, len)
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
}

pub fn encode_fingerprint(fp: &[(PathBuf, crate::common::FileStamp)]) -> Option<Vec<u8>> {
    encode(fp).map(|(b, _)| b)
}

pub fn decode_fingerprint(blob: &[u8]) -> Option<Vec<(PathBuf, crate::common::FileStamp)>> {
    let bytes = zstd::stream::decode_all(blob).ok()?;
    serde_json::from_slice(&bytes).ok()
}

/// records 载荷镜像:UsageRecord.source 是 &'static str(注册目录),
/// 无法从短生命周期 buffer 反序列化;镜像用 String,回转经 SOURCE_CATALOG 归位。
#[derive(serde::Serialize, serde::Deserialize)]
struct StoredRecord {
    source: String,
    session_id: String,
    agent: Option<String>,
    model: String,
    ts_ms: i64,
    caliber: u8,
    input_uncached: u64,
    cache_read: u64,
    cache_write: u64,
    cache_write_1h: u64,
    output: u64,
    reasoning_output: u64,
    cost_usd: Option<f64>,
    dedup_key: String,
}

impl From<&crate::model::UsageRecord> for StoredRecord {
    fn from(r: &crate::model::UsageRecord) -> Self {
        StoredRecord {
            source: r.source.to_string(),
            session_id: r.session_id.clone(),
            agent: r.agent.clone(),
            model: r.model.clone(),
            ts_ms: r.ts_ms,
            caliber: match r.caliber {
                crate::model::Caliber::RequestDelta => 0,
                crate::model::Caliber::SessionTotal => 1,
            },
            input_uncached: r.input_uncached,
            cache_read: r.cache_read,
            cache_write: r.cache_write,
            cache_write_1h: r.cache_write_1h,
            output: r.output,
            reasoning_output: r.reasoning_output,
            cost_usd: r.cost_usd,
            dedup_key: r.dedup_key.clone(),
        }
    }
}

fn static_source_id(id: &str) -> &'static str {
    let found = crate::SOURCE_CATALOG
        .iter()
        .find(|s| s.id == id)
        .map(|s| s.id);
    debug_assert!(found.is_some(), "records 载荷出现未注册源: {id}");
    found.unwrap_or("unknown")
}

impl From<StoredRecord> for crate::model::UsageRecord {
    fn from(r: StoredRecord) -> Self {
        crate::model::UsageRecord {
            source: static_source_id(&r.source),
            session_id: r.session_id,
            agent: r.agent,
            model: r.model,
            ts_ms: r.ts_ms,
            caliber: if r.caliber == 1 {
                crate::model::Caliber::SessionTotal
            } else {
                crate::model::Caliber::RequestDelta
            },
            input_uncached: r.input_uncached,
            cache_read: r.cache_read,
            cache_write: r.cache_write,
            cache_write_1h: r.cache_write_1h,
            output: r.output,
            reasoning_output: r.reasoning_output,
            cost_usd: r.cost_usd,
            dedup_key: r.dedup_key,
        }
    }
}

pub fn encode_records(records: &[crate::model::UsageRecord], stats: &ScanStats) -> Option<Vec<u8>> {
    let mirror: Vec<StoredRecord> = records.iter().map(StoredRecord::from).collect();
    encode(&(mirror, stats)).map(|(b, _)| b)
}

pub fn decode_records(blob: &[u8]) -> Option<(Vec<crate::model::UsageRecord>, ScanStats)> {
    let bytes = zstd::stream::decode_all(blob).ok()?;
    let (mirror, stats): (Vec<StoredRecord>, ScanStats) = serde_json::from_slice(&bytes).ok()?;
    Some((
        mirror
            .into_iter()
            .map(crate::model::UsageRecord::from)
            .collect(),
        stats,
    ))
}

pub fn store_ready() -> bool {
    ready()
}

// —— doctor store) ——

pub struct DoctorSnapshot {
    pub mode: String,
    pub db_path: PathBuf,
    pub db_bytes: u64,
    pub quick_check_ok: Option<bool>,
    pub rotations: u64,
    pub capped_files: u64,
    pub gc_files: u64,
    /// (source, file_state 行数, 最近更新 ms)
    pub files_by_source: Vec<(String, u64, i64)>,
    /// (source, 最近更新 ms)
    pub sources: Vec<(String, i64)>,
    /// 游标滞后:now − file_state 全表最近更新 ms(无行为 None;。
    pub cursor_lag_ms: Option<i64>,
}

pub fn doctor_snapshot() -> DoctorSnapshot {
    let path = state_db_path();
    let db_bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
    let cell_peek = store_cell();
    let guard_peek = cell_peek.lock().unwrap_or_else(|e| e.into_inner());
    let mode = match &*guard_peek {
        StoreState::Disabled => "off".to_string(),
        StoreState::Failed => "failed".to_string(),
        StoreState::FutureSchema => "future-schema".to_string(),
        StoreState::Incompatible => "incompatible-cache".to_string(),
        StoreState::Enabled(_) if incremental_enabled() => "on".to_string(),
        StoreState::Enabled(_) => "off".to_string(),
    };
    drop(guard_peek);
    let mut snap = DoctorSnapshot {
        mode: mode.clone(),
        db_path: path,
        db_bytes,
        quick_check_ok: None,
        rotations: 0,
        capped_files: 0,
        gc_files: 0,
        files_by_source: Vec::new(),
        sources: Vec::new(),
        cursor_lag_ms: None,
    };
    if !incremental_enabled() {
        snap.quick_check_ok = None;
        return snap;
    }
    if mode == "future-schema" || mode == "incompatible-cache" {
        snap.quick_check_ok = None;
        return snap;
    }
    if !ready() {
        snap.quick_check_ok = Some(false);
        return snap;
    }
    // doctor 主动执行 quick_check doctor/异常后)。
    snap.quick_check_ok =
        with_conn(|conn| conn.query_row("PRAGMA quick_check", [], |r| r.get::<_, String>(0)))
            .map(|v| v == "ok");
    if snap.quick_check_ok != Some(true) {
        return snap; // 只报告;自愈轮转发生在下次打开失败的异常路径
    }
    let _ = with_conn(|conn| {
        let mut stmt = conn.prepare(
            "SELECT source, COUNT(*), MAX(updated_ms) FROM file_state GROUP BY source ORDER BY source",
        )?;
        snap.files_by_source = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, i64>(1)? as u64,
                    r.get(2)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let mut stmt =
            conn.prepare("SELECT source, updated_ms FROM source_state ORDER BY source")?;
        snap.sources = stmt
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get(1)?)))?
            .collect::<Result<Vec<_>, _>>()?;
        let newest: Option<i64> =
            conn.query_row("SELECT MAX(updated_ms) FROM file_state", [], |r| r.get(0))?;
        let now = chrono::Local::now().timestamp_millis();
        snap.cursor_lag_ms = newest.map(|ms| now - ms);
        for (k, want) in [
            ("rotations", &mut snap.rotations),
            ("capped_files", &mut snap.capped_files),
            ("gc_files", &mut snap.gc_files),
        ] {
            let v: Option<String> = conn
                .query_row("SELECT v FROM kv WHERE k = ?1", [k], |r| r.get(0))
                .ok();
            *want = v.and_then(|s| s.parse().ok()).unwrap_or(0);
        }
        Ok::<(), rusqlite::Error>(())
    });
    snap
}

/// kv 写入口(测试用)。
#[doc(hidden)]
pub fn set_kv_for_tests(key: &str, value: &str) {
    let _ = with_conn(|conn| {
        conn.execute(
            "INSERT INTO kv(k, v) VALUES (?1, ?2) ON CONFLICT(k) DO UPDATE SET v = ?2",
            rusqlite::params![key, value],
        )?;
        Ok::<(), rusqlite::Error>(())
    });
}

/// 重置 store 连接(仅测试;按当前环境重新初始化,等价"新进程")。
#[doc(hidden)]
pub fn reset_for_tests() {
    let cell = STORE.get_or_init(|| Mutex::new(StoreState::Disabled));
    let mut guard = cell.lock().unwrap_or_else(|e| e.into_inner());
    let old = std::mem::replace(&mut *guard, StoreState::Failed);
    drop(old);
    *guard = init_state();
}

/// Close the test connection before externally editing an isolated fixture.
#[doc(hidden)]
pub fn close_for_tests() {
    if let Some(cell) = STORE.get() {
        let mut guard = cell.lock().unwrap_or_else(|e| e.into_inner());
        *guard = StoreState::Failed;
    }
}

/// 清除 set_schema_version_for_tests 的覆盖(0 = 未覆盖)。
#[doc(hidden)]
pub fn clear_schema_version_for_tests() {
    TEST_SCHEMA_VERSION.store(0, std::sync::atomic::Ordering::Relaxed);
}
