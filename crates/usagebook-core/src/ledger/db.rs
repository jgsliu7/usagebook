use super::types::*;
use crate::store::lifecycle::Lease;
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

pub struct Ledger {
    pub(crate) conn: Connection,
    pub(crate) path: PathBuf,
    _maintenance: Lease,
    _writer: Option<Lease>,
    optimize_next: Instant,
    optimize_first: bool,
}
pub fn default_path() -> PathBuf {
    if let Some(p) = std::env::var_os("USAGEBOOK_LEDGER_PATH") {
        return p.into();
    }
    #[cfg(target_os = "macos")]
    let base = std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_default()
        .join("Library/Application Support");
    #[cfg(windows)]
    let base = std::env::var_os("LOCALAPPDATA")
        .map(PathBuf::from)
        .unwrap_or_default();
    #[cfg(not(any(target_os = "macos", windows)))]
    let base = std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            std::env::var_os("HOME")
                .map(PathBuf::from)
                .unwrap_or_default()
                .join(".local/share")
        });
    base.join("usagebook/usage-ledger.db")
}
pub(crate) fn lock_paths(path: &Path) -> (PathBuf, PathBuf) {
    (
        path.with_extension("maintenance.lock"),
        path.with_extension("writer.lock"),
    )
}
/// All aliases must acquire the same leases and use the same WAL pathname.
pub(crate) fn canonical_path(path: &Path) -> Result<PathBuf> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => {
            let resolved = path.canonicalize()?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                if std::fs::metadata(&resolved)?.nlink() != 1 {
                    return Err(LedgerError::InvalidInput(
                        "ledger hard links are unsupported; use one database pathname".into(),
                    ));
                }
            }
            Ok(resolved)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let parent = path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new("."));
            let parent = parent.canonicalize().map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    LedgerError::NotInitialized
                } else {
                    e.into()
                }
            })?;
            let name = path
                .file_name()
                .ok_or_else(|| LedgerError::InvalidInput("invalid database path".into()))?;
            Ok(parent.join(name))
        }
        Err(e) => Err(e.into()),
    }
}
/// An interrupted install is existing history, even while its main file is moved.
pub fn exists_or_recoverable(path: &Path) -> bool {
    std::fs::symlink_metadata(path).is_ok() || path.with_extension("install.json").exists()
}
pub(crate) fn private_file(_path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(_path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}
fn install_new_file(temp: &Path, final_path: &Path) -> Result<()> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::ffi::OsStrExt;
        let old = std::ffi::CString::new(temp.as_os_str().as_bytes())
            .map_err(|_| LedgerError::InvalidInput("invalid temporary path".into()))?;
        let new = std::ffi::CString::new(final_path.as_os_str().as_bytes())
            .map_err(|_| LedgerError::InvalidInput("invalid database path".into()))?;
        // Both paths share a directory. Never fall back to rename(), which can
        // replace a concurrently created ledger.
        let result = unsafe {
            libc::renameat2(
                libc::AT_FDCWD,
                old.as_ptr(),
                libc::AT_FDCWD,
                new.as_ptr(),
                libc::RENAME_NOREPLACE,
            )
        };
        if result != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
    }
    #[cfg(target_os = "macos")]
    {
        use std::os::unix::ffi::OsStrExt;
        let old = std::ffi::CString::new(temp.as_os_str().as_bytes())
            .map_err(|_| LedgerError::InvalidInput("invalid temporary path".into()))?;
        let new = std::ffi::CString::new(final_path.as_os_str().as_bytes())
            .map_err(|_| LedgerError::InvalidInput("invalid database path".into()))?;
        let result = unsafe { libc::renamex_np(old.as_ptr(), new.as_ptr(), libc::RENAME_EXCL) };
        if result != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        use windows_sys::Win32::Storage::FileSystem::{MoveFileExW, MOVEFILE_WRITE_THROUGH};
        let old: Vec<u16> = temp.as_os_str().encode_wide().chain(Some(0)).collect();
        let new: Vec<u16> = final_path
            .as_os_str()
            .encode_wide()
            .chain(Some(0))
            .collect();
        // Without MOVEFILE_REPLACE_EXISTING or COPY_ALLOWED this is a same-volume,
        // no-overwrite move. The SQLite handle was closed before reaching here.
        if unsafe { MoveFileExW(old.as_ptr(), new.as_ptr(), MOVEFILE_WRITE_THROUGH) } == 0 {
            return Err(std::io::Error::last_os_error().into());
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
    return Err(LedgerError::InvalidInput(
        "atomic no-overwrite install unsupported on this platform".into(),
    ));
    Ok(())
}
fn sync_parent(_path: &Path) -> Result<()> {
    #[cfg(unix)]
    std::fs::File::open(_path.parent().unwrap_or(Path::new(".")))?.sync_all()?;
    Ok(())
}
pub(crate) fn open_connection(path: &Path, write: bool) -> Result<Connection> {
    let flags = if write {
        OpenFlags::SQLITE_OPEN_READ_WRITE
    } else {
        OpenFlags::SQLITE_OPEN_READ_ONLY
    };
    let c = Connection::open_with_flags(path, flags | OpenFlags::SQLITE_OPEN_NO_MUTEX)?;
    c.busy_timeout(std::time::Duration::from_millis(5000))?;
    c.execute_batch("PRAGMA foreign_keys=ON; PRAGMA temp_store=FILE;")?;
    // There is only one writer. Give its repeated identity lookups more cache
    // while leaving reader connections at 8 MiB and within the 64 MiB budget.
    c.pragma_update(None, "cache_size", if write { -32768 } else { -8192 })?;
    if write {
        // A v1 ledger upgrades in place before validation; read-only opens of
        // v1 stay UnsupportedVersion until any writer (serve/refresh) passes.
        // Errors here (missing table, corrupt) fall through to validate().
        let schema = c
            .query_row(
                "SELECT schema_version FROM ledger_meta WHERE singleton=1",
                [],
                |r| r.get::<_, i64>(0),
            )
            .optional()?
            .unwrap_or(0);
        if schema == 1 {
            migrate_v1_to_v2(&c)?;
        }
    }
    validate(&c)?;
    if write {
        c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")?;
    }
    Ok(c)
}
/// One-time v1 -> v2 upgrade under the writer lease: rebuilds the three
/// tables whose definitions changed — source_objects loses the never-read
/// audit cursor columns (audit_generation/audit_offset/audit_digest), and
/// usage_rollups loses the unused hour-grain spec. Every fact, derived row,
/// and suppression rule is preserved by column-name copy. The whole upgrade
/// runs in one transaction; a failure rolls back to an intact v1 database.
/// The table definitions must stay byte-identical to schema.sql; the
/// ledger_schema_v2 test enforces that against a fresh init.
pub(crate) fn migrate_v1_to_v2(c: &Connection) -> Result<()> {
    let result = c.execute_batch(
        r#"
PRAGMA foreign_keys=OFF;
BEGIN IMMEDIATE;
CREATE TABLE ledger_meta_v2 (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 schema_version INTEGER NOT NULL CHECK(schema_version=2),
 ledger_uuid TEXT NOT NULL, epoch TEXT NOT NULL,
 commit_seq INTEGER NOT NULL CHECK(commit_seq>=0),
 derived_version INTEGER NOT NULL CHECK(derived_version>0),
 identity_version INTEGER NOT NULL CHECK(identity_version>0),
 normalization_version INTEGER NOT NULL CHECK(normalization_version>0),
 report_timezone TEXT NOT NULL, tzdb_version TEXT NOT NULL,
 created_ms INTEGER NOT NULL
) STRICT;
INSERT INTO ledger_meta_v2 SELECT 1,2,ledger_uuid,epoch,commit_seq,derived_version,identity_version,normalization_version,report_timezone,tzdb_version,created_ms FROM ledger_meta;
DROP TABLE ledger_meta;
ALTER TABLE ledger_meta_v2 RENAME TO ledger_meta;
CREATE TABLE source_objects_v2 (
 object_id INTEGER PRIMARY KEY AUTOINCREMENT,
 root_id TEXT NOT NULL REFERENCES source_roots(root_id),
 object_key TEXT NOT NULL, locator TEXT NOT NULL,
 content_generation TEXT NOT NULL,
 byte_length INTEGER NOT NULL CHECK(byte_length>=0), mtime_ns INTEGER,
 -- Full SHA-256 of a completely imported file; NULL means replay on change.
 quick_fingerprint BLOB CHECK(quick_fingerprint IS NULL OR length(quick_fingerprint)=32),
 object_revision INTEGER NOT NULL CHECK(object_revision>=0),
 state TEXT NOT NULL CHECK(state IN('ready','missing','error','replay_required')),
 parser_version TEXT NOT NULL,
 saved_offset INTEGER CHECK(saved_offset>=0),
 checkpoint_format INTEGER, checkpoint BLOB,
 target_generation TEXT, target_end INTEGER CHECK(target_end>=0),
 target_done INTEGER NOT NULL DEFAULT 0 CHECK(target_done IN(0,1)),
 last_observed_ms INTEGER,
 audit_completed_ms INTEGER, audit_due_ms INTEGER NOT NULL,
 error_code TEXT,
 failure_count INTEGER NOT NULL DEFAULT 0 CHECK(failure_count>=0),
 retry_after_ms INTEGER,
 UNIQUE(root_id,object_key),
 CHECK((checkpoint IS NULL AND checkpoint_format IS NULL AND saved_offset IS NULL)
    OR (checkpoint IS NOT NULL AND length(checkpoint)<=65536 AND checkpoint_format IS NOT NULL AND checkpoint_format>0 AND saved_offset IS NOT NULL)),
 CHECK(state<>'replay_required' OR saved_offset IS NULL),
 CHECK((target_generation IS NULL AND target_end IS NULL AND target_done=0)
    OR (target_generation IS NOT NULL AND target_end IS NOT NULL))
) STRICT;
INSERT INTO source_objects_v2(object_id,root_id,object_key,locator,content_generation,byte_length,mtime_ns,quick_fingerprint,object_revision,state,parser_version,saved_offset,checkpoint_format,checkpoint,target_generation,target_end,target_done,last_observed_ms,audit_completed_ms,audit_due_ms,error_code,failure_count,retry_after_ms)
 SELECT object_id,root_id,object_key,locator,content_generation,byte_length,mtime_ns,quick_fingerprint,object_revision,state,parser_version,saved_offset,checkpoint_format,checkpoint,target_generation,target_end,target_done,last_observed_ms,audit_completed_ms,audit_due_ms,error_code,failure_count,retry_after_ms FROM source_objects;
DROP TABLE source_objects;
ALTER TABLE source_objects_v2 RENAME TO source_objects;
CREATE INDEX objects_retry ON source_objects(retry_after_ms,object_id) WHERE retry_after_ms IS NOT NULL;
CREATE INDEX objects_audit ON source_objects(audit_due_ms,object_id);
CREATE TABLE usage_rollups_v2 (
 derived_version INTEGER NOT NULL CHECK(derived_version>0),
 grain TEXT NOT NULL CHECK(grain='day'),
 bucket_start_ms INTEGER NOT NULL, bucket_end_ms INTEGER NOT NULL CHECK(bucket_end_ms>bucket_start_ms),
 local_day TEXT NOT NULL, report_source TEXT NOT NULL, model_key TEXT NOT NULL,
 event_count INTEGER NOT NULL CHECK(event_count>=0),
 bucket_revision INTEGER NOT NULL CHECK(bucket_revision>=0),
 input_uncached_known_sum INTEGER NOT NULL CHECK(input_uncached_known_sum>=0),
 input_uncached_known_count INTEGER NOT NULL CHECK(input_uncached_known_count>=0), input_uncached_exact_count INTEGER NOT NULL CHECK(input_uncached_exact_count>=0),
 cache_read_known_sum INTEGER NOT NULL CHECK(cache_read_known_sum>=0),
 cache_read_known_count INTEGER NOT NULL CHECK(cache_read_known_count>=0), cache_read_exact_count INTEGER NOT NULL CHECK(cache_read_exact_count>=0),
 cache_write_known_sum INTEGER NOT NULL CHECK(cache_write_known_sum>=0),
 cache_write_known_count INTEGER NOT NULL CHECK(cache_write_known_count>=0), cache_write_exact_count INTEGER NOT NULL CHECK(cache_write_exact_count>=0),
 output_normal_known_sum INTEGER NOT NULL CHECK(output_normal_known_sum>=0),
 output_normal_known_count INTEGER NOT NULL CHECK(output_normal_known_count>=0), output_normal_exact_count INTEGER NOT NULL CHECK(output_normal_exact_count>=0),
 output_reasoning_known_sum INTEGER NOT NULL CHECK(output_reasoning_known_sum>=0),
 output_reasoning_known_count INTEGER NOT NULL CHECK(output_reasoning_known_count>=0), output_reasoning_exact_count INTEGER NOT NULL CHECK(output_reasoning_exact_count>=0),
 input_total_known_sum INTEGER NOT NULL CHECK(input_total_known_sum>=0),
 input_total_known_count INTEGER NOT NULL CHECK(input_total_known_count>=0), input_total_exact_count INTEGER NOT NULL CHECK(input_total_exact_count>=0),
 output_total_known_sum INTEGER NOT NULL CHECK(output_total_known_sum>=0),
 output_total_known_count INTEGER NOT NULL CHECK(output_total_known_count>=0), output_total_exact_count INTEGER NOT NULL CHECK(output_total_exact_count>=0),
 total_tokens_known_sum INTEGER NOT NULL CHECK(total_tokens_known_sum>=0),
 total_tokens_known_count INTEGER NOT NULL CHECK(total_tokens_known_count>=0), total_tokens_exact_count INTEGER NOT NULL CHECK(total_tokens_exact_count>=0),
 cache_write_1h_known_sum INTEGER NOT NULL CHECK(cache_write_1h_known_sum>=0),
 cache_write_1h_known_count INTEGER NOT NULL CHECK(cache_write_1h_known_count>=0), cache_write_1h_exact_count INTEGER NOT NULL CHECK(cache_write_1h_exact_count>=0),
 request_count_known_sum INTEGER NOT NULL CHECK(request_count_known_sum>=0),
 request_count_known_count INTEGER NOT NULL CHECK(request_count_known_count>=0), request_count_exact_count INTEGER NOT NULL CHECK(request_count_exact_count>=0),
 PRIMARY KEY(derived_version,grain,bucket_start_ms,report_source,model_key),
 CHECK(input_uncached_exact_count<=input_uncached_known_count AND input_uncached_known_count<=event_count AND (input_uncached_known_count>0 OR input_uncached_known_sum=0)),
 CHECK(cache_read_exact_count<=cache_read_known_count AND cache_read_known_count<=event_count AND (cache_read_known_count>0 OR cache_read_known_sum=0)),
 CHECK(cache_write_exact_count<=cache_write_known_count AND cache_write_known_count<=event_count AND (cache_write_known_count>0 OR cache_write_known_sum=0)),
 CHECK(output_normal_exact_count<=output_normal_known_count AND output_normal_known_count<=event_count AND (output_normal_known_count>0 OR output_normal_known_sum=0)),
 CHECK(output_reasoning_exact_count<=output_reasoning_known_count AND output_reasoning_known_count<=event_count AND (output_reasoning_known_count>0 OR output_reasoning_known_sum=0)),
 CHECK(input_total_exact_count<=input_total_known_count AND input_total_known_count<=event_count AND (input_total_known_count>0 OR input_total_known_sum=0)),
 CHECK(output_total_exact_count<=output_total_known_count AND output_total_known_count<=event_count AND (output_total_known_count>0 OR output_total_known_sum=0)),
 CHECK(total_tokens_exact_count<=total_tokens_known_count AND total_tokens_known_count<=event_count AND (total_tokens_known_count>0 OR total_tokens_known_sum=0)),
 CHECK(cache_write_1h_exact_count<=cache_write_1h_known_count AND cache_write_1h_known_count<=event_count AND (cache_write_1h_known_count>0 OR cache_write_1h_known_sum=0)),
 CHECK(request_count_exact_count<=request_count_known_count AND request_count_known_count<=event_count AND (request_count_known_count>0 OR request_count_known_sum=0))
 ) STRICT;
INSERT INTO usage_rollups_v2(derived_version,grain,bucket_start_ms,bucket_end_ms,local_day,report_source,model_key,event_count,bucket_revision,input_uncached_known_sum,input_uncached_known_count,input_uncached_exact_count,cache_read_known_sum,cache_read_known_count,cache_read_exact_count,cache_write_known_sum,cache_write_known_count,cache_write_exact_count,output_normal_known_sum,output_normal_known_count,output_normal_exact_count,output_reasoning_known_sum,output_reasoning_known_count,output_reasoning_exact_count,input_total_known_sum,input_total_known_count,input_total_exact_count,output_total_known_sum,output_total_known_count,output_total_exact_count,total_tokens_known_sum,total_tokens_known_count,total_tokens_exact_count,cache_write_1h_known_sum,cache_write_1h_known_count,cache_write_1h_exact_count,request_count_known_sum,request_count_known_count,request_count_exact_count)
 SELECT derived_version,grain,bucket_start_ms,bucket_end_ms,local_day,report_source,model_key,event_count,bucket_revision,input_uncached_known_sum,input_uncached_known_count,input_uncached_exact_count,cache_read_known_sum,cache_read_known_count,cache_read_exact_count,cache_write_known_sum,cache_write_known_count,cache_write_exact_count,output_normal_known_sum,output_normal_known_count,output_normal_exact_count,output_reasoning_known_sum,output_reasoning_known_count,output_reasoning_exact_count,input_total_known_sum,input_total_known_count,input_total_exact_count,output_total_known_sum,output_total_known_count,output_total_exact_count,total_tokens_known_sum,total_tokens_known_count,total_tokens_exact_count,cache_write_1h_known_sum,cache_write_1h_known_count,cache_write_1h_exact_count,request_count_known_sum,request_count_known_count,request_count_exact_count FROM usage_rollups;
DROP TABLE usage_rollups;
ALTER TABLE usage_rollups_v2 RENAME TO usage_rollups;
CREATE INDEX rollups_window ON usage_rollups(derived_version,grain,local_day,report_source,model_key);
UPDATE sqlite_sequence SET name='source_objects' WHERE name='source_objects_v2';
COMMIT;
PRAGMA foreign_keys=ON;
"#,
    );
    if let Err(error) = result {
        let _ = c.execute_batch("ROLLBACK; PRAGMA foreign_keys=ON;");
        return Err(error.into());
    }
    let mut violations = 0usize;
    {
        let mut stmt = c.prepare("PRAGMA foreign_key_check")?;
        let mut rows = stmt.query([])?;
        while rows.next()?.is_some() {
            violations += 1;
        }
    }
    if violations > 0 {
        return Err(LedgerError::CorruptLedger);
    }
    // No fact changed, but stamps move so no stale cursor survives the upgrade.
    bump(c)?;
    Ok(())
}
pub(crate) fn validate(c: &Connection) -> Result<()> {
    let (schema,identity,normalization):(i64,i64,i64)=c.query_row("SELECT schema_version,identity_version,normalization_version FROM ledger_meta WHERE singleton=1",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).map_err(|_|LedgerError::CorruptLedger)?;
    if schema != 2 || identity != 1 || normalization != 1 {
        return Err(LedgerError::UnsupportedVersion);
    }
    let bad:i64=c.query_row("SELECT count(*) FROM ingest_runs WHERE state='publishing' AND (resolver_version NOT IN ('ledger-resolver-v1','ledger-resolver-v2') OR identity_version<>1 OR epoch<>(SELECT epoch FROM ledger_meta))",[],|r|r.get(0))?;
    if bad != 0 {
        return Err(LedgerError::UpgradeRequiresDrain);
    }
    let bad:i64=c.query_row("SELECT count(*) FROM source_roots WHERE identity_version<>1 OR parser_version<>'claude-ledger-v1'",[],|r|r.get(0))?;
    if bad != 0 {
        return Err(LedgerError::UnsupportedVersion);
    }
    Ok(())
}
impl Ledger {
    pub fn init(path: impl AsRef<Path>, timezone: &str) -> Result<Self> {
        let path = path.as_ref();
        timezone
            .parse::<chrono_tz::Tz>()
            .map_err(|_| LedgerError::InvalidInput("unknown IANA timezone".into()))?;
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                #[cfg(unix)]
                let existed = parent.exists();
                std::fs::create_dir_all(parent)?;
                #[cfg(unix)]
                if !existed {
                    use std::os::unix::fs::PermissionsExt;
                    std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))?;
                }
            }
        }
        let canonical = canonical_path(path)?;
        let path = canonical.as_path();
        let (maintenance, writer) = lock_paths(path);
        let lease = Lease::exclusive(&maintenance)?;
        let writer = Lease::exclusive(&writer)?;
        if path.with_extension("install.json").exists() {
            return Err(LedgerError::CorruptLedger);
        }
        if path.exists() {
            return Err(LedgerError::AlreadyInitialized);
        }
        let temp = path.with_extension(format!("{}.init.tmp", new_epoch()));
        let f = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        drop(f);
        let result = (|| -> Result<()> {
            private_file(&temp)?;
            let c = Connection::open_with_flags(&temp, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
            c.execute_batch("PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; BEGIN IMMEDIATE;")?;
            c.execute_batch(include_str!("schema.sql"))?;
            c.execute(
                "INSERT INTO ledger_meta VALUES(1,2,?1,?2,0,1,1,1,?3,?4,?5)",
                rusqlite::params![
                    new_epoch(),
                    new_epoch(),
                    timezone,
                    chrono_tz::IANA_TZDB_VERSION,
                    now_ms()
                ],
            )?;
            regression_stop("init_before_schema_commit");
            c.execute_batch("COMMIT; PRAGMA wal_checkpoint(TRUNCATE);")?;
            drop(c);
            regression_stop("init_after_schema_commit");
            let checked = Connection::open_with_flags(&temp, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
            super::maintenance::check_conn(&checked)?;
            drop(checked);
            for suffix in ["-wal", "-shm"] {
                let mut sidecar = temp.as_os_str().to_os_string();
                sidecar.push(suffix);
                match std::fs::remove_file(PathBuf::from(sidecar)) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
            }
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&temp)?
                .sync_all()?;
            regression_stop("init_before_publish");
            install_new_file(&temp, path)?;
            regression_stop("init_after_publish");
            sync_parent(path)?;
            Ok(())
        })();
        if result.is_err() {
            // Once published the final file is valid and must remain available.
            let _ = std::fs::remove_file(&temp);
            for suffix in ["-wal", "-shm"] {
                let mut sidecar = temp.as_os_str().to_os_string();
                sidecar.push(suffix);
                let _ = std::fs::remove_file(PathBuf::from(sidecar));
            }
        }
        result?;
        drop(writer);
        drop(lease);
        Self::open(path, true)
    }
    pub fn open(path: impl AsRef<Path>, write: bool) -> Result<Self> {
        let path = canonical_path(path.as_ref())?;
        // Recovery, if needed, happens under an exclusive lease before any SQLite handle opens.
        super::maintenance::recover_install(&path)?;
        let (maintenance, writer) = lock_paths(&path);
        let lease = Lease::shared(&maintenance)?;
        let writer = if write {
            Some(Lease::exclusive(&writer)?)
        } else {
            None
        };
        if !path.exists() {
            return Err(LedgerError::NotInitialized);
        }
        let conn = open_connection(&path, write)?;
        Ok(Self {
            conn,
            path,
            _maintenance: lease,
            _writer: writer,
            optimize_next: Instant::now(),
            optimize_first: true,
        })
    }
    pub fn connection(&self) -> &Connection {
        &self.conn
    }
    pub fn path(&self) -> &Path {
        &self.path
    }
    pub(crate) fn require_writer(&self) -> Result<()> {
        if self._writer.is_some() {
            Ok(())
        } else {
            Err(LedgerError::PermissionDenied)
        }
    }
    pub fn stamp(&self) -> Result<Stamp> {
        stamp(&self.conn)
    }
    pub fn checkpoint(&self) -> Result<()> {
        self.require_writer()?;
        self.conn.execute_batch("PRAGMA wal_checkpoint(PASSIVE);")?;
        Ok(())
    }
    /// Small best-effort statistics maintenance, called by the existing writer
    /// after import or during idle refresh. It never changes ledger revisions.
    pub fn optimize_if_due(&mut self) -> Result<bool> {
        self.require_writer()?;
        if Instant::now() < self.optimize_next {
            return Ok(false);
        }
        if !self.conn.is_autocommit() {
            return Err(LedgerError::InvalidInput(
                "statistics maintenance requires an idle writer".into(),
            ));
        }
        let started = Instant::now();
        // Busy waits do not invoke the VM progress handler, so bound both.
        // Bundled STAT4 still counts index pages before limited analysis.
        // Give the initial statistics pass room; later idle checks stay shorter.
        let budget = Duration::from_secs(if self.optimize_first { 30 } else { 5 });
        self.conn.busy_timeout(Duration::from_millis(100))?;
        self.conn
            .progress_handler(1000, Some(move || started.elapsed() >= budget))?;
        let result = self.conn.execute_batch(if self.optimize_first {
            // An explicit mask replaces defaults. Bundled SQLite 3.53 needs
            // bit 0x10 to keep its temporary analysis limit enabled.
            "PRAGMA optimize=0x10012;"
        } else {
            "PRAGMA optimize;"
        });
        let clear = self.conn.progress_handler(0, None::<fn() -> bool>);
        let busy = self.conn.busy_timeout(Duration::from_millis(5000));
        self.optimize_next = Instant::now()
            + if result.is_ok() {
                Duration::from_secs(24 * 60 * 60)
            } else {
                Duration::from_secs(60)
            };
        if result.is_ok() {
            self.optimize_first = false;
        }
        clear?;
        busy?;
        result?;
        Ok(true)
    }
}
pub fn stamp(c: &Connection) -> Result<Stamp> {
    Ok(c.query_row(
        "SELECT ledger_uuid,epoch,commit_seq,report_timezone,derived_version FROM ledger_meta",
        [],
        |r| {
            Ok(Stamp {
                ledger_uuid: r.get(0)?,
                epoch: r.get(1)?,
                commit_seq: r.get::<_, i64>(2)?.to_string(),
                report_timezone: r.get(3)?,
                derived_version: r.get::<_, i64>(4)?.to_string(),
                caliber_version: 1,
                legacy_generation: None,
            })
        },
    )?)
}
pub(crate) fn bump(c: &Connection) -> Result<i64> {
    let old: i64 = c.query_row("SELECT commit_seq FROM ledger_meta", [], |r| r.get(0))?;
    let next = old.checked_add(1).ok_or(LedgerError::NumericOverflow)?;
    c.execute("UPDATE ledger_meta SET commit_seq=?1", [next])?;
    Ok(next)
}
pub(crate) fn change_epoch(c: &Connection) -> Result<String> {
    let epoch = new_epoch();
    c.execute("UPDATE ledger_meta SET epoch=?1", [&epoch])?;
    c.execute(
        "UPDATE ingest_runs SET epoch=?1 WHERE state='publishing'",
        [&epoch],
    )?;
    bump(c)?;
    Ok(epoch)
}
/// 宿主机当前 IANA 时区名；读取失败或 chrono-tz 不识别时返回 None。
/// 未显式指定 `--timezone` 时的默认值来源；调用方须自行决定推断失败的处置，
/// 本函数绝不静默回退到某个具体城市（见 review/1022/glm R-11）。
pub fn host_timezone_hint() -> Option<String> {
    iana_time_zone::get_timezone()
        .ok()
        .filter(|name| super::metrics::timezone(name).is_ok())
}

/// A transient query context when the user has not initialized durable storage.
/// It never writes a ledger file and every source remains explicitly unprotected.
pub fn live_connection(timezone: &str) -> Result<Connection> {
    super::metrics::timezone(timezone)?;
    let c = Connection::open_in_memory()?;
    c.execute_batch(include_str!("schema.sql"))?;
    c.execute(
        "INSERT INTO ledger_meta VALUES(1,2,?1,?2,0,1,1,1,?3,?4,?5)",
        rusqlite::params![
            new_epoch(),
            new_epoch(),
            timezone,
            chrono_tz::IANA_TZDB_VERSION,
            now_ms()
        ],
    )?;
    Ok(c)
}

#[cfg(test)]
mod tests {
    #[test]
    fn host_timezone_hint_is_none_or_valid_iana() {
        if let Some(name) = super::host_timezone_hint() {
            assert!(super::super::metrics::timezone(&name).is_ok(), "{name}");
        }
    }
}
