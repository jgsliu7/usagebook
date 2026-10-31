use super::types::*;
use crate::store::lifecycle::Lease;
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

#[cfg(feature = "regression-hooks")]
pub use super::query_indexes::{
    regression_fail_cleanup as regression_query_index_fail_cleanup,
    regression_next_budget as regression_query_index_next_budget, QueryIndexCleanupFailure,
};

pub struct Ledger {
    pub(crate) conn: Connection,
    pub(crate) path: PathBuf,
    _maintenance: Lease,
    _writer: Option<Lease>,
    optimize_next: Instant,
    optimize_first: bool,
    query_index_retry_at: Option<Instant>,
    writer_poisoned: bool,
    pub(crate) claude_readers: Option<super::ingest::ReaderSliceState>,
}
impl Drop for Ledger {
    fn drop(&mut self) {
        if let Some(state) = self.claude_readers.take() {
            state.cancel(&self.path);
        }
    }
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
/// A main database that is missing while its WAL/SHM/journal sidecars remain is
/// not a fresh installation: publishing a new database beside them lets SQLite
/// replay the old WAL into the new file. Refuse instead of guessing.
fn check_orphaned_sidecars(path: &Path) -> Result<()> {
    for suffix in ["-wal", "-shm", "-journal"] {
        let sidecar = sidecar_path(path, suffix);
        match sidecar.symlink_metadata() {
            Ok(_) => {
                return Err(LedgerError::InvalidInput(format!(
                    "ledger sidecar {} exists but the main ledger file is missing; restore a backup with ledger rescue, or remove all -wal/-shm/-journal files for this path and retry (do not copy only a main file next to these sidecars)",
                    sidecar.display()
                )));
            }
            // A missing path is the normal fresh-install case. Any other
            // metadata error must not be mistaken for "no sidecar".
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}
/// Append a SQLite sidecar suffix without `Path::with_extension`, which would
/// replace the database extension instead of preserving the complete name.
fn sidecar_path(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(suffix);
    PathBuf::from(name)
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
fn configured_connection(path: &Path, write: bool) -> Result<Connection> {
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
    Ok(c)
}
/// Restore is the only read-only entry point that accepts a known older schema.
/// Its caller copies this handle into a private staging file before upgrading.
pub(crate) fn open_restore_source(path: &Path) -> Result<Connection> {
    let c = configured_connection(path, false)?;
    validate_versions(&c, &[1, 2, 3, 4, 5, 6, 7, 8])?;
    validate_historical_versions(&c)?;
    Ok(c)
}
pub(crate) fn open_connection(path: &Path, write: bool) -> Result<Connection> {
    open_connection_with_index_outcome(path, write).map(|(conn, _)| conn)
}

fn open_connection_with_index_outcome(
    path: &Path,
    write: bool,
) -> Result<(Connection, Option<super::query_indexes::InstallOutcome>)> {
    if write {
        // Check every independently versioned contract before opening a writer
        // or running migration/journal pragmas. Unsupported databases are not
        // partially upgraded merely because their schema happens to be known.
        drop(open_restore_source(path)?);
    }
    let c = configured_connection(path, write)?;
    if write {
        validate_versions(&c, &[1, 2, 3, 4, 5, 6, 7, 8])?;
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
        if schema == 1 || schema == 2 {
            migrate_v2_to_v3(&c)?;
        }
        if schema <= 3 {
            migrate_v3_to_v4(&c)?;
        }
        if schema <= 4 {
            migrate_v4_to_v5(&c)?;
        }
        if schema <= 5 {
            migrate_v5_to_v6(&c)?;
        }
        if schema <= 6 {
            migrate_v6_to_v7(&c)?;
        }
        if schema <= 7 {
            migrate_v7_to_v8(&c)?;
        }
    }
    validate(&c)?;
    let indexes = if write {
        c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")?;
        ensure_read_caches(&c)?;
        c.execute_batch(include_str!("claude_protection_schema.sql"))?;
        Some(super::query_indexes::install(
            &c,
            super::query_indexes::OPEN_BUDGET,
        )?)
    } else {
        None
    };
    Ok((c, indexes))
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
/// v3 adds acquisition manifests and finality evidence without changing any
/// existing fact. The observation table is rebuilt using the canonical DDL;
/// its original AUTOINCREMENT watermark survives even after explicit forget.
fn migrate_v2_to_v3(c: &Connection) -> Result<()> {
    let canonical = include_str!("schema.sql");
    let table_ddl = |name: &str| -> Result<&str> {
        let start = canonical
            .find(&format!("CREATE TABLE {name} ("))
            .ok_or(LedgerError::CorruptLedger)?;
        let end = canonical[start..]
            .find(") STRICT;")
            .ok_or(LedgerError::CorruptLedger)?
            + start
            + ") STRICT;".len();
        Ok(&canonical[start..end])
    };
    c.execute_batch("PRAGMA foreign_keys=OFF; BEGIN IMMEDIATE;")?;
    let result = (|| -> Result<()> {
        let sequence: Option<i64> = c
            .query_row(
                "SELECT seq FROM sqlite_sequence WHERE name='usage_observations'",
                [],
                |r| r.get(0),
            )
            .optional()?;
        c.execute_batch(
            &table_ddl("ledger_meta")?
                .replace("CHECK(schema_version=8)", "CHECK(schema_version=3)")
                .replacen(
                    "CREATE TABLE ledger_meta (",
                    "CREATE TABLE ledger_meta_v3 (",
                    1,
                ),
        )?;
        c.execute_batch("INSERT INTO ledger_meta_v3 SELECT singleton,3,ledger_uuid,epoch,commit_seq,derived_version,identity_version,normalization_version,report_timezone,tzdb_version,created_ms FROM ledger_meta; DROP TABLE ledger_meta; ALTER TABLE ledger_meta_v3 RENAME TO ledger_meta;")?;
        c.execute_batch(&table_ddl("usage_observations")?.replacen(
            "CREATE TABLE usage_observations (",
            "CREATE TABLE usage_observations_v3 (",
            1,
        ))?;
        let columns = super::ingest::OBSERVATION_COLUMNS;
        c.execute_batch(&format!("INSERT INTO usage_observations_v3({columns}) SELECT {columns} FROM usage_observations; DROP TABLE usage_observations; ALTER TABLE usage_observations_v3 RENAME TO usage_observations;"))?;
        if let Some(sequence) = sequence {
            c.execute(
                "UPDATE sqlite_sequence SET seq=MAX(seq,?1) WHERE name='usage_observations'",
                [sequence],
            )?;
        }
        for line in canonical
            .lines()
            .filter(|line| line.starts_with("CREATE INDEX observations_"))
        {
            c.execute_batch(line)?;
        }
        let trigger_start = canonical
            .find("CREATE TRIGGER observations_immutable")
            .ok_or(LedgerError::CorruptLedger)?;
        let trigger_end = canonical[trigger_start..]
            .find("END;")
            .ok_or(LedgerError::CorruptLedger)?
            + trigger_start
            + 4;
        c.execute_batch(&canonical[trigger_start..trigger_end])?;
        c.execute_batch(include_str!("durable_schema.sql"))?;
        c.execute_batch(include_str!("coverage_schema.sql"))?;
        let violation: Option<String> = c
            .query_row("PRAGMA foreign_key_check", [], |r| r.get(0))
            .optional()?;
        if violation.is_some() {
            return Err(LedgerError::CorruptLedger);
        }
        bump(c)?;
        c.execute_batch("COMMIT;")?;
        Ok(())
    })();
    if result.is_err() {
        let _ = c.execute_batch("ROLLBACK;");
    }
    c.execute_batch("PRAGMA foreign_keys=ON;")?;
    result
}

/// v4 decouples deletion/coverage controls from the lifetime of their support,
/// and adds the bounded runtime, snapshot and explicit-adoption state. Ordinary
/// observations and their selected numeric values are copied unchanged.
fn migrate_v3_to_v4(c: &Connection) -> Result<()> {
    let canonical = include_str!("schema.sql");
    let table = |name: &str| -> Result<&str> {
        let start = canonical
            .find(&format!("CREATE TABLE {name} ("))
            .ok_or(LedgerError::CorruptLedger)?;
        let end = start
            + canonical[start..]
                .find(") STRICT;")
                .ok_or(LedgerError::CorruptLedger)?
            + ") STRICT;".len();
        Ok(&canonical[start..end])
    };
    c.execute_batch("PRAGMA foreign_keys=OFF; BEGIN IMMEDIATE;")?;
    let result = (|| -> Result<()> {
        let sequence: Option<i64> = c
            .query_row(
                "SELECT seq FROM sqlite_sequence WHERE name='suppression_rules'",
                [],
                |r| r.get(0),
            )
            .optional()?;
        c.execute_batch(
            &table("ledger_meta")?
                .replace("CHECK(schema_version=8)", "CHECK(schema_version=4)")
                .replacen(
                    "CREATE TABLE ledger_meta (",
                    "CREATE TABLE ledger_meta_v4 (",
                    1,
                ),
        )?;
        c.execute_batch("INSERT INTO ledger_meta_v4 SELECT singleton,4,ledger_uuid,epoch,commit_seq,derived_version,identity_version,normalization_version,report_timezone,tzdb_version,created_ms FROM ledger_meta; DROP TABLE ledger_meta; ALTER TABLE ledger_meta_v4 RENAME TO ledger_meta;")?;
        c.execute_batch(&table("suppression_rules")?.replacen(
            "CREATE TABLE suppression_rules (",
            "CREATE TABLE suppression_rules_v4 (",
            1,
        ))?;
        c.execute_batch("INSERT INTO suppression_rules_v4 SELECT * FROM suppression_rules; DROP TABLE suppression_rules; ALTER TABLE suppression_rules_v4 RENAME TO suppression_rules;")?;
        if let Some(sequence) = sequence {
            c.execute(
                "UPDATE sqlite_sequence SET seq=MAX(seq,?1) WHERE name='suppression_rules'",
                [sequence],
            )?;
        }
        for line in canonical
            .lines()
            .filter(|line| line.starts_with("CREATE ") && line.contains("INDEX suppression_"))
        {
            c.execute_batch(line)?;
        }
        // v3 wrote the report channel into the native-tool slot. That channel
        // has never represented a native tool, so these rules matched no CC
        // request. Preserve their intended report-source/time restriction.
        c.execute("UPDATE suppression_rules SET native_tool=NULL WHERE kind IN('source','window') AND report_source='ccswitch-rollup' AND native_tool='ccswitch-rollup'", [])?;
        c.execute_batch("CREATE TEMP TABLE coverage_v3 AS SELECT * FROM durable_coverage; CREATE TEMP TABLE channels_v3 AS SELECT * FROM durable_channel_choices; DROP TABLE durable_coverage; DROP TABLE durable_channel_choices;")?;
        c.execute_batch(include_str!("coverage_schema.sql"))?;
        c.execute_batch("INSERT INTO durable_coverage SELECT * FROM coverage_v3; INSERT INTO durable_channel_choices SELECT * FROM channels_v3; DROP TABLE coverage_v3; DROP TABLE channels_v3;")?;
        c.execute_batch(include_str!("runtime_schema.sql"))?;
        c.execute_batch(include_str!("durable_snapshots_schema.sql"))?;
        c.execute_batch(include_str!("adoption_schema.sql"))?;
        let violation: Option<String> = c
            .query_row("PRAGMA foreign_key_check", [], |r| r.get(0))
            .optional()?;
        if violation.is_some() {
            return Err(LedgerError::CorruptLedger);
        }
        bump(c)?;
        c.execute_batch("COMMIT;")?;
        Ok(())
    })();
    if result.is_err() {
        let _ = c.execute_batch("ROLLBACK;");
    }
    c.execute_batch("PRAGMA foreign_keys=ON;")?;
    result
}

/// v5 makes physical-row deletion controls a required protocol. An older v4
/// writer must refuse this ledger instead of rebuilding a forgotten projection
/// after its source row changes time or identity fields. No facts or epochs move.
fn migrate_v4_to_v5(c: &Connection) -> Result<()> {
    let canonical = include_str!("schema.sql");
    let start = canonical
        .find("CREATE TABLE ledger_meta (")
        .ok_or(LedgerError::CorruptLedger)?;
    let end = start
        + canonical[start..]
            .find(") STRICT;")
            .ok_or(LedgerError::CorruptLedger)?
        + ") STRICT;".len();
    c.execute_batch("PRAGMA foreign_keys=OFF; BEGIN IMMEDIATE;")?;
    let result = (|| -> Result<()> {
        c.execute_batch(
            &canonical[start..end]
                .replace("CHECK(schema_version=8)", "CHECK(schema_version=5)")
                .replacen(
                    "CREATE TABLE ledger_meta (",
                    "CREATE TABLE ledger_meta_v5 (",
                    1,
                ),
        )?;
        c.execute_batch("INSERT INTO ledger_meta_v5 SELECT singleton,5,ledger_uuid,epoch,commit_seq,derived_version,identity_version,normalization_version,report_timezone,tzdb_version,created_ms FROM ledger_meta; DROP TABLE ledger_meta; ALTER TABLE ledger_meta_v5 RENAME TO ledger_meta;")?;
        // Includes creation (never replacement) of the permanent projection
        // tombstones, in the same transaction as the version gate.
        ensure_read_caches(c)?;
        validate_versions(c, &[5])?;
        validate_v5_controls(c)?;
        let violation: Option<String> = c
            .query_row("PRAGMA foreign_key_check", [], |r| r.get(0))
            .optional()?;
        if violation.is_some() {
            return Err(LedgerError::CorruptLedger);
        }
        bump(c)?;
        c.execute_batch("COMMIT;")?;
        Ok(())
    })();
    if result.is_err() {
        let _ = c.execute_batch("ROLLBACK;");
    }
    c.execute_batch("PRAGMA foreign_keys=ON;")?;
    result
}

/// v6 requires immutable time-allocation evidence. Original observations,
/// adopted values, epochs and controls are untouched; old writers fail closed.
fn migrate_v5_to_v6(c: &Connection) -> Result<()> {
    let canonical = include_str!("schema.sql");
    let start = canonical
        .find("CREATE TABLE ledger_meta (")
        .ok_or(LedgerError::CorruptLedger)?;
    let end = start
        + canonical[start..]
            .find(") STRICT;")
            .ok_or(LedgerError::CorruptLedger)?
        + ") STRICT;".len();
    c.execute_batch("PRAGMA foreign_keys=OFF; BEGIN IMMEDIATE;")?;
    let result = (|| -> Result<()> {
        validate_v5_controls(c)?;
        c.execute_batch(
            &canonical[start..end]
                .replace("CHECK(schema_version=8)", "CHECK(schema_version=6)")
                .replacen(
                    "CREATE TABLE ledger_meta (",
                    "CREATE TABLE ledger_meta_v6 (",
                    1,
                ),
        )?;
        c.execute_batch("INSERT INTO ledger_meta_v6 SELECT singleton,6,ledger_uuid,epoch,commit_seq,derived_version,identity_version,normalization_version,report_timezone,tzdb_version,created_ms FROM ledger_meta; DROP TABLE ledger_meta; ALTER TABLE ledger_meta_v6 RENAME TO ledger_meta;")?;
        c.execute_batch(include_str!("time_allocation_schema.sql"))?;
        validate_v6(c)?;
        let violation: Option<String> = c
            .query_row("PRAGMA foreign_key_check", [], |r| r.get(0))
            .optional()?;
        if violation.is_some() {
            return Err(LedgerError::CorruptLedger);
        }
        bump(c)?;
        c.execute_batch("COMMIT;")?;
        Ok(())
    })();
    if result.is_err() {
        let _ = c.execute_batch("ROLLBACK;");
    }
    c.execute_batch("PRAGMA foreign_keys=ON;")?;
    result
}

/// v7 adds sealed historical time revisions without changing any amount or
/// original time evidence. Old binaries refuse the upgraded ledger.
fn migrate_v6_to_v7(c: &Connection) -> Result<()> {
    let canonical = include_str!("schema.sql");
    let start = canonical
        .find("CREATE TABLE ledger_meta (")
        .ok_or(LedgerError::CorruptLedger)?;
    let end = start
        + canonical[start..]
            .find(") STRICT;")
            .ok_or(LedgerError::CorruptLedger)?
        + ") STRICT;".len();
    c.execute_batch("PRAGMA foreign_keys=OFF; BEGIN IMMEDIATE;")?;
    let result = (|| -> Result<()> {
        validate_v6(c)?;
        c.execute_batch(
            &canonical[start..end]
                .replace("CHECK(schema_version=8)", "CHECK(schema_version=7)")
                .replacen(
                    "CREATE TABLE ledger_meta (",
                    "CREATE TABLE ledger_meta_v7 (",
                    1,
                ),
        )?;
        c.execute_batch("INSERT INTO ledger_meta_v7 SELECT singleton,7,ledger_uuid,epoch,commit_seq,derived_version,identity_version,normalization_version,report_timezone,tzdb_version,created_ms FROM ledger_meta; DROP TABLE ledger_meta; ALTER TABLE ledger_meta_v7 RENAME TO ledger_meta;")?;
        c.execute_batch(include_str!("time_repair_schema.sql"))?;
        super::time_repair::preserve_upgrade_capture(c)?;
        validate_v7(c)?;
        let violation: Option<String> = c
            .query_row("PRAGMA foreign_key_check", [], |r| r.get(0))
            .optional()?;
        if violation.is_some() {
            return Err(LedgerError::CorruptLedger);
        }
        bump(c)?;
        c.execute_batch("COMMIT;")?;
        Ok(())
    })();
    if result.is_err() {
        let _ = c.execute_batch("ROLLBACK;");
    }
    c.execute_batch("PRAGMA foreign_keys=ON;")?;
    result
}

/// The v8 contract makes protected Claude history unavailable to older writers.
/// The writer lease excludes an already-running old writer during migration.
fn migrate_v7_to_v8(c: &Connection) -> Result<()> {
    let canonical = include_str!("schema.sql");
    let start = canonical
        .find("CREATE TABLE ledger_meta (")
        .ok_or(LedgerError::CorruptLedger)?;
    let end = start
        + canonical[start..]
            .find(") STRICT;")
            .ok_or(LedgerError::CorruptLedger)?
        + ") STRICT;".len();
    c.execute_batch("PRAGMA foreign_keys=OFF; BEGIN IMMEDIATE;")?;
    let result = (|| -> Result<()> {
        validate_v7(c)?;
        c.execute_batch(&canonical[start..end].replacen(
            "CREATE TABLE ledger_meta (",
            "CREATE TABLE ledger_meta_v8 (",
            1,
        ))?;
        c.execute_batch("INSERT INTO ledger_meta_v8 SELECT singleton,8,ledger_uuid,epoch,commit_seq,derived_version,identity_version,normalization_version,report_timezone,tzdb_version,created_ms FROM ledger_meta; DROP TABLE ledger_meta; ALTER TABLE ledger_meta_v8 RENAME TO ledger_meta;")?;
        c.execute_batch(include_str!("claude_protection_schema.sql"))?;
        validate(c)?;
        let violation: Option<String> = c
            .query_row("PRAGMA foreign_key_check", [], |r| r.get(0))
            .optional()?;
        if violation.is_some() {
            return Err(LedgerError::CorruptLedger);
        }
        bump(c)?;
        regression_stop("migration_v8_before_commit");
        c.execute_batch("COMMIT;")?;
        Ok(())
    })();
    if result.is_err() {
        let _ = c.execute_batch("ROLLBACK;");
    }
    c.execute_batch("PRAGMA foreign_keys=ON;")?;
    result
}

pub(crate) fn validate(c: &Connection) -> Result<()> {
    validate_versions(c, &[8])?;
    validate_v7(c)?;
    c.prepare("SELECT identity,entry_json,reason FROM claude_protection_pending LIMIT 0")
        .map_err(|_| LedgerError::CorruptLedger)?;
    Ok(())
}
fn validate_v7(c: &Connection) -> Result<()> {
    validate_versions(c, &[7, 8])?;
    validate_v6(c)?;
    c.prepare("SELECT parent_observation_id,version,provenance,member_count,parent_hash,policy_hash,evidence_json,manifest_hash,created_ms FROM durable_time_revisions LIMIT 0").map_err(|_|LedgerError::CorruptLedger)?;
    c.prepare("SELECT parent_observation_id,member_key,content_hash,occurred_ms,metrics_json,cost_units,cost_scale FROM durable_time_revision_members LIMIT 0").map_err(|_|LedgerError::CorruptLedger)?;
    c.prepare(
        "SELECT parent_observation_id,basis,attempted_ms,reason FROM time_repair_attempts LIMIT 0",
    )
    .map_err(|_| LedgerError::CorruptLedger)?;
    c.prepare("SELECT report_source,ledger_uuid,metadata_json,entry_count FROM time_repair_saved_sources LIMIT 0").map_err(|_|LedgerError::CorruptLedger)?;
    c.prepare("SELECT report_source,ordinal,entry_json FROM time_repair_saved_entries LIMIT 0")
        .map_err(|_| LedgerError::CorruptLedger)?;
    c.prepare("SELECT report_source,root_id,row_key,identity_family,namespace_key,layout_json,projection_json,row_hash FROM time_repair_saved_projection LIMIT 0").map_err(|_|LedgerError::CorruptLedger)?;
    let triggers:i64=c.query_row("SELECT COUNT(*) FROM sqlite_schema WHERE type='trigger' AND name IN('time_revision_immutable','time_revision_member_immutable','time_revision_member_sealed','time_revision_no_detach','time_revision_member_no_detach','time_revision_exclusive','time_allocation_exclusive')",[],|r|r.get(0))?;
    if triggers != 7 {
        return Err(LedgerError::CorruptLedger);
    }
    Ok(())
}

/// Derived caches are safe to discard and are epoch-bound. The projection DDL
/// also declares permanent tombstones: only initialization/migration may create
/// them; validation rejects a v5 ledger whose controls have disappeared.
fn ensure_read_caches(c: &Connection) -> Result<()> {
    let transaction = if c.is_autocommit() {
        Some(c.unchecked_transaction()?)
    } else {
        None
    };
    c.execute_batch(include_str!("legacy_capture_schema.sql"))?;
    c.execute_batch(include_str!("read_model_schema.sql"))?;
    c.execute_batch(include_str!("source_projection_schema.sql"))?;
    c.execute_batch(include_str!("minimax_model_schema.sql"))?;
    let has_domain: bool = c
        .prepare("PRAGMA table_info(source_projection_scopes)")?
        .query_map([], |r| r.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .iter()
        .any(|name| name == "reader_domain");
    if !has_domain {
        c.execute_batch("ALTER TABLE source_projection_scopes ADD COLUMN reader_domain TEXT")?;
    }
    if let Some(transaction) = transaction {
        transaction.commit()?;
    }
    Ok(())
}

fn validate_v6(c: &Connection) -> Result<()> {
    validate_versions(c, &[6, 7, 8])?;
    validate_v5_controls(c)?;
    c.prepare("SELECT parent_observation_id,version,provenance,complete,member_count,manifest_hash,created_ms FROM durable_time_allocations LIMIT 0").map_err(|_|LedgerError::CorruptLedger)?;
    c.prepare("SELECT parent_observation_id,member_key,occurred_ms,metrics_json,cost_units,cost_scale FROM durable_time_members LIMIT 0").map_err(|_|LedgerError::CorruptLedger)?;
    let triggers:i64=c.query_row("SELECT COUNT(*) FROM sqlite_schema WHERE type='trigger' AND name IN('time_allocation_immutable','time_member_immutable','time_member_sealed','time_allocation_no_detach','time_member_no_detach')",[],|r|r.get(0))?;
    if triggers != 5 {
        return Err(LedgerError::CorruptLedger);
    }
    Ok(())
}
fn validate_v5_controls(c: &Connection) -> Result<()> {
    c.prepare("SELECT root_id,table_name,row_key_hash FROM source_projection_tombstones LIMIT 0")
        .map_err(|_| LedgerError::CorruptLedger)?;
    Ok(())
}
fn validate_versions(c: &Connection, supported_schemas: &[i64]) -> Result<()> {
    let (schema,identity,normalization):(i64,i64,i64)=c.query_row("SELECT schema_version,identity_version,normalization_version FROM ledger_meta WHERE singleton=1",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).map_err(|_|LedgerError::CorruptLedger)?;
    if !supported_schemas.contains(&schema) || identity != 1 || normalization != 1 {
        return Err(LedgerError::UnsupportedVersion);
    }
    let bad:i64=c.query_row("SELECT count(*) FROM ingest_runs WHERE state='publishing' AND (resolver_version NOT IN ('ledger-resolver-v1','ledger-resolver-v2','ledger-resolver-v3') OR identity_version<>1 OR epoch<>(SELECT epoch FROM ledger_meta))",[],|r|r.get(0))?;
    if bad != 0 {
        return Err(LedgerError::UpgradeRequiresDrain);
    }
    let versions: Vec<(String, String, i64)> = c
        .prepare(
            "SELECT identity_family,parser_version,identity_version FROM source_roots
             UNION SELECT r.identity_family,o.parser_version,r.identity_version
             FROM source_objects o JOIN source_roots r ON r.root_id=o.root_id",
        )?
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
        .collect::<rusqlite::Result<_>>()?;
    if versions.iter().any(|(family, parser, identity)| {
        !super::durable::supported_parser(family, parser, *identity)
    }) {
        return Err(LedgerError::UnsupportedVersion);
    }
    let bad_checkpoint: bool = c.query_row(
        "SELECT EXISTS(SELECT 1 FROM source_objects WHERE checkpoint_format IS NOT NULL AND checkpoint_format NOT IN(1,2))", [], |r|r.get(0),
    )?;
    if bad_checkpoint {
        return Err(LedgerError::UnsupportedVersion);
    }
    if schema >= 3 {
        let versions: Vec<(String,String,i64,i64)> = c.prepare(
            "SELECT DISTINCT r.identity_family,i.parser_version,i.identity_version,i.semantics_version FROM durable_imports i JOIN source_roots r ON r.root_id=i.root_id",
        )?.query_map([], |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?.collect::<rusqlite::Result<_>>()?;
        if versions
            .iter()
            .any(|(family, parser, identity, semantics)| {
                *semantics != 1 || !super::durable::supported_parser(family, parser, *identity)
            })
        {
            return Err(LedgerError::UnsupportedVersion);
        }
    }
    Ok(())
}
/// Historical parser/semantic versions are checked once at the read-only
/// writer/restore boundary, before any DDL. Ordinary dashboard reads validate
/// the current protocol metadata without rescanning the entire fact history.
fn validate_historical_versions(c: &Connection) -> Result<()> {
    let mut st = c.prepare("SELECT DISTINCT identity_family,parser_version,identity_version,semantics_version FROM usage_observations")?;
    let mut rows = st.query([])?;
    while let Some(r) = rows.next()? {
        let family: String = r.get(0)?;
        let parser: String = r.get(1)?;
        let identity: i64 = r.get(2)?;
        let semantics: i64 = r.get(3)?;
        if semantics != 1 || !super::durable::supported_parser(&family, &parser, identity) {
            return Err(LedgerError::UnsupportedVersion);
        }
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
        check_orphaned_sidecars(path)?;
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
            c.execute_batch(include_str!("durable_schema.sql"))?;
            c.execute_batch(include_str!("coverage_schema.sql"))?;
            c.execute_batch(include_str!("runtime_schema.sql"))?;
            c.execute_batch(include_str!("durable_snapshots_schema.sql"))?;
            c.execute_batch(include_str!("adoption_schema.sql"))?;
            c.execute_batch(include_str!("time_allocation_schema.sql"))?;
            c.execute_batch(include_str!("time_repair_schema.sql"))?;
            c.execute_batch(include_str!("claude_protection_schema.sql"))?;
            ensure_read_caches(&c)?;
            c.execute(
                "INSERT INTO ledger_meta VALUES(1,8,?1,?2,0,1,1,1,?3,?4,?5)",
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
        let (conn, indexes) = open_connection_with_index_outcome(&path, write)?;
        Ok(Self {
            conn,
            path,
            _maintenance: lease,
            _writer: writer,
            optimize_next: Instant::now(),
            optimize_first: true,
            query_index_retry_at: (indexes == Some(super::query_indexes::InstallOutcome::Deferred))
                .then(|| Instant::now() + super::query_indexes::RETRY_DELAY),
            writer_poisoned: false,
            claude_readers: None,
        })
    }
    pub fn connection(&self) -> &Connection {
        &self.conn
    }
    pub fn path(&self) -> &Path {
        &self.path
    }
    pub(crate) fn require_writer(&self) -> Result<()> {
        if self.writer_poisoned {
            return Err(LedgerError::InvalidInput(
                "writer requires reopening after query index maintenance failure".into(),
            ));
        }
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
    /// Retry only optional query indexes that could not be built at open.
    /// A fatal error retires this writer; callers must not continue writing
    /// through the connection and must close/reopen it before further work.
    pub fn maintain_query_indexes_if_due(&mut self) -> Result<bool> {
        self.require_writer()?;
        let Some(due) = self.query_index_retry_at else {
            return Ok(false);
        };
        if Instant::now() < due {
            return Ok(false);
        }
        match super::query_indexes::install(&self.conn, super::query_indexes::IDLE_BUDGET) {
            Ok(outcome) => {
                self.query_index_retry_at = (outcome
                    == super::query_indexes::InstallOutcome::Deferred)
                    .then(|| Instant::now() + super::query_indexes::RETRY_DELAY);
                Ok(true)
            }
            Err(error) => {
                self.writer_poisoned = true;
                Err(error)
            }
        }
    }
    #[cfg(feature = "regression-hooks")]
    pub fn regression_query_index_retry_now(&mut self) {
        self.query_index_retry_at = Some(Instant::now());
    }
    #[cfg(feature = "regression-hooks")]
    pub fn regression_query_index_retry_pending(&self) -> bool {
        self.query_index_retry_at.is_some()
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
    c.execute_batch(include_str!("durable_schema.sql"))?;
    c.execute_batch(include_str!("coverage_schema.sql"))?;
    c.execute_batch(include_str!("runtime_schema.sql"))?;
    c.execute_batch(include_str!("durable_snapshots_schema.sql"))?;
    c.execute_batch(include_str!("adoption_schema.sql"))?;
    c.execute_batch(include_str!("time_allocation_schema.sql"))?;
    c.execute_batch(include_str!("time_repair_schema.sql"))?;
    c.execute_batch(include_str!("claude_protection_schema.sql"))?;
    ensure_read_caches(&c)?;
    c.execute(
        "INSERT INTO ledger_meta VALUES(1,8,?1,?2,0,1,1,1,?3,?4,?5)",
        rusqlite::params![
            new_epoch(),
            new_epoch(),
            timezone,
            chrono_tz::IANA_TZDB_VERSION,
            now_ms()
        ],
    )?;
    super::query_indexes::install(&c, super::query_indexes::OPEN_BUDGET)?;
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

    #[test]
    fn init_refuses_sidecars_without_main_database() {
        use std::time::{SystemTime, UNIX_EPOCH};

        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "usagebook-orphan-sidecars-{}-{nanos}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("usage-ledger.db");

        // Any stale sidecar must block a fresh install before SQLite can
        // associate it with the new main database.
        for suffix in ["-wal", "-shm", "-journal"] {
            std::fs::write(super::sidecar_path(&path, suffix), b"stale").unwrap();
            let result = super::Ledger::init(&path, "UTC");
            assert!(
                matches!(result, Err(crate::ledger::LedgerError::InvalidInput(_))),
                "suffix {suffix} must be rejected"
            );
            assert!(!path.exists(), "init must not publish a database");
            std::fs::remove_file(super::sidecar_path(&path, suffix)).unwrap();
        }

        // With the sidecars gone, the same path initializes normally.
        let ledger = super::Ledger::init(&path, "UTC").unwrap();
        drop(ledger);

        // An existing main database still returns the established error; the
        // orphan guard must not preempt or hide the normal-open path.
        std::fs::write(super::sidecar_path(&path, "-wal"), b"stale").unwrap();
        let result = super::Ledger::init(&path, "UTC");
        assert!(matches!(
            result,
            Err(crate::ledger::LedgerError::AlreadyInitialized)
        ));
        std::fs::remove_file(super::sidecar_path(&path, "-wal")).unwrap();
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
