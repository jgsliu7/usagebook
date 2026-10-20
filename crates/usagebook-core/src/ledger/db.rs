use super::types::*;
use crate::store::lifecycle::Lease;
use rusqlite::{Connection, OpenFlags};
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
pub(crate) fn private_file(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
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
    c.execute_batch("PRAGMA foreign_keys=ON; PRAGMA temp_store=FILE; PRAGMA cache_size=-8192;")?;
    validate(&c)?;
    if write {
        c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")?;
    }
    Ok(c)
}
pub(crate) fn validate(c: &Connection) -> Result<()> {
    let (schema,identity,normalization):(i64,i64,i64)=c.query_row("SELECT schema_version,identity_version,normalization_version FROM ledger_meta WHERE singleton=1",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).map_err(|_|LedgerError::CorruptLedger)?;
    if schema != 1 || identity != 1 || normalization != 1 {
        return Err(LedgerError::UnsupportedVersion);
    }
    let bad:i64=c.query_row("SELECT count(*) FROM ingest_runs WHERE state='publishing' AND (resolver_version<>'ledger-resolver-v1' OR identity_version<>1 OR epoch<>(SELECT epoch FROM ledger_meta))",[],|r|r.get(0))?;
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
        let f = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)?;
        drop(f);
        private_file(path)?;
        let c = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
        c.execute_batch("PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; BEGIN IMMEDIATE;")?;
        c.execute_batch(include_str!("schema.sql"))?;
        c.execute(
            "INSERT INTO ledger_meta VALUES(1,1,?1,?2,0,1,1,1,?3,?4,?5)",
            rusqlite::params![
                new_epoch(),
                new_epoch(),
                timezone,
                chrono_tz::IANA_TZDB_VERSION,
                now_ms()
            ],
        )?;
        c.execute_batch("COMMIT; PRAGMA wal_checkpoint(TRUNCATE);")?;
        drop(c);
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
/// A transient query context when the user has not initialized durable storage.
/// It never writes a ledger file and every source remains explicitly unprotected.
pub fn live_connection(timezone: &str) -> Result<Connection> {
    super::metrics::timezone(timezone)?;
    let c = Connection::open_in_memory()?;
    c.execute_batch(include_str!("schema.sql"))?;
    c.execute(
        "INSERT INTO ledger_meta VALUES(1,1,?1,?2,0,1,1,1,?3,?4,?5)",
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
