//! Offline maintenance. Preview and application share the same transaction and guards.
use super::{db, types::*};
use crate::store::lifecycle::Lease;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

pub struct Maintenance {
    path: PathBuf,
    _maintenance: Lease,
    _writer: Lease,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForgetScope {
    pub source: Option<String>,
    pub identity: Option<String>,
    pub all: bool,
    pub start_ms: Option<i64>,
    pub end_ms: Option<i64>,
}
#[derive(Clone, Debug, Serialize)]
pub struct Preview {
    pub operation: String,
    pub preview_token: String,
    pub affected_candidates: String,
    pub affected_events: String,
    pub warnings: Vec<String>,
    pub replacement: Option<ReplacementImpact>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub released_rule: Option<ReleasedRule>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uncovered_time: Option<UncoveredTime>,
}
/// Usage candidates left outside a dated forget because their time is unknown.
/// Counts exclude identity-only projections and candidates already in its closure.
#[derive(Clone, Debug, Serialize)]
pub struct UncoveredTime {
    pub request_candidates: String,
    pub day_candidates: String,
}
/// Removing a rule does not itself import records. Its persisted scope is the
/// useful preview; deleted payloads cannot provide a reliable reimport count.
#[derive(Clone, Debug, Serialize)]
pub struct ReleasedRule {
    pub rule_id: String,
    pub kind: String,
    pub report_source: Option<String>,
    pub identity_family: Option<String>,
    pub namespace_key: Option<String>,
    pub identity_key: Option<String>,
    pub session_key: Option<String>,
    pub link_version: Option<String>,
    pub link_key: Option<String>,
    pub native_tool: Option<String>,
    pub start_ms: Option<String>,
    pub end_ms: Option<String>,
    pub same_operation_rules_remaining: String,
    pub possible_reimport_candidates: Option<String>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InstallMarker {
    version: u32,
    pending: String,
    recovery: String,
    old_hash: String,
    new_hash: String,
    epoch: String,
}
fn marker_path(p: &Path) -> PathBuf {
    p.with_extension("install.json")
}
fn backup_target(path: &Path, output: &Path) -> Result<PathBuf> {
    let name = output
        .file_name()
        .ok_or_else(|| LedgerError::InvalidInput("invalid backup output".into()))?;
    let parent = output
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."))
        .canonicalize()?;
    let target = parent.join(name);
    let (maintenance, writer) = db::lock_paths(path);
    let mut wal = path.as_os_str().to_os_string();
    wal.push("-wal");
    let mut shm = path.as_os_str().to_os_string();
    shm.push("-shm");
    let mut journal = path.as_os_str().to_os_string();
    journal.push("-journal");
    let stem = path.file_stem().unwrap_or_default().to_string_lossy();
    let output_name = name.to_string_lossy();
    let temporary_install = output_name.starts_with(&format!("{stem}."))
        && [
            ".init.tmp",
            ".install.tmp",
            ".restore.pending.db",
            ".recovery.db",
        ]
        .iter()
        .any(|suffix| output_name.ends_with(suffix));
    if target == path
        || target.as_os_str() == wal.as_os_str()
        || target.as_os_str() == shm.as_os_str()
        || target.as_os_str() == journal.as_os_str()
        || target == maintenance
        || target == writer
        || target == marker_path(path)
        || temporary_install
    {
        return Err(LedgerError::InvalidInput(
            "backup output is reserved for the ledger".into(),
        ));
    }
    if target.symlink_metadata().is_ok() {
        return Err(LedgerError::InvalidInput("output already exists".into()));
    }
    Ok(target)
}
fn hash_file(path: &Path) -> Result<String> {
    let mut f = std::fs::File::open(path)?;
    let mut h = Sha256::new();
    let mut buf = [0u8; 65536];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        h.update(&buf[..n])
    }
    Ok(format!("{:x}", h.finalize()))
}
fn sync_file(p: &Path) -> Result<()> {
    // Windows FlushFileBuffers requires a handle opened for writing.
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(p)?
        .sync_all()?;
    Ok(())
}
fn sync_dir(_p: &Path) -> Result<()> {
    #[cfg(unix)]
    std::fs::File::open(_p.parent().unwrap_or(Path::new(".")))?.sync_all()?;
    Ok(())
}
fn publish_install_marker(path: &Path, marker: &InstallMarker) -> Result<()> {
    let bytes = serde_json::to_vec(marker).map_err(|_| LedgerError::CorruptLedger)?;
    let published = marker_path(path);
    let temporary = path.with_extension(format!("{}.install.tmp", new_epoch()));
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    let result = (|| -> Result<()> {
        let mut file = file;
        db::private_file(&temporary)?;
        regression_stop("install_marker_created");
        // Only regression builds split the write, so a killed process can leave
        // a genuinely partial temporary marker without exposing it to recovery.
        #[cfg(feature = "regression-hooks")]
        {
            let middle = bytes.len() / 2;
            file.write_all(&bytes[..middle])?;
            regression_stop("install_marker_partial");
            file.write_all(&bytes[middle..])?;
        }
        #[cfg(not(feature = "regression-hooks"))]
        file.write_all(&bytes)?;
        file.sync_all()?;
        // Close before rename: Windows must not retain an open marker handle.
        drop(file);
        regression_stop("install_marker_synced");
        // The exclusive maintenance lease prevents competing publishers. Keep
        // unexpected formal markers intact rather than replacing their evidence.
        if published.exists() {
            return Err(LedgerError::CorruptLedger);
        }
        std::fs::rename(&temporary, &published)?;
        regression_stop("install_marker_published");
        sync_dir(&published)
    })();
    if result.is_err() {
        // If publication succeeded, the formal marker must remain for recovery.
        // Otherwise only this invocation's temporary file may be removed.
        let _ = std::fs::remove_file(&temporary);
    }
    result
}
pub(crate) fn check_conn(c: &Connection) -> Result<()> {
    db::validate(c)?;
    check_integrity(c)?;
    check_selected_events(c)?;
    super::adoption::check(c)?;
    super::time_allocation::check(c)?;
    super::time_repair::check(c)?;
    check_daily_rollups(c)
}
fn check_integrity(c: &Connection) -> Result<()> {
    let result: String = c.query_row("PRAGMA integrity_check", [], |r| r.get(0))?;
    if result != "ok" {
        return Err(LedgerError::CorruptLedger);
    }
    let fk: Option<String> = c
        .query_row("PRAGMA foreign_key_check", [], |r| r.get(0))
        .optional()?;
    if fk.is_some() {
        return Err(LedgerError::CorruptLedger);
    }
    Ok(())
}

fn check_selected_events(c: &Connection) -> Result<()> {
    // The event identity may be canonicalized; its selected raw identity need not
    // equal it. All copied fields and the selection's scope/action must agree.
    let invalid: bool = c.query_row(
        "SELECT EXISTS(SELECT 1 FROM main.usage_events e
         JOIN main.usage_observations o ON o.observation_id=e.selected_observation_id
         WHERE e.identity_family IS NOT o.identity_family
            OR e.namespace_key IS NOT o.namespace_key
            OR e.fact_kind IS NOT o.fact_kind
            OR e.report_source IS NOT o.report_source
            OR e.native_tool IS NOT o.native_tool
            OR e.model_key IS NOT o.model_key
            OR e.model_display IS NOT o.model_display
            OR e.session_key IS NOT o.session_key
            OR e.occurred_ms IS NOT o.occurred_ms
            OR e.day_start_ms IS NOT o.day_start_ms
            OR e.day_end_ms IS NOT o.day_end_ms
            OR (o.eligibility<>'eligible' AND NOT (
                o.eligibility='pending_finality' AND EXISTS(
                    SELECT 1 FROM adoption_items a JOIN adoption_policies p ON p.policy_id=a.policy_id
                    WHERE a.selected_observation_id=o.observation_id AND p.status='active')))
            OR (e.state='active' AND o.action<>'upsert')
            OR (e.state='retracted' AND o.action<>'retract'))",
        [],
        |r| r.get(0),
    )?;
    if invalid {
        return Err(LedgerError::CorruptLedger);
    }
    Ok(())
}

fn check_daily_rollups(c: &Connection) -> Result<()> {
    // Only TEMP is writable here. The shadow lets the existing checked metric
    // evaluator rebuild from selected facts without touching the original sums.
    c.execute_batch(
        "CREATE TEMP TABLE usage_rollups AS SELECT * FROM main.usage_rollups WHERE 0;",
    )?;
    let result = (|| -> Result<()> {
        c.execute_batch("CREATE UNIQUE INDEX temp.check_rollups_key ON usage_rollups(derived_version,grain,bucket_start_ms,report_source,model_key);
            CREATE INDEX temp.check_rollups_day ON usage_rollups(derived_version,grain,local_day,report_source,model_key);")?;
        super::resolve::rebuild_rollups(c)?;
        let mut columns = String::from("derived_version,grain,bucket_start_ms,bucket_end_ms,local_day,report_source,model_key,event_count");
        for name in METRIC_NAMES {
            columns.push_str(&format!(
                ",{name}_known_sum,{name}_known_count,{name}_exact_count"
            ));
        }
        let current = "grain='day' AND derived_version=(SELECT derived_version FROM main.ledger_meta WHERE singleton=1)";
        let original = format!("SELECT {columns} FROM main.usage_rollups WHERE {current}");
        let rebuilt = format!("SELECT {columns} FROM temp.usage_rollups WHERE {current}");
        let different: bool = c.query_row(
            &format!(
                "SELECT EXISTS({original} EXCEPT {rebuilt}) OR EXISTS({rebuilt} EXCEPT {original})"
            ),
            [],
            |r| r.get(0),
        )?;
        if different {
            return Err(LedgerError::CorruptLedger);
        }
        Ok(())
    })();
    // Clean up on both successful comparisons and failed rebuilds/comparisons.
    // A successful check must never leave the shadow hiding the main table.
    let cleanup = c
        .execute_batch("DROP TABLE temp.usage_rollups;")
        .map_err(LedgerError::from);
    result.and(cleanup)
}
fn checkpoint(c: &Connection) -> Result<()> {
    let (busy, log, done): (i64, i64, i64) =
        c.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })?;
    if busy != 0 || log != done {
        return Err(LedgerError::Busy);
    }
    Ok(())
}
fn clean_sidecars(p: &Path) -> Result<()> {
    for suffix in ["-wal", "-shm"] {
        let p = PathBuf::from(format!("{}{suffix}", p.display()));
        if p.exists() {
            if suffix == "-wal" && std::fs::metadata(&p)?.len() > 0 {
                return Err(LedgerError::CorruptLedger);
            }
            std::fs::remove_file(p)?;
        }
    }
    Ok(())
}
fn check_backup_wal(input: &Path) -> Result<()> {
    let input_wal = PathBuf::from(format!("{}-wal", input.display()));
    if input_wal.exists() && std::fs::metadata(input_wal)?.len() > 0 {
        return Err(LedgerError::InvalidInput("backup has an active WAL".into()));
    }
    Ok(())
}
fn readonly_check(p: &Path) -> Result<()> {
    let c = db::open_connection(p, false)?;
    check_conn(&c)
}
fn check_install_epoch(p: &Path, epoch: &str) -> Result<()> {
    let c = db::open_connection(p, false)?;
    check_conn(&c)?;
    if db::stamp(&c)?.epoch != epoch {
        return Err(LedgerError::CorruptLedger);
    }
    Ok(())
}
/// Copy the SQLite snapshot into a file that this invocation already owns.
/// Validation is deliberately separate: an older backup is validated only
/// after its private copy has passed through the existing schema migration.
fn copy_database_contents(c: &Connection, dest: &Path) -> Result<Connection> {
    let mut target =
        Connection::open_with_flags(dest, rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE)?;
    {
        let backup = rusqlite::backup::Backup::new(c, &mut target)?;
        backup.run_to_completion(256, std::time::Duration::from_millis(1), None)?;
    }
    target.execute_batch(
        "PRAGMA foreign_keys=ON; PRAGMA temp_store=FILE; PRAGMA cache_size=-8192;",
    )?;
    Ok(target)
}
fn create_database_file(dest: &Path) -> Result<()> {
    let f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(dest)?;
    drop(f);
    Ok(())
}
fn copy_database(c: &Connection, dest: &Path) -> Result<()> {
    create_database_file(dest)?;
    db::private_file(dest)?;
    let target = copy_database_contents(c, dest)?;
    // Ordinary backups still require the complete current-version checks.
    check_conn(&target)?;
    checkpoint(&target)?;
    drop(target);
    clean_sidecars(dest)?;
    sync_file(dest)?;
    Ok(())
}
/// Own every early restore failure, including file creation, copy and migration.
/// A published install marker transfers ownership to the recovery protocol.
struct RestorePending<'a> {
    ledger: &'a Path,
    path: PathBuf,
    created: bool,
}
impl Drop for RestorePending<'_> {
    fn drop(&mut self) {
        if self.created && !marker_path(self.ledger).exists() {
            // This uninstalled copy is disposable even if migration left a WAL
            // or rollback journal. No source/current database is touched here.
            for suffix in ["-wal", "-shm", "-journal"] {
                let _ =
                    std::fs::remove_file(PathBuf::from(format!("{}{suffix}", self.path.display())));
            }
            let _ = std::fs::remove_file(&self.path);
        }
    }
}
fn restore_stage(name: &str) -> Result<()> {
    regression_stop(name);
    #[cfg(feature = "regression-hooks")]
    if std::env::var("USAGEBOOK_LEDGER_RESTORE_FAIL_AT").as_deref() == Ok(name) {
        return Err(LedgerError::InvalidInput("injected restore failure".into()));
    }
    Ok(())
}

pub fn recover_install(path: &Path) -> Result<()> {
    let canonical = db::canonical_path(path)?;
    let path = canonical.as_path();
    if !marker_path(path).exists() {
        return Ok(());
    }
    let (a, b) = db::lock_paths(path);
    let _a = Lease::exclusive(&a)?;
    let _b = Lease::exclusive(&b)?;
    recover_locked(path)
}
fn recover_locked(path: &Path) -> Result<()> {
    let marker = marker_path(path);
    if !marker.exists() {
        return Ok(());
    }
    let bytes = std::fs::read(&marker)?;
    if bytes.len() > 16384 {
        return Err(LedgerError::CorruptLedger);
    }
    let m: InstallMarker =
        serde_json::from_slice(&bytes).map_err(|_| LedgerError::CorruptLedger)?;
    if m.version != 1
        || Path::new(&m.pending).file_name().and_then(|s| s.to_str()) != Some(&m.pending)
        || Path::new(&m.recovery).file_name().and_then(|s| s.to_str()) != Some(&m.recovery)
    {
        return Err(LedgerError::CorruptLedger);
    }
    let dir = path.parent().unwrap_or(Path::new("."));
    let pending = dir.join(&m.pending);
    let recovery = dir.join(&m.recovery);
    if path.exists() {
        let hash = hash_file(path)?;
        if hash == m.old_hash && !recovery.exists() {
            readonly_check(path)?;
            if pending.exists() {
                if hash_file(&pending)? != m.new_hash {
                    return Err(LedgerError::CorruptLedger);
                }
                std::fs::remove_file(&pending)?;
            }
        } else if hash == m.new_hash {
            check_install_epoch(path, &m.epoch)?;
        } else {
            return Err(LedgerError::CorruptLedger);
        }
    } else if recovery.exists()
        && hash_file(&recovery)? == m.old_hash
        && pending.exists()
        && hash_file(&pending)? == m.new_hash
    {
        check_install_epoch(&pending, &m.epoch)?;
        std::fs::rename(&pending, path)?;
    } else {
        return Err(LedgerError::CorruptLedger);
    }
    std::fs::remove_file(marker)?;
    sync_dir(path)?;
    Ok(())
}
impl Maintenance {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = db::canonical_path(path.as_ref())?;
        let (a, b) = db::lock_paths(&path);
        let maintenance = Lease::exclusive(&a)?;
        let writer = Lease::exclusive(&b)?;
        recover_locked(&path)?;
        if !path.exists() {
            return Err(LedgerError::NotInitialized);
        }
        Ok(Self {
            path,
            _maintenance: maintenance,
            _writer: writer,
        })
    }
    pub fn check(&self) -> Result<()> {
        // check is the only read-only maintenance op; a v1 ledger reports
        // UnsupportedVersion there, so upgrade it once through a writer first.
        if let Err(LedgerError::UnsupportedVersion) = db::open_connection(&self.path, false) {
            drop(db::open_connection(&self.path, true)?);
        }
        let c = db::open_connection(&self.path, false)?;
        check_conn(&c)
    }
    pub fn backup(&self, output: &Path) -> Result<()> {
        let output = backup_target(&self.path, output)?;
        let c = db::open_connection(&self.path, true)?;
        check_conn(&c)?;
        let temp = output.with_extension(format!("{}.backup.tmp", new_epoch()));
        if let Err(error) = copy_database(&c, &temp) {
            let _ = std::fs::remove_file(&temp);
            return Err(error);
        }
        drop(c);
        if let Err(error) = backup_target(&self.path, &output) {
            let _ = std::fs::remove_file(&temp);
            return Err(error);
        }
        if let Err(error) = std::fs::hard_link(&temp, &output) {
            let _ = std::fs::remove_file(&temp);
            return Err(error.into());
        }
        std::fs::remove_file(&temp)?;
        sync_dir(&output)?;
        Ok(())
    }
    pub fn pause(&self, source: &str, paused: bool) -> Result<()> {
        let c = db::open_connection(&self.path, true)?;
        c.execute_batch("BEGIN IMMEDIATE")?;
        if c.execute(
            "UPDATE source_roots SET enabled=?1 WHERE report_source=?2",
            params![!paused, source],
        )? == 0
        {
            return Err(LedgerError::InvalidInput("source is not registered".into()));
        }
        change_epoch_preserving_read_caches(&c)?;
        regression_stop("pause_before_commit");
        c.execute_batch("COMMIT")?;
        regression_stop("pause_after_commit");
        Ok(())
    }
    pub fn forget(&self, scope: &ForgetScope, apply: Option<&str>) -> Result<Preview> {
        validate_scope(scope)?;
        let c = db::open_connection(&self.path, true)?;
        c.execute_batch("BEGIN IMMEDIATE")?;
        validate_registered(&c, scope)?;
        c.execute_batch("CREATE TEMP TABLE delete_ids(id INTEGER PRIMARY KEY); CREATE TEMP TABLE affected_units(family TEXT,namespace TEXT,publish_key TEXT,visible INTEGER,PRIMARY KEY(family,namespace,publish_key));")?;
        select_scope(&c, scope)?;
        validate_indivisible_days(&c, scope)?;
        super::adoption::validate_forget(&c, scope)?;
        super::adoption::before_forget(&c)?;
        let operation =
            serde_json::to_string(scope).map_err(|_| LedgerError::InvalidInput("scope".into()))?;
        let op = hash_parts("forget", &[&operation]);
        add_explicit_rules(&c, scope, &op)?;
        // Legacy previews contain saved numeric projections. Any deletion
        // invalidates and erases these bounded, unapplied staging values.
        c.execute(
            "DELETE FROM ledger_runtime_state WHERE state_key LIKE 'legacy-preview/%'",
            [],
        )?;
        let staged_removed = expand_deletion(&c, &op, false)?;
        super::source_projection::retain_suppressed_row_keys(&c)?;
        let retained_capture = clear_forgotten_read_caches(&c, scope)?;
        validate_indivisible_days(&c, scope)?;
        let mut preview = preview(&c, "forget", &operation)?;
        if staged_removed > 0 {
            preview.warnings.push(format!("{staged_removed} matching rows are also removed from sealed snapshot checkpoints; unrelated unread rows remain."));
        }
        if scope.start_ms.is_some() {
            let uncovered = uncovered_time(&c, scope)?;
            if uncovered.request_candidates != "0" || uncovered.day_candidates != "0" {
                preview.warnings.push(format!(
                    "Date filtering leaves {} request candidates without a timestamp and {} day candidates without a known interval outside this deletion. Their dates cannot be determined.",
                    uncovered.request_candidates, uncovered.day_candidates
                ));
            }
            preview.uncovered_time = Some(uncovered);
        }
        if let Some(token) = apply {
            if token != preview.preview_token {
                return Err(LedgerError::InvalidInput(
                    "preview expired; preview again".into(),
                ));
            }
            delete_selected(&c)?;
            if scope.start_ms.is_none() {
                if let Some(source) = &scope.source {
                    c.execute(
                        "UPDATE source_roots SET enabled=0 WHERE report_source=?1",
                        [source],
                    )?;
                } else if scope.all {
                    c.execute("UPDATE source_roots SET enabled=0", [])?;
                }
            }
            change_epoch_preserving_read_caches(&c)?;
            if let Some(capture) = retained_capture {
                capture
                    .persist(&c)
                    .map_err(|_| LedgerError::CorruptLedger)?;
                super::read_model::replace(&c, &capture).map_err(|_| LedgerError::CorruptLedger)?;
            }
            regression_stop("forget_before_commit");
            c.execute_batch("COMMIT")?;
            regression_stop("forget_after_commit");
            checkpoint(&c)?;
        } else {
            c.execute_batch("ROLLBACK")?;
        }
        Ok(preview)
    }
    pub fn unsuppress(&self, id: i64, apply: Option<&str>) -> Result<Preview> {
        let c = db::open_connection(&self.path, true)?;
        c.execute_batch("BEGIN IMMEDIATE")?;
        let rule = c
            .query_row(
                "SELECT kind,report_source,identity_family,namespace_key,identity_key,session_key,
                    link_version,link_key,native_tool,start_ms,end_ms,
                    (SELECT count(*) FROM suppression_rules other
                     WHERE other.operation_id=s.operation_id AND other.rule_id<>s.rule_id)
             FROM suppression_rules s WHERE rule_id=?1",
                [id],
                |r| {
                    Ok(ReleasedRule {
                        rule_id: id.to_string(),
                        kind: r.get(0)?,
                        report_source: r.get(1)?,
                        identity_family: r.get(2)?,
                        namespace_key: r.get(3)?,
                        identity_key: r.get(4)?,
                        session_key: r.get(5)?,
                        link_version: r.get::<_, Option<i64>>(6)?.map(|n| n.to_string()),
                        link_key: r.get(7)?,
                        native_tool: r.get(8)?,
                        start_ms: r.get::<_, Option<i64>>(9)?.map(|n| n.to_string()),
                        end_ms: r.get::<_, Option<i64>>(10)?.map(|n| n.to_string()),
                        same_operation_rules_remaining: r.get::<_, i64>(11)?.to_string(),
                        possible_reimport_candidates: None,
                    })
                },
            )
            .optional()?
            .ok_or_else(|| LedgerError::InvalidInput("unknown rule".into()))?;
        let stamp = db::stamp(&c)?;
        let token = hash_parts(
            "unsuppress",
            &[&stamp.epoch, &stamp.commit_seq, &id.to_string()],
        );
        let result=Preview{operation:"unsuppress".into(),preview_token:token.clone(),affected_candidates:"0".into(),affected_events:"0".into(),replacement:None,released_rule:Some(rule),uncovered_time:None,warnings:vec![
            "Only this rule is removed; other identity, session and coverage rules remain. A later refresh may import source records again.".into(),
            "Immediate candidate/event changes are zero. The number of records that may be reimported is unknown; source files are not scanned for this preview.".into()
        ]};
        if let Some(given) = apply {
            if given != token {
                return Err(LedgerError::InvalidInput("preview expired".into()));
            }
            c.execute("DELETE FROM suppression_rules WHERE rule_id=?1", [id])?;
            change_epoch_preserving_read_caches(&c)?;
            regression_stop("unsuppress_before_commit");
            c.execute_batch("COMMIT")?;
            regression_stop("unsuppress_after_commit");
        } else {
            c.execute_batch("ROLLBACK")?;
        }
        Ok(result)
    }
}
fn validate_scope(s: &ForgetScope) -> Result<()> {
    if usize::from(s.all) + usize::from(s.source.is_some()) + usize::from(s.identity.is_some()) != 1
    {
        return Err(LedgerError::InvalidInput(
            "choose one of source, all, identity".into(),
        ));
    }
    if s.start_ms.is_some() != s.end_ms.is_some()
        || matches!((s.start_ms,s.end_ms),(Some(a),Some(b)) if b<=a)
    {
        return Err(LedgerError::InvalidInput("invalid window".into()));
    }
    Ok(())
}
fn validate_registered(c: &Connection, s: &ForgetScope) -> Result<()> {
    if let Some(source) = &s.source {
        let exists: bool = c.query_row("SELECT EXISTS(SELECT 1 FROM source_roots WHERE report_source=?1) OR EXISTS(SELECT 1 FROM usage_observations WHERE report_source=?1) OR EXISTS(SELECT 1 FROM legacy_capture_entries WHERE report_source=?1)",[source],|r|r.get(0))?;
        if !exists {
            return Err(LedgerError::InvalidInput("source is not registered".into()));
        }
    }
    if let Some(key) = &s.identity {
        let count: i64 = c.query_row(
            "SELECT count(*) FROM usage_observations o WHERE o.identity_key=?1",
            [key],
            |r| r.get(0),
        )?;
        if count == 0 {
            return Err(LedgerError::InvalidInput(
                "identity is not present in saved evidence".into(),
            ));
        }
    }
    Ok(())
}
fn select_scope(c: &Connection, s: &ForgetScope) -> Result<()> {
    c.execute("INSERT INTO delete_ids SELECT observation_id FROM usage_observations WHERE (?1 IS NULL OR report_source=?1) AND (?2 IS NULL OR identity_key=?2) AND (?3 IS NULL OR (occurred_ms>=?3 AND occurred_ms<?4) OR (day_start_ms<?4 AND day_end_ms>?3))",params![s.source,s.identity,s.start_ms,s.end_ms])?;
    // An adopted CC day gets its calendar from the explicit decision. A dated
    // deletion of that original report channel must use the same calendar.
    if s.source.is_some() && s.start_ms.is_some() {
        c.execute("INSERT OR IGNORE INTO delete_ids SELECT a.source_observation_id FROM adoption_items a JOIN usage_observations original ON original.observation_id=a.source_observation_id JOIN usage_observations selected ON selected.observation_id=a.selected_observation_id WHERE original.report_source=?1 AND ((selected.occurred_ms>=?2 AND selected.occurred_ms<?3) OR (selected.day_start_ms<?3 AND selected.day_end_ms>?2))",params![s.source,s.start_ms,s.end_ms])?;
    }
    Ok(())
}
fn validate_indivisible_days(c: &Connection, s: &ForgetScope) -> Result<()> {
    if let Some(key) = &s.identity {
        let expanded_day: bool = c.query_row(
            "SELECT NOT EXISTS(SELECT 1 FROM usage_observations WHERE identity_key=?1 AND fact_kind='day') AND EXISTS(SELECT 1 FROM usage_observations o JOIN delete_ids d ON d.id=o.observation_id WHERE o.fact_kind='day')",
            [key], |r| r.get(0),
        )?;
        if expanded_day {
            return Err(LedgerError::InvalidInput("the selected request overlaps indivisible daily evidence; select the full aggregate interval or first provide a proved split".into()));
        }
    }
    let Some(start) = s.start_ms else {
        return Ok(());
    };
    let end = s
        .end_ms
        .ok_or_else(|| LedgerError::InvalidInput("invalid window".into()))?;
    let outside: Option<(i64, i64)> = c.query_row(
        "SELECT o.day_start_ms,o.day_end_ms FROM usage_observations o JOIN delete_ids d ON d.id=o.observation_id WHERE o.fact_kind='day' AND o.day_start_ms IS NOT NULL AND (o.day_start_ms<?1 OR o.day_end_ms>?2) LIMIT 1",
        params![start,end], |r| Ok((r.get(0)?,r.get(1)?)),
    ).optional()?;
    if let Some((a, b)) = outside {
        return Err(LedgerError::InvalidInput(format!("indivisible daily evidence spans [{a},{b}); select its whole interval or first provide a proved split")));
    }
    Ok(())
}
/// Adoption links may add a derived observation whose native identity in turn
/// reaches another saved revision. Iterate both closures before showing a plan.
fn expand_deletion(c: &Connection, op: &str, restore: bool) -> Result<u64> {
    let mut scrubbed = 0_u64;
    loop {
        let before: i64 = c.query_row("SELECT count(*) FROM delete_ids", [], |r| r.get(0))?;
        super::adoption::before_forget(c)?;
        expand_rules(c, op, restore)?;
        expand_reported_identities(c, op)?;
        let erased = super::ccswitch::scrub_snapshot_checkpoints(c, |o| {
            super::ingest::exact_suppressed(c, o)
        })?;
        scrubbed = scrubbed
            .checked_add(erased)
            .ok_or(LedgerError::NumericOverflow)?;
        let after: i64 = c.query_row("SELECT count(*) FROM delete_ids", [], |r| r.get(0))?;
        if before == after && erased == 0 {
            return Ok(scrubbed);
        }
    }
}
/// Strong source identities are useful deletion evidence even before the user
/// adopts their reported values. Preserve that relation in both directions.
fn expand_reported_identities(c: &Connection, op: &str) -> Result<()> {
    let mut st = c.prepare("SELECT o.observation_id,d.projection_json,EXISTS(SELECT 1 FROM delete_ids x WHERE x.id=o.observation_id) FROM usage_observations o JOIN durable_evidence d USING(observation_id) WHERE o.report_source='ccswitch-rollup'")?;
    let mut rows = st.query([])?;
    while let Some(row) = rows.next()? {
        let id: i64 = row.get(0)?;
        let json: String = row.get(1)?;
        let selected: bool = row.get(2)?;
        let p: serde_json::Value =
            serde_json::from_str(&json).map_err(|_| LedgerError::CorruptLedger)?;
        if let Some(probe) = super::ccswitch::reported_identity_probe(&p)? {
            if selected || super::ingest::exact_suppressed(c, &probe)? {
                add_identity_controls(c, &probe, op)?;
                c.execute("INSERT OR IGNORE INTO delete_ids VALUES(?1)", [id])?;
            }
        }
    }
    drop(rows);
    drop(st);
    c.execute(&format!("INSERT OR IGNORE INTO delete_ids SELECT observation_id FROM usage_observations o WHERE {EXACT_RULES}"), [])?;
    Ok(())
}
fn uncovered_time(c: &Connection, s: &ForgetScope) -> Result<UncoveredTime> {
    c.query_row(
        "SELECT count(CASE WHEN fact_kind='request' AND occurred_ms IS NULL THEN 1 END),
                count(CASE WHEN fact_kind='day' AND day_start_ms IS NULL THEN 1 END)
         FROM usage_observations o
         WHERE (?1 IS NULL OR report_source=?1) AND (?2 IS NULL OR identity_key=?2)
           AND eligibility<>'identity_only'
           AND NOT EXISTS(SELECT 1 FROM delete_ids d WHERE d.id=o.observation_id)",
        params![s.source, s.identity],
        |r| {
            Ok(UncoveredTime {
                request_candidates: r.get::<_, i64>(0)?.to_string(),
                day_candidates: r.get::<_, i64>(1)?.to_string(),
            })
        },
    )
    .map_err(LedgerError::from)
}
fn add_explicit_rules(c: &Connection, s: &ForgetScope, op: &str) -> Result<()> {
    if let (None, Some(a)) = (s.identity.as_ref(), s.start_ms) {
        let b = s.end_ms.unwrap();
        c.execute("INSERT INTO suppression_rules(kind,report_source,native_tool,start_ms,end_ms,operation_id,created_ms)VALUES('window',?1,NULL,?2,?3,?4,?5)",params![s.source,a,b,op,now_ms()])?;
    } else if let Some(source) = &s.source {
        c.execute("INSERT OR IGNORE INTO suppression_rules(kind,report_source,native_tool,operation_id,created_ms)VALUES('source',?1,NULL,?2,?3)",params![source,op,now_ms()])?;
    } else if s.all {
        c.execute("INSERT OR IGNORE INTO suppression_rules(kind,report_source,native_tool,operation_id,created_ms) SELECT 'source',report_source,NULL,?1,?2 FROM (SELECT report_source FROM source_roots UNION SELECT report_source FROM usage_observations UNION SELECT report_source FROM legacy_capture_entries)", params![op,now_ms()])?;
    }
    Ok(())
}
fn add_identity_controls(c: &Connection, o: &Observation, op: &str) -> Result<()> {
    if let Some(key) = &o.identity_key {
        c.execute("INSERT OR IGNORE INTO suppression_rules(kind,identity_family,namespace_key,identity_key,operation_id,created_ms)VALUES('identity',?1,?2,?3,?4,?5)",params![o.identity_family,o.namespace_key,key,op,now_ms()])?;
    }
    if let Some(session) = &o.session_key {
        c.execute("INSERT OR IGNORE INTO suppression_rules(kind,identity_family,namespace_key,session_key,operation_id,created_ms)VALUES('session',?1,?2,?3,?4,?5)",params![o.identity_family,o.namespace_key,session,op,now_ms()])?;
    }
    if o.fact_kind == "request" {
        if let Some(message) = &o.native_message_id {
            let link = identity_link(&o.identity_family, &o.namespace_key, message);
            c.execute("INSERT OR IGNORE INTO suppression_rules(kind,identity_family,namespace_key,link_version,link_key,operation_id,created_ms)VALUES('identity_link',?1,?2,1,?3,?4,?5)",params![o.identity_family,o.namespace_key,link,op,now_ms()])?;
        }
    }
    if o.eligibility != "identity_only" {
        let interval = if let Some(t) = o.occurred_ms {
            Some((t, t.checked_add(1).ok_or(LedgerError::NumericOverflow)?))
        } else {
            o.day_start_ms.zip(o.day_end_ms)
        };
        if let Some((a, b)) = interval {
            c.execute("INSERT OR IGNORE INTO suppression_rules(kind,native_tool,namespace_key,start_ms,end_ms,operation_id,created_ms)VALUES('coverage_interval',?1,?2,?3,?4,?5,?6)",params![o.native_tool,o.namespace_key,a,b,op,now_ms()])?;
            if o.fact_kind == "day" {
                c.execute("INSERT OR IGNORE INTO suppression_rules(kind,native_tool,namespace_key,start_ms,end_ms,operation_id,created_ms)VALUES('consumption_interval',?1,?2,?3,?4,?5,?6)",params![o.native_tool,o.namespace_key,a,b,op,now_ms()])?;
            }
        } else {
            // A source day may acquire its calendar only through explicit
            // adoption. Its durable date tombstone is more precise than the
            // old unbound-family fallback and must not erase adjacent days.
            if o.fact_kind == "day" && o.report_source == "ccswitch-rollup" {
                if let Some(date) = &o.native_date {
                    if super::adoption::suppresses_unbound_day(c, &o.native_tool, date)? {
                        return Ok(());
                    }
                }
            }
            c.execute("INSERT OR IGNORE INTO suppression_rules(kind,native_tool,namespace_key,operation_id,created_ms)VALUES('coverage_family',?1,?2,?3,?4)",params![o.native_tool,o.namespace_key,op,now_ms()])?;
        }
    }
    Ok(())
}
// SQL exact rules are deliberately independent of statistical aliases.
const EXACT_RULES:&str="EXISTS(SELECT 1 FROM suppression_rules s WHERE
 (s.kind='source' AND s.report_source=o.report_source AND (s.native_tool IS NULL OR s.native_tool=o.native_tool))
 OR (s.kind='identity' AND s.identity_family=o.identity_family AND s.namespace_key=o.namespace_key AND s.identity_key=o.identity_key)
 OR (s.kind='session' AND o.fact_kind IN('session_total','session_cost') AND s.identity_family=o.identity_family AND s.namespace_key=o.namespace_key AND s.session_key=o.session_key)
 OR (s.kind='window' AND (s.report_source IS NULL OR s.report_source=o.report_source) AND (s.native_tool IS NULL OR s.native_tool=o.native_tool) AND ((o.occurred_ms>=s.start_ms AND o.occurred_ms<s.end_ms) OR (o.day_start_ms<s.end_ms AND o.day_end_ms>s.start_ms)))
 OR (s.kind='consumption_interval' AND s.native_tool=o.native_tool AND s.namespace_key=o.namespace_key AND ((o.occurred_ms>=s.start_ms AND o.occurred_ms<s.end_ms) OR (o.day_start_ms<s.end_ms AND o.day_end_ms>s.start_ms)))
 OR (o.fact_kind='day' AND s.native_tool=o.native_tool AND s.namespace_key=o.namespace_key AND (s.kind='coverage_family' OR (s.kind='coverage_interval' AND o.day_start_ms<s.end_ms AND o.day_end_ms>s.start_ms)))) OR EXISTS(SELECT 1 FROM adoption_forget_ranges f WHERE f.consumer_tool=o.native_tool AND (f.identity_family IS NULL OR (f.identity_family=o.identity_family AND f.namespace_key=o.namespace_key)) AND ((o.occurred_ms>=f.start_ms AND o.occurred_ms<f.end_ms) OR (o.day_start_ms<f.end_ms AND o.day_end_ms>f.start_ms)))";
fn expand_rules(c: &Connection, op: &str, restore: bool) -> Result<()> {
    let mut proof_stamp: Option<String> = None;
    loop {
        loop {
            let before: i64 = c.query_row("SELECT count(*) FROM delete_ids", [], |r| r.get(0))?;
            let mut st = c.prepare(
                "SELECT o.* FROM usage_observations o JOIN delete_ids d ON d.id=o.observation_id",
            )?;
            let mut rows = st.query([])?;
            while let Some(row) = rows.next()? {
                let o = super::resolve::load_observation(row)?;
                add_identity_controls(c, &o, op)?;
            }
            drop(rows);
            drop(st);
            c.execute(&format!("INSERT OR IGNORE INTO delete_ids SELECT observation_id FROM usage_observations o WHERE {EXACT_RULES}"),[])?;
            let after: i64 = c.query_row("SELECT count(*) FROM delete_ids", [], |r| r.get(0))?;
            if before == after {
                break;
            }
        }
        let mut expanded = false;
        let mut st=c.prepare("SELECT o.* FROM usage_observations o WHERE fact_kind='request' AND native_message_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM delete_ids d WHERE d.id=o.observation_id)")?;
        let mut rows = st.query([])?;
        while let Some(row) = rows.next()? {
            let o = super::resolve::load_observation(row)?;
            let link = identity_link(
                &o.identity_family,
                &o.namespace_key,
                o.native_message_id.as_deref().unwrap(),
            );
            let matched:bool=c.query_row("SELECT EXISTS(SELECT 1 FROM suppression_rules WHERE kind='identity_link' AND identity_family=?1 AND namespace_key=?2 AND link_key=?3)",params![o.identity_family,o.namespace_key,link],|r|r.get(0))?;
            if !matched {
                continue;
            }
            let proof = super::ingest::prove_identity_connection(c, &o);
            if !matches!(proof, Ok(Some(_))) {
                return Err(if restore {
                    LedgerError::RestoreIdentityNeedsReview
                } else {
                    LedgerError::IdentityLinkNeedsReview
                });
            }
            // Every expanded relation must share one fixed source target set.
            let current = probe_digest(c)?;
            if proof_stamp.as_ref().is_some_and(|old| old != &current) {
                return Err(LedgerError::StaleObservation);
            }
            proof_stamp = Some(current);
            super::ingest::verify_identity_probe(c)?;
            add_identity_controls(c, &o, op)?;
            c.execute(
                "INSERT OR IGNORE INTO delete_ids VALUES(?1)",
                [o.observation_id],
            )?;
            expanded = true;
        }
        if !expanded {
            break;
        }
    }
    if proof_stamp.is_some() {
        super::ingest::verify_identity_probe(c)?;
    }
    Ok(())
}
fn probe_digest(c: &Connection) -> Result<String> {
    let mut h = Sha256::new();
    for sql in ["SELECT root_id,locator,parser_version,identity_version FROM identity_probe_roots ORDER BY root_id","SELECT locator,byte_length,mtime_ns FROM identity_probe_files ORDER BY locator"]{let mut st=c.prepare(sql)?;let count=st.column_count();let mut rows=st.query([])?;while let Some(row)=rows.next()?{for i in 0..count{match row.get_ref(i)?{rusqlite::types::ValueRef::Null=>h.update([0]),rusqlite::types::ValueRef::Integer(n)=>{h.update([1]);h.update(n.to_be_bytes())},rusqlite::types::ValueRef::Text(s)=>{h.update([2]);h.update((s.len()as u64).to_be_bytes());h.update(s)},_=>return Err(LedgerError::CorruptLedger)}}}}
    Ok(format!("{:x}", h.finalize()))
}

fn preview(c: &Connection, operation: &str, parameters: &str) -> Result<Preview> {
    let stamp = db::stamp(c)?;
    let mut hash = Sha256::new();
    hash.update(parameters.as_bytes());
    let mut st = c.prepare("SELECT id FROM delete_ids ORDER BY id")?;
    let mut rows = st.query([])?;
    let mut count = 0_i64;
    while let Some(row) = rows.next()? {
        hash.update(row.get::<_, i64>(0)?.to_be_bytes());
        count = count.checked_add(1).ok_or(LedgerError::NumericOverflow)?;
    }
    // Drive preview from the selected candidates, not every historical event.
    // The three branches cover a selected observation, its raw identity, and
    // its already-proven canonical alias; UNION counts an event only once.
    let affected: i64 = if count == 0 {
        0
    } else {
        c.query_row("SELECT count(*) FROM (
            SELECT e.rowid FROM delete_ids d CROSS JOIN usage_events e ON e.selected_observation_id=d.id
            UNION
            SELECT e.rowid FROM delete_ids d CROSS JOIN usage_observations o ON o.observation_id=d.id CROSS JOIN usage_events e ON e.identity_family=o.identity_family AND e.namespace_key=o.namespace_key AND e.identity_key=o.identity_key
            UNION
            SELECT e.rowid FROM delete_ids d CROSS JOIN usage_observations o ON o.observation_id=d.id CROSS JOIN identity_alias a ON a.identity_family=o.identity_family AND a.namespace_key=o.namespace_key AND a.alias_key=o.identity_key CROSS JOIN usage_events e ON e.identity_family=a.identity_family AND e.namespace_key=a.namespace_key AND e.identity_key=a.canonical_key
        )",[],|r|r.get(0))?
    };
    let token = hash_parts(
        "maintenance-preview/v1",
        &[
            operation,
            &stamp.epoch,
            &stamp.commit_seq,
            &format!("{:x}", hash.finalize()),
        ],
    );
    Ok(Preview{operation:operation.into(),preview_token:token,affected_candidates:count.to_string(),affected_events:affected.to_string(),replacement:None,released_rule:None,uncovered_time:None,warnings:vec!["All revisions of matching identities and related session totals/costs are removed. Source files and independent backups remain.".into()]})
}
fn delete_selected(c: &Connection) -> Result<()> {
    // Also cover unread snapshot rows and observations reached by a later
    // identity proof. The CC saver rechecks its outgoing checkpoint before
    // commit, so an older in-memory block cannot write these values back.
    super::ccswitch::scrub_snapshot_checkpoints(c, |o| super::ingest::exact_suppressed(c, o))?;
    // Persist each unit's pre-maintenance publication horizon in bounded disk TEMP.
    c.execute_batch("CREATE TEMP TABLE IF NOT EXISTS affected_units(family TEXT,namespace TEXT,publish_key TEXT,visible INTEGER,PRIMARY KEY(family,namespace,publish_key));")?;
    let mut st=c.prepare("SELECT DISTINCT o.identity_family,o.namespace_key,o.identity_key,o.native_message_id FROM usage_observations o JOIN delete_ids d ON d.id=o.observation_id")?;
    let mut rows = st.query([])?;
    while let Some(r) = rows.next()? {
        let family: String = r.get(0)?;
        let ns: String = r.get(1)?;
        let key: Option<String> = r.get(2)?;
        let message: Option<String> = r.get(3)?;
        let publish = match (family.as_str(), message, key) {
            ("claude", Some(message), _) => format!(
                "m:{}",
                message
                    .as_bytes()
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>()
            ),
            (_, _, Some(key)) => format!("k:{key}"),
            _ => continue,
        };
        let visible = super::resolve::visible_horizon(c, &family, &ns, &publish)?;
        c.execute(
            "INSERT OR IGNORE INTO affected_units VALUES(?1,?2,?3,?4)",
            params![family, ns, publish, visible],
        )?;
    }
    drop(rows);
    drop(st);
    let mut st = c.prepare("SELECT family,namespace,publish_key,visible FROM affected_units")?;
    let mut rows = st.query([])?;
    while let Some(r) = rows.next()? {
        super::resolve::remove_unit_events(
            c,
            &r.get::<_, String>(0)?,
            &r.get::<_, String>(1)?,
            &r.get::<_, String>(2)?,
        )?;
    }
    drop(rows);
    drop(st);
    c.execute("UPDATE source_objects SET checkpoint=NULL,checkpoint_format=NULL,saved_offset=NULL,state='replay_required',target_done=0,object_revision=object_revision+1 WHERE object_key NOT LIKE 'cc-values-v2:%' AND object_id IN(SELECT object_id FROM usage_observations WHERE observation_id IN(SELECT id FROM delete_ids))",[])?;
    c.execute("UPDATE durable_snapshots SET gap_count=gap_count+(SELECT count(*) FROM durable_snapshot_members m JOIN delete_ids d ON d.id=m.observation_id WHERE m.snapshot_id=durable_snapshots.snapshot_id) WHERE EXISTS(SELECT 1 FROM durable_snapshot_members m JOIN delete_ids d ON d.id=m.observation_id WHERE m.snapshot_id=durable_snapshots.snapshot_id)",[])?;
    c.execute(
        "DELETE FROM durable_snapshot_members WHERE observation_id IN(SELECT id FROM delete_ids)",
        [],
    )?;
    let removed = c.execute(
        "DELETE FROM usage_observations WHERE observation_id IN(SELECT id FROM delete_ids)",
        [],
    )?;
    if removed > 0 {
        super::resolve::invalidate_run_units(c)?;
    }
    let mut st = c.prepare("SELECT family,namespace,publish_key,visible FROM affected_units")?;
    let mut rows = st.query([])?;
    while let Some(r) = rows.next()? {
        super::resolve::rebuild_unit(
            c,
            &r.get::<_, String>(0)?,
            &r.get::<_, String>(1)?,
            &r.get::<_, String>(2)?,
            r.get(3)?,
            super::resolve::RebuildCause::ExplicitDelete,
        )?;
    }
    drop(rows);
    drop(st);
    c.execute("DELETE FROM affected_units", [])?;
    Ok(())
}
pub(crate) fn suppress_incoming(c: &Connection, o: &Observation) -> Result<()> {
    add_incoming_controls(c, o)?;
    finish_incoming_suppression(c)
}
pub(crate) fn add_incoming_controls(c: &Connection, o: &Observation) -> Result<()> {
    add_identity_controls(c, o, "suppressed-revision")
}
pub(crate) fn finish_incoming_suppression(c: &Connection) -> Result<()> {
    c.execute_batch("CREATE TEMP TABLE IF NOT EXISTS delete_ids(id INTEGER PRIMARY KEY); DELETE FROM delete_ids;")?;
    c.execute(&format!("INSERT INTO delete_ids SELECT observation_id FROM usage_observations o WHERE {EXACT_RULES}"),[])?;
    expand_deletion(c, "suppressed-revision", false)?;
    delete_selected(c)?;
    c.execute("DELETE FROM delete_ids", [])?;
    Ok(())
}
impl Maintenance {
    pub fn set_timezone(&self, tz: &str) -> Result<()> {
        super::metrics::timezone(tz)?;
        let c = db::open_connection(&self.path, true)?;
        c.execute_batch("BEGIN IMMEDIATE")?;
        let old: i64 = c.query_row("SELECT derived_version FROM ledger_meta", [], |r| r.get(0))?;
        let next = old.checked_add(1).ok_or(LedgerError::NumericOverflow)?;
        c.execute(
            "UPDATE ledger_meta SET report_timezone=?1,derived_version=?2,tzdb_version=?3",
            params![tz, next, chrono_tz::IANA_TZDB_VERSION],
        )?;
        super::resolve::rebuild_rollups(&c)?;
        c.execute(
            "DELETE FROM usage_rollups WHERE derived_version<>?1",
            [next],
        )?;
        change_epoch_preserving_read_caches(&c)?;
        regression_stop("timezone_before_commit");
        c.execute_batch("COMMIT")?;
        regression_stop("timezone_after_commit");
        checkpoint(&c)?;
        Ok(())
    }
    pub fn restore(&self, input: &Path, apply: Option<&str>) -> Result<Preview> {
        let canonical_input = std::fs::canonicalize(input)?;
        let input = canonical_input.as_path();
        if input == std::fs::canonicalize(&self.path)? {
            return Err(LedgerError::InvalidInput(
                "backup must differ from current ledger".into(),
            ));
        }
        // Never read a moving backup with an unmerged WAL as a standalone export.
        check_backup_wal(input)?;
        let input_hash = hash_file(input)?;
        let current = db::open_connection(&self.path, true)?;
        check_conn(&current)?;
        let before = db::stamp(&current)?;
        let source = db::open_restore_source(input)?;
        check_integrity(&source)?;
        let mut staging = RestorePending {
            ledger: &self.path,
            path: self
                .path
                .with_extension(format!("{}.restore.pending.db", new_epoch())),
            created: false,
        };
        let result = (|| -> Result<Preview> {
            let pending = &staging.path;
            create_database_file(pending)?;
            staging.created = true;
            db::private_file(pending)?;
            restore_stage("restore_pending_created")?;
            let copied = copy_database_contents(&source, pending)?;
            restore_stage("restore_pending_copied")?;
            drop(copied);
            drop(source);
            check_backup_wal(input)?;
            if hash_file(input)? != input_hash {
                return Err(LedgerError::StaleObservation);
            }
            let c = db::open_connection(pending, true)?;
            restore_stage("restore_pending_migrated")?;
            check_conn(&c)?;
            restore_stage("restore_pending_checked")?;
            c.execute_batch(
                "BEGIN IMMEDIATE; CREATE TEMP TABLE delete_ids(id INTEGER PRIMARY KEY);",
            )?;
            remember_restore_controls(&current, &c)?;
            let saved_capture = super::legacy::LegacySnapshot::restore_capture(&c)
                .map_err(|_| LedgerError::CorruptLedger)?;
            merge_rules(&current, &c)?;
            super::source_projection::merge_tombstones(&current, &c)?;
            merge_routes(&current, &c)?;
            merge_coverage_controls(&current, &c)?;
            super::adoption::merge_restore(&current, &c)?;
            super::time_allocation::validate_restore(&current, &c)?;
            super::time_repair::validate_restore(&current, &c)?;
            c.execute(&format!("INSERT INTO delete_ids SELECT observation_id FROM usage_observations o WHERE {EXACT_RULES}"),[])?;
            expand_deletion(&c, "restore-protection", true)?;
            super::source_projection::retain_suppressed_row_keys(&c)?;
            super::ccswitch::scrub_snapshot_checkpoints(&c, |o| {
                super::ingest::exact_suppressed(&c, o)
            })?;
            // A restored writer attempt/cutover permit is not evidence that
            // this process has successfully resumed that old runtime state.
            c.execute("DELETE FROM ledger_runtime_state", [])?;
            let retained_capture = prepare_restored_history(&c, saved_capture)?;
            let mut preview = preview(&c, "restore", &input_hash)?;
            // Preview the database that will actually be installed, including
            // deletion closure and rebuilt compatibility facts. This is rolled
            // back on preview-only calls and never mutates the source backup.
            delete_selected(&c)?;
            clear_query_projection(&c)?;
            if let Some(capture) = &retained_capture {
                persist_restored_capture(&c, capture)?;
                super::read_model::replace(&c, capture).map_err(|_| LedgerError::CorruptLedger)?;
            }
            preview.replacement = Some(replacement_impact(&current, &c)?);
            let rules = rule_digest(&current)?;
            preview.preview_token = hash_parts(
                "restore-preview/v2",
                &[
                    "restore-schema-v1-to-v2/1",
                    &input_hash,
                    &before.epoch,
                    &before.commit_seq,
                    &rules,
                    &preview.preview_token,
                ],
            );
            preview.warnings.push("Replacement restores the backup's history; newer current usage is not merged. Protected sources stay on the ledger route.".into());
            if let Some(given) = apply {
                if given != preview.preview_token {
                    return Err(LedgerError::InvalidInput(
                        "preview expired; preview again".into(),
                    ));
                }
                let epoch = change_epoch_preserving_read_caches(&c)?;
                check_conn(&c)?;
                regression_stop("restore_before_commit");
                c.execute_batch("COMMIT")?;
                regression_stop("restore_after_commit");
                checkpoint(&c)?;
                drop(c);
                clean_sidecars(pending)?;
                readonly_check(pending)?;
                sync_file(pending)?;
                sync_dir(pending)?;
                checkpoint(&current)?;
                drop(current);
                clean_sidecars(&self.path)?;
                check_backup_wal(input)?;
                if hash_file(input)? != input_hash {
                    return Err(LedgerError::StaleObservation);
                }
                let recovery = self
                    .path
                    .with_extension(format!("{}.recovery.db", new_epoch()));
                let marker = InstallMarker {
                    version: 1,
                    pending: pending.file_name().unwrap().to_string_lossy().into(),
                    recovery: recovery.file_name().unwrap().to_string_lossy().into(),
                    old_hash: hash_file(&self.path)?,
                    new_hash: hash_file(pending)?,
                    epoch,
                };
                let p = marker_path(&self.path);
                publish_install_marker(&self.path, &marker)?;
                regression_stop("install_prepared");
                std::fs::rename(&self.path, &recovery)?;
                sync_dir(&self.path)?;
                regression_stop("install_old_renamed");
                std::fs::rename(pending, &self.path)?;
                sync_dir(&self.path)?;
                regression_stop("install_new_installed");
                check_install_epoch(&self.path, &marker.epoch)?;
                std::fs::remove_file(&p)?;
                sync_dir(&p)?;
            } else {
                c.execute_batch("ROLLBACK")?;
                drop(c);
            }
            Ok(preview)
        })();
        result
    }
}
fn rule_digest(c: &Connection) -> Result<String> {
    let mut st=c.prepare("SELECT kind,report_source,identity_family,namespace_key,identity_key,session_key,link_version,link_key,native_tool,start_ms,end_ms FROM suppression_rules ORDER BY kind,report_source,identity_family,namespace_key,identity_key,session_key,link_key,native_tool,start_ms,end_ms")?;
    let mut rows = st.query([])?;
    let mut h = Sha256::new();
    h.update(super::source_projection::tombstone_digest(c)?.as_bytes());
    while let Some(r) = rows.next()? {
        for i in 0..11 {
            match r.get_ref(i)? {
                rusqlite::types::ValueRef::Null => h.update([0]),
                rusqlite::types::ValueRef::Integer(n) => {
                    h.update([1]);
                    h.update(n.to_be_bytes())
                }
                rusqlite::types::ValueRef::Text(s) => {
                    h.update([2]);
                    h.update((s.len() as u64).to_be_bytes());
                    h.update(s)
                }
                _ => return Err(LedgerError::CorruptLedger),
            }
        }
    }
    Ok(format!("{:x}", h.finalize()))
}
/// Fingerprint durable maintenance/domain controls, not the ordinary stream of
/// newly published identities. An unrelated request must not expire a preview.
pub(crate) fn adoption_control_digest(c: &Connection) -> Result<String> {
    let mut h = Sha256::new();
    h.update(rule_digest(c)?.as_bytes());
    for sql in [
        "SELECT root_id,identity_family,namespace_key,report_source,native_tool,identity_version,parser_version,route,enabled FROM source_roots ORDER BY root_id",
        "SELECT identity_family,namespace_key,identity_key,native_tool,model_key,start_ms,end_ms,rule_version FROM durable_coverage ORDER BY identity_family,namespace_key,identity_key",
        "SELECT identity_family,namespace_key,session_key,channel FROM durable_channel_choices ORDER BY identity_family,namespace_key,session_key",
        "SELECT consumer_tool,start_ms,end_ms,native_timezone,identity_family,namespace_key FROM adoption_forget_ranges ORDER BY consumer_tool,start_ms,end_ms,native_timezone,identity_family,namespace_key",
    ] {
        h.update(sql.as_bytes());
        let mut st = c.prepare(sql)?;
        let columns = st.column_count();
        let mut rows = st.query([])?;
        while let Some(row) = rows.next()? {
            h.update([3]);
            for i in 0..columns {
                match row.get_ref(i)? {
                    rusqlite::types::ValueRef::Null => h.update([0]),
                    rusqlite::types::ValueRef::Integer(n) => { h.update([1]); h.update(n.to_be_bytes()); }
                    rusqlite::types::ValueRef::Text(v) => { h.update([2]); h.update((v.len() as u64).to_be_bytes()); h.update(v); }
                    _ => return Err(LedgerError::CorruptLedger),
                }
            }
        }
    }
    Ok(format!("{:x}", h.finalize()))
}
fn merge_rules(from: &Connection, to: &Connection) -> Result<()> {
    let mut st=from.prepare("SELECT kind,report_source,identity_family,namespace_key,identity_key,session_key,link_version,link_key,native_tool,start_ms,end_ms,operation_id,created_ms FROM suppression_rules")?;
    let mut rows = st.query([])?;
    while let Some(r) = rows.next()? {
        let mut values = Vec::with_capacity(13);
        for i in 0..13 {
            values.push(r.get::<_, rusqlite::types::Value>(i)?);
        }
        to.execute("INSERT OR IGNORE INTO suppression_rules(kind,report_source,identity_family,namespace_key,identity_key,session_key,link_version,link_key,native_tool,start_ms,end_ms,operation_id,created_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",rusqlite::params_from_iter(values))?;
    }
    Ok(())
}
/// Controls survive rollback independently of their first observation. A live
/// adopted day, however, cannot be copied as an owner without its chosen value:
/// that would claim protection while silently losing the contribution.
fn merge_coverage_controls(from: &Connection, to: &Connection) -> Result<()> {
    let mut st = from.prepare("SELECT identity_family,namespace_key,identity_key,native_tool,model_key,start_ms,end_ms,first_observation_id,rule_version,created_ms FROM durable_coverage")?;
    let mut rows = st.query([])?;
    while let Some(r) = rows.next()? {
        let family: String = r.get(0)?;
        let namespace: String = r.get(1)?;
        let key: String = r.get(2)?;
        let hash: Option<Vec<u8>> = from.query_row(
            "SELECT o.normalized_hash FROM usage_events e JOIN usage_observations o ON o.observation_id=e.selected_observation_id WHERE e.identity_family=?1 AND e.namespace_key=?2 AND e.identity_key=?3 AND e.state='active'",
            params![family,namespace,key], |r|r.get(0),
        ).optional()?;
        if let Some(hash) = hash {
            let present: bool = to.query_row(
                "SELECT EXISTS(SELECT 1 FROM usage_events e JOIN usage_observations o ON o.observation_id=e.selected_observation_id WHERE e.identity_family=?1 AND e.namespace_key=?2 AND e.identity_key=?3 AND e.state='active' AND o.normalized_hash=?4)",
                params![family,namespace,key,hash], |r|r.get(0),
            )?;
            if !present {
                return Err(LedgerError::InvalidInput("backup lacks the selected evidence for a current daily coverage; revise or withdraw that range before restoring".into()));
            }
        }
        let conflict: bool = to.query_row(
            "SELECT EXISTS(SELECT 1 FROM durable_coverage WHERE identity_family=?1 AND namespace_key=?2 AND identity_key=?3 AND (native_tool<>?4 OR model_key<>?5 OR start_ms<>?6 OR end_ms<>?7))",
            params![family,namespace,key,r.get::<_,String>(3)?,r.get::<_,String>(4)?,r.get::<_,i64>(5)?,r.get::<_,i64>(6)?], |r|r.get(0),
        )?;
        if conflict {
            return Err(LedgerError::IdentityMappingRequired);
        }
        let values = (0..10)
            .map(|i| r.get::<_, rusqlite::types::Value>(i))
            .collect::<rusqlite::Result<Vec<_>>>()?;
        to.execute(
            "INSERT OR IGNORE INTO durable_coverage VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
            rusqlite::params_from_iter(values),
        )?;
    }
    drop(rows);
    drop(st);
    let mut st = from.prepare("SELECT identity_family,namespace_key,session_key,channel,first_observation_id,created_ms FROM durable_channel_choices")?;
    let mut rows = st.query([])?;
    while let Some(r) = rows.next()? {
        let conflict: bool = to.query_row(
            "SELECT EXISTS(SELECT 1 FROM durable_channel_choices WHERE identity_family=?1 AND namespace_key=?2 AND session_key=?3 AND channel<>?4)",
            params![r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,String>(3)?], |r|r.get(0),
        )?;
        if conflict {
            return Err(LedgerError::IdentityMappingRequired);
        }
        let values = (0..6)
            .map(|i| r.get::<_, rusqlite::types::Value>(i))
            .collect::<rusqlite::Result<Vec<_>>>()?;
        to.execute(
            "INSERT OR IGNORE INTO durable_channel_choices VALUES(?1,?2,?3,?4,?5,?6)",
            rusqlite::params_from_iter(values),
        )?;
    }
    Ok(())
}
fn merge_routes(from: &Connection, to: &Connection) -> Result<()> {
    let mut st=from.prepare("SELECT root_id,identity_family,namespace_key,collector_key,report_source,native_tool,locator,identity_version,parser_version FROM source_roots WHERE route='ledger'")?;
    let mut rows = st.query([])?;
    while let Some(r) = rows.next()? {
        let source: String = r.get(4)?;
        let id: String = r.get(0)?;
        let conflict:bool=to.query_row("SELECT EXISTS(SELECT 1 FROM source_roots WHERE root_id=?1 AND (identity_family<>?2 OR namespace_key<>?3 OR identity_version<>?4))",params![id,r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,i64>(7)?],|row|row.get(0))?;
        if conflict {
            return Err(LedgerError::IdentityMappingRequired);
        }
        let mut values = Vec::with_capacity(9);
        for i in 0..9 {
            values.push(r.get::<_, rusqlite::types::Value>(i)?);
        }
        to.execute("INSERT OR IGNORE INTO source_roots(root_id,identity_family,namespace_key,collector_key,report_source,native_tool,locator,identity_version,parser_version,enabled,route)VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,0,'ledger')",rusqlite::params_from_iter(values))?;
        to.execute(
            "UPDATE source_roots SET route='ledger' WHERE report_source=?1",
            [source],
        )?;
    }
    Ok(())
}
#[derive(Clone, Debug, Serialize)]
pub struct ReplacementImpact {
    pub current_candidates_absent_from_backup: String,
    pub current_events_absent_or_changed_in_backup: String,
    pub current_first_ms: Option<String>,
    pub current_last_ms: Option<String>,
    pub backup_first_ms: Option<String>,
    pub backup_last_ms: Option<String>,
    pub affected_sources: Vec<String>,
    pub current_capture_entries_absent_or_changed_in_backup: String,
    pub current_projection_rows_absent_or_changed_in_backup: String,
    pub current_model_rows_absent_or_changed_in_backup: String,
}
fn replacement_impact(current: &Connection, backup: &Connection) -> Result<ReplacementImpact> {
    backup.execute_batch("CREATE TEMP TABLE restore_current_candidates(family TEXT,namespace TEXT,candidate TEXT,hash BLOB,source TEXT); CREATE TEMP TABLE restore_current_events(family TEXT,namespace TEXT,identity TEXT,state TEXT,hash BLOB,source TEXT);")?;
    let mut st=current.prepare("SELECT identity_family,namespace_key,candidate_key,normalized_hash,report_source FROM usage_observations")?;
    let mut rows = st.query([])?;
    while let Some(r) = rows.next()? {
        backup.execute(
            "INSERT INTO restore_current_candidates VALUES(?1,?2,?3,?4,?5)",
            params![
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, Vec<u8>>(3)?,
                r.get::<_, String>(4)?
            ],
        )?;
    }
    drop(rows);
    drop(st);
    let mut st=current.prepare("SELECT e.identity_family,e.namespace_key,e.identity_key,e.state,o.normalized_hash,e.report_source FROM usage_events e LEFT JOIN usage_observations o ON o.observation_id=e.selected_observation_id")?;
    let mut rows = st.query([])?;
    while let Some(r) = rows.next()? {
        backup.execute(
            "INSERT INTO restore_current_events VALUES(?1,?2,?3,?4,?5,?6)",
            params![
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, Option<Vec<u8>>>(4)?,
                r.get::<_, Option<String>>(5)?
            ],
        )?;
    }
    let missing="NOT EXISTS(SELECT 1 FROM usage_observations o WHERE o.identity_family=c.family AND o.namespace_key=c.namespace AND o.candidate_key=c.candidate AND o.normalized_hash=c.hash)";
    let missing_candidates: i64 = backup.query_row(
        &format!("SELECT count(*) FROM restore_current_candidates c WHERE {missing}"),
        [],
        |r| r.get(0),
    )?;
    let missing_events:i64=backup.query_row("SELECT count(*) FROM restore_current_events c WHERE NOT EXISTS(SELECT 1 FROM usage_events e LEFT JOIN usage_observations o ON o.observation_id=e.selected_observation_id WHERE e.identity_family=c.family AND e.namespace_key=c.namespace AND e.identity_key=c.identity AND e.state=c.state AND o.normalized_hash IS c.hash)",[],|r|r.get(0))?;
    let extent = |c: &Connection| -> Result<(Option<String>, Option<String>)> {
        let (a, b): (Option<i64>, Option<i64>) = c.query_row(
            "SELECT min(occurred_ms),max(occurred_ms) FROM usage_events WHERE state='active'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        Ok((a.map(|n| n.to_string()), b.map(|n| n.to_string())))
    };
    let (a, b) = extent(current)?;
    let (c, d) = extent(backup)?;
    let mut st = backup.prepare(&format!(
        "SELECT DISTINCT source FROM restore_current_candidates c WHERE {missing} ORDER BY source"
    ))?;
    let mut sources = st
        .query_map([], |r| r.get(0))?
        .collect::<rusqlite::Result<Vec<String>>>()?;
    let (capture, projection, models, saved_sources) = saved_history_impact(current, backup)?;
    sources.extend(saved_sources);
    sources.sort();
    sources.dedup();
    Ok(ReplacementImpact {
        current_candidates_absent_from_backup: missing_candidates.to_string(),
        current_events_absent_or_changed_in_backup: missing_events.to_string(),
        current_first_ms: a,
        current_last_ms: b,
        backup_first_ms: c,
        backup_last_ms: d,
        affected_sources: sources,
        current_capture_entries_absent_or_changed_in_backup: capture.to_string(),
        current_projection_rows_absent_or_changed_in_backup: projection.to_string(),
        current_model_rows_absent_or_changed_in_backup: models.to_string(),
    })
}

// Controls have their own content identity: equal ledger epochs do not imply
// equal suppression rules or projection tombstones.
const RESTORE_RULE_COLUMNS: &str = "kind,report_source,identity_family,namespace_key,identity_key,session_key,link_version,link_key,native_tool,start_ms,end_ms";
fn remember_restore_controls(current: &Connection, c: &Connection) -> Result<()> {
    c.execute_batch(&format!(
        "CREATE TEMP TABLE restore_prior_rules AS SELECT {RESTORE_RULE_COLUMNS} FROM suppression_rules;
         CREATE TEMP TABLE restore_prior_tombstones AS SELECT * FROM source_projection_tombstones;
         CREATE TEMP TABLE restore_prior_forget_ranges AS SELECT * FROM adoption_forget_ranges;
         CREATE TEMP TABLE restore_control_roots AS SELECT root_id,report_source,identity_family,namespace_key FROM source_roots;
         CREATE TEMP TABLE restore_saved_capture_entries AS SELECT * FROM legacy_capture_entries;
         CREATE TEMP TABLE restore_saved_capture_state AS SELECT * FROM legacy_capture_state;"
    ))?;
    let mut statement = current
        .prepare("SELECT root_id,report_source,identity_family,namespace_key FROM source_roots")?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        c.execute(
            "INSERT INTO restore_control_roots VALUES(?1,?2,?3,?4)",
            params![
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?
            ],
        )?;
    }
    Ok(())
}
fn new_restore_rule() -> String {
    format!(
        "NOT EXISTS(SELECT 1 FROM restore_prior_rules old WHERE {})",
        RESTORE_RULE_COLUMNS
            .split(',')
            .map(|name| format!("old.{name} IS s.{name}"))
            .collect::<Vec<_>>()
            .join(" AND ")
    )
}
fn clear_query_projection(c: &Connection) -> Result<()> {
    super::time_repair::clear_staging(c)?;
    c.execute_batch(
        "DELETE FROM query_live_facts; DELETE FROM query_live_state;
        DELETE FROM legacy_capture_entries; DELETE FROM legacy_capture_state;",
    )?;
    Ok(())
}
/// Keep historical inputs unchanged whenever their privacy can be proved. Old
/// compatibility rows do not carry every durable identity / SQLite row mapping;
/// an ambiguous new control must refuse replacement, never silently erase an
/// unrelated source or guess that two records are the same.
fn prepare_restored_history(
    c: &Connection,
    snapshot: Option<super::legacy::LegacySnapshot>,
) -> Result<Option<super::legacy::LegacySnapshot>> {
    use super::legacy::LegacyEntry;
    let mut snapshot = snapshot;
    if let Some(saved) = &mut snapshot {
        let policy =
            super::legacy::CapturePrivacy::load(c).map_err(|_| LedgerError::CorruptLedger)?;
        let ordinals = c
            .prepare(
                "SELECT ordinal,report_source FROM restore_saved_capture_entries ORDER BY ordinal",
            )?
            .query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if ordinals.len() != saved.entries.len() {
            return Err(LedgerError::CorruptLedger);
        }
        for (entry, (ordinal, source)) in saved.entries.iter().zip(&ordinals) {
            if entry.report_source() != source {
                return Err(LedgerError::CorruptLedger);
            }
            if policy
                .suppresses(c, entry)
                .map_err(|e| LedgerError::InvalidInput(e.message))?
            {
                c.execute(
                    "DELETE FROM restore_saved_capture_entries WHERE ordinal=?1",
                    [ordinal],
                )?;
            }
        }
        saved
            .filter_suppressed(c)
            .map_err(|e| LedgerError::InvalidInput(e.message))?;
        let mut retained = Vec::with_capacity(saved.entries.len());
        let mut checked = std::collections::BTreeSet::new();
        for entry in saved.entries.drain(..) {
            let source = entry.report_source();
            let native = match &entry {
                LegacyEntry::Request { native_tool, .. } => native_tool.as_str(),
                LegacyEntry::SessionTotal { record, .. } => record.source,
                LegacyEntry::Day(day) => day.native_tool.as_str(),
            };
            if checked.insert((source.to_owned(), native.to_owned())) {
                reject_ambiguous_restore_controls(c, source, native)?;
            }
            retained.push(entry);
        }
        saved.entries = retained;
        saved.generation = new_epoch();
        // Routing controls can have been merged from the current database.
        saved.routing_hash =
            super::query::QueryService::routing_hash(c).map_err(|_| LedgerError::CorruptLedger)?;
    }
    // Saved projections are historical inputs too, even without a published
    // compatibility capture. Their own tombstone matcher has already run.
    let sources = c.prepare("SELECT DISTINCT r.report_source,r.native_tool FROM source_projection_rows p JOIN source_roots r USING(root_id)")?
        .query_map([], |r| Ok((r.get::<_,String>(0)?, r.get::<_,String>(1)?)))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for (source, native) in sources {
        // Projection tombstones have exact row keys and were applied already;
        // identity controls still lack that map for some legacy schemas.
        reject_ambiguous_restore_rules(c, &source, &native)?;
    }
    // Whole-source erasure also removes model attribution retained separately.
    let forget_minimax: bool = c.query_row("SELECT EXISTS(SELECT 1 FROM suppression_rules WHERE kind='source' AND report_source='minimax' AND (native_tool IS NULL OR native_tool='minimax'))", [], |r|r.get(0))?;
    if forget_minimax {
        c.execute("DELETE FROM minimax_model_files", [])?;
    }
    Ok(snapshot)
}
/// Keep source input bytes, including decimal monetary literals, unchanged.
/// Re-serializing an f64 decoded with a different JSON path can round by one ULP;
/// a maintenance round trip must not rewrite historical amounts or their hashes.
fn persist_restored_capture(c: &Connection, saved: &super::legacy::LegacySnapshot) -> Result<()> {
    use std::collections::BTreeMap;
    super::legacy::CapturePrivacy::load(c)
        .and_then(|p| p.validate(c, saved))
        .map_err(|e| LedgerError::InvalidInput(e.message))?;
    let (uuid, epoch): (String, String) =
        c.query_row("SELECT ledger_uuid,epoch FROM ledger_meta", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })?;
    let raw: String = c.query_row(
        "SELECT metadata_json FROM restore_saved_capture_state WHERE singleton=1",
        [],
        |r| r.get(0),
    )?;
    let mut metadata: serde_json::Value =
        serde_json::from_str(&raw).map_err(|_| LedgerError::CorruptLedger)?;
    let new_hash = || {
        let mut hash = Sha256::new();
        hash.update(b"usagebook/legacy-source-content/v1");
        hash
    };
    let mut hashes: BTreeMap<String, Sha256> = saved
        .sources
        .keys()
        .map(|source| (source.clone(), new_hash()))
        .collect();
    let mut statement = c.prepare(
        "SELECT report_source,entry_json FROM restore_saved_capture_entries ORDER BY ordinal",
    )?;
    let mut rows = statement.query([])?;
    let mut count = 0usize;
    while let Some(row) = rows.next()? {
        let source: String = row.get(0)?;
        let encoded: String = row.get(1)?;
        let hash = hashes.entry(source).or_insert_with(new_hash);
        hash.update((encoded.len() as u64).to_be_bytes());
        hash.update(encoded.as_bytes());
        count += 1;
    }
    if count != saved.entries.len() {
        return Err(LedgerError::CorruptLedger);
    }
    let hashes: BTreeMap<_, _> = hashes
        .into_iter()
        .map(|(source, hash)| (source, format!("{:x}", hash.finalize())))
        .collect();
    metadata["generation"] = serde_json::json!(saved.generation);
    metadata["routing_hash"] = serde_json::json!(saved.routing_hash);
    metadata["sources"] =
        serde_json::to_value(&saved.sources).map_err(|_| LedgerError::CorruptLedger)?;
    metadata["entries"] = serde_json::json!([]);
    metadata["entry_count"] = serde_json::json!(count);
    metadata["source_content_hash_version"] = serde_json::json!(1);
    metadata["source_content_hashes"] = serde_json::json!(hashes);
    c.execute(
        "INSERT INTO legacy_capture_entries SELECT * FROM restore_saved_capture_entries",
        [],
    )?;
    c.execute(
        "INSERT INTO legacy_capture_state VALUES(1,?1,?2,?3,?4)",
        params![uuid, epoch, saved.generation, metadata.to_string()],
    )?;
    Ok(())
}

fn reject_ambiguous_restore_controls(c: &Connection, source: &str, native: &str) -> Result<()> {
    let new_tombstone: bool = c.query_row(
        "SELECT EXISTS(SELECT 1 FROM source_projection_tombstones s
         WHERE (EXISTS(SELECT 1 FROM restore_control_roots r WHERE r.root_id=s.root_id AND r.report_source=?1)
          OR NOT EXISTS(SELECT 1 FROM restore_control_roots r WHERE r.root_id=s.root_id)) AND NOT EXISTS(SELECT 1 FROM restore_prior_tombstones old
          WHERE old.root_id=s.root_id AND old.table_name=s.table_name AND old.row_key_hash=s.row_key_hash))",
        [source], |r| r.get(0))?;
    if new_tombstone {
        return Err(ambiguous_restore(source));
    }
    reject_ambiguous_restore_rules(c, source, native)
}
fn reject_ambiguous_restore_rules(c: &Connection, source: &str, native: &str) -> Result<()> {
    let ambiguous: bool = c.query_row(&format!(
        "SELECT EXISTS(SELECT 1 FROM suppression_rules s WHERE {} AND s.kind NOT IN('source','window') AND
         ((s.identity_family IS NOT NULL AND (s.identity_family IN(?1,?2) OR NOT EXISTS(SELECT 1 FROM restore_control_roots r WHERE r.identity_family=s.identity_family AND r.namespace_key=s.namespace_key) OR EXISTS(SELECT 1 FROM restore_control_roots r WHERE r.report_source=?1 AND r.identity_family=s.identity_family AND r.namespace_key=s.namespace_key))) OR s.native_tool=?2)) OR
         EXISTS(SELECT 1 FROM adoption_forget_ranges f WHERE f.consumer_tool=?2 AND f.identity_family IS NOT NULL
           AND NOT EXISTS(SELECT 1 FROM restore_prior_forget_ranges old WHERE old.consumer_tool=f.consumer_tool AND old.start_ms=f.start_ms AND old.end_ms=f.end_ms AND old.native_timezone IS f.native_timezone AND old.identity_family IS f.identity_family AND old.namespace_key IS f.namespace_key))", new_restore_rule()),
        params![source, native], |r| r.get(0))?;
    if ambiguous {
        Err(ambiguous_restore(source))
    } else {
        Ok(())
    }
}
fn ambiguous_restore(source: &str) -> LedgerError {
    LedgerError::InvalidInput(format!("cannot safely restore saved {source} history: new privacy controls lack an exact compatibility mapping; current ledger and backup are unchanged"))
}
/// Compare input content, not row numbers or generations. Multiplicity matters
/// for compatibility captures; repeated equal rows must not conceal a loss.
fn saved_history_impact(
    current: &Connection,
    backup: &Connection,
) -> Result<(u64, u64, u64, Vec<String>)> {
    use std::collections::{BTreeMap, BTreeSet};
    let queries = [
        "SELECT report_source,entry_json FROM legacy_capture_entries",
        "SELECT s.report_source,json_array(p.root_id,p.table_name,p.row_key,p.projection_json) FROM source_projection_rows p JOIN source_projection_scopes s USING(root_id)",
        "SELECT 'minimax',json_array(f.path,f.root_path,f.session_id,r.line_offset,r.turn_id,r.timestamp_ms,r.model) FROM minimax_model_rows r JOIN minimax_model_files f USING(path)",
    ];
    let mut counts = [0u64; 3];
    let mut affected = BTreeSet::new();
    for (i, sql) in queries.iter().enumerate() {
        let mut remaining = BTreeMap::<(String, [u8; 32]), u64>::new();
        let mut st = backup.prepare(sql)?;
        let mut rows = st.query([])?;
        while let Some(row) = rows.next()? {
            let encoded: String = row.get(1)?;
            *remaining
                .entry((row.get(0)?, Sha256::digest(encoded.as_bytes()).into()))
                .or_default() += 1;
        }
        let mut st = current.prepare(sql)?;
        let mut rows = st.query([])?;
        while let Some(row) = rows.next()? {
            let encoded: String = row.get(1)?;
            let key: (String, [u8; 32]) = (row.get(0)?, Sha256::digest(encoded.as_bytes()).into());
            let amount = remaining.entry(key.clone()).or_default();
            if *amount > 0 {
                *amount -= 1;
            } else {
                counts[i] += 1;
                affected.insert(key.0);
            }
        }
    }
    Ok((
        counts[0],
        counts[1],
        counts[2],
        affected.into_iter().collect(),
    ))
}

/// Forget invalidates the affected compatibility sources; unrelated saved
/// views and source checkpoints must survive even if their inputs are gone.
fn clear_forgotten_read_caches(
    c: &Connection,
    scope: &ForgetScope,
) -> Result<Option<super::legacy::LegacySnapshot>> {
    use std::collections::BTreeSet;
    let previous = super::legacy::LegacySnapshot::restore_capture(c)
        .map_err(|_| LedgerError::CorruptLedger)?;
    let mut affected: BTreeSet<String> = c.prepare("SELECT DISTINCT report_source FROM usage_observations WHERE observation_id IN (SELECT id FROM delete_ids)")?
        .query_map([], |r| r.get(0))?.collect::<rusqlite::Result<_>>()?;
    if let Some(source) = &scope.source {
        affected.insert(source.clone());
    }
    if scope.all {
        super::time_repair::clear_staging(c)?;
    } else {
        for source in &affected {
            super::time_repair::clear_source_staging(c, source)?;
        }
    }
    if let (Some(snapshot), Some((start, end))) = (&previous, scope.start_ms.zip(scope.end_ms)) {
        for entry in &snapshot.entries {
            if !scope.all && !affected.contains(entry.report_source()) {
                continue;
            }
            if let super::legacy::LegacyEntry::Day(day) = entry {
                if day
                    .start_ms
                    .zip(day.end_ms)
                    .is_some_and(|(a, b)| a < end && b > start && (start > a || end < b))
                {
                    return Err(LedgerError::InvalidInput(
                        "saved daily summaries are indivisible; select the complete native day"
                            .into(),
                    ));
                }
            }
        }
    }
    if scope.identity.is_some()
        && previous.as_ref().is_some_and(|s| {
            s.entries
                .iter()
                .any(|row| affected.contains(row.report_source()))
        })
    {
        for source in &affected {
            let ledger_route: bool = c.query_row("SELECT EXISTS(SELECT 1 FROM source_roots WHERE report_source=?1 AND route='ledger')", [source], |r|r.get(0))?;
            if !ledger_route {
                return Err(LedgerError::InvalidInput("single-identity deletion cannot safely partition this source's compatibility view; source and date scopes are supported".into()));
            }
        }
    }
    if scope.all && scope.start_ms.is_none() {
        clear_read_caches(c)?;
        return Ok(None);
    }
    for source in &affected {
        if scope.start_ms.is_none() && scope.identity.is_none() {
            c.execute(
                "DELETE FROM source_projection_scopes WHERE report_source=?1",
                [source],
            )?;
        }
        if source == "minimax" && scope.start_ms.is_none() && scope.identity.is_none() {
            c.execute("DELETE FROM minimax_model_files", [])?;
        }
    }
    c.execute("DELETE FROM query_live_facts", [])?;
    c.execute("DELETE FROM query_live_state", [])?;
    c.execute("DELETE FROM legacy_capture_entries", [])?;
    c.execute("DELETE FROM legacy_capture_state", [])?;
    let Some(previous) = previous else {
        return Ok(None);
    };
    let mut retained =
        super::legacy::LegacySnapshot::new(new_epoch(), previous.routing_hash.clone());
    for (source, status) in &previous.sources {
        if !affected.contains(source) || scope.start_ms.is_some() {
            retained.sources.insert(source.clone(), status.clone());
            retained.retain_input_containers(source, &previous);
        }
    }
    retained.entries = previous
        .entries
        .into_iter()
        .filter(|entry| {
            if !scope.all && !affected.contains(entry.report_source()) {
                return true;
            }
            let Some((start, end)) = scope.start_ms.zip(scope.end_ms) else {
                return false;
            };
            // A dated forget cannot select unknown-time rows. Native daily
            // intervals remain indivisible, just as the durable observations.
            use super::legacy::LegacyEntry;
            match entry {
                LegacyEntry::Request { record, .. } | LegacyEntry::SessionTotal { record, .. } => {
                    record.ts_ms <= 0 || record.ts_ms < start || record.ts_ms >= end
                }
                LegacyEntry::Day(day) => day
                    .start_ms
                    .zip(day.end_ms)
                    .is_none_or(|(a, b)| a >= end || b <= start),
            }
        })
        .collect();
    Ok(Some(retained))
}

/// A restore replaces history, so all derived views must be rebuilt against it.
fn clear_read_caches(c: &Connection) -> Result<()> {
    super::time_repair::clear_staging(c)?;
    for name in [
        "query_live_facts",
        "query_live_state",
        "legacy_capture_entries",
        "legacy_capture_state",
        "source_projection_rows",
        "source_projection_scopes",
        "minimax_model_rows",
        "minimax_model_files",
    ] {
        let exists: bool = c.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?1)",
            [name],
            |r| r.get(0),
        )?;
        if exists {
            c.execute(&format!("DELETE FROM {name}"), [])?;
        }
    }
    Ok(())
}

/// Pausing collection changes controls, not saved history. Keep cache provenance
/// in the same maintenance transaction; deletion/restoration never use this path.
fn change_epoch_preserving_read_caches(c: &Connection) -> Result<String> {
    let previous: String = c.query_row("SELECT epoch FROM ledger_meta", [], |r| r.get(0))?;
    let next = db::change_epoch(c)?;
    for name in [
        "query_live_state",
        "legacy_capture_state",
        "source_projection_scopes",
        "minimax_model_files",
    ] {
        let exists: bool = c.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?1)",
            [name],
            |r| r.get(0),
        )?;
        if exists {
            c.execute(
                &format!("UPDATE {name} SET epoch=?1 WHERE epoch=?2"),
                params![next, previous],
            )?;
        }
    }
    Ok(next)
}
