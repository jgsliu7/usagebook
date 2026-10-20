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
fn sync_dir(p: &Path) -> Result<()> {
    #[cfg(unix)]
    std::fs::File::open(p.parent().unwrap_or(Path::new(".")))?.sync_all()?;
    Ok(())
}
fn check_conn(c: &Connection) -> Result<()> {
    db::validate(c)?;
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
    check_selected_events(c)?;
    check_daily_rollups(c)
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
            OR o.eligibility<>'eligible'
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
fn copy_database(c: &Connection, dest: &Path) -> Result<()> {
    if dest.exists() {
        return Err(LedgerError::InvalidInput("output already exists".into()));
    }
    let f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(dest)?;
    drop(f);
    db::private_file(dest)?;
    let mut target = Connection::open(dest)?;
    {
        let backup = rusqlite::backup::Backup::new(c, &mut target)?;
        backup.run_to_completion(256, std::time::Duration::from_millis(1), None)?;
    }
    target.execute_batch(
        "PRAGMA foreign_keys=ON; PRAGMA temp_store=FILE; PRAGMA cache_size=-8192;",
    )?;
    check_conn(&target)?;
    checkpoint(&target)?;
    drop(target);
    clean_sidecars(dest)?;
    sync_file(dest)?;
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
        let c = db::open_connection(&self.path, false)?;
        check_conn(&c)
    }
    pub fn backup(&self, output: &Path) -> Result<()> {
        if output.exists() {
            return Err(LedgerError::InvalidInput("output already exists".into()));
        }
        let c = db::open_connection(&self.path, true)?;
        check_conn(&c)?;
        let temp = output.with_extension(format!("{}.backup.tmp", new_epoch()));
        copy_database(&c, &temp)?;
        drop(c);
        if output.exists() {
            return Err(LedgerError::InvalidInput("output already exists".into()));
        }
        std::fs::hard_link(&temp, output)?;
        std::fs::remove_file(&temp)?;
        sync_dir(output)?;
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
        db::change_epoch(&c)?;
        regression_stop("pause_before_commit");
        c.execute_batch("COMMIT")?;
        regression_stop("pause_after_commit");
        Ok(())
    }
    pub fn forget(&self, scope: &ForgetScope, apply: Option<&str>) -> Result<Preview> {
        validate_scope(scope)?;
        let c = db::open_connection(&self.path, true)?;
        c.execute_batch("BEGIN IMMEDIATE")?;
        validate_protected(&c, scope)?;
        c.execute_batch("CREATE TEMP TABLE delete_ids(id INTEGER PRIMARY KEY); CREATE TEMP TABLE affected_units(family TEXT,namespace TEXT,publish_key TEXT,visible INTEGER,PRIMARY KEY(family,namespace,publish_key));")?;
        select_scope(&c, scope)?;
        let operation =
            serde_json::to_string(scope).map_err(|_| LedgerError::InvalidInput("scope".into()))?;
        let op = hash_parts("forget", &[&operation]);
        add_explicit_rules(&c, scope, &op)?;
        expand_rules(&c, &op, false)?;
        let preview = preview(&c, "forget", &operation)?;
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
            db::change_epoch(&c)?;
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
        let result=Preview{operation:"unsuppress".into(),preview_token:token.clone(),affected_candidates:"0".into(),affected_events:"0".into(),replacement:None,released_rule:Some(rule),warnings:vec![
            "Only this rule is removed; other identity, session and coverage rules remain. A later refresh may import source records again.".into(),
            "Immediate candidate/event changes are zero. The number of records that may be reimported is unknown; source files are not scanned for this preview.".into()
        ]};
        if let Some(given) = apply {
            if given != token {
                return Err(LedgerError::InvalidInput("preview expired".into()));
            }
            c.execute("DELETE FROM suppression_rules WHERE rule_id=?1", [id])?;
            db::change_epoch(&c)?;
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
fn validate_protected(c: &Connection, s: &ForgetScope) -> Result<()> {
    if s.all {
        return Err(LedgerError::SourceNotProtected);
    } // Other registered adapters remain live in the first release.
    if let Some(source) = &s.source {
        let(routes,total):(i64,i64)=c.query_row("SELECT COALESCE(sum(route='ledger'),0),count(*) FROM source_roots WHERE report_source=?1",[source],|r|Ok((r.get(0)?,r.get(1)?)))?;
        if routes == 0 || routes != total {
            return Err(LedgerError::SourceNotProtected);
        }
    }
    if let Some(key) = &s.identity {
        let count:i64=c.query_row("SELECT count(*) FROM usage_observations o JOIN source_roots r ON r.identity_family=o.identity_family AND r.namespace_key=o.namespace_key WHERE o.identity_key=?1 AND r.route='ledger'",[key],|r|r.get(0))?;
        if count == 0 {
            return Err(LedgerError::InvalidInput(
                "identity is not in a protected source".into(),
            ));
        }
    }
    Ok(())
}
fn select_scope(c: &Connection, s: &ForgetScope) -> Result<()> {
    c.execute("INSERT INTO delete_ids SELECT observation_id FROM usage_observations WHERE (?1 IS NULL OR report_source=?1) AND (?2 IS NULL OR identity_key=?2) AND (?3 IS NULL OR (occurred_ms>=?3 AND occurred_ms<?4) OR (day_start_ms<?4 AND day_end_ms>?3))",params![s.source,s.identity,s.start_ms,s.end_ms])?;
    Ok(())
}
fn add_explicit_rules(c: &Connection, s: &ForgetScope, op: &str) -> Result<()> {
    if s.identity.is_none() && s.start_ms.is_some() {
        let (a, b) = (s.start_ms.unwrap(), s.end_ms.unwrap());
        c.execute("INSERT INTO suppression_rules(kind,report_source,native_tool,start_ms,end_ms,operation_id,created_ms)VALUES('window',?1,?1,?2,?3,?4,?5)",params![s.source,a,b,op,now_ms()])?;
    } else if let Some(source) = &s.source {
        c.execute("INSERT OR IGNORE INTO suppression_rules(kind,report_source,native_tool,operation_id,created_ms)VALUES('source',?1,?1,?2,?3)",params![source,op,now_ms()])?;
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
        } else {
            c.execute("INSERT OR IGNORE INTO suppression_rules(kind,native_tool,namespace_key,operation_id,created_ms)VALUES('coverage_family',?1,?2,?3,?4)",params![o.native_tool,o.namespace_key,op,now_ms()])?;
        }
    }
    Ok(())
}
// SQL exact rules are deliberately independent of statistical aliases.
const EXACT_RULES:&str="EXISTS(SELECT 1 FROM suppression_rules s WHERE (s.kind='source' AND s.report_source=o.report_source AND s.native_tool=o.native_tool) OR (s.kind='identity' AND s.identity_family=o.identity_family AND s.namespace_key=o.namespace_key AND s.identity_key=o.identity_key) OR (s.kind='session' AND o.fact_kind IN('session_total','session_cost') AND s.identity_family=o.identity_family AND s.namespace_key=o.namespace_key AND s.session_key=o.session_key) OR (s.kind='window' AND (s.report_source IS NULL OR (s.report_source=o.report_source AND s.native_tool=o.native_tool)) AND ((o.occurred_ms>=s.start_ms AND o.occurred_ms<s.end_ms) OR (o.day_start_ms<s.end_ms AND o.day_end_ms>s.start_ms))) OR (o.fact_kind='day' AND s.native_tool=o.native_tool AND s.namespace_key=o.namespace_key AND (s.kind='coverage_family' OR (s.kind='coverage_interval' AND o.day_start_ms<s.end_ms AND o.day_end_ms>s.start_ms))))";
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
    let affected:i64=c.query_row("SELECT count(*) FROM usage_events e WHERE selected_observation_id IN(SELECT id FROM delete_ids) OR EXISTS(SELECT 1 FROM usage_observations o JOIN delete_ids d ON d.id=o.observation_id LEFT JOIN identity_alias a ON a.identity_family=o.identity_family AND a.namespace_key=o.namespace_key AND a.alias_key=o.identity_key WHERE o.identity_family=e.identity_family AND o.namespace_key=e.namespace_key AND (o.identity_key=e.identity_key OR a.canonical_key=e.identity_key))",[],|r|r.get(0))?;
    let token = hash_parts(
        "maintenance-preview/v1",
        &[
            operation,
            &stamp.epoch,
            &stamp.commit_seq,
            &format!("{:x}", hash.finalize()),
        ],
    );
    Ok(Preview{operation:operation.into(),preview_token:token,affected_candidates:count.to_string(),affected_events:affected.to_string(),replacement:None,released_rule:None,warnings:vec!["All revisions of matching identities and related session totals/costs are removed. Source files and independent backups remain.".into()]})
}
fn delete_selected(c: &Connection) -> Result<()> {
    // Persist each unit's pre-maintenance publication horizon in bounded disk TEMP.
    c.execute_batch("CREATE TEMP TABLE IF NOT EXISTS affected_units(family TEXT,namespace TEXT,publish_key TEXT,visible INTEGER,PRIMARY KEY(family,namespace,publish_key));")?;
    let mut st=c.prepare("SELECT DISTINCT o.identity_family,o.namespace_key,o.identity_key,o.native_message_id FROM usage_observations o JOIN delete_ids d ON d.id=o.observation_id")?;
    let mut rows = st.query([])?;
    while let Some(r) = rows.next()? {
        let family: String = r.get(0)?;
        let ns: String = r.get(1)?;
        let key: Option<String> = r.get(2)?;
        let message: Option<String> = r.get(3)?;
        let publish = if family == "claude" && message.is_some() {
            format!(
                "m:{}",
                message
                    .unwrap()
                    .as_bytes()
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>()
            )
        } else if let Some(key) = key {
            format!("k:{key}")
        } else {
            continue;
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
    c.execute("UPDATE source_objects SET checkpoint=NULL,checkpoint_format=NULL,saved_offset=NULL,state='replay_required',target_done=0,object_revision=object_revision+1 WHERE object_id IN(SELECT object_id FROM usage_observations WHERE observation_id IN(SELECT id FROM delete_ids))",[])?;
    c.execute(
        "DELETE FROM usage_observations WHERE observation_id IN(SELECT id FROM delete_ids)",
        [],
    )?;
    let mut st = c.prepare("SELECT family,namespace,publish_key,visible FROM affected_units")?;
    let mut rows = st.query([])?;
    while let Some(r) = rows.next()? {
        super::resolve::rebuild_unit(
            c,
            &r.get::<_, String>(0)?,
            &r.get::<_, String>(1)?,
            &r.get::<_, String>(2)?,
            r.get(3)?,
        )?;
    }
    drop(rows);
    drop(st);
    c.execute("DELETE FROM affected_units", [])?;
    Ok(())
}
pub(crate) fn suppress_incoming(c: &Connection, o: &Observation) -> Result<()> {
    add_identity_controls(c, o, "suppressed-revision")?;
    c.execute_batch("CREATE TEMP TABLE IF NOT EXISTS delete_ids(id INTEGER PRIMARY KEY); DELETE FROM delete_ids;")?;
    c.execute(&format!("INSERT INTO delete_ids SELECT observation_id FROM usage_observations o WHERE {EXACT_RULES}"),[])?;
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
        db::change_epoch(&c)?;
        regression_stop("timezone_before_commit");
        c.execute_batch("COMMIT")?;
        regression_stop("timezone_after_commit");
        checkpoint(&c)?;
        Ok(())
    }
    pub fn restore(&self, input: &Path, apply: Option<&str>) -> Result<Preview> {
        if std::fs::canonicalize(input)? == std::fs::canonicalize(&self.path)? {
            return Err(LedgerError::InvalidInput(
                "backup must differ from current ledger".into(),
            ));
        }
        // Never read a moving backup with an unmerged WAL as a standalone export.
        let input_wal = PathBuf::from(format!("{}-wal", input.display()));
        if input_wal.exists() && std::fs::metadata(input_wal)?.len() > 0 {
            return Err(LedgerError::InvalidInput("backup has an active WAL".into()));
        }
        let input_hash = hash_file(input)?;
        let current = db::open_connection(&self.path, true)?;
        check_conn(&current)?;
        let before = db::stamp(&current)?;
        let source = db::open_connection(input, false)?;
        check_conn(&source)?;
        let pending = self
            .path
            .with_extension(format!("{}.restore.pending.db", new_epoch()));
        copy_database(&source, &pending)?;
        drop(source);
        let result = (|| -> Result<Preview> {
            let c = db::open_connection(&pending, true)?;
            c.execute_batch(
                "BEGIN IMMEDIATE; CREATE TEMP TABLE delete_ids(id INTEGER PRIMARY KEY);",
            )?;
            merge_rules(&current, &c)?;
            merge_routes(&current, &c)?;
            c.execute(&format!("INSERT INTO delete_ids SELECT observation_id FROM usage_observations o WHERE {EXACT_RULES}"),[])?;
            expand_rules(&c, "restore-protection", true)?;
            let mut preview = preview(&c, "restore", &input_hash)?;
            preview.replacement = Some(replacement_impact(&current, &c)?);
            let rules = rule_digest(&current)?;
            preview.preview_token = hash_parts(
                "restore-preview/v1",
                &[
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
                delete_selected(&c)?;
                let epoch = db::change_epoch(&c)?;
                check_conn(&c)?;
                regression_stop("restore_before_commit");
                c.execute_batch("COMMIT")?;
                regression_stop("restore_after_commit");
                checkpoint(&c)?;
                drop(c);
                clean_sidecars(&pending)?;
                readonly_check(&pending)?;
                sync_file(&pending)?;
                sync_dir(&pending)?;
                checkpoint(&current)?;
                drop(current);
                clean_sidecars(&self.path)?;
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
                    new_hash: hash_file(&pending)?,
                    epoch,
                };
                let p = marker_path(&self.path);
                let mut f = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&p)?;
                db::private_file(&p)?;
                f.write_all(&serde_json::to_vec(&marker).map_err(|_| LedgerError::CorruptLedger)?)?;
                f.sync_all()?;
                drop(f);
                sync_dir(&p)?;
                regression_stop("install_prepared");
                std::fs::rename(&self.path, &recovery)?;
                sync_dir(&self.path)?;
                regression_stop("install_old_renamed");
                std::fs::rename(&pending, &self.path)?;
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
        if !marker_path(&self.path).exists() && pending.exists() {
            let _ = clean_sidecars(&pending);
            let _ = std::fs::remove_file(&pending);
        }
        result
    }
}
fn rule_digest(c: &Connection) -> Result<String> {
    let mut st=c.prepare("SELECT kind,report_source,identity_family,namespace_key,identity_key,session_key,link_version,link_key,native_tool,start_ms,end_ms FROM suppression_rules ORDER BY kind,report_source,identity_family,namespace_key,identity_key,session_key,link_key,native_tool,start_ms,end_ms")?;
    let mut rows = st.query([])?;
    let mut h = Sha256::new();
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
    let sources = st
        .query_map([], |r| r.get(0))?
        .collect::<rusqlite::Result<Vec<String>>>()?;
    Ok(ReplacementImpact {
        current_candidates_absent_from_backup: missing_candidates.to_string(),
        current_events_absent_or_changed_in_backup: missing_events.to_string(),
        current_first_ms: a,
        current_last_ms: b,
        backup_first_ms: c,
        backup_last_ms: d,
        affected_sources: sources,
    })
}
