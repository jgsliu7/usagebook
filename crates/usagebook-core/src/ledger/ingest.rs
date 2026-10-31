//! Bounded Claude acquisition; source failures stop one safe cursor, never the scope.
use super::claude::{self, CLAUDE_FAMILY, CLAUDE_NAMESPACE, MAX_LINE_BYTES, PARSER_VERSION};
use super::types::{hash_parts, new_epoch, now_ms, Observation};
use super::{Ledger, LedgerError, Result};
use rusqlite::{
    params, params_from_iter, types::Value as SqlValue, OptionalExtension, TransactionBehavior,
};
use serde::Serialize;
#[path = "ingest_reader.rs"]
mod reader;
pub fn reader_helper_main() -> Result<()> {
    reader::helper_main()
}
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, Metadata},
    io::{BufRead, BufReader, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

const SLICE_BYTES: u64 = 4 * 1024 * 1024;
const BATCH_ROWS: usize = 500;
// Small files are cheaper to replay than to seal and verify on every append.
const PREFIX_SEAL_MIN_BYTES: i64 = 1024 * 1024;
#[derive(Debug, Default, Serialize)]
pub struct RefreshSummary {
    pub files_seen: u64,
    pub files_read: u64,
    pub bytes_read: u64,
    pub parsed_bytes: u64,
    pub verification_bytes: u64,
    pub identity_probes: u64,
    pub identity_probe_bytes: u64,
    /// Reader heartbeats during bounded prefix/identity verification.
    pub reader_progress: u64,
    pub inserted: u64,
    pub duplicates: u64,
    pub suppressed: u64,
    pub ignored: u64,
    pub errors: u64,
    pub pending: u64,
    pub audit_completed: u64,
}
#[derive(Clone, Debug)]
struct Object {
    id: i64,
    root: String,
    path: PathBuf,
    generation: String,
    len: i64,
    mtime: Option<i64>,
    revision: i64,
    offset: i64,
    done: bool,
    state: String,
    retry: Option<i64>,
    audit_due: i64,
    audit_hash: Option<Sha256>,
    audit_hashed: i64,
}
fn metadata_stamp(meta: &Metadata) -> (i64, Option<i64>) {
    let ms = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .and_then(|d| i64::try_from(d.as_nanos()).ok());
    (i64::try_from(meta.len()).unwrap_or(i64::MAX), ms)
}
fn path_text(path: &Path) -> Result<String> {
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| LedgerError::InvalidInput("source path is not UTF-8".into()))
}
fn root_path(path: &Path) -> PathBuf {
    if path.file_name().is_some_and(|s| s == "projects") {
        path.to_owned()
    } else {
        path.join("projects")
    }
}
#[derive(Debug, PartialEq, Eq)]
enum EnumerationStatus {
    Complete,
    Partial,
    RootUnavailable,
}
#[must_use = "incomplete discovery must not certify missing files or identity evidence"]
struct EnumerationSummary {
    status: EnumerationStatus,
    errors: u64,
    first_error: Option<LedgerError>,
}
impl EnumerationSummary {
    fn complete(&self) -> bool {
        self.status == EnumerationStatus::Complete
    }
    fn record_error(&mut self, error: std::io::Error) {
        self.errors = self.errors.saturating_add(1);
        self.first_error.get_or_insert_with(|| source_io(error));
        if self.complete() {
            self.status = EnumerationStatus::Partial;
        }
    }
}
enum DiscoveryKind {
    Directory,
    File,
    Other,
}
struct DiscoveryEntry {
    path: PathBuf,
    kind: std::io::Result<DiscoveryKind>,
}
fn discovery_entry(entry: std::io::Result<fs::DirEntry>) -> std::io::Result<DiscoveryEntry> {
    let entry = entry?;
    Ok(DiscoveryEntry {
        path: entry.path(),
        kind: entry.file_type().map(|kind| {
            if kind.is_dir() {
                DiscoveryKind::Directory
            } else if kind.is_file() {
                DiscoveryKind::File
            } else {
                // Symlinks are deliberately not followed, including directory links.
                DiscoveryKind::Other
            }
        }),
    })
}
fn list_files(root: &Path, visit: impl FnMut(PathBuf) -> Result<()>) -> Result<EnumerationSummary> {
    list_files_with(root, visit, |path| {
        fs::read_dir(path).map(|entries| entries.map(discovery_entry))
    })
}
fn list_files_with<I: Iterator<Item = std::io::Result<DiscoveryEntry>>>(
    root: &Path,
    mut visit: impl FnMut(PathBuf) -> Result<()>,
    mut read_dir: impl FnMut(&Path) -> std::io::Result<I>,
) -> Result<EnumerationSummary> {
    let mut summary = EnumerationSummary {
        status: EnumerationStatus::Complete,
        errors: 0,
        first_error: None,
    };
    let entries = match read_dir(root) {
        Ok(entries) => entries,
        Err(error) => {
            summary.status = EnumerationStatus::RootUnavailable;
            summary.record_error(error);
            return Ok(summary);
        }
    };
    // Depth-first iterators retain O(directory depth), never a Vec of every child directory.
    let mut stack = vec![entries];
    while let Some(entries) = stack.last_mut() {
        let Some(entry) = entries.next() else {
            stack.pop();
            continue;
        };
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                summary.record_error(error);
                // The failed iterator item has no reliable path. Abandon only this
                // directory; retrying next could loop forever on a persistent fault.
                stack.pop();
                continue;
            }
        };
        let kind = match entry.kind {
            Ok(kind) => kind,
            Err(error) => {
                summary.record_error(error);
                continue;
            }
        };
        let path = entry.path;
        match kind {
            DiscoveryKind::Directory => match read_dir(&path) {
                Ok(entries) => stack.push(entries),
                Err(error) => summary.record_error(error),
            },
            DiscoveryKind::File
                if path.extension().is_some_and(|s| s == "jsonl")
                    && !(path.file_name().is_some_and(|s| s == "journal.jsonl")
                        && path.components().any(|s| s.as_os_str() == "subagents")) =>
            {
                // Ledger/SQL/cancellation failures from the callback are fatal,
                // not filesystem gaps that best-effort discovery may swallow.
                visit(path)?;
            }
            _ => {}
        }
    }
    Ok(summary)
}
// Preserve complete-record semantics while slow-path callers account and cancel
// every <=64 KiB read. Even a legal 16 MiB line cannot bypass the I/O budget.
fn read_bounded_line(
    input: &mut impl BufRead,
    line: &mut Vec<u8>,
    progress: &mut impl FnMut(u64) -> Result<()>,
) -> Result<usize> {
    while line.len() < MAX_LINE_BYTES + 2 {
        let available = input.fill_buf().map_err(source_io)?;
        if available.is_empty() {
            break;
        }
        let cap = available
            .len()
            .min(64 * 1024)
            .min(MAX_LINE_BYTES + 2 - line.len());
        let end = available[..cap]
            .iter()
            .position(|b| *b == b'\n')
            .map(|i| i + 1);
        let n = end.unwrap_or(cap);
        line.extend_from_slice(&available[..n]);
        input.consume(n);
        progress(n as u64)?;
        if end.is_some() {
            break;
        }
    }
    Ok(line.len())
}
fn backoff(failures: i64) -> i64 {
    30_000_i64
        .saturating_mul(
            1i64.checked_shl((failures.saturating_sub(1)).min(6) as u32)
                .unwrap_or(64),
        )
        .min(1_800_000)
}
fn checkpoint(generation: &str, offset: i64) -> Vec<u8> {
    let mut b = vec![1];
    b.extend_from_slice(&(generation.len() as u32).to_be_bytes());
    b.extend_from_slice(generation.as_bytes());
    b.extend_from_slice(&offset.to_be_bytes());
    b
}
fn source_io(e: std::io::Error) -> LedgerError {
    if e.kind() == std::io::ErrorKind::PermissionDenied {
        LedgerError::InvalidInput("SourcePermissionDenied".into())
    } else {
        LedgerError::Io(e)
    }
}
fn require_stamp(path: &Path, len: i64, mtime: Option<i64>) -> Result<()> {
    if metadata_stamp(&fs::metadata(path).map_err(source_io)?) != (len, mtime) {
        return Err(LedgerError::StaleObservation);
    }
    Ok(())
}

impl Ledger {
    /// New roots inherit the source's route and conservatively retain any pause.
    pub fn register_claude_root(&mut self, path: &Path) -> Result<String> {
        self.require_writer()?;
        let absolute = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()?.join(path)
        };
        let locator = path_text(&absolute)?;
        let id = hash_parts(
            "usagebook/root",
            &[CLAUDE_FAMILY, CLAUDE_NAMESPACE, "claude", &locator],
        );
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let routes: Vec<String> = tx
            .prepare("SELECT DISTINCT route FROM source_roots WHERE report_source='claude'")?
            .query_map([], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        if routes.len() > 1 {
            return Err(LedgerError::RouteInconsistent);
        }
        let route = routes.first().map(String::as_str).unwrap_or("live");
        // Pause, forget and restore can disable existing roots. A newly found
        // directory must not silently bypass that decision. Read and register
        // under the same writer transaction; an existing root stays unchanged.
        let enabled: bool = tx.query_row(
            "SELECT COALESCE(MIN(enabled),1) FROM source_roots WHERE report_source='claude'",
            [],
            |r| r.get(0),
        )?;
        let inserted=tx.execute("INSERT INTO source_roots(root_id,identity_family,namespace_key,collector_key,report_source,native_tool,locator,enabled,identity_version,parser_version,route) VALUES(?1,'claude','unknown:claude','claude','claude','claude',?2,?5,1,?3,?4) ON CONFLICT DO NOTHING",params![id,locator,PARSER_VERSION,route,enabled])?;
        // The finality-aware parser re-observes old files, but never rewrites
        // published selections. Missing old files retain their saved evidence.
        let upgraded = tx.execute(
            "UPDATE source_roots SET parser_version=?2,enumeration_done=0 WHERE root_id=?1 AND parser_version='claude-ledger-v1'",
            params![id, PARSER_VERSION],
        )?;
        if upgraded > 0 {
            tx.execute(
                "UPDATE source_objects SET parser_version=?2,state='replay_required',saved_offset=NULL,checkpoint_format=NULL,checkpoint=NULL,target_done=0,quick_fingerprint=NULL,object_revision=object_revision+1 WHERE root_id=?1",
                params![id, PARSER_VERSION],
            )?;
        }
        if inserted > 0 || upgraded > 0 {
            super::db::bump(&tx)?;
        }
        tx.commit()?;
        Ok(id)
    }
    /// A synchronous bounded scheduler; no full-history UsageRecord collection is created.
    pub fn refresh_claude(&mut self, roots: &[PathBuf], full: bool) -> Result<RefreshSummary> {
        self.refresh_claude_inner(roots, full, true, None)
    }
    /// Polling honors backoff; explicit refresh bypasses it once without clearing failure history.
    pub fn poll_claude(&mut self, roots: &[PathBuf]) -> Result<RefreshSummary> {
        self.refresh_claude_inner(roots, false, false, None)
    }
    /// Yield without cancelling stream readers or discarding their prefix hash.
    pub fn poll_claude_budgeted(
        &mut self,
        roots: &[PathBuf],
        budget: Duration,
    ) -> Result<RefreshSummary> {
        self.refresh_claude_inner(
            roots,
            false,
            false,
            Some(budget.max(Duration::from_millis(1))),
        )
    }
    fn refresh_claude_inner(
        &mut self,
        roots: &[PathBuf],
        full: bool,
        explicit: bool,
        budget: Option<Duration>,
    ) -> Result<RefreshSummary> {
        self.require_writer()?;
        let mut summary = RefreshSummary::default();
        if let Some(state) = self.claude_readers.as_ref() {
            let epoch: String = self.conn.query_row(
                "SELECT epoch FROM ledger_meta WHERE singleton=1",
                [],
                |r| r.get(0),
            )?;
            if state.epoch != epoch {
                if let Some(state) = self.claude_readers.take() {
                    state.cancel(&self.path);
                }
            }
        }
        if self.claude_readers.is_some() {
            self.drain_readers(&mut summary, budget)?;
            if budget.is_some() {
                summary.pending=self.conn.query_row("SELECT COUNT(*) FROM source_objects o JOIN source_roots r USING(root_id) WHERE r.report_source='claude' AND (o.target_done=0 OR o.state IN('error','replay_required'))",[],|r|r.get::<_,i64>(0))? as u64;
                return Ok(summary);
            }
        }
        if budget.is_some() {
            self.publish_some(100)?;
        } else {
            self.publish_pending()?;
        }
        for root in roots {
            self.register_claude_root(root)?;
        }
        let registered:Vec<(String,String,bool,Option<i64>)>=self.conn.prepare("SELECT root_id,locator,enabled,retry_after_ms FROM source_roots WHERE identity_family='claude' AND namespace_key='unknown:claude' ORDER BY root_id")?.query_map([],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?.collect::<rusqlite::Result<_>>()?;
        self.conn.execute_batch("CREATE TEMP TABLE IF NOT EXISTS acquisition_seen(object_id INTEGER PRIMARY KEY,probe_attempted INTEGER NOT NULL DEFAULT 0); DELETE FROM acquisition_seen; CREATE TEMP TABLE IF NOT EXISTS acquisition_queue(object_id INTEGER PRIMARY KEY,root_id TEXT NOT NULL,turn INTEGER NOT NULL,audit INTEGER NOT NULL,deferred INTEGER NOT NULL DEFAULT 0);DELETE FROM acquisition_queue;CREATE TEMP TABLE IF NOT EXISTS acquisition_proofs(object_id INTEGER PRIMARY KEY,prefix_len INTEGER NOT NULL,digest BLOB NOT NULL,target_len INTEGER NOT NULL,target_mtime INTEGER);DELETE FROM acquisition_proofs;CREATE INDEX IF NOT EXISTS temp.acquisition_queue_turn ON acquisition_queue(deferred,turn,root_id,object_id);")?;
        for (root_id, locator, enabled, retry) in registered {
            if !enabled {
                continue;
            }
            let mut turn = 0i64;
            if explicit || !retry.is_some_and(|t| t > now_ms()) {
                let enumerated = list_files(&root_path(Path::new(&locator)), |path| {
                    summary.files_seen += 1;
                    let object = match self.discover_object(&root_id, &path, full, explicit) {
                        Ok(o) => o,
                        Err(e) if is_source_error(&e) => {
                            summary.errors += 1;
                            let id = self.record_discovery_error(&root_id, &path, &e)?;
                            self.conn.execute(
                                "INSERT OR IGNORE INTO acquisition_seen(object_id) VALUES(?1)",
                                [id],
                            )?;
                            return Ok(());
                        }
                        Err(e) => return Err(e),
                    };
                    self.conn.execute(
                        "INSERT OR IGNORE INTO acquisition_seen(object_id) VALUES(?1)",
                        [object.id],
                    )?;
                    if (!object.done
                        || object.state != "ready"
                        || full
                        || object.audit_due <= now_ms())
                        && (explicit || full || !object.retry.is_some_and(|t| t > now_ms()))
                    {
                        self.conn.execute(
                            "INSERT OR IGNORE INTO acquisition_queue(object_id,root_id,turn,audit) VALUES(?1,?2,?3,?4)",
                            params![
                                object.id,
                                root_id,
                                turn,
                                full || object.audit_due <= now_ms()
                            ],
                        )?;
                        turn += 1;
                    }
                    Ok(())
                });
                let enumerated = enumerated?;
                if enumerated.complete() {
                    self.conn.execute("UPDATE source_roots SET enumeration_done=1,last_enumerated_ms=?2,error_code=NULL,failure_count=0,retry_after_ms=NULL WHERE root_id=?1",params![root_id,now_ms()])?;
                    self.conn.execute("UPDATE source_objects SET state='missing',error_code=CASE WHEN target_done=0 THEN 'SourceMissingBeforeComplete' ELSE NULL END WHERE root_id=?1 AND state<>'missing' AND object_id NOT IN(SELECT object_id FROM acquisition_seen)",[&root_id])?;
                } else {
                    summary.errors = summary.errors.saturating_add(enumerated.errors);
                    // Only a genuinely inaccessible root earns root-wide backoff.
                    // A local gap must leave healthy siblings discoverable next poll.
                    let error = enumerated
                        .first_error
                        .as_ref()
                        .expect("incomplete enumeration has a diagnostic");
                    if enumerated.status == EnumerationStatus::RootUnavailable {
                        self.fail_root(&root_id, error)?;
                    } else {
                        self.conn.execute("UPDATE source_roots SET enumeration_done=0,error_code=?2,failure_count=0,retry_after_ms=NULL WHERE root_id=?1",params![root_id,error.code()])?;
                    }
                }
            }
            // SQL retains the pending set; file count never becomes an in-memory Object Vec.
            self.conn.execute("INSERT OR IGNORE INTO acquisition_queue(object_id,root_id,turn,audit) SELECT object_id,root_id,?2+object_id,?3 FROM source_objects WHERE root_id=?1 AND state<>'missing' AND target_done=0 AND object_id NOT IN(SELECT object_id FROM acquisition_seen) AND (?4 OR retry_after_ms IS NULL OR retry_after_ms<=?5)",params![root_id,turn,full,explicit,now_ms()])?;
        }
        self.drain_readers(&mut summary, budget)?;
        summary.pending=self.conn.query_row("SELECT COUNT(*) FROM source_objects o JOIN source_roots r USING(root_id) WHERE r.report_source='claude' AND (o.target_done=0 OR o.state IN('error','replay_required'))",[],|r|r.get::<_,i64>(0))? as u64;
        Ok(summary)
    }
    fn discover_object(
        &mut self,
        root: &str,
        path: &Path,
        full: bool,
        explicit: bool,
    ) -> Result<Object> {
        self.conn.execute_batch("BEGIN IMMEDIATE")?;
        let result = self.discover_object_inner(root, path, full, explicit);
        match result {
            Ok(object) => {
                self.conn.execute_batch("COMMIT")?;
                Ok(object)
            }
            Err(error) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }
    fn discover_object_inner(
        &mut self,
        root: &str,
        path: &Path,
        full: bool,
        explicit: bool,
    ) -> Result<Object> {
        let locator = path_text(path)?;
        let meta = fs::metadata(path).map_err(source_io)?;
        if !meta.is_file() {
            return Err(LedgerError::InvalidInput(
                "source is not a regular file".into(),
            ));
        }
        let (len, mtime) = metadata_stamp(&meta);
        let old=self.conn.query_row("SELECT object_id,root_id,locator,content_generation,byte_length,mtime_ns,object_revision,COALESCE(saved_offset,0),target_done,state,retry_after_ms,audit_due_ms FROM source_objects WHERE root_id=?1 AND object_key=?2",params![root,locator],object_row).optional()?;
        if let Some(mut o) = old {
            let changed = o.len != len || o.mtime != mtime || o.state == "missing";
            if changed || o.state == "replay_required" {
                // Only a completed, sealed old file can resume at its previous
                // cursor. The full old prefix is verified before any new bytes
                // enter the ledger; a rewrite takes the existing replay path.
                if !full
                    && o.state == "ready"
                    && o.offset > 0
                    && (o.done || self.stream_checkpoint(&o)?.is_some())
                    && o.len >= PREFIX_SEAL_MIN_BYTES
                    && len > o.len
                    && o.audit_due > now_ms()
                {
                    let seal: Option<Vec<u8>> = self.conn.query_row(
                        "SELECT quick_fingerprint FROM source_objects WHERE object_id=?1",
                        [o.id],
                        |r| r.get(0),
                    )?;
                    let seal = if o.done {
                        seal
                    } else {
                        seal.or(self.stream_checkpoint(&o)?.map(|s| s.digest))
                    };
                    if let Some(seal) = seal {
                        // Keep the durable old checkpoint untouched until a reader
                        // proves it. A crash only loses this temporary plan.
                        self.conn.execute(
                            "INSERT INTO acquisition_proofs VALUES(?1,?2,?3,?4,?5)",
                            params![o.id, o.offset, seal, len, mtime],
                        )?;
                        o.done = false;
                        return Ok(o);
                    }
                }

                let generation = new_epoch();
                self.conn.execute("UPDATE source_objects SET content_generation=?2,byte_length=?3,mtime_ns=?4,object_revision=object_revision+1,state='ready',saved_offset=0,checkpoint_format=1,checkpoint=?5,target_generation=?2,target_end=?3,target_done=0,last_observed_ms=?6,error_code=NULL,retry_after_ms=NULL,quick_fingerprint=NULL WHERE object_id=?1",params![o.id,generation,len,mtime,checkpoint(&generation,0),now_ms()])?;
                super::db::bump(&self.conn)?;
                o.generation = generation;
                o.len = len;
                o.mtime = mtime;
                o.revision += 1;
                o.offset = 0;
                o.done = false;
                o.state = "ready".into();
                o.retry = None;
            } else {
                // An unchanged sealed audit uses the same bounded worker proof.
                // No source bytes are read while the discovery write lock is held.
                if !full
                    && o.state == "ready"
                    && o.done
                    && o.offset == o.len
                    && o.len >= PREFIX_SEAL_MIN_BYTES
                    && o.audit_due <= now_ms()
                    && (explicit || !o.retry.is_some_and(|t| t > now_ms()))
                {
                    let seal: Option<Vec<u8>> = self.conn.query_row(
                        "SELECT quick_fingerprint FROM source_objects WHERE object_id=?1",
                        [o.id],
                        |r| r.get(0),
                    )?;
                    if let Some(seal) = seal {
                        self.conn.execute(
                            "INSERT INTO acquisition_proofs VALUES(?1,?2,?3,?4,?5)",
                            params![o.id, o.len, seal, len, mtime],
                        )?;
                        return Ok(o);
                    }
                }
                if full || (o.audit_due <= now_ms() && (explicit || !o.retry.is_some_and(|t| t > now_ms())))
                    // In-memory SHA state cannot survive a crash/cancel. Replay a
                    // partial large file instead of certifying an unproved prefix.
                    || (!o.done && o.offset > 0 && o.len >= PREFIX_SEAL_MIN_BYTES
                        && self.stream_checkpoint(&o)?.is_none()
                        && (explicit || !o.retry.is_some_and(|t| t > now_ms())))
                {
                    self.conn.execute("UPDATE source_objects SET object_revision=object_revision+1,saved_offset=0,checkpoint_format=1,checkpoint=?2,target_generation=content_generation,target_end=byte_length,target_done=0,quick_fingerprint=NULL WHERE object_id=?1",params![o.id,checkpoint(&o.generation,0)])?;
                    super::db::bump(&self.conn)?;
                    o.offset = 0;
                    o.done = false;
                    o.revision += 1;
                }
            }
            return Ok(o);
        }
        let generation = new_epoch();
        self.conn.execute("INSERT INTO source_objects(root_id,object_key,locator,content_generation,byte_length,mtime_ns,object_revision,state,parser_version,saved_offset,checkpoint_format,checkpoint,target_generation,target_end,last_observed_ms,audit_due_ms) VALUES(?1,?2,?2,?3,?4,?5,0,'ready',?6,0,1,?7,?3,?4,?8,?9)",params![root,locator,generation,len,mtime,PARSER_VERSION,checkpoint(&generation,0),now_ms(),now_ms()+86_400_000])?;
        let object_id = self.conn.last_insert_rowid();
        super::db::bump(&self.conn)?;
        Ok(Object {
            id: object_id,
            root: root.to_owned(),
            path: path.to_owned(),
            generation,
            len,
            mtime,
            revision: 0,
            offset: 0,
            done: false,
            state: "ready".into(),
            retry: None,
            audit_due: now_ms() + 86_400_000,
            audit_hash: None,
            audit_hashed: 0,
        })
    }
    fn record_discovery_error(
        &mut self,
        root: &str,
        path: &Path,
        error: &LedgerError,
    ) -> Result<i64> {
        let locator = path_text(path)?;
        let existing: Option<i64> = self
            .conn
            .query_row(
                "SELECT object_id FROM source_objects WHERE root_id=?1 AND object_key=?2",
                params![root, locator],
                |r| r.get(0),
            )
            .optional()?;
        let id = if let Some(id) = existing {
            id
        } else {
            let generation = new_epoch();
            self.conn.execute("INSERT INTO source_objects(root_id,object_key,locator,content_generation,byte_length,object_revision,state,parser_version,saved_offset,checkpoint_format,checkpoint,target_generation,target_end,audit_due_ms)VALUES(?1,?2,?2,?3,0,0,'error',?4,0,1,?5,?3,0,?6)",params![root,locator,generation,PARSER_VERSION,checkpoint(&generation,0),now_ms()])?;
            self.conn.last_insert_rowid()
        };
        self.fail_object(id, error)?;
        Ok(id)
    }
    fn fail_root(&mut self, id: &str, e: &LedgerError) -> Result<()> {
        let n: i64 = self.conn.query_row(
            "SELECT failure_count+1 FROM source_roots WHERE root_id=?1",
            [id],
            |r| r.get(0),
        )?;
        self.conn.execute("UPDATE source_roots SET enumeration_done=0,error_code=?2,failure_count=?3,retry_after_ms=?4 WHERE root_id=?1",params![id,e.code(),n,now_ms()+backoff(n)])?;
        Ok(())
    }
    fn fail_object(&mut self, id: i64, e: &LedgerError) -> Result<()> {
        let n: i64 = self.conn.query_row(
            "SELECT failure_count+1 FROM source_objects WHERE object_id=?1",
            [id],
            |r| r.get(0),
        )?;
        self.conn.execute("UPDATE source_objects SET state='error',error_code=?2,target_done=0,failure_count=?3,retry_after_ms=?4 WHERE object_id=?1",params![id,e.code(),n,now_ms()+backoff(n)])?;
        Ok(())
    }
    fn stream_checkpoint(&self, o: &Object) -> Result<Option<reader::SavedStreamState>> {
        let Some(v) =
            super::collection::runtime_value(&self.conn, &format!("claude-stream/{}", o.id))?
        else {
            return Ok(None);
        };
        let epoch: String =
            self.conn
                .query_row("SELECT epoch FROM ledger_meta WHERE singleton=1", [], |r| {
                    r.get(0)
                })?;
        if v["generation"] != o.generation || v["epoch"] != epoch {
            return Ok(None);
        }
        let state: reader::SavedStreamState =
            serde_json::from_value(v["state"].clone()).map_err(|_| LedgerError::CorruptLedger)?;
        if state.offset != o.offset as u64 {
            return Ok(None);
        }
        Ok(Some(state))
    }
    #[allow(clippy::too_many_arguments)] // Evidence and its resumable proof commit together.
    fn save_batch(
        &mut self,
        o: &mut Object,
        epoch: &str,
        observations: &[Observation],
        offset: i64,
        done: bool,
        stream_state: Option<&reader::SavedStreamState>,
        summary: &mut RefreshSummary,
    ) -> Result<()> {
        if observations.is_empty() && offset == o.offset && !done {
            return Ok(());
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let valid:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM source_objects o JOIN source_roots r USING(root_id) JOIN ledger_meta m ON m.singleton=1 WHERE o.object_id=?1 AND o.object_revision=?2 AND o.content_generation=?3 AND m.epoch=?4 AND r.enabled=1 AND r.identity_version=1 AND r.parser_version=?5 AND o.parser_version=?5 AND o.saved_offset=?6 AND o.byte_length=?7 AND o.mtime_ns IS ?8)",params![o.id,o.revision,o.generation,epoch,PARSER_VERSION,o.offset,o.len,o.mtime],|r|r.get(0))?;
        if !valid {
            return Err(LedgerError::StaleObservation);
        }
        // An appended or rewritten object has old candidates under its prior
        // generation. Check those exact duplicates once before the normal
        // collision-and-insert path; a first import keeps the cheaper path.
        let first_generation: Option<String> = tx.query_row(
            "SELECT content_generation FROM usage_observations INDEXED BY observations_object WHERE object_id=?1 ORDER BY observation_id LIMIT 1",
            [o.id],
            |r| r.get(0),
        ).optional()?;
        let replaying = first_generation.is_some_and(|generation| generation != o.generation);
        let mut inserted = 0;
        let mut duplicates = 0;
        let mut suppressed = 0;
        for observation in observations {
            if exact_suppressed(&tx, observation)? {
                super::maintenance::add_incoming_controls(&tx, observation)?;
                suppressed += 1;
                continue;
            }
            if weak_link(&tx, observation)? {
                return Err(LedgerError::IdentityLinkNeedsReview);
            }
            if replaying && unchanged_candidate(&tx, observation)? {
                duplicates += 1;
                continue;
            }
            if insert_observation(&tx, observation)? {
                inserted += 1;
            } else {
                duplicates += 1;
            }
        }
        if suppressed > 0 {
            super::maintenance::finish_incoming_suppression(&tx)?;
        }
        let new_revision:i64=tx.query_row("UPDATE source_objects SET saved_offset=?2,checkpoint_format=1,checkpoint=?3,object_revision=object_revision+1,target_done=?4,state=CASE WHEN ?4 OR state='replay_required' THEN 'ready' ELSE state END,error_code=CASE WHEN ?4 THEN NULL ELSE error_code END,failure_count=CASE WHEN ?4 THEN 0 ELSE failure_count END,retry_after_ms=CASE WHEN ?4 THEN NULL ELSE retry_after_ms END WHERE object_id=?1 RETURNING object_revision",params![o.id,offset,checkpoint(&o.generation,offset),done],|r|r.get(0))?;
        if let Some(state) = stream_state {
            if state.offset != offset as u64 {
                return Err(LedgerError::StaleObservation);
            }
            super::collection::put_runtime_value(
                &tx,
                &format!("claude-stream/{}", o.id),
                &serde_json::json!({"version":2,"generation":o.generation,"epoch":epoch,"state":state}),
            )?;
        }
        super::db::bump(&tx)?;
        super::types::regression_stop("t2_before_commit");
        tx.commit()?;
        super::types::regression_stop("t2_after_commit");
        summary.inserted += inserted;
        summary.duplicates += duplicates;
        summary.suppressed += suppressed;
        o.offset = offset;
        o.revision = new_revision;
        o.done = done;
        Ok(())
    }
    fn complete_audit(
        &mut self,
        o: &Object,
        epoch: &str,
        summary: &mut RefreshSummary,
    ) -> Result<()> {
        // Incremental in-memory hash covers every byte of this fixed target; restart begins at zero.
        // No partial audit is certified, and no second full-file pass delays other objects.
        if o.audit_hashed != o.len || o.audit_hash.is_none() {
            return Ok(());
        }
        let digest = o.audit_hash.as_ref().unwrap().clone().finalize().to_vec();
        let changed = self.conn.execute("UPDATE source_objects SET audit_completed_ms=?2,audit_due_ms=?3,quick_fingerprint=?4 WHERE object_id=?1 AND target_done=1 AND object_revision=?5 AND content_generation=?6 AND saved_offset=?7 AND byte_length=?7 AND EXISTS(SELECT 1 FROM ledger_meta WHERE epoch=?8) AND EXISTS(SELECT 1 FROM source_roots r WHERE r.root_id=source_objects.root_id AND r.enabled=1 AND r.identity_version=1 AND r.parser_version=?9) AND parser_version=?9",params![o.id,now_ms(),now_ms()+86_400_000,digest,o.revision,o.generation,o.len,epoch,PARSER_VERSION])?;
        summary.audit_completed += changed as u64;
        Ok(())
    }

    fn fail_read(&self, task: &ActiveRead, code: &str) -> Result<bool> {
        let o = &task.object;
        let failures: i64 = self
            .conn
            .query_row(
                "SELECT failure_count+1 FROM source_objects WHERE object_id=?1",
                [o.id],
                |r| r.get(0),
            )
            .optional()?
            .unwrap_or(1);
        let changed = self.conn.execute("UPDATE source_objects SET state='error',error_code=?2,target_done=0,failure_count=failure_count+1,retry_after_ms=?3 WHERE object_id=?1 AND object_revision=?4 AND content_generation=?5 AND saved_offset=?6 AND EXISTS(SELECT 1 FROM ledger_meta WHERE epoch=?7) AND EXISTS(SELECT 1 FROM source_roots r WHERE r.root_id=source_objects.root_id AND r.enabled=1 AND r.identity_version=1 AND r.parser_version=?8) AND parser_version=?8", params![o.id,code,now_ms()+backoff(failures),o.revision,o.generation,o.offset,task.epoch,PARSER_VERSION])?;
        Ok(changed == 1)
    }
    fn mark_identity_wait(&self, task: &ActiveRead) -> Result<bool> {
        let o = &task.object;
        Ok(self.conn.execute("UPDATE source_objects SET state='error',error_code='IDENTITY_LINK_NEEDS_REVIEW',target_done=0,retry_after_ms=NULL WHERE object_id=?1 AND object_revision=?2 AND content_generation=?3 AND saved_offset=?4 AND EXISTS(SELECT 1 FROM ledger_meta WHERE epoch=?5) AND EXISTS(SELECT 1 FROM source_roots r WHERE r.root_id=source_objects.root_id AND r.enabled=1 AND r.identity_version=1 AND r.parser_version=?6) AND parser_version=?6",params![o.id,o.revision,o.generation,o.offset,task.epoch,PARSER_VERSION])? == 1)
    }
    fn accept_prefix(&mut self, task: &mut ActiveRead) -> Result<()> {
        let proof = task.proof.as_ref().ok_or(LedgerError::StaleObservation)?;
        let o = &mut task.object;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let persisted=proof.resume.as_ref().map(|state|serde_json::json!({"version":2,"generation":o.generation,"epoch":task.epoch,"state":state}).to_string());
        let valid: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM source_objects o JOIN source_roots r USING(root_id) JOIN ledger_meta m ON m.singleton=1 WHERE o.object_id=?1 AND o.object_revision=?2 AND o.content_generation=?3 AND m.epoch=?4 AND r.enabled=1 AND r.identity_version=1 AND r.parser_version=?5 AND o.parser_version=?5 AND o.saved_offset=?6 AND ((o.byte_length=?6 AND o.quick_fingerprint=?7 AND o.target_done=1) OR (o.target_done=0 AND o.byte_length>=?6 AND o.byte_length<=?9 AND EXISTS(SELECT 1 FROM ledger_runtime_state s WHERE s.state_key='claude-stream/'||o.object_id AND s.value_json=?8))))",params![o.id,o.revision,o.generation,task.epoch,PARSER_VERSION,proof.len,proof.digest,persisted,o.len],|r|r.get(0))?;
        if !valid {
            return Err(LedgerError::StaleObservation);
        }
        let changed_target: bool = tx.query_row(
            "SELECT byte_length<>?2 OR mtime_ns IS NOT ?3 FROM source_objects WHERE object_id=?1",
            params![o.id, o.len, o.mtime],
            |r| r.get(0),
        )?;
        if changed_target {
            tx.execute("UPDATE source_objects SET byte_length=?2,mtime_ns=?3,object_revision=object_revision+1,target_generation=content_generation,target_end=?2,target_done=0,last_observed_ms=?4,error_code=NULL,retry_after_ms=NULL,quick_fingerprint=NULL WHERE object_id=?1",params![o.id,o.len,o.mtime,now_ms()])?;
            super::db::bump(&tx)?;
            o.revision += 1;
            o.done = false;
        }
        // This plan proves one transition only. Keep the verified SHA state in
        // its live reader; a later identity retry must not consume the old plan.
        tx.execute("DELETE FROM acquisition_proofs WHERE object_id=?1", [o.id])?;
        tx.commit()?;
        task.proof_verified = true;
        Ok(())
    }
    fn replay_rejected_prefix(&mut self, task: &mut ActiveRead) -> Result<()> {
        // First verify ownership; a late result cannot reset a newer generation.
        self.accept_prefix(task)?;
        let o = &mut task.object;
        let generation = new_epoch();
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = tx.execute("UPDATE source_objects SET content_generation=?2,object_revision=object_revision+1,saved_offset=0,checkpoint=?3,target_generation=?2,target_done=0,quick_fingerprint=NULL,state='ready' WHERE object_id=?1 AND object_revision=?4 AND content_generation=?5 AND EXISTS(SELECT 1 FROM ledger_meta WHERE epoch=?6)",params![o.id,generation,checkpoint(&generation,0),o.revision,o.generation,task.epoch])?;
        if changed != 1 {
            return Err(LedgerError::StaleObservation);
        }
        super::db::bump(&tx)?;
        tx.commit()?;
        o.generation = generation;
        o.revision += 1;
        o.offset = 0;
        o.done = false;
        Ok(())
    }
    fn restart_unproved_stream(&mut self, o: &mut Object, epoch: &str) -> Result<()> {
        // A stopped reader's SHA state is gone, even if its safe checkpoint is
        // nonzero. All redispatch paths (identity success/defer/helper Busy) pass
        // here, so large files can only finish with a new complete proof.
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = tx.execute("UPDATE source_objects SET saved_offset=0,checkpoint_format=1,checkpoint=?2,object_revision=object_revision+1,target_generation=content_generation,target_end=byte_length,target_done=0,quick_fingerprint=NULL WHERE object_id=?1 AND object_revision=?3 AND content_generation=?4 AND saved_offset=?5 AND byte_length=?6 AND mtime_ns IS ?7 AND EXISTS(SELECT 1 FROM ledger_meta WHERE epoch=?8) AND EXISTS(SELECT 1 FROM source_roots r WHERE r.root_id=source_objects.root_id AND r.enabled=1 AND r.identity_version=1 AND r.parser_version=?9) AND parser_version=?9",params![o.id,checkpoint(&o.generation,0),o.revision,o.generation,o.offset,o.len,o.mtime,epoch,PARSER_VERSION])?;
        if changed != 1 {
            return Err(LedgerError::StaleObservation);
        }
        super::db::bump(&tx)?;
        tx.commit()?;
        o.offset = 0;
        o.revision += 1;
        o.done = false;
        Ok(())
    }
    fn finish_stream(
        &mut self,
        task: &mut ActiveRead,
        digest: &[u8],
        summary: &mut RefreshSummary,
    ) -> Result<()> {
        let o = &mut task.object;
        if o.offset != o.len || digest.len() != 32 {
            return Err(LedgerError::StaleObservation);
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = tx.execute("UPDATE source_objects SET target_done=1,state='ready',error_code=NULL,failure_count=0,retry_after_ms=NULL,quick_fingerprint=?2,audit_completed_ms=CASE WHEN ?3 THEN ?4 ELSE audit_completed_ms END,audit_due_ms=CASE WHEN ?3 THEN ?4+86400000 ELSE audit_due_ms END WHERE object_id=?1 AND object_revision=?5 AND content_generation=?6 AND saved_offset=?7 AND byte_length=?7 AND mtime_ns IS ?8 AND EXISTS(SELECT 1 FROM ledger_meta WHERE epoch=?9) AND EXISTS(SELECT 1 FROM source_roots r WHERE r.root_id=source_objects.root_id AND r.enabled=1 AND r.identity_version=1 AND r.parser_version=?10) AND parser_version=?10",params![o.id,digest,task.audit,now_ms(),o.revision,o.generation,o.len,o.mtime,task.epoch,PARSER_VERSION])?;
        if changed != 1 {
            return Err(LedgerError::StaleObservation);
        }
        if !o.done {
            super::db::bump(&tx)?;
        }
        tx.commit()?;
        o.done = true;
        if task.audit {
            summary.audit_completed += 1;
        }
        Ok(())
    }
}
fn object_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<Object> {
    Ok(Object {
        id: r.get(0)?,
        root: r.get(1)?,
        path: PathBuf::from(r.get::<_, String>(2)?),
        generation: r.get(3)?,
        len: r.get(4)?,
        mtime: r.get(5)?,
        revision: r.get(6)?,
        offset: r.get(7)?,
        done: r.get(8)?,
        state: r.get(9)?,
        retry: r.get(10)?,
        audit_due: r.get(11)?,
        audit_hash: None,
        audit_hashed: 0,
    })
}
fn is_source_error(e: &LedgerError) -> bool {
    matches!(
        e,
        LedgerError::InvalidInput(_)
            | LedgerError::InvalidUsageNumber
            | LedgerError::RecordTooLarge
            | LedgerError::IdentityLinkNeedsReview
            | LedgerError::Io(_)
            | LedgerError::StaleObservation
    )
}

/// Exact rules only. A session rule removes accumulated session facts, not unrelated requests.
pub(crate) fn exact_suppressed(conn: &rusqlite::Connection, o: &Observation) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM suppression_rules s WHERE
         (s.kind='source' AND s.report_source=?1 AND (s.native_tool IS NULL OR s.native_tool=?2))
         OR (s.kind='window' AND (s.report_source IS NULL OR s.report_source=?1)
             AND (s.native_tool IS NULL OR s.native_tool=?2)
             AND ((?3>=s.start_ms AND ?3<s.end_ms) OR (?9<s.end_ms AND ?10>s.start_ms)))
         OR (s.kind='identity' AND s.identity_family=?4 AND s.namespace_key=?5
             AND s.identity_key=?6)
         OR (s.kind='session' AND s.identity_family=?4 AND s.namespace_key=?5 AND s.session_key=?7 AND ?8 IN('session_total','session_cost'))
         OR (s.kind='consumption_interval' AND s.native_tool=?2 AND s.namespace_key=?5
             AND ((?3>=s.start_ms AND ?3<s.end_ms) OR (?9<s.end_ms AND ?10>s.start_ms))))
         OR EXISTS(SELECT 1 FROM adoption_forget_ranges f WHERE f.consumer_tool=?2
             AND (f.identity_family IS NULL OR (f.identity_family=?4 AND f.namespace_key=?5))
             AND ((?3>=f.start_ms AND ?3<f.end_ms) OR (?9<f.end_ms AND ?10>f.start_ms)))",
        params![o.report_source,o.native_tool,o.occurred_ms,o.identity_family,o.namespace_key,o.identity_key,o.session_key,o.fact_kind,o.day_start_ms,o.day_end_ms], |r|r.get(0),
    )?)
}
fn weak_link(conn: &rusqlite::Connection, o: &Observation) -> Result<bool> {
    if exact_suppressed(conn, o)? || o.fact_kind != "request" {
        return Ok(false);
    }
    let Some(m) = o.native_message_id.as_deref() else {
        return Ok(false);
    };
    Ok(conn.query_row("SELECT EXISTS(SELECT 1 FROM suppression_rules WHERE kind='identity_link' AND identity_family=?1 AND namespace_key=?2 AND link_version=1 AND link_key=?3)",params![o.identity_family,o.namespace_key,super::types::identity_link(&o.identity_family,&o.namespace_key,m)],|r|r.get(0))?)
}
fn suppressed_with_old_counterpart(conn: &rusqlite::Connection, o: &Observation) -> Result<bool> {
    if o.identity_family != CLAUDE_FAMILY
        || o.fact_kind != "request"
        || o.native_message_id.is_none()
        || !exact_suppressed(conn, o)?
    {
        return Ok(false);
    }
    let opposite_request_form = o.native_request_id.is_some();
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM usage_observations o INDEXED BY observations_claude_message_request WHERE o.identity_family='claude' AND o.namespace_key=?1 AND o.native_message_id=?2 AND o.fact_kind='request' AND (o.native_request_id IS NULL)=?3 AND NOT EXISTS(SELECT 1 FROM suppression_rules s WHERE s.kind='identity' AND s.identity_family=o.identity_family AND s.namespace_key=o.namespace_key AND s.identity_key=o.identity_key))",
        params![o.namespace_key,o.native_message_id,opposite_request_form],
        |r| r.get(0),
    )?)
}

pub const OBSERVATION_COLUMNS:&str="observation_id,identity_family,namespace_key,identity_key,candidate_key,normalized_hash,object_id,content_generation,record_key,parser_version,identity_version,semantics_version,observed_ms,eligibility,action,revision_authority,revision_no,fact_kind,report_source,native_tool,model_key,model_display,session_key,native_session_id,native_message_id,native_request_id,occurred_ms,native_date,native_timezone,day_start_ms,day_end_ms,input_uncached,cache_read,cache_write,output_normal,output_reasoning,input_total,output_total,total_tokens,cache_write_1h,request_count,cost_state,cost_units,cost_scale,cost_currency";
pub(crate) fn observation_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<Observation> {
    Ok(Observation {
        observation_id: r.get(0)?,
        identity_family: r.get(1)?,
        namespace_key: r.get(2)?,
        identity_key: r.get(3)?,
        candidate_key: r.get(4)?,
        normalized_hash: r.get(5)?,
        object_id: r.get(6)?,
        content_generation: r.get(7)?,
        record_key: r.get(8)?,
        parser_version: r.get(9)?,
        identity_version: r.get(10)?,
        semantics_version: r.get(11)?,
        observed_ms: r.get(12)?,
        eligibility: r.get(13)?,
        action: r.get(14)?,
        revision_authority: r.get(15)?,
        revision_no: r.get(16)?,
        fact_kind: r.get(17)?,
        report_source: r.get(18)?,
        native_tool: r.get(19)?,
        model_key: r.get(20)?,
        model_display: r.get(21)?,
        session_key: r.get(22)?,
        native_session_id: r.get(23)?,
        native_message_id: r.get(24)?,
        native_request_id: r.get(25)?,
        occurred_ms: r.get(26)?,
        native_date: r.get(27)?,
        native_timezone: r.get(28)?,
        day_start_ms: r.get(29)?,
        day_end_ms: r.get(30)?,
        metrics: [
            r.get(31)?,
            r.get(32)?,
            r.get(33)?,
            r.get(34)?,
            r.get(35)?,
            r.get(36)?,
            r.get(37)?,
            r.get(38)?,
            r.get(39)?,
            r.get(40)?,
        ],
        cost_state: r.get(41)?,
        cost_units: r.get(42)?,
        cost_scale: r.get(43)?,
        cost_currency: r.get(44)?,
    })
}
fn native_identity_tuple(o: &Observation) -> (String, Vec<Option<String>>) {
    match o.fact_kind.as_str() {
        "request" if o.native_request_id.is_some() => (
            "request/full".into(),
            vec![o.native_message_id.clone(), o.native_request_id.clone()],
        ),
        "request" => (
            "request/fallback".into(),
            vec![o.native_session_id.clone(), o.native_message_id.clone()],
        ),
        "session_total" => (
            "session_total".into(),
            vec![o.native_session_id.clone(), Some(o.model_key.clone())],
        ),
        "session_cost" => (
            "session_cost".into(),
            vec![
                o.native_session_id.clone(),
                Some(o.cost_currency.clone().unwrap_or_else(|| "USD".into())),
            ],
        ),
        _ => (o.fact_kind.clone(), vec![o.identity_key.clone()]),
    }
}
fn txt(v: &str) -> SqlValue {
    SqlValue::Text(v.to_owned())
}
fn opt(v: &Option<String>) -> SqlValue {
    v.as_deref().map(txt).unwrap_or(SqlValue::Null)
}
fn num(v: Option<i64>) -> SqlValue {
    v.map(SqlValue::Integer).unwrap_or(SqlValue::Null)
}
/// Caller owns the candidate+checkpoint transaction.
fn unchanged_candidate(conn: &rusqlite::Connection, o: &Observation) -> Result<bool> {
    let existing: Option<Observation> = conn.query_row(
        &format!("SELECT {OBSERVATION_COLUMNS} FROM usage_observations WHERE identity_family=?1 AND namespace_key=?2 AND candidate_key=?3 AND normalized_hash=?4"),
        params![o.identity_family, o.namespace_key, o.candidate_key, o.normalized_hash],
        observation_row,
    ).optional()?;
    let Some(existing) = existing else {
        return Ok(false);
    };
    // A changed identity or a hash collision still needs the original path's
    // full checks; only the same native fact may bypass them.
    Ok(existing.identity_key == o.identity_key
        && native_identity_tuple(&existing) == native_identity_tuple(o)
        && claude::normalized_bytes(&existing)? == claude::normalized_bytes(o)?)
}

pub(crate) fn insert_observation(conn: &rusqlite::Connection, o: &Observation) -> Result<bool> {
    if o.eligibility == "identity_only"
        && (o.native_message_id.is_none()
            || (o.native_session_id.is_none() && o.native_request_id.is_none())
            || o.identity_key.is_none()
            || o.metrics.iter().any(Option::is_some)
            || o.occurred_ms.is_some()
            || !o.model_key.is_empty()
            || o.model_display.is_some()
            || o.cost_state != "unknown"
            || o.fact_kind != "request"
            || o.action != "upsert")
    {
        return Err(LedgerError::InvalidInput(
            "invalid identity-only projection".into(),
        ));
    }
    if o.metrics.iter().flatten().any(|v| *v < 0) {
        return Err(LedgerError::InvalidUsageNumber);
    }
    if let Some(key) = &o.identity_key {
        let existing:Option<Observation>=conn.query_row(&format!("SELECT {OBSERVATION_COLUMNS} FROM usage_observations INDEXED BY observations_identity WHERE identity_family=?1 AND namespace_key=?2 AND identity_key=?3 ORDER BY observation_id LIMIT 1"),params![o.identity_family,o.namespace_key,key],observation_row).optional()?;
        if let Some(existing) = existing {
            if native_identity_tuple(&existing) != native_identity_tuple(o) {
                return Err(LedgerError::HashCollision);
            }
        }
    }
    let mut values = vec![
        txt(&o.identity_family),
        txt(&o.namespace_key),
        opt(&o.identity_key),
        txt(&o.candidate_key),
        SqlValue::Blob(o.normalized_hash.clone()),
        num(Some(o.object_id)),
        txt(&o.content_generation),
        txt(&o.record_key),
        txt(&o.parser_version),
        num(Some(o.identity_version)),
        num(Some(o.semantics_version)),
        num(Some(o.observed_ms)),
        txt(&o.eligibility),
        txt(&o.action),
        opt(&o.revision_authority),
        num(o.revision_no),
        txt(&o.fact_kind),
        txt(&o.report_source),
        txt(&o.native_tool),
        txt(&o.model_key),
        opt(&o.model_display),
        opt(&o.session_key),
        opt(&o.native_session_id),
        opt(&o.native_message_id),
        opt(&o.native_request_id),
        num(o.occurred_ms),
        opt(&o.native_date),
        opt(&o.native_timezone),
        num(o.day_start_ms),
        num(o.day_end_ms),
    ];
    values.extend(o.metrics.iter().map(|v| num(*v)));
    values.extend([
        txt(&o.cost_state),
        num(o.cost_units),
        num(o.cost_scale),
        opt(&o.cost_currency),
    ]);
    let columns = OBSERVATION_COLUMNS.split_once(',').unwrap().1;
    let sql=format!("INSERT INTO usage_observations({columns}) VALUES({}) ON CONFLICT(identity_family,namespace_key,candidate_key,normalized_hash) DO NOTHING",(1..=values.len()).map(|i|format!("?{i}")).collect::<Vec<_>>().join(","));
    let inserted = conn
        .prepare_cached(&sql)?
        .execute(params_from_iter(values))?;
    if inserted == 0 {
        let existing=conn.query_row(&format!("SELECT {OBSERVATION_COLUMNS} FROM usage_observations WHERE identity_family=?1 AND namespace_key=?2 AND candidate_key=?3 AND normalized_hash=?4"),params![o.identity_family,o.namespace_key,o.candidate_key,o.normalized_hash],observation_row)?;
        if claude::normalized_bytes(&existing)? != claude::normalized_bytes(o)? {
            return Err(LedgerError::HashCollision);
        }
    }
    Ok(inserted != 0)
}

impl Ledger {
    fn apply_identity_proof(
        &mut self,
        o: &Observation,
        proof: &reader::IdentityProof,
        purpose: reader::IdentityPurpose,
    ) -> Result<()> {
        // Bind only the identity evidence: unrelated healthy commits and safe
        // cursor advances must not starve a completed probe.
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let epoch: String =
            tx.query_row("SELECT epoch FROM ledger_meta WHERE singleton=1", [], |r| {
                r.get(0)
            })?;
        let message = o
            .native_message_id
            .as_deref()
            .ok_or(LedgerError::IncompleteEvidence)?;
        let original: Option<(String, String, i64, Option<i64>)> = tx.query_row(
            "SELECT locator,content_generation,byte_length,mtime_ns FROM source_objects WHERE object_id=?1",
            [o.object_id], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?;
        let Some((path, generation, len, mtime)) = original else {
            return Err(LedgerError::StaleObservation);
        };
        if epoch != proof.epoch || generation != o.content_generation {
            return Err(LedgerError::StaleObservation);
        }
        require_stamp(Path::new(&path), len, mtime)?;
        if !capture_identity_probe(&tx, &mut |_| Ok(()))?
            || identity_probe_digest(&tx, message)? != proof.guard_digest
        {
            return Err(LedgerError::StaleObservation);
        }
        match purpose {
            reader::IdentityPurpose::CounterpartAlreadySuppressed => {
                let suppressed: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM suppression_rules WHERE kind='identity' AND identity_family=?1 AND namespace_key=?2 AND identity_key=?3)", params![o.identity_family,o.namespace_key,proof.counterpart], |r| r.get(0))?;
                if !suppressed {
                    return Err(LedgerError::IdentityLinkNeedsReview);
                }
            }
            reader::IdentityPurpose::IncomingAlreadySuppressed => {
                if !exact_suppressed(&tx, o)? {
                    return Err(LedgerError::IdentityLinkNeedsReview);
                }
                let old: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM usage_observations WHERE identity_family=?1 AND namespace_key=?2 AND identity_key=?3 AND native_message_id=?4)",params![o.identity_family,o.namespace_key,proof.counterpart,message],|r|r.get(0))?;
                if !old {
                    return Err(LedgerError::IdentityLinkNeedsReview);
                }
                tx.execute("INSERT OR IGNORE INTO suppression_rules(kind,identity_family,namespace_key,identity_key,operation_id,created_ms)VALUES('identity',?1,?2,?3,'proved-late-identity',?4)",params![o.identity_family,o.namespace_key,proof.counterpart,now_ms()])?;
            }
        }
        verify_identity_probe(&tx)?;
        super::maintenance::suppress_incoming(&tx, o)?;
        verify_identity_probe(&tx)?;
        super::db::bump(&tx)?;
        tx.commit()?;
        Ok(())
    }
}

/// Complete two-pass proof for the narrow Claude full/fallback relation. No payload is saved.
/// The returned counterpart must itself have an exact identity suppression rule.
/// Caller must revalidate `verify_identity_probe` inside its write transaction.
pub(crate) fn prove_identity_connection(
    conn: &rusqlite::Connection,
    o: &Observation,
) -> Result<Option<String>> {
    Ok(prove_identity_connection_with(
        conn,
        o,
        reader::IdentityPurpose::CounterpartAlreadySuppressed,
        &mut |_| Ok(()),
    )?
    .map(|proof| proof.counterpart))
}
fn prove_identity_connection_with(
    conn: &rusqlite::Connection,
    o: &Observation,
    purpose: reader::IdentityPurpose,
    progress: &mut impl FnMut(u64) -> Result<()>,
) -> Result<Option<reader::IdentityProof>> {
    let Some(message) = o.native_message_id.as_deref() else {
        return Ok(None);
    };
    if o.identity_family != CLAUDE_FAMILY || o.namespace_key != CLAUDE_NAMESPACE {
        return Ok(None);
    }
    if !capture_identity_probe(conn, progress)? {
        return Ok(None);
    }
    // Optimistic identity guards avoid holding a main-database read snapshot
    // across source I/O. Healthy writes can checkpoint the WAL during a probe.
    let epoch: String =
        conn.query_row("SELECT epoch FROM ledger_meta WHERE singleton=1", [], |r| {
            r.get(0)
        })?;
    let guard_digest = identity_probe_digest(conn, message)?;
    let mut session = o.native_session_id.clone();
    let mut request = o.native_request_id.clone();
    if let Some(r) = request.as_deref() {
        let values = identity_values(conn, o, Some(r), None, true, progress)?;
        if values.len() != 1 {
            return Ok(None);
        }
        session = values.into_iter().next();
    }
    let Some(s) = session.as_deref() else {
        return Ok(None);
    };
    let requests = identity_values(conn, o, None, Some(s), false, progress)?;
    if requests.len() != 1 {
        return Ok(None);
    }
    let matched = requests.into_iter().next().unwrap();
    if request.as_ref().is_some_and(|r| r != &matched) {
        return Ok(None);
    }
    request = Some(matched);
    // A full-started proof already completed this direction. Only fallback
    // starts need the reverse scan: both forms read the fixed scope twice.
    if o.native_request_id.is_none() {
        let sessions = identity_values(conn, o, request.as_deref(), None, true, progress)?;
        if sessions.len() != 1 || sessions.first().map(String::as_str) != Some(s) {
            return Ok(None);
        }
    }
    let counterpart = if o.native_request_id.is_some() {
        claude::identity_key("request/fallback", &[s, message])
    } else {
        claude::identity_key("request/full", &[message, request.as_deref().unwrap()])
    };
    let suppressed = match purpose {
        reader::IdentityPurpose::CounterpartAlreadySuppressed => conn.query_row("SELECT EXISTS(SELECT 1 FROM suppression_rules WHERE kind='identity' AND identity_family=?1 AND namespace_key=?2 AND identity_key=?3)",params![o.identity_family,o.namespace_key,counterpart],|r|r.get(0))?,
        reader::IdentityPurpose::IncomingAlreadySuppressed => exact_suppressed(conn,o)?,
    };
    let current_epoch: String =
        conn.query_row("SELECT epoch FROM ledger_meta WHERE singleton=1", [], |r| {
            r.get(0)
        })?;
    if !suppressed
        || epoch != current_epoch
        || identity_probe_digest(conn, message)? != guard_digest
    {
        return Ok(None);
    }
    match verify_identity_probe(conn) {
        Ok(()) => {}
        Err(error)
            if is_source_error(&error) || matches!(error, LedgerError::IncompleteEvidence) =>
        {
            return Ok(None);
        }
        Err(error) => return Err(error),
    }
    Ok(Some(reader::IdentityProof {
        counterpart,
        guard_digest,
        epoch,
    }))
}
fn capture_identity_probe(
    conn: &rusqlite::Connection,
    progress: &mut impl FnMut(u64) -> Result<()>,
) -> Result<bool> {
    conn.execute_batch("CREATE TEMP TABLE IF NOT EXISTS identity_probe_roots(root_id TEXT PRIMARY KEY,locator TEXT NOT NULL,parser_version TEXT NOT NULL,identity_version INTEGER NOT NULL);CREATE TEMP TABLE IF NOT EXISTS identity_probe_files(locator TEXT PRIMARY KEY,byte_length INTEGER NOT NULL,mtime_ns INTEGER);DELETE FROM identity_probe_roots;DELETE FROM identity_probe_files;")?;
    conn.execute("INSERT INTO identity_probe_roots SELECT root_id,locator,parser_version,identity_version FROM source_roots WHERE identity_family=?1 AND namespace_key=?2",params![CLAUDE_FAMILY,CLAUDE_NAMESPACE])?;
    let roots: Vec<String> = conn
        .prepare("SELECT locator FROM identity_probe_roots ORDER BY root_id")?
        .query_map([], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    if roots.is_empty() {
        return Ok(false);
    }
    for root in roots {
        progress(0)?;
        let enumeration = list_files(&root_path(Path::new(&root)), |p| {
            progress(0)?;
            let (len, mtime) = metadata_stamp(&fs::metadata(&p).map_err(source_io)?);
            conn.execute(
                "INSERT OR IGNORE INTO identity_probe_files VALUES(?1,?2,?3)",
                params![path_text(&p)?, len, mtime],
            )?;
            Ok(())
        });
        match enumeration {
            Ok(summary) if summary.complete() => {}
            Ok(_) => return Ok(false),
            Err(error) if is_source_error(&error) => return Ok(false),
            Err(error) => return Err(error),
        }
    }
    // Missing registered objects cannot be certified by mere payload target_done.
    let missing:bool=conn.query_row("SELECT EXISTS(SELECT 1 FROM source_objects o JOIN source_roots r USING(root_id) WHERE r.identity_family=?1 AND r.namespace_key=?2 AND o.locator NOT IN(SELECT locator FROM identity_probe_files))",params![CLAUDE_FAMILY,CLAUDE_NAMESPACE],|r|r.get(0))?;
    if missing {
        return Ok(false);
    }
    Ok(true)
}
// Ordered, length-prefixed rows make this a fixed-size guard, never an unbounded
// list in a reader frame. The actual captured manifest stays in disk-backed TEMP.
fn identity_probe_digest(conn: &rusqlite::Connection, message: &str) -> Result<String> {
    let mut hash = Sha256::new();
    for sql in [
        "SELECT root_id,locator,parser_version,CAST(identity_version AS TEXT) FROM identity_probe_roots ORDER BY root_id",
        "SELECT locator,CAST(byte_length AS TEXT),COALESCE(CAST(mtime_ns AS TEXT),'missing') FROM identity_probe_files ORDER BY locator",
        "SELECT CAST(o.object_id AS TEXT),o.root_id,o.locator,o.content_generation,CAST(o.byte_length AS TEXT),COALESCE(CAST(o.mtime_ns AS TEXT),'missing'),COALESCE(o.target_generation,''),COALESCE(CAST(o.target_end AS TEXT),'missing'),o.parser_version FROM source_objects o JOIN identity_probe_roots r USING(root_id) ORDER BY o.object_id",
    ] {
        hash.update((sql.len() as u64).to_be_bytes()); hash.update(sql.as_bytes());
        let mut statement = conn.prepare(sql)?;
        let columns = statement.column_count();
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            for column in 0..columns {
                let value: String = row.get(column)?;
                hash.update((value.len() as u64).to_be_bytes()); hash.update(value.as_bytes());
            }
        }
    }
    // Match identity_values exactly: all saved revisions, including identity-only
    // and unresolved evidence, rather than just current selected observations.
    let mut stmt = conn.prepare("SELECT DISTINCT COALESCE(native_session_id,''),native_message_id,COALESCE(native_request_id,'') FROM usage_observations WHERE identity_family='claude' AND namespace_key='unknown:claude' AND native_message_id=?1 ORDER BY 1,2,3")?;
    let mut rows = stmt.query([message])?;
    while let Some(row) = rows.next()? {
        for column in 0..3 {
            let value: String = row.get(column)?;
            hash.update((value.len() as u64).to_be_bytes());
            hash.update(value.as_bytes());
        }
    }
    Ok(format!("{:x}", hash.finalize()))
}
fn identity_values(
    conn: &rusqlite::Connection,
    incoming: &Observation,
    request: Option<&str>,
    session: Option<&str>,
    want_sessions: bool,
    progress: &mut impl FnMut(u64) -> Result<()>,
) -> Result<Vec<String>> {
    let message = incoming
        .native_message_id
        .as_deref()
        .ok_or(LedgerError::IncompleteEvidence)?;
    let mut found = std::collections::BTreeSet::new();
    let mut collect = |s: Option<&str>, m: Option<&str>, r: Option<&str>| {
        if m != Some(message)
            || request.is_some_and(|x| Some(x) != r)
            || session.is_some_and(|x| Some(x) != s)
        {
            return;
        }
        if let Some(value) = if want_sessions { s } else { r } {
            if !value.is_empty() && found.len() < 2 {
                found.insert(value.to_owned());
            }
        }
    };
    collect(
        incoming.native_session_id.as_deref(),
        incoming.native_message_id.as_deref(),
        incoming.native_request_id.as_deref(),
    );
    {
        let mut stmt=conn.prepare("SELECT native_session_id,native_message_id,native_request_id FROM usage_observations WHERE identity_family='claude' AND namespace_key='unknown:claude' AND native_message_id=?1")?;
        let rows = stmt.query_map([message], |r| {
            Ok((
                r.get::<_, Option<String>>(0)?,
                r.get::<_, Option<String>>(1)?,
                r.get::<_, Option<String>>(2)?,
            ))
        })?;
        for row in rows {
            progress(0)?;
            let (s, m, r) = row?;
            collect(s.as_deref(), m.as_deref(), r.as_deref());
        }
    }
    let mut stmt = conn.prepare(
        "SELECT locator,byte_length,mtime_ns FROM identity_probe_files ORDER BY locator",
    )?;
    let rows = stmt.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, i64>(1)?,
            r.get::<_, Option<i64>>(2)?,
        ))
    })?;
    for row in rows {
        progress(0)?;
        let (path, len, mtime) = row?;
        require_stamp(Path::new(&path), len, mtime)?;
        let mut reader = BufReader::new(File::open(&path)?.take(len as u64));
        loop {
            let mut line = Vec::new();
            let n = read_bounded_line(&mut reader, &mut line, progress)?;
            if n == 0 {
                break;
            }
            if n > MAX_LINE_BYTES || !line.ends_with(b"\n") {
                return Err(LedgerError::IncompleteEvidence);
            }
            let value: serde_json::Value =
                serde_json::from_slice(&line).map_err(|_| LedgerError::IncompleteEvidence)?;
            if let Some((s, m, r)) = claude::identity_projection(&value)? {
                collect(s.as_deref(), m.as_deref(), r.as_deref());
            }
        }
        require_stamp(Path::new(&path), len, mtime)?;
    }
    Ok(found.into_iter().collect())
}
pub(crate) fn verify_identity_probe(conn: &rusqlite::Connection) -> Result<()> {
    let changed:bool=conn.query_row("SELECT EXISTS(SELECT root_id,locator,parser_version,identity_version FROM source_roots WHERE identity_family='claude' AND namespace_key='unknown:claude' EXCEPT SELECT * FROM identity_probe_roots) OR EXISTS(SELECT * FROM identity_probe_roots EXCEPT SELECT root_id,locator,parser_version,identity_version FROM source_roots WHERE identity_family='claude' AND namespace_key='unknown:claude')",[],|r|r.get(0))?;
    if changed {
        return Err(LedgerError::StaleObservation);
    }
    let mut stmt = conn.prepare("SELECT locator,byte_length,mtime_ns FROM identity_probe_files")?;
    for row in stmt.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, i64>(1)?,
            r.get::<_, Option<i64>>(2)?,
        ))
    })? {
        let (p, l, m) = row?;
        require_stamp(Path::new(&p), l, m)?;
    }
    // Detect newly created files as well as mutation of captured files.
    let roots: Vec<String> = conn
        .prepare("SELECT locator FROM identity_probe_roots")?
        .query_map([], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    let mut count = 0i64;
    for root in roots {
        let enumeration = list_files(&root_path(Path::new(&root)), |p| {
            let exists: bool = conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM identity_probe_files WHERE locator=?1)",
                [path_text(&p)?],
                |r| r.get(0),
            )?;
            if !exists {
                return Err(LedgerError::StaleObservation);
            }
            count += 1;
            Ok(())
        })?;
        if !enumeration.complete() {
            return Err(LedgerError::IncompleteEvidence);
        }
    }
    // Overlapping registered roots intentionally see a file more than once; identity still deduplicates.
    let expected: i64 = conn.query_row("SELECT COUNT(*) FROM identity_probe_files", [], |r| {
        r.get(0)
    })?;
    if count < expected {
        return Err(LedgerError::StaleObservation);
    }
    Ok(())
}

#[derive(Debug, Serialize)]
pub struct ClaudeAdmission {
    pub saved_candidates: u64,
    pub selected_events: u64,
    pub pending_identity: u64,
    pub pending_time: u64,
    pub conflicts: u64,
    pub identity_projections: u64,
    pub cumulative_facts: u64,
    pub incomplete_fields: u64,
    pub unclassified: u64,
}
impl Ledger {
    /// The adapter has a fixed, audited classification. Publication/classification precedes route change.
    /// Source I/O errors are visible partial coverage; they never veto healthy saved history.
    pub fn activate_claude_ledger(&mut self) -> Result<ClaudeAdmission> {
        self.require_writer()?;
        self.publish_pending()?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let roots: i64 = tx.query_row(
            "SELECT COUNT(*) FROM source_roots WHERE report_source='claude'",
            [],
            |r| r.get(0),
        )?;
        if roots == 0 {
            return Err(LedgerError::InvalidInput(
                "register a Claude root before activation".into(),
            ));
        }
        let invalid:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM source_roots WHERE report_source='claude' AND (identity_family<>'claude' OR namespace_key<>'unknown:claude' OR identity_version<>1 OR parser_version<>?1))",[PARSER_VERSION],|r|r.get(0))?;
        if invalid {
            return Err(LedgerError::IdentityMappingRequired);
        }
        let routes: i64 = tx.query_row(
            "SELECT COUNT(DISTINCT route) FROM source_roots WHERE report_source='claude'",
            [],
            |r| r.get(0),
        )?;
        if routes != 1 {
            return Err(LedgerError::RouteInconsistent);
        }
        let protected_live: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM source_roots WHERE report_source='claude' AND route='live') AND (EXISTS(SELECT 1 FROM legacy_capture_entries WHERE report_source='claude') OR EXISTS(SELECT 1 FROM claude_protection_pending))",
            [],
            |r| r.get(0),
        )?;
        if protected_live {
            return Err(LedgerError::InvalidInput(
                "Claude displayed history requires member-level reconciliation before route change"
                    .into(),
            ));
        }
        let count = |sql: &str| -> Result<u64> {
            Ok(tx.query_row(sql, [], |r| r.get::<_, i64>(0))? as u64)
        };
        let result=ClaudeAdmission{
            saved_candidates:count("SELECT COUNT(*) FROM usage_observations WHERE report_source='claude'")?,
            selected_events:count("SELECT COUNT(*) FROM usage_events WHERE report_source='claude' AND state='active'")?,
            pending_identity:count("SELECT COUNT(*) FROM usage_observations WHERE report_source='claude' AND eligibility='identity_unknown'")?,
            pending_time:count("SELECT COUNT(*) FROM usage_observations WHERE report_source='claude' AND eligibility='time_unknown'")?,
            conflicts:count("SELECT COUNT(*) FROM usage_events WHERE identity_family='claude' AND state='unresolved'")?,
            identity_projections:count("SELECT COUNT(*) FROM usage_observations WHERE report_source='claude' AND eligibility='identity_only'")?,
            cumulative_facts:count("SELECT COUNT(*) FROM usage_observations WHERE report_source='claude' AND fact_kind IN('session_total','session_cost')")?,
            incomplete_fields:count("SELECT COUNT(*) FROM usage_observations WHERE report_source='claude' AND fact_kind='request' AND eligibility<>'identity_only' AND (input_uncached IS NULL OR cache_read IS NULL OR cache_write IS NULL OR output_total IS NULL)")?,
            unclassified:count("SELECT COUNT(*) FROM usage_observations o LEFT JOIN identity_alias a ON a.identity_family=o.identity_family AND a.namespace_key=o.namespace_key AND a.alias_key=o.identity_key LEFT JOIN usage_events e ON e.identity_family=o.identity_family AND e.namespace_key=o.namespace_key AND e.identity_key=COALESCE(a.canonical_key,o.identity_key) WHERE o.report_source='claude' AND o.identity_key IS NOT NULL AND o.eligibility<>'identity_only' AND e.identity_key IS NULL")?,
        };
        if result.unclassified != 0 {
            return Err(LedgerError::IncompleteEvidence);
        }
        let invalid_selection:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM usage_events e JOIN usage_observations o ON o.observation_id=e.selected_observation_id WHERE e.identity_family='claude' AND (e.report_source IS NOT o.report_source OR e.model_key IS NOT o.model_key OR e.model_display IS NOT o.model_display OR e.fact_kind<>o.fact_kind OR e.occurred_ms IS NOT o.occurred_ms OR e.native_tool IS NOT o.native_tool OR e.session_key IS NOT o.session_key OR o.eligibility<>'eligible'))",[],|r|r.get(0))?;
        if invalid_selection {
            return Err(LedgerError::CorruptLedger);
        }
        let changed=tx.execute("UPDATE source_roots SET route='ledger' WHERE report_source='claude' AND route<>'ledger'",[])?;
        if changed > 0 {
            super::db::bump(&tx)?;
        }
        tx.commit()?;
        Ok(result)
    }
}

struct ActiveRead {
    object: Object,
    epoch: String,
    audit: bool,
    failed: bool,
    last_activity: Instant,
    identity: Option<(Observation, reader::IdentityPurpose)>,
    awaiting_identity: Option<(Observation, reader::IdentityPurpose)>,
    deferred_identity: bool,
    stream: bool,
    proof: Option<reader::PrefixProof>,
    proof_verified: bool,
}
pub(crate) struct ReaderSliceState {
    epoch: String,
    active: std::collections::BTreeMap<String, ActiveRead>,
    audit_state: Option<(i64, Sha256, i64)>,
    identity_inflight: bool,
    next_turn: i64,
}
impl ReaderSliceState {
    pub(crate) fn cancel(self, path: &Path) {
        let mut pool = reader::pool();
        for task in self.active.keys() {
            let _ = pool.cancel(task);
        }
        if pool.owner.as_deref() == Some(path) {
            pool.owner = None;
        }
    }
}
impl Ledger {
    fn requeue_object(
        &self,
        object: &Object,
        audit: bool,
        deferred: bool,
        turn: &mut i64,
    ) -> Result<()> {
        *turn = turn.checked_add(1).ok_or(LedgerError::NumericOverflow)?;
        self.conn.execute("INSERT OR REPLACE INTO acquisition_queue(object_id,root_id,turn,audit,deferred) VALUES(?1,?2,?3,?4,?5)", params![object.id,object.root,*turn,audit,deferred])?;
        Ok(())
    }
    fn drain_readers(
        &mut self,
        summary: &mut RefreshSummary,
        budget: Option<Duration>,
    ) -> Result<()> {
        let started = Instant::now();
        let mut pool = reader::pool();
        if pool
            .owner
            .as_deref()
            .is_some_and(|owner| owner != self.path.as_path())
        {
            return Err(LedgerError::Busy);
        }
        pool.owner = None;
        let epoch: String =
            self.conn
                .query_row("SELECT epoch FROM ledger_meta WHERE singleton=1", [], |r| {
                    r.get(0)
                })?;
        let (mut active, mut audit_state, mut identity_inflight, mut next_turn) =
            if let Some(mut state) = self.claude_readers.take() {
                // The writer deliberately paused with a bounded event queue. That
                // pause is backpressure, not a source's five-second I/O timeout.
                for task in state.active.values_mut() {
                    task.last_activity = Instant::now();
                }
                (
                    state.active,
                    state.audit_state,
                    state.identity_inflight,
                    state.next_turn,
                )
            } else {
                let turn = self.conn.query_row(
                    "SELECT COALESCE(MAX(turn),0) FROM acquisition_queue",
                    [],
                    |r| r.get(0),
                )?;
                (
                    std::collections::BTreeMap::<String, ActiveRead>::new(),
                    None,
                    false,
                    turn,
                )
            };
        loop {
            if budget.is_some_and(|limit| started.elapsed() >= limit) {
                pool.owner = Some(self.path.clone());
                self.claude_readers = Some(ReaderSliceState {
                    epoch,
                    active,
                    audit_state,
                    identity_inflight,
                    next_turn,
                });
                return Ok(());
            }

            while active.len() < pool.slots {
                let can_probe = !identity_inflight
                    && (pool.helper_available()
                        || (!pool.has_quarantined_reader() && pool.available() > 0));
                let queued:Option<(i64,bool)>=self.conn.query_row("SELECT q.object_id,q.audit FROM acquisition_queue q JOIN source_objects o USING(object_id) WHERE (q.audit=0 OR ?1 IS NULL OR q.object_id=?1) AND (?2 OR (o.error_code IS NOT 'SourceReadTimeout' AND ?3>0)) AND q.deferred<=?4 AND (o.byte_length<?5 OR ?6) ORDER BY q.deferred,q.turn,q.root_id,q.object_id LIMIT 1",params![audit_state.as_ref().map(|s|s.0),pool.helper_available(),pool.available() as i64,can_probe,PREFIX_SEAL_MIN_BYTES,!active.values().any(|t| t.stream)],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
                let Some((id, audit)) = queued else {
                    break;
                };
                let slow:bool=self.conn.query_row("SELECT error_code IS 'SourceReadTimeout' FROM source_objects WHERE object_id=?1",[id],|r|r.get(0))?;
                let use_helper = slow || pool.available() == 0;
                if use_helper && !pool.helper_available() {
                    break;
                }
                let mut object=self.conn.query_row("SELECT object_id,root_id,locator,content_generation,byte_length,mtime_ns,object_revision,COALESCE(saved_offset,0),target_done,state,retry_after_ms,audit_due_ms FROM source_objects WHERE object_id=?1",[id],object_row)?;
                let plan: Option<(i64,Vec<u8>,i64,Option<i64>)> = self.conn.query_row("SELECT prefix_len,digest,target_len,target_mtime FROM acquisition_proofs WHERE object_id=?1",[id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?;
                let resume = self.stream_checkpoint(&object)?;
                let mut proof = plan.map(|(len, digest, target_len, mtime)| {
                    object.len = target_len;
                    object.mtime = mtime;
                    reader::PrefixProof {
                        len,
                        digest,
                        resume: resume.clone(),
                    }
                });
                if proof.is_none() && object.offset > 0 {
                    if let Some(saved) = resume {
                        proof = Some(reader::PrefixProof {
                            len: object.offset,
                            digest: saved.digest.clone(),
                            resume: Some(saved),
                        });
                    }
                }
                let epoch: String = self.conn.query_row(
                    "SELECT epoch FROM ledger_meta WHERE singleton=1",
                    [],
                    |r| r.get(0),
                )?;
                if object.len >= PREFIX_SEAL_MIN_BYTES && proof.is_none() && object.offset > 0 {
                    self.restart_unproved_stream(&mut object, &epoch)?;
                }
                let stream = object.len >= PREFIX_SEAL_MIN_BYTES;
                if audit && !stream && audit_state.is_none() {
                    audit_state = Some((id, Sha256::new(), 0));
                }
                let task = new_epoch();
                let job = reader::Job {
                    task: task.clone(),
                    path: object.path.clone(),
                    root: object.root.clone(),
                    object_id: object.id,
                    generation: object.generation.clone(),
                    len: object.len,
                    mtime: object.mtime,
                    offset: object.offset,
                    audit,
                    stream,
                    proof: proof.clone(),
                    identity: None,
                    cancel: None,
                };
                self.conn
                    .execute("DELETE FROM acquisition_queue WHERE object_id=?1", [id])?;
                let dispatch = if use_helper {
                    pool.dispatch_helper(job)
                } else {
                    pool.dispatch(job)
                };
                if let Err(error) = dispatch {
                    summary.errors += 1;
                    self.fail_object(id, &error)?;
                    if audit {
                        audit_state = None;
                    }
                    continue;
                }
                summary.files_read += 1;
                active.insert(
                    task,
                    ActiveRead {
                        object,
                        epoch,
                        audit,
                        failed: false,
                        last_activity: Instant::now(),
                        identity: None,
                        awaiting_identity: None,
                        deferred_identity: false,
                        stream,
                        proof,
                        proof_verified: false,
                    },
                );
            }
            let queued: bool =
                self.conn
                    .query_row("SELECT EXISTS(SELECT 1 FROM acquisition_queue)", [], |r| {
                        r.get(0)
                    })?;
            if active.is_empty() && !queued {
                break;
            }
            let event = pool.next();
            if let Some(event) = event {
                let key = event.task().to_owned();
                let Some(mut task) = active.remove(&key) else {
                    continue;
                };
                task.last_activity = Instant::now();
                match event {
                    reader::Event::Progress { .. } => {
                        summary.reader_progress += 1;
                        active.insert(key, task);
                    }
                    reader::Event::Prefix {
                        accepted, version, ..
                    } => {
                        let result = reader::require_version(
                            &task.object.path,
                            task.object.len,
                            task.object.mtime,
                            &version,
                        )
                        .and_then(|_| {
                            if accepted {
                                self.accept_prefix(&mut task)
                            } else {
                                self.replay_rejected_prefix(&mut task)
                            }
                        });
                        match result {
                            Ok(()) if !accepted => {
                                self.requeue_object(
                                    &task.object,
                                    task.audit,
                                    false,
                                    &mut next_turn,
                                )?;
                                task.failed = true;
                            }
                            Ok(()) => {}
                            Err(LedgerError::StaleObservation) => {
                                if self.fail_read(&task, "STALE_OBSERVATION")? {
                                    summary.errors += 1;
                                }
                                task.failed = true;
                                pool.cancel(&key)?;
                            }
                            Err(error) => return Err(error),
                        }
                        active.insert(key, task);
                    }
                    reader::Event::Identity { proof, .. } => {
                        let outcome = match (task.identity.as_ref(), proof.as_ref()) {
                            (Some((o, purpose)), Some(proof)) => {
                                self.apply_identity_proof(o, proof, *purpose)
                            }
                            _ => Err(LedgerError::IdentityLinkNeedsReview),
                        };
                        match outcome {
                            Ok(()) => {
                                // Re-read from the last committed safe position. The
                                // candidate that caused the proof was never persisted.
                                self.requeue_object(&task.object, false, false, &mut next_turn)?;
                            }
                            Err(e)
                                if is_source_error(&e)
                                    || matches!(e, LedgerError::IncompleteEvidence) =>
                            {
                                self.fail_object(
                                    task.object.id,
                                    &LedgerError::IdentityLinkNeedsReview,
                                )?;
                                summary.errors += 1;
                            }
                            Err(e) => return Err(e),
                        }
                        task.failed = true;
                        active.insert(key, task);
                    }
                    reader::Event::Batch {
                        observations,
                        offset,
                        audit_bytes,
                        stream_state,
                        ignored,
                        version,
                        ..
                    } => {
                        if task.failed {
                            active.insert(key, task);
                            continue;
                        }
                        let mut weak = None;
                        for o in &observations {
                            if suppressed_with_old_counterpart(&self.conn, o)? {
                                weak = Some((
                                    o.clone(),
                                    reader::IdentityPurpose::IncomingAlreadySuppressed,
                                ));
                                break;
                            }
                            if weak_link(&self.conn, o)? {
                                weak = Some((
                                    o.clone(),
                                    reader::IdentityPurpose::CounterpartAlreadySuppressed,
                                ));
                                break;
                            }
                        }
                        if let Some((o, purpose)) = weak {
                            // Stop this payload reader at its previous safe cursor.
                            // No usages from the blocked batch enter the database.
                            if !self.mark_identity_wait(&task)? {
                                task.failed = true;
                                pool.cancel(&key)?;
                                active.insert(key, task);
                                continue;
                            }
                            let attempted: bool = self.conn.query_row("SELECT EXISTS(SELECT 1 FROM acquisition_seen WHERE object_id=?1 AND probe_attempted=1)", [task.object.id], |r| r.get(0))?;
                            if !attempted {
                                if !identity_inflight
                                    && (!pool.has_quarantined_reader() || pool.helper_available())
                                {
                                    identity_inflight = true;
                                    task.awaiting_identity = Some((o, purpose));
                                } else {
                                    // Retain only the object ID in the existing disk
                                    // queue after this payload reader has stopped.
                                    task.deferred_identity = true;
                                }
                            }
                            // One probe per object per refresh. A later weak record
                            // keeps its safe cursor for the next refresh, without
                            // treating the per-round budget as a source failure.
                            task.failed = true;
                            if task.audit {
                                audit_state = None;
                            }
                            pool.cancel(&key)?;
                            active.insert(key, task);
                            continue;
                        }
                        let saved = (|| -> Result<()> {
                            if task.proof.is_some() && !task.proof_verified {
                                return Err(LedgerError::StaleObservation);
                            }
                            if let Some(version) = version {
                                reader::require_version(
                                    &task.object.path,
                                    task.object.len,
                                    task.object.mtime,
                                    &version,
                                )?;
                            }
                            let done = !task.stream && offset == task.object.len;
                            self.save_batch(
                                &mut task.object,
                                &task.epoch,
                                &observations,
                                offset,
                                done,
                                stream_state.as_ref(),
                                summary,
                            )?;
                            if let Some(bytes) = audit_bytes {
                                let raw = reader::audit_bytes(&bytes)?;
                                if let Some((id, hash, hashed)) = audit_state.as_mut() {
                                    if *id == task.object.id {
                                        hash.update(&raw);
                                        *hashed += raw.len() as i64;
                                    }
                                }
                            }
                            summary.ignored += ignored;
                            Ok(())
                        })();
                        match saved {
                            Ok(()) => {
                                self.publish_some(100)?;
                                active.insert(key, task);
                            }
                            Err(LedgerError::StaleObservation) => {
                                if self.fail_read(&task, "STALE_OBSERVATION")? {
                                    summary.errors += 1;
                                }
                                task.failed = true;
                                pool.cancel(&key)?;
                                active.insert(key, task);
                            }
                            Err(e) if is_source_error(&e) => {
                                if self.fail_read(&task, e.code())? {
                                    summary.errors += 1;
                                }
                                if task.audit {
                                    audit_state = None;
                                }
                                task.failed = true;
                                active.insert(key, task);
                            }
                            Err(e) => return Err(e),
                        }
                    }
                    reader::Event::Done {
                        bytes,
                        parsed_bytes,
                        more,
                        error,
                        seal,
                        version,
                        ..
                    } => {
                        summary.bytes_read += bytes;
                        summary.parsed_bytes += parsed_bytes;
                        summary.verification_bytes += bytes.saturating_sub(parsed_bytes);
                        if task.deferred_identity {
                            self.requeue_object(&task.object, false, true, &mut next_turn)?;
                            continue;
                        }
                        if let Some((observation, purpose)) = task.awaiting_identity.take() {
                            let probe_task = new_epoch();
                            let job = reader::Job {
                                task: probe_task.clone(),
                                path: task.object.path.clone(),
                                root: task.object.root.clone(),
                                object_id: task.object.id,
                                generation: task.object.generation.clone(),
                                len: task.object.len,
                                mtime: task.object.mtime,
                                offset: task.object.offset,
                                audit: false,
                                stream: false,
                                proof: None,
                                cancel: None,
                                identity: Some(reader::IdentityJob {
                                    ledger: self.path.clone(),
                                    observation: observation.clone(),
                                    purpose,
                                }),
                            };
                            // A timed-out native probe keeps its original slot.
                            // Subsequent probes use the killable helper instead of
                            // consuming the remaining healthy reader slot.
                            let helper = pool.available() == 0 || pool.has_quarantined_reader();
                            let dispatched = if helper {
                                pool.dispatch_helper(job)
                            } else {
                                pool.dispatch(job)
                            };
                            if let Err(error) = dispatched {
                                identity_inflight = false;
                                if matches!(error, LedgerError::Busy) {
                                    self.requeue_object(&task.object, false, true, &mut next_turn)?;
                                } else {
                                    self.fail_object(
                                        task.object.id,
                                        &LedgerError::IdentityLinkNeedsReview,
                                    )?;
                                    summary.errors += 1;
                                }
                            } else {
                                self.conn.execute("INSERT INTO acquisition_seen(object_id,probe_attempted) VALUES(?1,1) ON CONFLICT(object_id) DO UPDATE SET probe_attempted=1", [task.object.id])?;
                                summary.identity_probes += 1;
                                task.identity = Some((observation, purpose));
                                task.audit = false;
                                task.failed = false;
                                task.last_activity = Instant::now();
                                active.insert(probe_task, task);
                            }
                            continue;
                        }
                        if task.identity.is_some() {
                            summary.identity_probe_bytes += bytes;
                            identity_inflight = false;
                            if !task.failed {
                                self.fail_object(
                                    task.object.id,
                                    &LedgerError::IdentityLinkNeedsReview,
                                )?;
                                summary.errors += 1;
                            }
                            continue;
                        }
                        if task.failed {
                            continue;
                        }
                        if let Some(code) = error {
                            if self.fail_read(&task, &code)? {
                                summary.errors += 1;
                            }
                            if task.audit {
                                audit_state = None;
                            }
                        } else if more {
                            self.requeue_object(&task.object, task.audit, false, &mut next_turn)?;
                        } else if let Some(digest) = seal {
                            let version = version.ok_or(LedgerError::StaleObservation)?;
                            let result = reader::require_version(
                                &task.object.path,
                                task.object.len,
                                task.object.mtime,
                                &version,
                            )
                            .and_then(|_| self.finish_stream(&mut task, &digest, summary));
                            match result {
                                Ok(()) => {}
                                Err(LedgerError::StaleObservation) => {
                                    if self.fail_read(&task, "STALE_OBSERVATION")? {
                                        summary.errors += 1;
                                    }
                                }
                                Err(error) => return Err(error),
                            }
                        } else if task.audit {
                            if let Some((id, hash, hashed)) = audit_state.take() {
                                if id == task.object.id && task.object.done {
                                    task.object.audit_hash = Some(hash);
                                    task.object.audit_hashed = hashed;
                                    self.complete_audit(&task.object, &task.epoch, summary)?;
                                }
                            }
                        }
                    }
                }
                continue;
            }
            // Only idle reception counts toward timeout; a busy bounded queue is backpressure.
            let timed_out: Vec<String> = active
                .iter()
                .filter(|(_, a)| a.last_activity.elapsed().as_secs() >= 5)
                .map(|(id, _)| id.clone())
                .collect();
            let had_timeouts = !timed_out.is_empty();
            for key in timed_out {
                if let Some(task) = active.remove(&key) {
                    pool.cancel(&key)?;
                    if task.identity.is_some()
                        || task.awaiting_identity.is_some()
                        || task.deferred_identity
                    {
                        if !task.deferred_identity {
                            identity_inflight = false;
                        }
                        self.fail_object(task.object.id, &LedgerError::IdentityLinkNeedsReview)?;
                        summary.errors += 1;
                        continue;
                    }
                    if self.fail_read(&task, "SourceReadTimeout")? {
                        summary.errors += 1;
                    }
                    if task.audit {
                        audit_state = None;
                    }
                }
            }
            // A timeout can release the identity scheduling gate. Give deferred
            // objects another dispatch turn before deciding no slot is usable.
            if had_timeouts {
                continue;
            }
            // Dispatch found no usable task and no active read can make progress.
            // This also covers deferred probes that require an unavailable helper
            // even while a native slot is reserved for healthy objects.
            if active.is_empty() && queued {
                return Err(LedgerError::IncompleteEvidence);
            }
        }
        if budget.is_some() {
            self.publish_some(100)?;
        } else {
            self.publish_pending()?;
        }
        Ok(())
    }
}

#[cfg(feature = "regression-hooks")]
pub fn stale_helper_completion_regression() -> bool {
    reader::stale_helper_completion_regression()
}

/// Exercise the asynchronous result/commit boundary with a deterministic change.
#[cfg(feature = "regression-hooks")]
pub fn identity_guard_change_regression(
    ledger: &mut Ledger,
    observation: &Observation,
    change: impl FnOnce(&mut Ledger) -> Result<()>,
) -> Result<()> {
    let reader = Ledger::open(ledger.path(), false)?;
    let proof = prove_identity_connection_with(
        &reader.conn,
        observation,
        reader::IdentityPurpose::CounterpartAlreadySuppressed,
        &mut |_| Ok(()),
    )?
    .ok_or(LedgerError::IncompleteEvidence)?;
    drop(reader);
    change(ledger)?;
    ledger.apply_identity_proof(
        observation,
        &proof,
        reader::IdentityPurpose::CounterpartAlreadySuppressed,
    )
}

#[cfg(feature = "regression-hooks")]
pub fn helper_input_backpressure_regression() -> Result<bool> {
    reader::helper_input_backpressure_regression()
}

#[cfg(feature = "regression-hooks")]
pub fn audit_heartbeat_regression(path: &Path) -> Result<bool> {
    reader::audit_heartbeat_regression(path)
}

/// Inject filesystem discovery failures without depending on platform permission semantics.
#[cfg(feature = "regression-hooks")]
pub fn discovery_fault_regression(
    root: &Path,
    fault_path: &Path,
    stage: &str,
    visit: impl FnMut(PathBuf) -> Result<()>,
) -> Result<(bool, bool, u64, Option<&'static str>)> {
    type Entries = Box<dyn Iterator<Item = std::io::Result<DiscoveryEntry>>>;
    let summary = list_files_with(root, visit, |path| -> std::io::Result<Entries> {
        if stage == "open" && path == fault_path {
            return Err(std::io::ErrorKind::PermissionDenied.into());
        }
        if stage == "next" && path == fault_path {
            // Infinite errors make accidental retries observable as a hanging regression.
            return Ok(Box::new(std::iter::repeat_with(|| {
                Err(std::io::ErrorKind::PermissionDenied.into())
            })));
        }
        let fault_path = fault_path.to_owned();
        let type_fault = stage == "file_type";
        let entry_fault = stage == "entry";
        Ok(Box::new(fs::read_dir(path)?.map(move |entry| {
            let mut entry = discovery_entry(entry)?;
            if entry.path == fault_path {
                if entry_fault {
                    return Err(std::io::ErrorKind::PermissionDenied.into());
                }
                if type_fault {
                    entry.kind = Err(std::io::ErrorKind::PermissionDenied.into());
                }
            }
            Ok(entry)
        })))
    })?;
    Ok((
        summary.complete(),
        summary.status == EnumerationStatus::RootUnavailable,
        summary.errors,
        summary.first_error.as_ref().map(LedgerError::code),
    ))
}

/// Exercise both strict identity discovery passes, including changes between them.
#[cfg(feature = "regression-hooks")]
pub fn identity_discovery_regression(
    ledger: &mut Ledger,
    progress: &mut impl FnMut(u64) -> Result<()>,
    change: impl FnOnce(&mut Ledger) -> Result<()>,
) -> Result<()> {
    if !capture_identity_probe(&ledger.conn, progress)? {
        return Err(LedgerError::IncompleteEvidence);
    }
    change(ledger)?;
    verify_identity_probe(&ledger.conn)
}

#[cfg(feature = "regression-hooks")]
pub fn stream_reader_regression(
    path: &Path,
    prefix_len: i64,
    digest: Vec<u8>,
    callback: impl FnMut(&str) -> Result<bool>,
) -> Result<serde_json::Value> {
    reader::stream_reader_regression(path, prefix_len, digest, callback)
}

#[cfg(feature = "regression-hooks")]
pub fn stream_guard_regression(
    ledger: &mut Ledger,
    stage: &str,
    change: impl FnOnce(&mut Ledger) -> Result<()>,
) -> Result<()> {
    let object=ledger.conn.query_row("SELECT object_id,root_id,locator,content_generation,byte_length,mtime_ns,object_revision,COALESCE(saved_offset,0),target_done,state,retry_after_ms,audit_due_ms FROM source_objects LIMIT 1",[],object_row)?;
    let digest: Vec<u8> = ledger.conn.query_row(
        "SELECT quick_fingerprint FROM source_objects LIMIT 1",
        [],
        |r| r.get(0),
    )?;
    let epoch: String = ledger
        .conn
        .query_row("SELECT epoch FROM ledger_meta", [], |r| r.get(0))?;
    let mut task = ActiveRead {
        proof: Some(reader::PrefixProof {
            len: object.len,
            digest: digest.clone(),
            resume: None,
        }),
        object,
        epoch,
        audit: true,
        failed: false,
        last_activity: Instant::now(),
        identity: None,
        awaiting_identity: None,
        deferred_identity: false,
        stream: true,
        proof_verified: true,
    };
    change(ledger)?;
    match stage {
        "prefix" => ledger.accept_prefix(&mut task),
        "batch" => {
            let offset = task.object.offset;
            ledger.save_batch(
                &mut task.object,
                &task.epoch,
                &[],
                offset,
                true,
                None,
                &mut RefreshSummary::default(),
            )
        }
        "seal" => ledger.finish_stream(&mut task, &digest, &mut RefreshSummary::default()),
        "restart" => ledger.restart_unproved_stream(&mut task.object, &task.epoch),
        _ => Err(LedgerError::InvalidInput("bad regression stage".into())),
    }
}

#[cfg(feature = "regression-hooks")]
pub fn helper_stream_regression(
    path: &Path,
    prefix_len: i64,
    digest: Vec<u8>,
) -> Result<serde_json::Value> {
    reader::helper_stream_regression(path, prefix_len, digest)
}
