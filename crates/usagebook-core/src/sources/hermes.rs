//! Hermes sessions-only accounting; see docs/数据源档案/25-hermes.md.
//! Output contains reasoning. Archive visibility is not billing eligibility.
//! Only standalone rows and fully proved CLI compression chains are additive.

use crate::common::{env_roots, file_stamp, home, SourceFingerprint};
use crate::model::{Caliber, PreparedSourceScan, ScanStats, UsageRecord, UsageSource};
use rusqlite::types::Value as SqlValue;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

pub struct HermesSource;

#[cfg(feature = "regression-hooks")]
type SnapshotHook = (PathBuf, Box<dyn FnOnce()>);
#[cfg(feature = "regression-hooks")]
thread_local! {
    static AFTER_SNAPSHOT: std::cell::RefCell<Option<SnapshotHook>> = const { std::cell::RefCell::new(None) };
}
/// Run an isolated concurrent writer after SQLite has pinned the read snapshot.
#[cfg(feature = "regression-hooks")]
#[doc(hidden)]
pub fn __after_sqlite_snapshot_for_tests(path: PathBuf, hook: impl FnOnce() + 'static) {
    let path = path.canonicalize().unwrap_or(path);
    AFTER_SNAPSHOT.with(|slot| *slot.borrow_mut() = Some((path, Box::new(hook))));
}

fn failure(stats: &mut ScanStats, reason: &str) {
    stats.failed = true;
    let note = format!("Hermes 用量不完整: {reason}");
    if !stats.notes.contains(&note) {
        stats.notes.push(note);
    }
}

// Preserve supported integer, real and numeric-text shapes, but never turn an
// invalid counter into zero (in particular, malformed reasoning is not absent).
fn counter(v: &SqlValue) -> Option<u64> {
    match v {
        SqlValue::Null => Some(0),
        SqlValue::Integer(i) if *i >= 0 => Some(*i as u64),
        SqlValue::Real(f) if f.is_finite() && *f >= 0.0 && *f < u64::MAX as f64 => Some(*f as u64),
        SqlValue::Text(s) => s.trim().parse().ok(),
        _ => None,
    }
}
fn cost(v: &SqlValue) -> Result<Option<f64>, ()> {
    let n = match v {
        SqlValue::Null => return Ok(None),
        SqlValue::Real(f) => Some(*f),
        SqlValue::Integer(i) => Some(*i as f64),
        SqlValue::Text(s) => s.trim().parse().ok(),
        _ => None,
    };
    n.filter(|n| n.is_finite() && *n >= 0.0).map(Some).ok_or(())
}
fn text(v: &SqlValue) -> Option<String> {
    match v {
        SqlValue::Text(s) => Some(s.clone()),
        SqlValue::Integer(i) => Some(i.to_string()),
        _ => None,
    }
}
fn identifier(v: &SqlValue) -> Option<String> {
    text(v).filter(|s| !s.trim().is_empty())
}
fn timestamp(v: &SqlValue) -> Option<i64> {
    match v {
        SqlValue::Integer(n) if *n > 0 => Some(crate::common::secs_or_ms(*n)),
        SqlValue::Real(f) => seconds(*f),
        SqlValue::Text(s) => seconds(s.trim().parse().ok()?),
        _ => None,
    }
}
fn seconds(f: f64) -> Option<i64> {
    (f.is_finite() && f > 0.0 && f * 1000.0 < i64::MAX as f64).then_some((f * 1000.0) as i64)
}
const MARKERS: [&str; 3] = ["_branched_from", "_delegate_from", "_reset_from"];
fn markers(v: &SqlValue) -> Result<[Option<String>; 3], ()> {
    if matches!(v, SqlValue::Null) {
        return Ok(Default::default());
    }
    let SqlValue::Text(s) = v else { return Err(()) };
    let value: serde_json::Value = serde_json::from_str(s).map_err(|_| ())?;
    let obj = value.as_object().ok_or(())?;
    let mut out = [None, None, None];
    for (i, name) in MARKERS.iter().enumerate() {
        out[i] = match obj.get(*name) {
            None | Some(serde_json::Value::Null) => None,
            Some(serde_json::Value::String(s)) if !s.trim().is_empty() => Some(s.clone()),
            _ => return Err(()),
        };
    }
    Ok(out)
}
struct Session {
    id: String,
    parent: Result<Option<String>, ()>,
    source: Option<String>,
    markers: Result<[Option<String>; 3], ()>,
    start: Option<i64>,
    end: Option<i64>,
    end_reason: Option<String>,
    record: Option<UsageRecord>,
}
const MAX_CHAIN: usize = 1024;

/// A marker inherited from an ancestor is not evidence against this direct edge.
/// It still cannot make a branch/delegate/reset ancestor edge additive: every
/// edge to the root must be independently proved. No guessed subtraction/max.
fn proved_chain(id: &str, rows: &HashMap<String, Session>, full_schema: bool) -> bool {
    let Some(row) = rows.get(id) else {
        return false;
    };
    if matches!(row.parent, Ok(None)) {
        return true;
    }
    if !full_schema {
        return false;
    }
    let mut chain = Vec::new();
    let mut seen = HashSet::new();
    let mut current = row;
    loop {
        if chain.len() >= MAX_CHAIN || !seen.insert(current.id.as_str()) {
            return false;
        }
        chain.push(current);
        match &current.parent {
            Ok(None) => break,
            Ok(Some(parent)) => match rows.get(parent) {
                Some(parent) => current = parent,
                None => return false,
            },
            Err(_) => return false,
        }
    }
    for (index, pair) in chain.windows(2).enumerate() {
        let (child, parent) = (pair[0], pair[1]);
        if child.source.as_deref() != Some("cli")
            || parent.source.as_deref() != Some("cli")
            || parent.end_reason.as_deref() != Some("compression")
        {
            return false;
        }
        let (Some(ps), Some(pe), Some(cs)) = (parent.start, parent.end, child.start) else {
            return false;
        };
        // Official publication inserts the child BEFORE ending the parent.
        // Neither ordering of cs vs pe establishes or disproves the edge.
        if pe < ps || cs < ps {
            return false;
        }
        let (Ok(cm), Ok(pm)) = (&child.markers, &parent.markers) else {
            return false;
        };
        for (slot, marker) in cm.iter().enumerate() {
            if pm[slot].is_some() && pm[slot] != *marker {
                return false;
            }
            if let Some(marker) = marker {
                if marker == &parent.id
                    || pm[slot].as_ref() != Some(marker)
                    || !chain[index + 2..].iter().any(|a| &a.id == marker)
                {
                    return false;
                }
            }
        }
    }
    true
}

const COLUMNS: [&str; 15] = [
    "id",
    "model",
    "started_at",
    "input_tokens",
    "output_tokens",
    "cache_read_tokens",
    "cache_write_tokens",
    "reasoning_tokens",
    "actual_cost_usd",
    "estimated_cost_usd",
    "parent_session_id",
    "source",
    "model_config",
    "ended_at",
    "end_reason",
];
fn read_database(db: &Path, domain: &str, stats: &mut ScanStats) -> Result<Vec<UsageRecord>, ()> {
    let conn = crate::common::open_sqlite_readonly(db).map_err(|_| ())?;
    conn.execute_batch("BEGIN DEFERRED").map_err(|_| ())?;
    let exists: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='sessions')",
            [],
            |r| r.get(0),
        )
        .map_err(|_| ())?;
    if !exists {
        return Ok(Vec::new());
    }
    let mut stmt = conn
        .prepare("PRAGMA table_info(sessions)")
        .map_err(|_| ())?;
    let columns = stmt
        .query_map([], |r| r.get::<_, String>(1))
        .map_err(|_| ())?
        .collect::<Result<HashSet<_>, _>>()
        .map_err(|_| ())?;
    if ["id", "model", "started_at"]
        .iter()
        .any(|c| !columns.contains(*c))
    {
        return Err(());
    }
    let full_schema = COLUMNS.iter().all(|c| columns.contains(*c));
    let sql = format!(
        "SELECT {} FROM sessions",
        COLUMNS
            .iter()
            .map(|c| {
                if columns.contains(*c) {
                    (*c).to_string()
                } else {
                    format!("NULL AS {c}")
                }
            })
            .collect::<Vec<_>>()
            .join(",")
    );
    #[cfg(feature = "regression-hooks")]
    {
        let hook = AFTER_SNAPSHOT.with(|slot| {
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
    let mut stmt = conn.prepare(&sql).map_err(|_| ())?;
    let mut cursor = stmt.query([]).map_err(|_| ())?;
    let mut sessions = HashMap::new();
    let mut duplicate_ids = HashSet::new();
    while let Some(row) = cursor.next().map_err(|_| ())? {
        stats.lines_total += 1;
        let vals = (0..COLUMNS.len())
            .map(|i| row.get::<_, SqlValue>(i))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| ())?;
        let Some(id) = identifier(&vals[0]) else {
            failure(stats, "会话身份无效");
            continue;
        };
        let parent = match &vals[10] {
            SqlValue::Null => Ok(None),
            v => identifier(v).map(Some).ok_or(()),
        };
        let start = timestamp(&vals[2]);
        let counts = vals[3..8].iter().map(counter).collect::<Option<Vec<_>>>();
        let costs = cost(&vals[8]).and_then(|actual| match actual {
            Some(_) => Ok(actual),
            None => cost(&vals[9]),
        });
        let model = text(&vals[1]).unwrap_or_default().trim().to_string();
        let record = match (counts, costs) {
            (Some(n), Ok(cost_usd)) if n[4] <= n[1] => {
                let has_usage = n.iter().any(|n| *n > 0) || cost_usd.is_some_and(|c| c > 0.0);
                if !has_usage || model.is_empty() {
                    None
                } else if let Some(ts_ms) = start {
                    Some(UsageRecord {
                        source: "hermes",
                        session_id: id.clone(),
                        agent: None,
                        model,
                        ts_ms,
                        caliber: Caliber::RequestDelta,
                        request_count: 1,
                        input_uncached: n[0],
                        cache_read: n[2],
                        cache_write: n[3],
                        cache_write_1h: 0,
                        output: n[1] - n[4],
                        reasoning_output: n[4],
                        cost_usd,
                        dedup_key: format!("hermes:{domain}:{id}"),
                    })
                } else {
                    failure(stats, "会话时间无效");
                    None
                }
            }
            _ => {
                failure(stats, "计量值或思考子集无效");
                None
            }
        };
        let session = Session {
            id: id.clone(),
            parent,
            source: text(&vals[11]),
            markers: markers(&vals[12]),
            start,
            end: timestamp(&vals[13]),
            end_reason: text(&vals[14]),
            record,
        };
        if sessions.insert(id.clone(), session).is_some() {
            duplicate_ids.insert(id);
            failure(stats, "会话身份不唯一");
        }
    }
    drop(cursor);
    drop(stmt);
    conn.execute_batch("COMMIT").map_err(|_| ())?;
    for id in duplicate_ids {
        sessions.remove(&id);
    }
    let mut records = Vec::new();
    for row in sessions.values() {
        if let Some(record) = &row.record {
            if proved_chain(&row.id, &sessions, full_schema) {
                records.push(record.clone());
            } else {
                failure(stats, "子会话关系未证实");
            }
        }
    }
    records.sort_by(|a, b| a.dedup_key.cmp(&b.dedup_key));
    Ok(records)
}

struct DatabaseInput {
    path: PathBuf,
    domain: String,
}
struct Inputs {
    databases: Vec<DatabaseInput>,
    fingerprint: SourceFingerprint,
    failed: bool,
}
impl Inputs {
    fn discover(roots: &[PathBuf]) -> Self {
        let mut input = Self {
            databases: Vec::new(),
            fingerprint: Vec::new(),
            failed: false,
        };
        let mut blocked = HashSet::new();
        let mut aliases = roots.to_vec();
        aliases.sort();
        aliases.dedup();
        for alias in aliases {
            let Ok(path) = alias.canonicalize() else {
                input.failed = true;
                continue;
            };
            if !path.is_file() {
                input.failed = true;
                continue;
            }
            let Some(stamp) = file_stamp(&path) else {
                input.failed = true;
                continue;
            };
            let identity = stamp.identity_key();
            let ids = identity.split('/').take(2).collect::<Vec<_>>();
            let domain = if ids.len() == 2 && ids.iter().all(|s| !s.is_empty()) {
                format!("file:{}/{}", ids[0], ids[1])
            } else {
                format!("path:{}", path.display())
            };
            input.fingerprint.push((alias.clone(), stamp.clone()));
            input.fingerprint.push((path.clone(), stamp));
            // A symlink to the main DB has sidecars beside its canonical target.
            // Observe both names (including hard-link aliases), never only alias-wal.
            for base in [&alias, &path] {
                for suffix in ["-wal", "-shm", "-journal"] {
                    let mut name = base.as_os_str().to_os_string();
                    name.push(suffix);
                    let p = PathBuf::from(name);
                    match std::fs::symlink_metadata(&p) {
                        Ok(_) => match file_stamp(&p) {
                            Some(s) => input.fingerprint.push((p, s)),
                            None => input.failed = true,
                        },
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                        Err(_) => input.failed = true,
                    }
                }
            }
            if let Some(existing) = input.databases.iter_mut().find(|db| db.domain == domain) {
                if existing.path != path {
                    let wal_exists = |p: &Path| {
                        let mut name = p.as_os_str().to_os_string();
                        name.push("-wal");
                        PathBuf::from(name).is_file()
                    };
                    match (wal_exists(&existing.path), wal_exists(&path)) {
                        (false, true) => existing.path = path,
                        (true, true) => {
                            input.failed = true;
                            blocked.insert(domain);
                        }
                        _ => {}
                    }
                }
            } else {
                input.databases.push(DatabaseInput { path, domain });
            }
        }
        input.databases.retain(|db| !blocked.contains(&db.domain));
        input.fingerprint.sort_by(|a, b| a.0.cmp(&b.0));
        input.fingerprint.dedup_by(|a, b| a.0 == b.0);
        input
    }
}
impl PreparedSourceScan for Inputs {
    fn fingerprint(&self) -> Option<SourceFingerprint> {
        (!self.failed).then(|| self.fingerprint.clone())
    }
    fn scan(&self) -> (Vec<UsageRecord>, ScanStats) {
        let mut stats = ScanStats::default();
        let mut records = Vec::new();
        if self.failed {
            failure(&mut stats, "数据库或旁路文件不可读");
        }
        for db in &self.databases {
            stats.files_seen += 1;
            match read_database(&db.path, &db.domain, &mut stats) {
                Ok(rows) => {
                    stats.files_parsed += 1;
                    records.extend(rows);
                }
                Err(_) => failure(&mut stats, "数据库一致读取失败"),
            }
        }
        (records, stats)
    }
}
impl UsageSource for HermesSource {
    fn id(&self) -> &'static str {
        "hermes"
    }
    fn discover_roots(&self) -> Vec<PathBuf> {
        let mut dbs = Vec::new();
        let mut homes = env_roots("HERMES_HOME");
        if homes.is_empty() {
            if let Ok(la) = std::env::var("LOCALAPPDATA") {
                if !la.trim().is_empty() {
                    homes.push(PathBuf::from(la.trim()).join("hermes"));
                }
            }
            homes.push(home().join(".hermes"));
        }
        for home in homes {
            let main = home.join("state.db");
            if main.is_file() {
                dbs.push(main);
            }
            if let Ok(rd) = std::fs::read_dir(home.join("profiles")) {
                for e in rd.flatten() {
                    let db = e.path().join("state.db");
                    if db.is_file() {
                        dbs.push(db);
                    }
                }
            }
        }
        dbs
    }
    fn prepare_scan(&self, roots: &[PathBuf]) -> Option<Box<dyn PreparedSourceScan>> {
        Some(Box::new(Inputs::discover(roots)))
    }
    fn scan_roots(
        &self,
        roots: &[PathBuf],
        _max_files: Option<usize>,
    ) -> (Vec<UsageRecord>, ScanStats) {
        Inputs::discover(roots).scan()
    }
}
