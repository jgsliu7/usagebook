//! Goose: real provider calls and undated historical evidence are separate.
//! Both tables are reconciled in one read transaction, scoped to physical database
//! identity plus session ID. See docs/数据源档案/23-goose.md §7.
use crate::common::{env_roots, home, iso_to_ms, secs_or_ms};
use crate::model::{Caliber, ScanStats, SessionTotalPrecision, UsageRecord, UsageSource};
use chrono::{NaiveDate, NaiveDateTime};
use rusqlite::{types::Value as SqlValue, Connection};
use std::{
    collections::{BTreeMap, HashSet},
    path::{Path, PathBuf},
    time::Duration,
};

pub struct GooseSource;
fn goose_time(v: &SqlValue) -> Option<i64> {
    match v {
        SqlValue::Text(s) => iso_to_ms(s)
            .or_else(|| s.parse::<i64>().ok().map(secs_or_ms))
            .or_else(|| {
                NaiveDate::parse_from_str(s, "%Y-%m-%d")
                    .ok()?
                    .and_hms_opt(0, 0, 0)
                    .map(|t| t.and_utc().timestamp_millis())
            })
            .or_else(|| {
                NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                    .ok()
                    .map(|t| t.and_utc().timestamp_millis())
            }),
        SqlValue::Integer(n) if *n > 0 => Some(secs_or_ms(*n)),
        SqlValue::Real(n) if n.is_finite() && *n > 0.0 && *n < i64::MAX as f64 => {
            Some(secs_or_ms(*n as i64))
        }
        _ => None,
    }
}
fn text(v: &SqlValue) -> Option<String> {
    match v {
        SqlValue::Text(s) => Some(s.clone()),
        SqlValue::Integer(n) => Some(n.to_string()),
        _ => None,
    }
}
fn number(v: &SqlValue) -> Result<Option<i64>, ()> {
    match v {
        SqlValue::Null => Ok(None),
        SqlValue::Integer(n) if *n >= 0 => Ok(Some(*n)),
        SqlValue::Text(s) => s
            .trim()
            .parse::<i64>()
            .ok()
            .filter(|n| *n >= 0)
            .map(Some)
            .ok_or(()),
        SqlValue::Real(n)
            if n.is_finite() && *n >= 0.0 && n.fract() == 0.0 && *n < i64::MAX as f64 =>
        {
            Ok(Some(*n as i64))
        }
        _ => Err(()),
    }
}
// inclusive input, output, total, cache read, cache write. A complete comparable
// vector is mandatory for subtraction; never clamp individual differences.
#[derive(Clone, Debug)]
struct Evidence {
    values: [Option<i64>; 5],
    invalid: bool,
}
impl Evidence {
    fn read(values: &[SqlValue]) -> Self {
        let mut out = Self {
            values: [None; 5],
            invalid: false,
        };
        for (i, v) in values.iter().take(5).enumerate() {
            match number(v) {
                Ok(n) => out.values[i] = n,
                Err(()) => out.invalid = true,
            }
        }
        if crate::ledger::metrics::evaluate(&out.projection()).is_err() {
            out.invalid = true;
        }
        out
    }
    fn projection(&self) -> [Option<i64>; 10] {
        let [input, output, total, read, write] = self.values;
        let uncached = input
            .zip(read)
            .zip(write)
            .and_then(|((i, r), w)| i.checked_sub(r)?.checked_sub(w))
            .filter(|n| *n >= 0);
        [
            uncached,
            read,
            write,
            output,
            Some(0),
            input,
            output,
            total,
            None,
            None,
        ]
    }
    fn complete(&self) -> Option<[i64; 5]> {
        if self.invalid {
            return None;
        }
        Some([
            self.values[0]?,
            self.values[1]?,
            self.values[2]?,
            self.values[3]?,
            self.values[4]?,
        ])
    }
    fn present(&self) -> bool {
        self.invalid || self.values.iter().any(Option::is_some)
    }
    fn nonzero(&self) -> bool {
        self.values.iter().flatten().any(|n| *n > 0)
    }
}
#[derive(Default)]
struct Session {
    accumulated: Option<Evidence>,
    // Non-accumulated columns are a latest-context snapshot, never a fallback.
    latest_only: bool,
    requests: Vec<[i64; 5]>,
    carried: Vec<Evidence>,
    invalid_detail: bool,
}
fn sum_vectors<'a>(vectors: impl Iterator<Item = &'a [i64; 5]>) -> Option<[i64; 5]> {
    let mut total = [0i64; 5];
    for v in vectors {
        for i in 0..5 {
            total[i] = total[i].checked_add(v[i])?;
        }
    }
    Some(total)
}
fn residual(a: [i64; 5], d: [i64; 5]) -> Option<[i64; 5]> {
    let mut out = [0i64; 5];
    for i in 0..5 {
        out[i] = a[i].checked_sub(d[i]).filter(|n| *n >= 0)?;
    }
    let evidence = Evidence {
        values: out.map(Some),
        invalid: false,
    };
    crate::ledger::metrics::evaluate(&evidence.projection()).ok()?;
    Some(out)
}
fn physical_id(path: &Path) -> Option<String> {
    let stamp = crate::common::file_stamp(path)?;
    let identity = stamp.identity_key();
    if !identity.is_empty() {
        Some(identity.split('/').take(2).collect::<Vec<_>>().join("/"))
    } else {
        path.canonicalize()
            .ok()
            .map(|p| p.to_string_lossy().into_owned())
    }
}
fn columns(conn: &Connection, table: &str) -> rusqlite::Result<HashSet<String>> {
    let mut st = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let result = st.query_map([], |r| r.get(1))?.collect();
    result
}
fn optional(cols: &HashSet<String>, name: &str) -> String {
    if cols.contains(name) {
        name.into()
    } else {
        "NULL".into()
    }
}
fn values(row: &rusqlite::Row<'_>, count: usize) -> rusqlite::Result<Vec<SqlValue>> {
    (0..count).map(|i| row.get(i)).collect()
}
fn record(session: &str, key: String, metrics: [Option<i64>; 10], caliber: Caliber) -> UsageRecord {
    UsageRecord {
        source: "goose",
        session_id: session.into(),
        agent: None,
        model: String::new(),
        ts_ms: 0,
        caliber,
        input_uncached: metrics[0].unwrap_or(0) as u64,
        cache_read: metrics[1].unwrap_or(0) as u64,
        cache_write: metrics[2].unwrap_or(0) as u64,
        cache_write_1h: 0,
        output: metrics[3].unwrap_or(0) as u64,
        reasoning_output: 0,
        cost_usd: None,
        request_count: 0,
        dedup_key: key,
    }
}
impl UsageSource for GooseSource {
    fn id(&self) -> &'static str {
        "goose"
    }
    /// roots 直接返回 sessions.db 文件路径(而非目录):本源不递归,多库并列(§1)。
    fn discover_roots(&self) -> Vec<PathBuf> {
        let mut dbs: Vec<PathBuf> = Vec::new();
        if let Ok(v) = std::env::var("AI_USAGE_GOOSE_DB") {
            for seg in v.split(',') {
                let p = PathBuf::from(seg.trim());
                if !p.as_os_str().is_empty() && p.is_file() {
                    dbs.push(p);
                }
            }
        }
        for root in env_roots("GOOSE_PATH_ROOT") {
            let db = root.join("data/sessions/sessions.db");
            if db.is_file() {
                dbs.push(db);
            }
        }
        if dbs.is_empty() {
            let h = home();
            // §6.1:现行默认是 XDG(~/.local/share/goose);Library 与 Block 形态为
            // 旧版/平台兜底;Windows 为 %APPDATA%\Block\goose\data\sessions。
            let mut cands = vec![
                h.join(".local/share/goose/sessions/sessions.db"),
                h.join("Library/Application Support/goose/sessions/sessions.db"),
                h.join(".local/share/Block/goose/sessions/sessions.db"),
            ];
            if let Ok(appdata) = std::env::var("APPDATA") {
                if !appdata.trim().is_empty() {
                    cands.push(
                        std::path::PathBuf::from(appdata.trim())
                            .join("Block/goose/data/sessions/sessions.db"),
                    );
                }
            }
            for c in cands {
                if c.is_file() {
                    dbs.push(c);
                }
            }
        }
        // 规范化去重(同库经不同路径写法发现两次)
        let mut seen = HashSet::new();
        dbs.retain(|p| p.canonicalize().ok().is_some_and(|c| seen.insert(c)));
        dbs
    }

    fn scan_roots(
        &self,
        roots: &[PathBuf],
        _max_files: Option<usize>,
    ) -> (Vec<UsageRecord>, ScanStats) {
        let mut stats = ScanStats::default();
        let mut records = Vec::new();
        let mut seen = HashSet::new();
        for db in roots {
            stats.files_seen += 1;
            let Some(identity) = physical_id(db) else {
                stats.failed = true;
                stats.notes.push("goose 数据库身份不可读".into());
                continue;
            };
            if !seen.insert(identity.clone()) {
                continue;
            }
            match scan_database(db, &identity) {
                Ok((mut rows, mut evidence, count)) => {
                    stats.files_parsed += 1;
                    stats.lines_total += count;
                    records.append(&mut rows);
                    stats.session_total_precision.append(&mut evidence);
                }
                Err(error) => {
                    stats.failed = true;
                    stats.notes.push(format!("goose 数据库读取失败: {error}"));
                }
            }
        }
        if !stats.session_total_precision.is_empty() {
            stats.notes.push(format!(
                "goose {} 个会话有未分配日期/模型的累计或对账证据",
                stats.session_total_precision.len()
            ));
        }
        (records, stats)
    }
}
fn scan_database(
    db: &Path,
    identity: &str,
) -> rusqlite::Result<(
    Vec<UsageRecord>,
    BTreeMap<String, SessionTotalPrecision>,
    u64,
)> {
    let mut conn = crate::common::open_sqlite_readonly(db)?;
    conn.busy_timeout(Duration::from_secs(5))?;
    conn.pragma_update(None, "query_only", true)?;
    let tx = conn.transaction()?;
    let sc = columns(&tx, "sessions")?;
    if !sc.contains("id") {
        return Err(rusqlite::Error::InvalidQuery);
    }
    let mut sessions = BTreeMap::<String, Session>::new();
    let names = [
        "accumulated_input_tokens",
        "accumulated_output_tokens",
        "accumulated_total_tokens",
        "accumulated_cache_read_tokens",
        "accumulated_cache_write_tokens",
        "input_tokens",
        "output_tokens",
        "total_tokens",
    ];
    let sql = format!(
        "SELECT id,{} FROM sessions",
        names
            .iter()
            .map(|n| optional(&sc, n))
            .collect::<Vec<_>>()
            .join(",")
    );
    let mut count = 0;
    {
        let mut st = tx.prepare(&sql)?;
        let mut rows = st.query([])?;
        while let Some(row) = rows.next()? {
            let v = values(row, 9)?;
            count += 1;
            let id = text(&v[0])
                .filter(|s| !s.is_empty())
                .ok_or(rusqlite::Error::InvalidQuery)?;
            let a = Evidence::read(&v[1..6]);
            let latest_only = v[6..9]
                .iter()
                .any(|v| number(v).is_ok_and(|n| n.is_some_and(|n| n > 0)))
                && !a.nonzero();
            sessions.insert(
                id,
                Session {
                    accumulated: Some(a),
                    latest_only,
                    ..Session::default()
                },
            );
        }
    }
    let lc = columns(&tx, "usage_ledger")?;
    let mut records = Vec::new();
    if !lc.is_empty() {
        for required in [
            "id",
            "session_id",
            "created_timestamp",
            "model",
            "input_tokens",
            "output_tokens",
            "cache_read_tokens",
            "cache_write_tokens",
        ] {
            if !lc.contains(required) {
                return Err(rusqlite::Error::InvalidQuery);
            }
        }
        let sql=format!("SELECT id,session_id,created_timestamp,model,input_tokens,output_tokens,{},cache_read_tokens,cache_write_tokens,{},{} FROM usage_ledger ORDER BY id",optional(&lc,"total_tokens"),optional(&lc,"cost"),optional(&lc,"cost_source"));
        let mut st = tx.prepare(&sql)?;
        let mut rows = st.query([])?;
        while let Some(row) = rows.next()? {
            let v = values(row, 11)?;
            count += 1;
            let id = text(&v[0]).ok_or(rusqlite::Error::InvalidQuery)?;
            let session_id = text(&v[1])
                .filter(|s| !s.is_empty())
                .ok_or(rusqlite::Error::InvalidQuery)?;
            let session = sessions.entry(session_id.clone()).or_default();
            let mut e = Evidence::read(&v[4..9]);
            // Older schemas have no redundant total column; derive only when
            // input/output are known, never substitute for an explicit unknown.
            if !lc.contains("total_tokens") {
                e.values[2] = e.values[0]
                    .zip(e.values[1])
                    .and_then(|(a, b)| a.checked_add(b));
            }
            let provenance = text(&v[10]).unwrap_or_default();
            if provenance == "carried_forward" {
                session.carried.push(e);
                continue;
            }
            let (Some(vector), Some(ts)) = (e.complete(), goose_time(&v[2])) else {
                session.invalid_detail = true;
                continue;
            };
            // Includes real compaction calls and explicit zero-usage provider calls.
            let mut r = record(
                &format!("{identity}:{session_id}"),
                format!("goose:{identity}:ledger:{id}"),
                e.projection(),
                Caliber::RequestDelta,
            );
            r.ts_ms = ts;
            r.model = text(&v[3])
                .filter(|m| !m.trim().is_empty())
                .unwrap_or_default();
            r.request_count = 1;
            if provenance == "provider_reported" {
                r.cost_usd = match v[9] {
                    SqlValue::Real(n) if n.is_finite() && n >= 0.0 => Some(n),
                    SqlValue::Integer(n) if n >= 0 => Some(n as f64),
                    _ => None,
                };
            }
            session.requests.push(vector);
            records.push(r);
        }
    }
    let mut precisions = BTreeMap::new();
    for (session_id, session) in sessions {
        if let Some((metrics, reason)) = reconcile(&session) {
            let key = format!("goose:{identity}:history:{session_id}");
            records.push(record(
                &format!("{identity}:{session_id}"),
                key.clone(),
                metrics,
                Caliber::SessionTotal,
            ));
            precisions.insert(
                key,
                SessionTotalPrecision {
                    metrics,
                    reason: reason.into(),
                },
            );
        }
    }
    tx.commit()?;
    Ok((records, precisions, count))
}
fn reconcile(s: &Session) -> Option<([Option<i64>; 10], &'static str)> {
    let unknown = |reason| Some(([None; 10], reason));
    if s.invalid_detail {
        return unknown("GOOSE_REQUEST_EVIDENCE_INCOMPLETE");
    }
    let d = match sum_vectors(s.requests.iter()) {
        Some(d) => d,
        None => return unknown("GOOSE_USAGE_OVERFLOW"),
    };
    let carry: Option<Vec<_>> = s.carried.iter().map(Evidence::complete).collect();
    let c = match carry.as_ref().and_then(|v| sum_vectors(v.iter())) {
        Some(c) => c,
        None => return unknown("GOOSE_CARRY_EVIDENCE_INCONSISTENT"),
    };
    let a = s.accumulated.as_ref();
    if a.is_some_and(|a| a.invalid) {
        return unknown("GOOSE_ACCUMULATED_INCONSISTENT");
    }
    if let Some(a) = a.and_then(Evidence::complete) {
        let Some(rest) = residual(a, d) else {
            return unknown("GOOSE_ACCUMULATED_CONFLICT");
        };
        if !s.carried.is_empty() && c != rest {
            return unknown("GOOSE_CARRY_RECONCILIATION_CONFLICT");
        }
        if rest.iter().any(|n| *n > 0) || !s.carried.is_empty() {
            return Some((
                Evidence {
                    values: rest.map(Some),
                    invalid: false,
                }
                .projection(),
                "GOOSE_UNALLOCATED_HISTORY",
            ));
        }
        if s.latest_only && s.requests.is_empty() {
            return unknown("GOOSE_CONTEXT_WITHOUT_CUMULATIVE_USAGE");
        }
        return None;
    }
    if s.requests.is_empty() && s.carried.is_empty() {
        if s.latest_only {
            return unknown("GOOSE_CONTEXT_WITHOUT_CUMULATIVE_USAGE");
        }
        return a
            .filter(|a| a.nonzero())
            .map(|a| (a.projection(), "GOOSE_UNALLOCATED_HISTORY"));
    }
    if a.is_some_and(Evidence::present) {
        return unknown("GOOSE_ACCUMULATED_PRECISION_UNKNOWN");
    }
    if s.carried.len() == 1 {
        return Some((s.carried[0].projection(), "GOOSE_UNALLOCATED_HISTORY"));
    }
    if !s.carried.is_empty() {
        return unknown("GOOSE_CARRY_RECONCILIATION_UNKNOWN");
    }
    // Ledger-only schema/session: no conflicting historical evidence was observed.
    None
}
