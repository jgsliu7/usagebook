//! Copilot CLI: per-session database authority, with uncovered sessions supplied by
//! shutdown/OTel. All three channels use the same inclusive-token contract.
//! See docs/数据源档案/19-copilot.md for protocol evidence and conservative failures.

use crate::common::{env_roots, flex_to_ms, home, walk_with};
use crate::model::{Caliber, ScanStats, UsageRecord, UsageSource};
use rusqlite::types::Value as SqlValue;
use rusqlite::Connection;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};

pub struct CopilotSource;

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug)]
enum OtelKind {
    AgentSummarySpan,
    AgentTurnLog,
    InferenceLog,
    ChatSpan,
}

/// Gross input/output, cache-read/write, reasoning. Raw counters and exclusive
/// buckets must BOTH be monotone before cumulative subtraction is justified.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Counts([u64; 5]);
impl Counts {
    fn exclusive(self) -> Result<[u64; 5], &'static str> {
        let [input, output, read, write, reasoning] = self.0;
        let cached = read.checked_add(write).ok_or("TOKEN_RANGE")?;
        if cached > input || reasoning > output {
            return Err("TOKEN_SUBSET");
        }
        if input
            .checked_add(output)
            .is_none_or(|n| n > i64::MAX as u64)
        {
            return Err("TOKEN_RANGE");
        }
        Ok([input - cached, read, write, output - reasoning, reasoning])
    }
    fn shutdown(v: &Value) -> Result<Self, &'static str> {
        if !v.is_object() {
            return Err("TOKEN_FIELD");
        }
        Ok(Self([
            token(v.get("inputTokens"))?,
            token(v.get("outputTokens"))?,
            token(v.get("cacheReadTokens"))?,
            token(v.get("cacheWriteTokens"))?,
            token(v.get("reasoningTokens"))?,
        ]))
    }
}

struct ShutdownSnap {
    ts_ms: i64,
    event_id: String,
    counts: Counts,
    buckets: [u64; 5],
}

struct OtelRow {
    ts_ms: i64,
    trace: String,
    session: String,
    model: String,
    content_key: String,
    kind: OtelKind,
    buckets: [u64; 5],
}

/// Zero-token database rows still establish a session/model's database channel.
#[derive(Default)]
struct Coverage {
    sessions: HashMap<String, HashSet<String>>,
}
impl Coverage {
    fn covers(&self, session: &str) -> bool {
        self.sessions.contains_key(session)
    }
    fn check_overlap(&self, session: &str, model: &str, nonzero: bool) -> Result<(), &'static str> {
        let Some(models) = self.sessions.get(session) else {
            return Ok(());
        };
        // A positive model absent from every database row proves a gap. Token
        // totals, timestamps and requests.count do not: failed/retried calls
        // have no established one-to-one contract with assistant_usage_events.
        if nonzero && !models.contains(model_identity(model)) {
            return Err("STORE_COVERAGE_GAP");
        }
        Ok(())
    }
}

impl UsageSource for CopilotSource {
    fn id(&self) -> &'static str {
        "copilot"
    }

    fn discover_roots(&self) -> Vec<PathBuf> {
        let mut roots = env_roots("COPILOT_HOME");
        if roots.is_empty() {
            let d = home().join(".copilot");
            if d.is_dir() {
                roots.push(d);
            }
        }
        roots
    }

    fn scan_roots(
        &self,
        roots: &[PathBuf],
        max_files: Option<usize>,
    ) -> (Vec<UsageRecord>, ScanStats) {
        let mut stats = ScanStats::default();
        if roots.is_empty() {
            stats.notes.push("未发现 ~/.copilot 或 COPILOT_HOME".into());
            return (vec![], stats);
        }
        let mut records = Vec::new();
        let mut physical_roots = HashSet::new();
        let mut ss_files = 0usize;
        let mut otel_files = 0usize;
        for root in roots {
            let root = match root.canonicalize() {
                Ok(root) => root,
                Err(_) => {
                    fail(&mut stats, "ROOT_READ");
                    continue;
                }
            };
            if !physical_roots.insert(root.clone()) {
                continue;
            }
            // Debug formatting escapes non-UTF8 paths rather than collapsing
            // distinct roots through lossy path display. JSON keys delimit fields.
            let domain = format!("{root:?}");
            let mut seen = HashMap::<String, [u64; 5]>::new();
            let store_db = root.join("session-store.db");
            let coverage = match store_db.try_exists() {
                Ok(true) => {
                    stats.files_seen += 1;
                    let result = scan_session_store(&store_db, &domain, &mut records, &mut stats);
                    stats.files_parsed += 1;
                    result
                }
                Ok(false) => Coverage::default(),
                Err(_) => {
                    fail(&mut stats, "STORE_READ");
                    Coverage::default()
                }
            };
            // These maps live inside the physical root: same session/trace text
            // in two installations cannot suppress each other's records.
            let mut last_shutdown = HashMap::<(String, String), i64>::new();
            let mut snaps = BTreeMap::<(String, String), Vec<ShutdownSnap>>::new();
            let mut event_payloads = HashMap::<(String, String), Vec<u8>>::new();
            let entries = match std::fs::read_dir(root.join("session-state")) {
                Ok(rd) => match rd.collect::<Result<Vec<_>, _>>() {
                    Ok(mut entries) => {
                        entries.sort_by_key(|e| e.path());
                        entries
                    }
                    Err(_) => {
                        fail(&mut stats, "SESSION_READ");
                        Vec::new()
                    }
                },
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
                Err(_) => {
                    fail(&mut stats, "SESSION_READ");
                    Vec::new()
                }
            };
            for entry in entries {
                match std::fs::metadata(entry.path()) {
                    Ok(md) if md.is_dir() => {}
                    Ok(_) => continue, // session-state may also contain bookkeeping files
                    Err(_) => {
                        fail(&mut stats, "SESSION_READ");
                        continue;
                    }
                }
                let p = entry.path().join("events.jsonl");
                match p.try_exists() {
                    Ok(false) => continue,
                    Err(_) => {
                        fail(&mut stats, "SESSION_READ");
                        continue;
                    }
                    Ok(true) => {}
                }
                let Some(session) = entry
                    .file_name()
                    .to_str()
                    .filter(|s| !s.trim().is_empty())
                    .map(str::to_owned)
                else {
                    fail(&mut stats, "SESSION_ID");
                    continue;
                };
                ss_files += 1;
                stats.files_seen += 1;
                let outcome = crate::common::for_each_line(&p, |line| {
                    let Ok(v) = serde_json::from_str::<Value>(line) else {
                        stats.lines_bad_json += 1;
                        fail(&mut stats, "SESSION_JSON");
                        return;
                    };
                    let kind = v.get("type").and_then(Value::as_str);
                    if kind == Some("session.start") {
                        if let Some(id) = v.pointer("/data/sessionId") {
                            if id.as_str() != Some(session.as_str()) {
                                fail(&mut stats, "SESSION_ID_CONFLICT");
                            }
                        }
                    }
                    if kind != Some("session.shutdown") {
                        if !kind.is_some_and(|t| t.starts_with("session.")) {
                            stats.lines_unknown_type += 1;
                        }
                        return;
                    }
                    let Some(metrics) = v.pointer("/data/modelMetrics").and_then(Value::as_object)
                    else {
                        fail(&mut stats, "SHUTDOWN_METRICS");
                        return;
                    };
                    let event_id = match optional_id(v.get("id")) {
                        Ok(id) => id.unwrap_or_default(),
                        Err(code) => {
                            fail(&mut stats, code);
                            return;
                        }
                    };
                    if !event_id.is_empty() {
                        // Hash the entire event only in scan memory; raw context,
                        // tokenDetails and message content never enter a cache.
                        let payload =
                            Sha256::digest(serde_json::to_vec(&v).unwrap_or_default()).to_vec();
                        let key = (session.clone(), event_id.clone());
                        if let Some(old) = event_payloads.get(&key) {
                            if old != &payload {
                                fail(&mut stats, "SHUTDOWN_ID_CONFLICT");
                            }
                            return;
                        }
                        event_payloads.insert(key, payload);
                    }
                    let ts_ms = v
                        .get("timestamp")
                        .and_then(Value::as_str)
                        .and_then(crate::common::iso_to_ms)
                        .unwrap_or(0);
                    for (model, metric) in metrics {
                        if model.trim().is_empty() {
                            fail(&mut stats, "MODEL_ID");
                            continue;
                        }
                        let Some(usage) = metric.get("usage") else {
                            fail(&mut stats, "SHUTDOWN_USAGE");
                            continue;
                        };
                        let parsed = (|| {
                            let counts = Counts::shutdown(usage)?;
                            let buckets = counts.exclusive()?;
                            coverage.check_overlap(
                                &session,
                                model,
                                buckets.iter().any(|n| *n > 0),
                            )?;
                            Ok((counts, buckets))
                        })();
                        match parsed {
                            Err(code) => fail(&mut stats, code),
                            Ok((counts, buckets)) if !coverage.covers(&session) => {
                                snaps
                                    .entry((session.clone(), model.clone()))
                                    .or_default()
                                    .push(ShutdownSnap {
                                        ts_ms,
                                        event_id: event_id.clone(),
                                        counts,
                                        buckets,
                                    });
                            }
                            Ok(_) => {} // ordinary overlap: database owns this session
                        }
                    }
                });
                outcome.apply_to_stats(&mut stats);
                stats.files_parsed += 1;
                if max_files.is_some_and(|m| ss_files >= m) {
                    break;
                }
            }
            for ((session, model), seq) in snaps {
                let mut previous: Option<(Counts, [u64; 5])> = None;
                for (ordinal, s) in seq.iter().enumerate() {
                    let delta = if let Some((raw, exclusive)) = previous {
                        if s.counts.0.iter().zip(raw.0).any(|(a, b)| *a < b)
                            || s.buckets.iter().zip(exclusive).any(|(a, b)| *a < b)
                        {
                            // Neither resume, timestamp, nor sessionStartTime
                            // proves a new accumulation segment. Fail closed.
                            fail(&mut stats, "SHUTDOWN_NON_MONOTONE");
                            break;
                        }
                        std::array::from_fn(|i| s.buckets[i] - exclusive[i])
                    } else {
                        s.buckets
                    };
                    previous = Some((s.counts, s.buckets));
                    last_shutdown
                        .entry((session.clone(), model_identity(&model).to_owned()))
                        .and_modify(|cut| *cut = (*cut).max(s.ts_ms))
                        .or_insert(s.ts_ms);
                    if delta.iter().all(|n| *n == 0) {
                        continue;
                    }
                    let identity = if s.event_id.is_empty() {
                        format!("line:{ordinal}")
                    } else {
                        s.event_id.clone()
                    };
                    let key = key(&[&domain, "shutdown", &session, &model, &identity]);
                    records.push(record(&session, &model, s.ts_ms, delta, key));
                }
            }

            let files = walk_with(&root.join("otel"), None, &|p| {
                p.extension().is_some_and(|x| x == "jsonl")
            });
            let mut trace_best = HashMap::<(String, String), OtelKind>::new();
            let mut otel_rows = Vec::new();
            for p in files {
                otel_files += 1;
                stats.files_seen += 1;
                let mtime_ms = p
                    .metadata()
                    .ok()
                    .and_then(|m| m.modified().ok())
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|d| d.as_millis() as i64)
                    .unwrap_or(0);
                let outcome = crate::common::for_each_line(&p, |line| {
                    let Ok(v) = serde_json::from_str::<Value>(line) else {
                        stats.lines_bad_json += 1;
                        fail(&mut stats, "OTEL_JSON");
                        return;
                    };
                    match parse_otel(&v, &domain, mtime_ms) {
                        Err(code) => fail(&mut stats, code),
                        Ok(None) => {
                            stats.lines_unknown_type += 1;
                        }
                        Ok(Some(row)) => {
                            if row.buckets.iter().all(|n| *n == 0) {
                                return;
                            }
                            if let Err(code) =
                                coverage.check_overlap(&row.session, &row.model, true)
                            {
                                fail(&mut stats, code);
                                return;
                            }
                            if coverage.covers(&row.session) {
                                return;
                            }
                            if !row.trace.is_empty() {
                                let k = (row.session.clone(), row.trace.clone());
                                trace_best
                                    .entry(k)
                                    .and_modify(|best| *best = (*best).max(row.kind))
                                    .or_insert(row.kind);
                            }
                            otel_rows.push(row);
                        }
                    }
                });
                outcome.apply_to_stats(&mut stats);
                stats.files_parsed += 1;
                if max_files.is_some_and(|m| otel_files >= m) {
                    break;
                }
            }
            for row in otel_rows {
                if last_shutdown
                    .get(&(row.session.clone(), model_identity(&row.model).to_owned()))
                    .is_some_and(|cut| *cut > 0 && row.ts_ms <= *cut)
                {
                    continue;
                }
                if trace_best
                    .get(&(row.session.clone(), row.trace.clone()))
                    .is_some_and(|best| *best > row.kind)
                {
                    continue;
                }
                if let Some(old) = seen.get(&row.content_key) {
                    if old != &row.buckets {
                        fail(&mut stats, "OTEL_ID_CONFLICT");
                    }
                    continue;
                }
                seen.insert(row.content_key.clone(), row.buckets);
                records.push(record(
                    &row.session,
                    &row.model,
                    row.ts_ms,
                    row.buckets,
                    row.content_key,
                ));
            }
        }
        stats.notes.push(format!(
            "session-state 文件 {ss_files} · otel 文件 {otel_files}"
        ));
        (records, stats)
    }
}

fn parse_otel(v: &Value, domain: &str, mtime_ms: i64) -> Result<Option<OtelRow>, &'static str> {
    let attrs = v.get("attributes").unwrap_or(&Value::Null);
    let a = |k: &str| attrs.get(k);
    let is_span = v.get("type").and_then(Value::as_str) == Some("span");
    let name = v.get("name").and_then(Value::as_str).unwrap_or("");
    let event_name = v.get("event.name").and_then(Value::as_str).unwrap_or("");
    let body = v.get("body").and_then(Value::as_str).unwrap_or("");
    let kind = if is_span
        && (a("gen_ai.operation.name").and_then(Value::as_str) == Some("chat")
            || name.starts_with("chat "))
    {
        OtelKind::ChatSpan
    } else if !is_span
        && (event_name == "gen_ai.client.inference.operation.details"
            || body.starts_with("GenAI inference:"))
    {
        OtelKind::InferenceLog
    } else if !is_span && event_name == "copilot_chat.agent.turn" {
        OtelKind::AgentTurnLog
    } else if is_span && a("gen_ai.operation.name").and_then(Value::as_str) == Some("invoke_agent")
    {
        OtelKind::AgentSummarySpan
    } else {
        return Ok(None);
    };
    let counts = Counts([
        token(a("gen_ai.usage.input_tokens"))?,
        token(a("gen_ai.usage.output_tokens"))?,
        token(a("gen_ai.usage.cache_read.input_tokens"))?,
        aliased_token(
            a("gen_ai.usage.cache_write.input_tokens"),
            a("gen_ai.usage.cache_creation.input_tokens"),
        )?,
        aliased_token(
            a("gen_ai.usage.reasoning.output_tokens"),
            a("gen_ai.usage.reasoning_tokens"),
        )?,
    ]);
    let buckets = counts.exclusive()?;
    if buckets.iter().all(|n| *n == 0) {
        return Ok(None);
    }
    let mut session = None;
    for field in [
        "gen_ai.conversation.id",
        "copilot_chat.session_id",
        "copilot_chat.chat_session_id",
        "session.id",
    ] {
        if let Some(id) = optional_id(a(field))? {
            if session.as_ref().is_some_and(|old| old != &id) {
                return Err("SESSION_ID_CONFLICT");
            }
            session = Some(id);
        }
    }
    // trace/response/interaction IDs identify operations, not a proven session.
    let session = session.ok_or("OTEL_SESSION_ID")?;
    let trace = optional_id(v.get("traceId"))?.unwrap_or_default();
    let model = ["gen_ai.response.model", "gen_ai.request.model"]
        .iter()
        .find_map(|k| a(k).and_then(Value::as_str))
        .map(model_identity)
        .unwrap_or("unknown")
        .to_owned();
    let ts_ms = ["endTime", "startTime", "hrTime", "_hrTime", "time"]
        .iter()
        .find_map(|k| flex_to_ms(v.get(*k)))
        .or_else(|| {
            v.get("timeUnixNano")
                .and_then(Value::as_f64)
                .map(|n| (n / 1e6) as i64)
        })
        .unwrap_or(mtime_ms);
    let operation = optional_id(v.get("spanId"))?
        .or(optional_id(a("gen_ai.response.id"))?)
        .unwrap_or_else(|| trace.clone());
    let content_key = key(&[
        domain,
        "otel",
        &session,
        &operation,
        &ts_ms.to_string(),
        &model,
    ]);
    Ok(Some(OtelRow {
        ts_ms,
        trace,
        session,
        model,
        content_key,
        kind,
        buckets,
    }))
}

fn record(session: &str, model: &str, ts_ms: i64, b: [u64; 5], dedup_key: String) -> UsageRecord {
    UsageRecord {
        source: "copilot",
        session_id: session.to_owned(),
        agent: None,
        model: model.to_owned(),
        ts_ms,
        caliber: Caliber::RequestDelta,
        request_count: 1,
        input_uncached: b[0],
        cache_read: b[1],
        cache_write: b[2],
        cache_write_1h: 0,
        output: b[3],
        reasoning_output: b[4],
        cost_usd: None,
        dedup_key,
    }
}

fn scan_session_store(
    db: &Path,
    domain: &str,
    records: &mut Vec<UsageRecord>,
    stats: &mut ScanStats,
) -> Coverage {
    let mut coverage = Coverage::default();
    let result = (|| -> rusqlite::Result<()> {
        let conn = crate::common::open_sqlite_readonly(db)?;
        let exists: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='assistant_usage_events')", [], |r| r.get(0))?;
        if !exists {
            return Ok(());
        }
        let columns = columns(&conn)?;
        let nano = if columns.contains("total_nano_aiu") {
            "total_nano_aiu"
        } else {
            "NULL"
        };
        let reason = if columns.contains("reasoning_tokens") {
            "reasoning_tokens"
        } else {
            "0"
        };
        let sql = format!("SELECT id, session_id, agent_id, model, input_tokens, output_tokens, cache_read_tokens, cache_write_tokens, {reason}, {nano}, created_at FROM assistant_usage_events ORDER BY id");
        let mut stmt = conn.prepare(&sql)?;
        let mut rows = stmt.query([])?;
        let mut ids = HashMap::<String, Vec<SqlValue>>::new();
        while let Some(row) = rows.next()? {
            stats.lines_total += 1;
            let vals: Vec<SqlValue> = (0..11).map(|i| row.get(i)).collect::<Result<_, _>>()?;
            let parsed = (|| {
                let id = db_id(&vals[0])?;
                if let Some(old) = ids.get(&id) {
                    return if old == &vals {
                        Ok(None)
                    } else {
                        Err("STORE_ID_CONFLICT")
                    };
                }
                ids.insert(id.clone(), vals.clone());
                let session = sql_text(&vals[1])
                    .filter(|s| !s.trim().is_empty())
                    .ok_or("SESSION_ID")?;
                let model = sql_text(&vals[3])
                    .filter(|s| !s.trim().is_empty())
                    .unwrap_or("unknown");
                let raw = Counts([
                    sql_token(&vals[4])?,
                    sql_token(&vals[5])?,
                    sql_token(&vals[6])?,
                    sql_token(&vals[7])?,
                    sql_token(&vals[8])?,
                ]);
                let buckets = raw.exclusive()?;
                let nano = match &vals[9] {
                    SqlValue::Null => 0,
                    v => sql_token(v)?,
                };
                let ts_ms = sql_text(&vals[10])
                    .and_then(|s| {
                        chrono::NaiveDateTime::parse_from_str(s.trim(), "%Y-%m-%d %H:%M:%S").ok()
                    })
                    .map(|t| t.and_utc().timestamp_millis())
                    .unwrap_or(0);
                coverage
                    .sessions
                    .entry(session.to_owned())
                    .or_default()
                    .insert(model_identity(model).to_owned());
                if buckets.iter().all(|n| *n == 0) {
                    return Ok(None);
                }
                let mut row = record(session, model, ts_ms, buckets, key(&[domain, "store", &id]));
                row.agent = sql_text(&vals[2])
                    .map(str::trim)
                    .filter(|a| !a.is_empty() && *a != "main")
                    .map(str::to_owned);
                row.cost_usd = (nano > 0).then(|| nano as f64 / 1e9 * 0.01);
                Ok(Some(row))
            })();
            match parsed {
                Ok(Some(row)) => records.push(row),
                Ok(None) => {}
                Err(code) => fail(stats, code),
            }
        }
        Ok(())
    })();
    if result.is_err() {
        fail(stats, "STORE_READ");
    }
    coverage
}

fn columns(conn: &Connection) -> rusqlite::Result<HashSet<String>> {
    let mut stmt = conn.prepare("PRAGMA table_info(assistant_usage_events)")?;
    let names = stmt.query_map([], |row| row.get::<_, String>(1))?.collect();
    names
}
fn key(parts: &[&str]) -> String {
    format!(
        "copilot:{}",
        serde_json::to_string(parts).expect("string array")
    )
}
fn model_identity(model: &str) -> &str {
    model
        .strip_suffix("-1m-internal")
        .or_else(|| model.strip_suffix("-1m"))
        .unwrap_or(model)
}
fn fail(stats: &mut ScanStats, code: &str) {
    stats.failed = true;
    let note = format!("Copilot {code}：来源读取不完整");
    if !stats.notes.contains(&note) {
        stats.notes.push(note);
    }
}
fn optional_id(v: Option<&Value>) -> Result<Option<String>, &'static str> {
    match v {
        None => Ok(None),
        Some(Value::String(s)) if !s.trim().is_empty() => Ok(Some(s.clone())),
        _ => Err("IDENTITY_FIELD"),
    }
}
fn token(v: Option<&Value>) -> Result<u64, &'static str> {
    let n = match v {
        None => 0,
        Some(Value::Number(n)) => match n.as_u64() {
            Some(n) => n,
            None => return n.as_f64().ok_or("TOKEN_FIELD").and_then(float_token),
        },
        Some(Value::String(s))
            if !s.trim().is_empty() && s.trim().bytes().all(|b| b.is_ascii_digit()) =>
        {
            s.trim().parse().map_err(|_| "TOKEN_RANGE")?
        }
        _ => return Err("TOKEN_FIELD"),
    };
    if n > i64::MAX as u64 {
        Err("TOKEN_RANGE")
    } else {
        Ok(n)
    }
}
fn float_token(n: f64) -> Result<u64, &'static str> {
    // Beyond 2^53, a floating-point number cannot prove an exact integer count.
    if n.is_finite() && n >= 0.0 && n.fract() == 0.0 && n <= 9_007_199_254_740_991.0 {
        Ok(n as u64)
    } else {
        Err("TOKEN_FIELD")
    }
}
fn aliased_token(a: Option<&Value>, b: Option<&Value>) -> Result<u64, &'static str> {
    match (a, b) {
        (Some(a), Some(b)) => {
            let a = token(Some(a))?;
            if a != token(Some(b))? {
                Err("TOKEN_ALIAS_CONFLICT")
            } else {
                Ok(a)
            }
        }
        (a, b) => token(a.or(b)),
    }
}
fn sql_token(v: &SqlValue) -> Result<u64, &'static str> {
    match v {
        SqlValue::Integer(n) if *n >= 0 => Ok(*n as u64),
        SqlValue::Real(n) => float_token(*n),
        SqlValue::Text(s) => token(Some(&Value::String(s.clone()))),
        _ => Err("TOKEN_FIELD"),
    }
}
fn sql_text(v: &SqlValue) -> Option<&str> {
    if let SqlValue::Text(s) = v {
        Some(s)
    } else {
        None
    }
}
fn db_id(v: &SqlValue) -> Result<String, &'static str> {
    match v {
        SqlValue::Integer(n) if *n >= 0 => Ok(n.to_string()),
        SqlValue::Text(s) if !s.trim().is_empty() => Ok(s.clone()),
        _ => Err("STORE_ID"),
    }
}
