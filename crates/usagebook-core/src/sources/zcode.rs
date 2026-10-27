//! ZCode 适配器 —— 依据 docs/数据源档案/04-zcode.md v1.0。
//!
//! 统计主源:<root>/cli/db/db.sqlite 的 model_usage 账本(本机 238MB/2724 行);
//! rollout/*.jsonl 为本机独有转录,v1 仅探测不读(档案 §2)。
//! SQLite 纪律(档案 §5.6):只读连接 + busy_timeout + query_only,不改 journal mode,
//! 与 ZCode WAL 并发兼容(db.sqlite-shm/-wal 常驻)。
//! 口径(档案 §4 硬约束):input_tokens **含** cache_read+cache_creation(inclusive)→ 减除;
//! output_tokens 含 reasoning → 减除;cache 桶之和超 input 时钳制(形状判定兜底)。
//! 过滤(档案 §5.1):仅 status=='completed'(error/cancelled 跳过计数)。
//! 去重键:zcode-sqlite:{id}(主键,天然幂等,档案 §5.2)。

use crate::common::*;
use crate::model::*;
use rusqlite::types::Value as SqlValue;
use rusqlite::Connection;
use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Duration;

pub struct ZcodeSource;

/// epoch 值归一为毫秒:<1e12 视为秒 ×1000(档案 00-总览 §五 横切规范)。
fn norm_ms(t: i64) -> i64 {
    if t > 0 && t < 1_000_000_000_000 {
        t * 1000
    } else {
        t
    }
}

/// SQLite 值 → epoch 毫秒(INTEGER/REAL 归一;TEXT 尝试 ISO 或数字)。
pub(crate) fn value_to_ms(v: &SqlValue) -> Option<i64> {
    match v {
        SqlValue::Integer(i) if *i > 0 => Some(norm_ms(*i)),
        SqlValue::Real(f) if *f > 0.0 => Some(norm_ms(*f as i64)),
        SqlValue::Text(s) => iso_to_ms(s).or_else(|| s.parse::<i64>().ok().map(norm_ms)),
        _ => None,
    }
}

/// SQLite 值 → u64(负值/NULL 记 0)。
pub(crate) fn value_to_u64(v: &SqlValue) -> u64 {
    match v {
        SqlValue::Integer(i) => (*i).max(0) as u64,
        SqlValue::Real(f) if *f > 0.0 => *f as u64,
        SqlValue::Text(s) => s.parse::<u64>().unwrap_or(0),
        _ => 0,
    }
}

pub(crate) fn value_to_string(v: &SqlValue) -> Option<String> {
    match v {
        SqlValue::Text(s) => Some(s.clone()),
        SqlValue::Integer(i) => Some(i.to_string()),
        _ => None,
    }
}

/// 探测 model_usage 列集合(逐列探测,档案 §8;缺必需列 → 整库忽略)。
fn probe_columns(conn: &Connection) -> Option<Vec<String>> {
    let n: i64 = conn
        .query_row(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='model_usage'",
            [],
            |r| r.get(0),
        )
        .ok()?;
    if n == 0 {
        return None;
    }
    let mut stmt = conn.prepare("PRAGMA table_info(model_usage)").ok()?;
    let cols: Vec<String> = stmt
        .query_map([], |r| r.get::<_, String>(1))
        .ok()?
        .flatten()
        .collect();
    Some(cols)
}

impl UsageSource for ZcodeSource {
    fn id(&self) -> &'static str {
        "zcode"
    }

    fn discover_roots(&self) -> Vec<PathBuf> {
        // ZCODE_HOME 逗号多根,设置后不回退默认根(档案 §1)
        let mut roots = env_roots("ZCODE_HOME");
        if roots.is_empty() {
            let p = home().join(".zcode");
            if p.is_dir() {
                roots.push(p);
            }
        }
        roots
    }

    fn input_containers(&self, roots: &[PathBuf]) -> Vec<PathBuf> {
        roots
            .iter()
            .map(|root| root.join("cli/db/db.sqlite"))
            .collect()
    }

    fn scan_roots(
        &self,
        roots: &[std::path::PathBuf],
        _max_files: Option<usize>,
    ) -> (Vec<UsageRecord>, ScanStats) {
        // max_files 对本源无意义:sqlite 单库全量读,只 SELECT 需要的列
        let mut stats = ScanStats::default();
        if roots.is_empty() {
            stats.notes.push("未发现 ~/.zcode 或 ZCODE_HOME".into());
            return (vec![], stats);
        }

        let mut records: Vec<UsageRecord> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new(); // 幂等兜底(多根同库拷贝)
        let mut sessions: HashSet<String> = HashSet::new();
        let mut models: HashSet<String> = HashSet::new();
        let mut ts_min = i64::MAX;
        let mut ts_max = i64::MIN;

        for root in roots {
            // rollout 仅探测计数(v1.1 归档候选,统计不读;档案 §2)
            let rollout = walk_jsonl(&root.join("cli/rollout"), None).len();
            if rollout > 0 {
                stats
                    .notes
                    .push(format!("rollout/ {} 个转录文件仅探测不读(v1.1)", rollout));
            }
            let db = root.join("cli/db/db.sqlite");
            stats.files_seen += rollout + 1;
            if !db.is_file() {
                stats
                    .notes
                    .push(format!("根 {} 下无 cli/db/db.sqlite", root.display()));
                continue;
            }

            // 只读打开 + busy_timeout + query_only;失败注记不 panic(档案 §5.6)
            let conn = match crate::common::open_sqlite_readonly(&db) {
                Ok(c) => c,
                Err(e) => {
                    stats.notes.push(format!("db.sqlite 打开失败: {}", e));
                    stats.failed = true;
                    continue;
                }
            };
            if let Err(e) = conn.busy_timeout(Duration::from_secs(5)) {
                stats.notes.push(format!("busy_timeout 设置失败: {}", e));
                stats.failed = true;
            }
            let _ = conn.pragma_update(None, "query_only", true);

            for record in read_usage(&conn, &mut stats) {
                if seen.insert(record.dedup_key.clone()) {
                    sessions.insert(record.session_id.clone());
                    models.insert(record.model.clone());
                    ts_min = ts_min.min(record.ts_ms);
                    ts_max = ts_max.max(record.ts_ms);
                    records.push(record);
                }
            }
        }

        if !records.is_empty() {
            stats.notes.push(format!(
                "时间范围 {} ~ {} · 会话 {} · 模型 {}",
                ms_to_local_date(ts_min),
                ms_to_local_date(ts_max),
                sessions.len(),
                models.iter().cloned().collect::<Vec<_>>().join(", ")
            ));
        }
        (records, stats)
    }
}

fn read_usage(conn: &Connection, stats: &mut ScanStats) -> Vec<UsageRecord> {
    let mut records = Vec::new();
    let mut seen = HashSet::new();
    let mut skipped_no_time = 0u64;
    let Some(cols) = probe_columns(conn) else {
        stats
            .notes
            .push("库中无 model_usage 表,忽略(非 ZCode 账本)".into());
        return records;
    };
    const REQUIRED: [&str; 6] = [
        "id",
        "session_id",
        "model_id",
        "started_at",
        "input_tokens",
        "output_tokens",
    ];
    let missing: Vec<&str> = REQUIRED
        .iter()
        .filter(|c| !cols.contains(&c.to_string()))
        .cloned()
        .collect();
    if !missing.is_empty() {
        stats.notes.push(format!(
            "model_usage 缺必需列 {:?},整库忽略(布局待适配)",
            missing
        ));
        return records;
    }

    // 布局判定(仅注记;modern = session 表有 version 列,档案 §3.1)
    let layout = conn
        .prepare("PRAGMA table_info(session)")
        .and_then(|mut s| {
            Ok(s.query_map([], |r| r.get::<_, String>(1))?
                .flatten()
                .any(|c| c == "version"))
        })
        .unwrap_or(false);
    stats.notes.push(format!(
        "布局判定: {}(session.version 列{})",
        if layout { "modern" } else { "legacy" },
        if layout { "在" } else { "缺" }
    ));

    // 状态分布(error/cancelled 跳过但要计数,档案 §5.1)
    let has = |c: &str| cols.contains(&c.to_string());
    if has("status") {
        if let Ok(mut stmt) = conn.prepare(
            "SELECT status, count(*) FROM model_usage GROUP BY status ORDER BY count(*) DESC",
        ) {
            if let Ok(mut rows) = stmt.query([]) {
                let mut dist = String::new();
                while let Ok(Some(r)) = rows.next() {
                    let s: String = r
                        .get::<_, Option<String>>(0)
                        .unwrap_or_default()
                        .unwrap_or_default();
                    let n: i64 = r.get(1).unwrap_or(0);
                    dist.push_str(&format!("{}={}, ", s, n));
                }
                stats.notes.push(format!(
                    "model_usage 状态分布: {}",
                    dist.trim_end_matches(", ")
                ));
            }
        }
    }

    // 固定列序 SELECT:可选列缺失用 NULL 占位 → 行索引恒定
    let opt = |c: &str| {
        if has(c) {
            c.to_string()
        } else {
            "NULL".to_string()
        }
    };
    let sql = format!(
        "SELECT id, session_id, model_id, started_at, input_tokens, output_tokens, \
         {}, {}, {}, {}, {}, {} FROM model_usage{}",
        opt("reasoning_tokens"),
        opt("cache_read_input_tokens"),
        opt("cache_creation_input_tokens"),
        opt("completed_at"),
        opt("duration_ms"),
        opt("status"),
        if has("status") {
            " WHERE status='completed'"
        } else {
            ""
        }
    );
    let Ok(mut stmt) = conn.prepare(&sql) else {
        stats.notes.push("model_usage 查询构建失败".into());
        stats.failed = true;
        return records;
    };
    let Ok(mut rows) = stmt.query([]) else {
        stats.notes.push("model_usage 查询执行失败".into());
        stats.failed = true;
        return records;
    };
    while let Some(row) = crate::common::next_sqlite_row(&mut rows, "zcode", stats) {
        // 0 id, 1 session_id, 2 model_id, 3 started_at, 4 input, 5 output,
        // 6 reasoning, 7 cache_read, 8 cache_write, 9 completed_at, 10 duration_ms, 11 status
        let vals: Vec<SqlValue> = (0..12)
            .filter_map(|i| row.get::<_, SqlValue>(i).ok())
            .collect();
        if vals.len() < 12 {
            stats.lines_bad_json += 1;
            continue;
        }
        let (Some(id), Some(session_id), model) = (
            value_to_string(&vals[0]),
            value_to_string(&vals[1]),
            value_to_string(&vals[2]).unwrap_or_else(|| "unknown".into()),
        ) else {
            stats.lines_bad_json += 1;
            continue;
        };
        stats.lines_total += 1;
        // 时间:started_at 优先;缺失用 completed_at − duration_ms 回推(档案 §5.4)
        let ts_ms = value_to_ms(&vals[3]).or_else(|| {
            value_to_ms(&vals[9]).map(|c| c.saturating_sub(value_to_u64(&vals[10]) as i64))
        });
        let Some(ts_ms) = ts_ms else {
            skipped_no_time += 1;
            continue;
        };

        // inclusive 减除 + 钳制(档案 §4):input 含两 cache 桶;cache 和超 input 时钳到 input
        let input = value_to_u64(&vals[4]);
        let output_raw = value_to_u64(&vals[5]);
        let cache_read = value_to_u64(&vals[7]).min(input);
        let cache_write = value_to_u64(&vals[8]).min(input - cache_read);
        let input_uncached = input - cache_read - cache_write;
        let reasoning = value_to_u64(&vals[6]).min(output_raw);
        let output = output_raw - reasoning;
        if input + output_raw == 0 {
            continue; // 全零行不入账
        }

        let key = format!("zcode-sqlite:{}", id);
        if seen.insert(key.clone()) {
            records.push(UsageRecord {
                source: "zcode",
                session_id,
                agent: None,
                model,
                ts_ms,
                caliber: Caliber::RequestDelta,
                request_count: 1,
                input_uncached,
                cache_read,
                cache_write,
                cache_write_1h: 0,
                output,
                reasoning_output: reasoning,
                cost_usd: None,
                dedup_key: key,
            });
        }
    }
    stats.files_parsed += 1;
    if skipped_no_time > 0 {
        stats.notes.push(format!(
            "{} 行无可用时间戳(started_at/completed_at 均缺),跳过",
            skipped_no_time
        ));
    }
    records
}
/// Parse only the saved database projection, keeping source integer/text/real
/// conversion, completed-state filtering and inclusive token buckets unchanged.
pub fn scan_saved(conn: &Connection) -> (Vec<UsageRecord>, ScanStats) {
    let mut stats = ScanStats::default();
    let mut records = Vec::new();
    let mut seen = HashSet::new();
    match crate::ledger::source_projection::saved_databases(conn, "zcode") {
        Ok(databases) => {
            for db in databases {
                stats.files_seen += 1;
                for record in read_usage(&db.connection, &mut stats) {
                    if seen.insert(record.dedup_key.clone()) {
                        records.push(record);
                    }
                }
            }
        }
        Err(_) => {
            stats.failed = true;
            stats.notes.push("ZCode 已保存数据库尚未完整".into());
        }
    }
    (records, stats)
}
