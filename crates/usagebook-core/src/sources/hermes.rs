//! Hermes 适配器 —— 依据 docs/数据源档案/25-hermes.md(Community,本机未实证)。
//!
//! 根:HERMES_HOME(多根,每根一个主目录)→ <home>/state.db + 全部 profiles/<name>/state.db(§1)
//! 库格式(§2):sessions 表五桶列 + started_at(epoch 秒,可带小数)
//! 会话级累计裁决(§4,同 22-droid/23-goose):整会话总量计入 started_at(会话开始,稳定)
//! 五桶直取无减除(§3);成本 actual > estimated(§5)
//! 去重键:hermes:{db路径}:{id}(§5,多 profile 同 id 不撞)

use crate::common::{env_roots, home};
use crate::model::{Caliber, ScanStats, UsageSource, UsageRecord};
use rusqlite::types::Value as SqlValue;
use rusqlite::{Connection, OpenFlags};
use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Duration;

pub struct HermesSource;

/// SQLite 值 → u64。
fn v_u64(v: &SqlValue) -> u64 {
    match v {
        SqlValue::Integer(i) => (*i).max(0) as u64,
        SqlValue::Real(f) if *f > 0.0 => *f as u64,
        SqlValue::Text(s) => s.trim().parse().unwrap_or(0),
        _ => 0,
    }
}

/// SQLite 值 → u64 成本。
fn v_f64(v: &SqlValue) -> Option<f64> {
    match v {
        SqlValue::Real(f) => Some(*f),
        SqlValue::Integer(i) => Some(*i as f64),
        SqlValue::Text(s) => s.trim().parse().ok(),
        _ => None,
    }
}

/// SQLite 值 → 文本。
fn v_text(v: &SqlValue) -> Option<String> {
    match v {
        SqlValue::Text(s) => Some(s.clone()),
        SqlValue::Integer(i) => Some(i.to_string()),
        _ => None,
    }
}

/// 只读打开 + busy_timeout + query_only(zcode SQLite 纪律)。
fn open_ro(db: &std::path::Path) -> Option<Connection> {
    let conn = Connection::open_with_flags(
        db,
        OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .ok()?;
    let _ = conn.busy_timeout(Duration::from_secs(5));
    let _ = conn.pragma_update(None, "query_only", true);
    Some(conn)
}

/// 列存在性防御(§6.2,旧 schema 可能缺判据列)。
fn has_column(conn: &Connection, table: &str, col: &str) -> bool {
    let Ok(mut stmt) = conn.prepare(&format!("PRAGMA table_info({table})")) else {
        return false;
    };
    let Ok(mut rows) = stmt.query([]) else {
        return false;
    };
    while let Ok(Some(r)) = rows.next() {
        if let Ok(name) = r.get::<_, String>(1) {
            if name == col {
                return true;
            }
        }
    }
    false
}

/// 表是否存在。
fn has_table(conn: &Connection, name: &str) -> bool {
    conn.query_row(
        "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?1",
        [name],
        |r| r.get::<_, i64>(0),
    )
    .unwrap_or(0)
        > 0
}

impl UsageSource for HermesSource {
    fn id(&self) -> &'static str {
        "hermes"
    }

    /// roots 直接返回 state.db 文件路径(§1):每主目录 1 + 每 profile 1。
    /// HERMES_HOME 未设置时回退默认 ~/.hermes(§1 实装发现遗漏,实库 2.4G 因此漏扫)。
    fn discover_roots(&self) -> Vec<PathBuf> {
        let mut dbs = Vec::new();
        let mut homes = env_roots("HERMES_HOME");
        if homes.is_empty() {
            // Windows 官方默认 %LOCALAPPDATA%\hermes(上游 hermes_constants.py 平台分支);
            // 其余平台 ~/.hermes
            if let Ok(la) = std::env::var("LOCALAPPDATA") {
                if !la.trim().is_empty() {
                    homes.push(std::path::PathBuf::from(la.trim()).join("hermes"));
                }
            }
            homes.push(home().join(".hermes"));
        }
        for home_dir in homes {
            let main = home_dir.join("state.db");
            if main.is_file() {
                dbs.push(main);
            }
            if let Ok(rd) = std::fs::read_dir(home_dir.join("profiles")) {
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

    fn scan_roots(&self, roots: &[std::path::PathBuf], _max_files: Option<usize>) -> (Vec<UsageRecord>, ScanStats) {
        // max_files 对本源无意义:sqlite 单库全量读
        let mut stats = ScanStats::default();
        if roots.is_empty() {
            stats.notes.push("未发现 state.db 或 HERMES_HOME".into());
            return (vec![], stats);
        }

        let mut records: Vec<UsageRecord> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        let mut skipped_no_time = 0u64;

        for db in roots {
            stats.files_seen += 1;
            let Some(conn) = open_ro(db) else {
                stats.notes.push(format!("{} 打开失败", db.display()));
                stats.failed = true;
                continue;
            };
            if !has_table(&conn, "sessions") {
                stats.notes.push(format!("{} 无 sessions 表,忽略", db.display()));
                continue;
            }
            // §6.2:官方 usage_totals 行判据(parent NULL + msg≥1 + 未归档),列缺省防御
            let mut sql = "SELECT id, model, started_at, input_tokens, output_tokens, \
                       cache_read_tokens, cache_write_tokens, reasoning_tokens, \
                       actual_cost_usd, estimated_cost_usd FROM sessions".to_string();
            if has_column(&conn, "sessions", "parent_session_id")
                && has_column(&conn, "sessions", "message_count")
                && has_column(&conn, "sessions", "archived")
            {
                sql.push_str(" WHERE parent_session_id IS NULL \
                       AND COALESCE(message_count,0) >= 1 AND COALESCE(archived,0) = 0");
            }
            let Ok(mut stmt) = conn.prepare(&sql) else {
                stats.notes.push(format!("{} sessions 查询构建失败", db.display()));
                stats.failed = true;
                continue;
            };
            let Ok(mut rows) = stmt.query([]) else {
                stats.notes.push(format!("{} sessions 查询执行失败", db.display()));
                stats.failed = true;
                continue;
            };
            stats.files_parsed += 1;
            while let Ok(Some(row)) = rows.next() {
                let vals: Vec<SqlValue> = (0..10).filter_map(|i| row.get::<_, SqlValue>(i).ok()).collect();
                if vals.len() < 10 {
                    stats.lines_bad_json += 1;
                    continue;
                }
                let Some(id) = v_text(&vals[0]) else {
                    stats.lines_bad_json += 1;
                    continue;
                };
                stats.lines_total += 1;
                let model = v_text(&vals[1]).unwrap_or_default().trim().to_string();
                if model.is_empty() {
                    continue;
                }
                let (i, o) = (v_u64(&vals[3]), v_u64(&vals[4]));
                let (cr, cw, rs) = (v_u64(&vals[5]), v_u64(&vals[6]), v_u64(&vals[7]));
                if i == 0 && o == 0 && cr == 0 && cw == 0 && rs == 0 {
                    continue;
                }
                // started_at epoch 秒(可带小数)→ 毫秒,会话开始锚定(§4)
                let ts_ms = match &vals[2] {
                    SqlValue::Real(f) if *f > 0.0 => Some((*f * 1000.0) as i64),
                    SqlValue::Integer(n) if *n > 0 => Some(crate::common::secs_or_ms(*n)),
                    SqlValue::Text(s) => s.trim().parse::<f64>().ok().filter(|f| *f > 0.0).map(|f| (f * 1000.0) as i64),
                    _ => None,
                };
                let Some(ts_ms) = ts_ms else {
                    skipped_no_time += 1;
                    continue;
                };
                // 成本 actual > estimated(§5)
                let cost_usd = v_f64(&vals[8]).or_else(|| v_f64(&vals[9]));
                let key = format!("hermes:{}:{}", db.display(), id);
                if seen.insert(key.clone()) {
                    records.push(UsageRecord {
                        source: "hermes",
                        session_id: id.clone(),
                        agent: None,
                        model,
                        ts_ms,
                        caliber: Caliber::RequestDelta, // 会话终态一次性入账(§5)
                        input_uncached: i,
                        cache_read: cr,
                        cache_write: cw,
                        cache_write_1h: 0,
                        output: o,
                        reasoning_output: rs,
                        cost_usd,
                        dedup_key: key,
                    });
                }
            }
        }

        if skipped_no_time > 0 {
            stats
                .notes
                .push(format!("{} 行 started_at 不可解析,跳过", skipped_no_time));
        }
        (records, stats)
    }
}
