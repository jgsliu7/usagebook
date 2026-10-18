//! Goose 适配器 —— 依据 docs/数据源档案/23-goose.md(源码一手取证+实库对账)。
//!
//! 根(roots 直接是 db 文件,§6.1):AI_USAGE_GOOSE_DB(解析器自有约定)/ GOOSE_PATH_ROOT/data/
//!     sessions/sessions.db / 默认候选(~/.local/share/goose 现行 XDG;Library 与 Block 形态兜底)
//! 双通道(§6.3,ledger 首选):usage_ledger 逐请求表(v15+)存在且非空 → 逐请求入账;
//!     否则回退 sessions.accumulated_*(会话级,§4 整会话计入 created_at)
//! 五桶(§6.3):input_tokens **已含 cache**(子集)→ input_uncached = in − cr − cw;
//!     reasoning 无列恒 0;compaction 行(is_compaction=1)跳过并计数
//! 成本(§6.3):cost 仅 provider_reported 落 cost_usd(estimated 不落,model.rs 纪律)
//! 去重键:goose:{db路径}:ledger:{id} / 会话兜底 goose:{db路径}:{id}(§6.3)

use crate::common::{env_roots, home, iso_to_ms, secs_or_ms};
use crate::model::{Caliber, ScanStats, UsageRecord, UsageSource};
use chrono::NaiveDate;
use rusqlite::types::Value as SqlValue;
use rusqlite::Connection;
use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Duration;

pub struct GooseSource;

/// goose created_at 多格式(§2):RFC3339 > naive "YYYY-MM-DD HH:MM:SS"(UTC)>
/// "YYYY-MM-DD" > epoch 秒/毫秒数字 → 毫秒。
fn goose_time(v: &SqlValue) -> Option<i64> {
    match v {
        SqlValue::Text(s) => {
            if let Some(ms) = iso_to_ms(s) {
                return Some(ms);
            }
            if let Ok(n) = s.parse::<i64>() {
                return Some(secs_or_ms(n));
            }
            if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                return d
                    .and_hms_opt(0, 0, 0)
                    .map(|t| t.and_utc().timestamp_millis());
            }
            NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                .ok()
                .map(|t| t.and_utc().timestamp_millis())
        }
        SqlValue::Integer(i) if *i > 0 => Some(secs_or_ms(*i)),
        SqlValue::Real(f) if *f > 0.0 => Some(secs_or_ms(*f as i64)),
        _ => None,
    }
}

use chrono::NaiveDateTime;

/// SQLite 值 → 文本(zcode value_to_string 同款)。
fn v_text(v: &SqlValue) -> Option<String> {
    match v {
        SqlValue::Text(s) => Some(s.clone()),
        SqlValue::Integer(i) => Some(i.to_string()),
        _ => None,
    }
}

/// SQLite 值 → u64(§2 缺列按 NULL 读 → 0)。
fn v_u64(v: &SqlValue) -> u64 {
    match v {
        SqlValue::Integer(i) => (*i).max(0) as u64,
        SqlValue::Real(f) if *f > 0.0 => *f as u64,
        SqlValue::Text(s) => s.trim().parse().unwrap_or(0),
        _ => 0,
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
        roots: &[std::path::PathBuf],
        _max_files: Option<usize>,
    ) -> (Vec<UsageRecord>, ScanStats) {
        // max_files 对本源无意义:sqlite 单库全量读
        let mut stats = ScanStats::default();
        if roots.is_empty() {
            stats
                .notes
                .push("未发现 goose sessions.db 或 GOOSE_PATH_ROOT/AI_USAGE_GOOSE_DB".into());
            return (vec![], stats);
        }

        let mut records: Vec<UsageRecord> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        let mut skipped_no_time = 0u64;

        for db in roots {
            stats.files_seen += 1;
            let conn = match crate::common::open_sqlite_readonly(db) {
                Ok(c) => c,
                Err(e) => {
                    stats
                        .notes
                        .push(format!("{} 打开失败: {}", db.display(), e));
                    stats.failed = true;
                    continue;
                }
            };
            if let Err(e) = conn.busy_timeout(Duration::from_secs(5)) {
                stats.notes.push(format!("busy_timeout 设置失败: {}", e));
                stats.failed = true;
            }
            let _ = conn.pragma_update(None, "query_only", true);

            // 缺表整库忽略(§2)
            let has_sessions = conn
                .query_row(
                    "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='sessions'",
                    [],
                    |r| r.get::<_, i64>(0),
                )
                .unwrap_or(0);
            if has_sessions == 0 {
                stats
                    .notes
                    .push(format!("{} 无 sessions 表,忽略", db.display()));
                continue;
            }

            // ===== 首选通道:usage_ledger 逐请求表(v15+,§6.3)=====
            let has_ledger = conn
                .query_row(
                    "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='usage_ledger'",
                    [],
                    |r| r.get::<_, i64>(0),
                )
                .unwrap_or(0)
                > 0;
            if has_ledger {
                stats.files_parsed += 1;
                scan_ledger(db, &conn, &mut records, &mut seen, &mut stats);
                continue;
            }
            stats.notes.push(format!(
                "{} 无 usage_ledger(老库),回退 sessions 会话级(§6.3)",
                db.display()
            ));

            // ===== 兜底通道:sessions.accumulated_*(会话级;cache 列 v14+ 缺省防御)=====
            let has_cache_col = has_column(&conn, "sessions", "accumulated_cache_read_tokens");
            let cache_sel = if has_cache_col {
                ", COALESCE(accumulated_cache_read_tokens,0), COALESCE(accumulated_cache_write_tokens,0)"
            } else {
                ", 0, 0"
            };
            let sql = format!(
                "SELECT id, model_config_json, created_at, \
                       total_tokens, input_tokens, output_tokens, \
                       accumulated_total_tokens, accumulated_input_tokens, accumulated_output_tokens \
                       {cache_sel} FROM sessions"
            );
            let Ok(mut stmt) = conn.prepare(&sql) else {
                stats
                    .notes
                    .push(format!("{} sessions 查询构建失败", db.display()));
                stats.failed = true;
                continue;
            };
            let rows = stmt.query([]);
            let mut rows = match rows {
                Ok(r) => r,
                Err(e) => {
                    // 新旧列可能部分缺失(旧库),按 NULL 读仍失败才放弃(§2)
                    stats
                        .notes
                        .push(format!("{} sessions 查询失败: {}", db.display(), e));
                    stats.failed = true;
                    continue;
                }
            };
            stats.files_parsed += 1;
            while let Ok(Some(row)) = rows.next() {
                let vals: Vec<SqlValue> = (0..11)
                    .filter_map(|i| row.get::<_, SqlValue>(i).ok())
                    .collect();
                if vals.len() < 11 {
                    stats.lines_bad_json += 1;
                    continue;
                }
                let Some(id) = v_text(&vals[0]) else {
                    stats.lines_bad_json += 1;
                    continue;
                };
                stats.lines_total += 1;
                // 用量行判据(§2):model_config_json 非空白
                let config = v_text(&vals[1]).unwrap_or_default().trim().to_string();
                if config.is_empty() {
                    continue;
                }
                // 新列优先,旧列回落(§3)
                let acc = |new: usize, old: usize| {
                    let n = v_u64(&vals[new]);
                    if n > 0 {
                        n
                    } else {
                        v_u64(&vals[old])
                    }
                };
                let input_incl = acc(7, 4); // accumulated_input_tokens(含 cache,§6.3)
                let output = acc(8, 5);
                let total = acc(6, 3);
                let cr = v_u64(&vals[9]);
                let cw = v_u64(&vals[10]);
                if input_incl == 0 && output == 0 && total == 0 {
                    continue;
                }
                // input 已含 cache(子集)→ 拆五桶(§6.3);reasoning 无列恒 0
                let input_uncached = input_incl.saturating_sub(cr).saturating_sub(cw);
                // 会话开始时刻锚定(§4)
                let Some(ts_ms) = goose_time(&vals[2]) else {
                    skipped_no_time += 1;
                    continue;
                };
                let model = serde_json::from_str::<serde_json::Value>(&config)
                    .ok()
                    .and_then(|v| {
                        v.get("model_name")
                            .and_then(|m| m.as_str())
                            .map(|s| s.to_string())
                    })
                    .filter(|m| !m.is_empty())
                    .unwrap_or_else(|| "unknown".into());
                let key = format!("goose:{}:{}", db.display(), id);
                if seen.insert(key.clone()) {
                    records.push(UsageRecord {
                        source: "goose",
                        session_id: id.clone(),
                        agent: None,
                        model,
                        ts_ms,
                        caliber: Caliber::RequestDelta, // 会话终态一次性入账(§5)
                        input_uncached,
                        cache_read: cr,
                        cache_write: cw,
                        cache_write_1h: 0,
                        output,
                        reasoning_output: 0, // 无 reasoning 列(§6.3,原 total−in−out 公式作废)
                        cost_usd: None,
                        dedup_key: key,
                    });
                }
            }
        }

        if skipped_no_time > 0 {
            stats
                .notes
                .push(format!("{} 行 created_at 不可解析,跳过", skipped_no_time));
        }
        (records, stats)
    }
}

/// usage_ledger 逐请求通道(§6.3):五桶拆分 + compaction 跳过 + provider_reported 成本。
fn scan_ledger(
    db: &std::path::Path,
    conn: &Connection,
    records: &mut Vec<UsageRecord>,
    seen: &mut HashSet<String>,
    stats: &mut ScanStats,
) {
    let has_cost_col = has_column(conn, "usage_ledger", "cost_source");
    let cost_sel = if has_cost_col {
        ", cost, cost_source"
    } else {
        ", NULL, NULL"
    };
    let sql = format!(
        "SELECT id, session_id, created_timestamp, model, \
                input_tokens, output_tokens, cache_read_tokens, cache_write_tokens \
                {cost_sel}, is_compaction FROM usage_ledger ORDER BY id"
    );
    let Ok(mut stmt) = conn.prepare(&sql) else {
        stats
            .notes
            .push(format!("{} usage_ledger 查询构建失败", db.display()));
        stats.failed = true;
        return;
    };
    let Ok(mut rows) = stmt.query([]) else {
        stats
            .notes
            .push(format!("{} usage_ledger 查询执行失败", db.display()));
        stats.failed = true;
        return;
    };
    let mut skipped_compaction = 0u64;
    while let Ok(Some(row)) = rows.next() {
        let vals: Vec<SqlValue> = (0..11)
            .filter_map(|i| row.get::<_, SqlValue>(i).ok())
            .collect();
        if vals.len() < 11 {
            stats.lines_bad_json += 1;
            continue;
        }
        let Some(lid) = v_text(&vals[0]) else {
            stats.lines_bad_json += 1;
            continue;
        };
        stats.lines_total += 1;
        if v_u64(&vals[10]) == 1 {
            // compaction 行的旧列语义是"保留上下文数"非真实请求量(§6.3)
            skipped_compaction += 1;
            continue;
        }
        let session_id = v_text(&vals[1]).unwrap_or_default();
        let Some(ts_ms) = goose_time(&vals[2]) else {
            stats.lines_bad_json += 1;
            continue;
        };
        let model = v_text(&vals[3]).unwrap_or_default();
        let model = if model.trim().is_empty() {
            "unknown".into()
        } else {
            model
        };
        let input_incl = v_u64(&vals[4]); // 已含 cache(子集,§6.3)
        let output = v_u64(&vals[5]);
        let cr = v_u64(&vals[6]);
        let cw = v_u64(&vals[7]);
        let cost = match &vals[8] {
            SqlValue::Real(f) if *f > 0.0 => Some(*f),
            SqlValue::Integer(n) if *n > 0 => Some(*n as f64), // INTEGER 正值成本兼容(2026-10-17)
            _ => None,
        };
        let cost_source =
            v_text(&vals.get(9).cloned().unwrap_or(SqlValue::Null)).unwrap_or_default();
        if input_incl == 0 && output == 0 && cr == 0 && cw == 0 {
            continue; // 全零跳过(三跳过)
        }
        let key = format!("goose:{}:ledger:{}", db.display(), lid);
        if seen.insert(key.clone()) {
            records.push(UsageRecord {
                source: "goose",
                session_id,
                agent: None,
                model,
                ts_ms,
                caliber: Caliber::RequestDelta,
                input_uncached: input_incl.saturating_sub(cr).saturating_sub(cw),
                cache_read: cr,
                cache_write: cw,
                cache_write_1h: 0,
                output,
                reasoning_output: 0, // ledger 无 reasoning 列(§6.3)
                // 估算不落(model.rs 纪律):仅 provider_reported 视为官方实报
                cost_usd: if cost_source == "provider_reported" {
                    cost
                } else {
                    None
                },
                dedup_key: key,
            });
        }
    }
    if skipped_compaction > 0 {
        stats
            .notes
            .push(format!("compaction 行跳过 {skipped_compaction} 条(§6.3)"));
    }
}

/// 列存在性防御(旧库缺 v14+ 列,§6.2)。
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn goose_time_formats() {
        assert_eq!(
            goose_time(&SqlValue::Text("2026-01-02 03:04:05".into())),
            Some(1767323045000)
        );
        assert!(goose_time(&SqlValue::Text("2026-01-02".into())).is_some());
        assert!(goose_time(&SqlValue::Text("2026-01-02T03:04:05Z".into())).is_some());
        assert_eq!(
            goose_time(&SqlValue::Integer(1750000000)),
            Some(1750000000000)
        );
        assert_eq!(
            goose_time(&SqlValue::Integer(1750000000000)),
            Some(1750000000000)
        );
        assert_eq!(goose_time(&SqlValue::Null), None);
    }
}
