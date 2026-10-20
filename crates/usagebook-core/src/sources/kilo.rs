//! Kilo Code 适配器 —— 依据 docs/数据源档案/24-kilo.md(Community,本机未实证)。
//!
//! 根:KILO_DATA_DIR / KILO_HOME(多根,设置后不回退)或默认 ~/.local/share/kilo(§1)
//! 库格式(§2):kilo.db → message(id, session_id, data JSON),opencode 同源 schema
//! 五桶(§3):tokens 五桶直取 + total 差额兜底(output==0 补 output);cache 非对象按 0
//! 时间(§4):time.completed > time.created(秒/毫秒自适应);皆缺跳过计数
//! 去重键:kilo:{id 列}(§5,主键幂等;流式更新重扫取每 id 终值)

use crate::common::{env_roots, home, json_u64};
use crate::model::{Caliber, ScanStats, UsageRecord, UsageSource};
use rusqlite::types::Value as SqlValue;
use std::collections::HashSet;
use std::path::PathBuf;

pub struct KiloSource;

/// SQLite 值 → 文本(zcode value_to_string 同款)。
fn v_text(v: &SqlValue) -> Option<String> {
    match v {
        SqlValue::Text(s) => Some(s.clone()),
        SqlValue::Integer(i) => Some(i.to_string()),
        _ => None,
    }
}

impl UsageSource for KiloSource {
    fn id(&self) -> &'static str {
        "kilo"
    }

    fn discover_roots(&self) -> Vec<PathBuf> {
        let mut roots = env_roots("KILO_DATA_DIR");
        for r in env_roots("KILO_HOME") {
            roots.push(r);
        }
        roots.retain(|p| p.is_dir());
        if roots.is_empty() {
            let d = home().join(".local/share/kilo");
            if d.is_dir() {
                roots.push(d);
            }
        }
        roots
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
                .push("未发现 ~/.local/share/kilo 或 KILO_DATA_DIR/KILO_HOME".into());
            return (vec![], stats);
        }

        let mut records: Vec<UsageRecord> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        let mut skipped_no_time = 0u64;
        let mut total_gap = 0u64;

        for root in roots {
            let db = root.join("kilo.db");
            stats.files_seen += 1;
            if !db.is_file() {
                stats
                    .notes
                    .push(format!("根 {} 下无 kilo.db", root.display()));
                continue;
            }
            let conn = match crate::common::open_sqlite_readonly(&db) {
                Ok(c) => c,
                Err(e) => {
                    stats.notes.push(format!("kilo.db 打开失败: {}", e));
                    stats.failed = true;
                    continue;
                }
            };

            let has_table = conn
                .query_row(
                    "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='message'",
                    [],
                    |r| r.get::<_, i64>(0),
                )
                .unwrap_or(0);
            if has_table == 0 {
                stats
                    .notes
                    .push(format!("{} 无 message 表,忽略", db.display()));
                continue;
            }
            let sql = "SELECT id, session_id, data FROM message";
            let Ok(mut stmt) = conn.prepare(sql) else {
                stats.notes.push("message 查询构建失败".into());
                stats.failed = true;
                continue;
            };
            let Ok(mut rows) = stmt.query([]) else {
                stats.notes.push("message 查询执行失败".into());
                stats.failed = true;
                continue;
            };
            stats.files_parsed += 1;
            while let Ok(Some(row)) = rows.next() {
                let vals: Vec<SqlValue> = (0..3)
                    .filter_map(|i| row.get::<_, SqlValue>(i).ok())
                    .collect();
                if vals.len() < 3 {
                    stats.lines_bad_json += 1;
                    continue;
                }
                let (Some(id), Some(session), Some(data)) =
                    (v_text(&vals[0]), v_text(&vals[1]), v_text(&vals[2]))
                else {
                    stats.lines_bad_json += 1;
                    continue;
                };
                stats.lines_total += 1;
                let Ok(doc) = serde_json::from_str::<serde_json::Value>(&data) else {
                    stats.lines_bad_json += 1;
                    continue;
                };
                // 用量行判据(§2)
                if doc.get("role").and_then(|x| x.as_str()) != Some("assistant") {
                    continue;
                }
                let Some(tk) = doc.get("tokens") else {
                    continue;
                };
                let (i, o) = (json_u64(tk.get("input")), json_u64(tk.get("output")));
                let (cr, cw) = match tk.get("cache") {
                    Some(c) if c.is_object() => (json_u64(c.get("read")), json_u64(c.get("write"))),
                    _ => (0, 0),
                };
                let rs = json_u64(tk.get("reasoning"));
                if i == 0 && o == 0 && cr == 0 && cw == 0 && rs == 0 {
                    continue;
                }
                // total 差额兜底(§3):output==0 补 output,否则忽略计数
                let sum = i + o + cr + cw + rs;
                let total = json_u64(tk.get("total"));
                let output = if total > sum && o == 0 {
                    total - sum
                } else {
                    if total > sum {
                        total_gap += 1;
                    }
                    o
                };
                // 时间戳链(§4):completed > created
                let ts_ms = ["completed", "created"]
                    .iter()
                    .find_map(|k| doc.pointer(&format!("/time/{k}")).and_then(|x| x.as_i64()))
                    .map(crate::common::secs_or_ms);
                let Some(ts_ms) = ts_ms else {
                    skipped_no_time += 1;
                    continue;
                };
                let model = ["modelID", "model", "modelId"]
                    .iter()
                    .find_map(|k| doc.get(*k).and_then(|x| x.as_str()))
                    .unwrap_or("unknown")
                    .to_string();
                let key = format!("kilo:{}", id);
                if seen.insert(key.clone()) {
                    records.push(UsageRecord {
                        source: "kilo",
                        session_id: doc
                            .get("sessionID")
                            .and_then(|x| x.as_str())
                            .map(|s| s.to_string())
                            .unwrap_or(session),
                        agent: None,
                        model,
                        ts_ms,
                        caliber: Caliber::RequestDelta,
                        request_count: 1,
                        input_uncached: i,
                        cache_read: cr,
                        cache_write: cw,
                        cache_write_1h: 0,
                        output,
                        reasoning_output: rs,
                        cost_usd: doc.get("cost").and_then(|x| x.as_f64()),
                        dedup_key: key,
                    });
                }
            }
        }

        if skipped_no_time > 0 {
            stats.notes.push(format!(
                "{} 行 time.completed/created 均缺,跳过",
                skipped_no_time
            ));
        }
        if total_gap > 0 {
            stats.notes.push(format!(
                "total 差额超五桶和且 output≠0,忽略差额 {total_gap} 行(§3)"
            ));
        }
        (records, stats)
    }
}
