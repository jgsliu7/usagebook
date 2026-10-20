//! Qoder 适配器 —— 依据 docs/数据源档案/27-qoder.md(Community,本机未实证)。
//!
//! 三通道(§1):IDE local.db(Qoder/QoderCN 两变体,roots 直接是 db 文件)+
//!     ~/.qoder/projects 与 ~/.qoderwork/projects 的 *.jsonl(CLI)
//! scan_roots 按形态分流:文件 → IDE;目录 → CLI
//! IDE(§2):chat_message.role=='assistant',token_info {prompt−cached inclusive,completion};
//!     model_info {model_key>model};gmt_create 毫秒/秒容错
//! CLI(§3):type=="assistant" 且 message.usage Anthropic 原生五桶直通
//! 去重键(§4):qoder:ide:{db}:{id} / qoder:{message.id}

use crate::common::{for_each_line, home, iso_to_ms, json_u64, secs_or_ms, walk_with};
use crate::model::{Caliber, ScanStats, UsageRecord, UsageSource};
use rusqlite::types::Value as SqlValue;
use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Duration;

pub struct QoderSource;

/// SQLite 值 → 文本。
fn v_text(v: &SqlValue) -> Option<String> {
    match v {
        SqlValue::Text(s) => Some(s.clone()),
        SqlValue::Integer(i) => Some(i.to_string()),
        _ => None,
    }
}

/// IDE 通道(§2):local.db chat_message 表。
fn scan_ide_db(
    db: &std::path::Path,
    records: &mut Vec<UsageRecord>,
    seen: &mut HashSet<String>,
    stats: &mut ScanStats,
) {
    stats.files_seen += 1;
    let conn = match crate::common::open_sqlite_readonly(db) {
        Ok(c) => c,
        Err(e) => {
            stats
                .notes
                .push(format!("{} 打开失败: {}", db.display(), e));
            stats.failed = true;
            return;
        }
    };
    if let Err(e) = conn.busy_timeout(Duration::from_secs(5)) {
        stats.notes.push(format!("busy_timeout 设置失败: {}", e));
        stats.failed = true;
    }
    let _ = conn.pragma_update(None, "query_only", true);

    let has_table = conn
        .query_row(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='chat_message'",
            [],
            |r| r.get::<_, i64>(0),
        )
        .unwrap_or(0);
    if has_table == 0 {
        stats
            .notes
            .push(format!("{} 无 chat_message 表,忽略", db.display()));
        return;
    }
    let sql = "SELECT m.id, m.gmt_create, m.token_info, m.model_info, m.session_id \
               FROM chat_message m \
               WHERE m.role='assistant' AND m.token_info IS NOT NULL AND length(m.token_info) > 2";
    let Ok(mut stmt) = conn.prepare(sql) else {
        stats.notes.push("chat_message 查询构建失败".into());
        stats.failed = true;
        return;
    };
    let Ok(mut rows) = stmt.query([]) else {
        stats.notes.push("chat_message 查询执行失败".into());
        stats.failed = true;
        return;
    };
    stats.files_parsed += 1;
    while let Ok(Some(row)) = rows.next() {
        let vals: Vec<SqlValue> = (0..5)
            .filter_map(|i| row.get::<_, SqlValue>(i).ok())
            .collect();
        if vals.len() < 5 {
            stats.lines_bad_json += 1;
            continue;
        }
        let Some(id) = v_text(&vals[0]) else {
            stats.lines_bad_json += 1;
            continue;
        };
        stats.lines_total += 1;
        // token_info JSON 文本(§2)
        let Some(Ok(ti)) = v_text(&vals[2]).map(|s| serde_json::from_str::<serde_json::Value>(&s))
        else {
            stats.lines_bad_json += 1;
            continue;
        };
        let prompt = json_u64(ti.get("prompt_tokens"));
        let cached = json_u64(ti.get("cached_tokens"));
        let completion = json_u64(ti.get("completion_tokens"));
        let input = prompt.saturating_sub(cached); // inclusive 减除(§2)
        if input == 0 && completion == 0 && cached == 0 {
            continue;
        }
        let ts_ms = match &vals[1] {
            SqlValue::Integer(n) if *n > 0 => secs_or_ms(*n),
            SqlValue::Text(s) => s.trim().parse::<i64>().ok().map(secs_or_ms).unwrap_or(0),
            _ => 0,
        };
        if ts_ms <= 0 {
            stats.lines_unknown_type += 1;
            continue;
        }
        // model_info:model_key > model(§2)
        let model = v_text(&vals[3])
            .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
            .and_then(|mi| {
                ["model_key", "model"]
                    .iter()
                    .find_map(|k| mi.get(*k).and_then(|x| x.as_str()).map(|s| s.to_string()))
                    .filter(|m| !m.is_empty())
            })
            .unwrap_or_else(|| "unknown".into());
        let session = v_text(&vals[4]).unwrap_or_else(|| id.clone());
        let key = format!("qoder:ide:{}:{}", db.display(), id);
        if seen.insert(key.clone()) {
            records.push(UsageRecord {
                source: "qoder",
                session_id: session,
                agent: None,
                model,
                ts_ms,
                caliber: Caliber::RequestDelta,
                request_count: 1,
                input_uncached: input,
                cache_read: cached,
                cache_write: 0, // token_info 无 cache_write(§2)
                cache_write_1h: 0,
                output: completion,
                reasoning_output: 0,
                cost_usd: None,
                dedup_key: key,
            });
        }
    }
}

impl UsageSource for QoderSource {
    fn id(&self) -> &'static str {
        "qoder"
    }

    /// roots 混合:IDE db 文件 + CLI projects 目录(§1)。
    fn discover_roots(&self) -> Vec<PathBuf> {
        let mut roots: Vec<PathBuf> = Vec::new();
        let base = home().join("Library/Application Support");
        for v in ["Qoder", "QoderCN"] {
            let db = base.join(v).join("SharedClientCache/cache/db/local.db");
            if db.is_file() {
                roots.push(db);
            }
        }
        for v in [".qoder", ".qoderwork"] {
            let d = home().join(v).join("projects");
            if d.is_dir() {
                roots.push(d);
            }
        }
        roots
    }

    fn scan_roots(
        &self,
        roots: &[std::path::PathBuf],
        max_files: Option<usize>,
    ) -> (Vec<UsageRecord>, ScanStats) {
        let mut stats = ScanStats::default();
        if roots.is_empty() {
            stats
                .notes
                .push("未发现 Qoder/QoderCN local.db 或 ~/.qoder(~work)/projects".into());
            return (vec![], stats);
        }

        let mut records: Vec<UsageRecord> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        let mut dup_dropped = 0u64;

        for root in roots {
            if root.is_file() {
                // IDE 通道(§2)
                scan_ide_db(root, &mut records, &mut seen, &mut stats);
                continue;
            }
            // CLI 通道(§3):目录 → 递归 *.jsonl
            let files = walk_with(root, max_files, &|p| {
                p.extension().is_some_and(|x| x == "jsonl")
            });
            stats.files_seen += files.len();
            stats.notes.push(format!(
                "CLI 根 {}:{} 个 *.jsonl",
                root.display(),
                files.len()
            ));
            for path in &files {
                let (total, bad) = for_each_line(path, |line| {
                    let Ok(obj) = serde_json::from_str::<serde_json::Value>(line) else {
                        return;
                    };
                    if obj.get("type").and_then(|x| x.as_str()) != Some("assistant") {
                        return;
                    }
                    let Some(msg) = obj.get("message") else {
                        return;
                    };
                    let Some(u) = msg.get("usage") else { return };
                    let (i, o) = (
                        json_u64(u.get("input_tokens")),
                        json_u64(u.get("output_tokens")),
                    );
                    let cr = json_u64(u.get("cache_read_input_tokens"));
                    let cw = json_u64(u.get("cache_creation_input_tokens"));
                    if i == 0 && o == 0 && cr == 0 && cw == 0 {
                        return;
                    }
                    let ts_ms = obj
                        .get("timestamp")
                        .and_then(|x| x.as_str())
                        .and_then(iso_to_ms)
                        .unwrap_or(0);
                    if ts_ms <= 0 {
                        return;
                    }
                    let model = msg
                        .get("model")
                        .and_then(|x| x.as_str())
                        .unwrap_or("unknown")
                        .to_string();
                    let key = match msg.get("id").and_then(|x| x.as_str()) {
                        Some(mid) => format!("qoder:{mid}"),
                        None => format!("qoder:{}:{}:{}", path.display(), ts_ms, records.len()),
                    };
                    if !seen.insert(key.clone()) {
                        dup_dropped += 1;
                        return;
                    }
                    records.push(UsageRecord {
                        source: "qoder",
                        session_id: path
                            .file_stem()
                            .and_then(|s| s.to_str())
                            .unwrap_or("qoder-cli")
                            .to_string(),
                        agent: None,
                        model,
                        ts_ms,
                        caliber: Caliber::RequestDelta,
                        request_count: 1,
                        input_uncached: i,
                        cache_read: cr,
                        cache_write: cw,
                        cache_write_1h: 0,
                        output: o,
                        reasoning_output: 0,
                        cost_usd: None,
                        dedup_key: key,
                    });
                });
                stats.lines_total += total;
                stats.lines_bad_json += bad;
                stats.files_parsed += 1;
            }
        }

        if dup_dropped > 0 {
            stats
                .notes
                .push(format!("去重键命中丢弃 {dup_dropped} 条(§4)"));
        }
        (records, stats)
    }
}
