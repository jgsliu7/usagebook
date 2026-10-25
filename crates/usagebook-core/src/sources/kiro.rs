//! Kiro 适配器 —— 依据 docs/数据源档案/26-kiro.md(Community,本机未实证)。
//!
//! 根(roots 直接是 db 文件,§1):KIRO_CLI_DB_PATH 或默认 macOS kiro-cli/data.sqlite3
//! 库格式(§2):conversations_v2(conversation_id, value JSON)→ $.user_turn_metadata.requests[]
//! 逐请求真实计数 input/output_token_count;char/4 估算兜底不采用(§2,同 JSONL 通道裁决)
//! 模型规范化(§3):请求级 > 会话级 model_info.model_id;arn/前缀/版本后缀剥离
//! 逐请求时间戳 request_start_timestamp_ms(§4)→ RequestDelta
//! 去重键:kiro:{request_id}(§4)

use crate::common::{home, json_u64};
use crate::model::{Caliber, ScanStats, UsageRecord, UsageSource};
use rusqlite::types::Value as SqlValue;
use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Duration;

pub struct KiroSource;

/// 模型规范化(§3):小写;arn: 取最后 / 之后;剥 anthropic./openai./aws. 前缀;
/// 剥 :数字 与 -YYYYMMDD-vN;空或 auto → kiro-cli-agent。
fn norm_model(raw: &str) -> String {
    let s = raw.trim();
    if s.is_empty() {
        return "kiro-cli-agent".into();
    }
    let base = if s.starts_with("arn:") {
        s.rsplit('/').next().unwrap_or(s)
    } else {
        s
    };
    let lowered = base.to_lowercase();
    let stripped = lowered
        .strip_prefix("anthropic.")
        .or_else(|| lowered.strip_prefix("openai."))
        .or_else(|| lowered.strip_prefix("aws."))
        .unwrap_or(&lowered);
    // 剥 :数字 版本后缀
    let mut no_colon = stripped.to_string();
    if let Some(p) = no_colon.find(':') {
        no_colon.truncate(p);
    }
    // 剥 -YYYYMMDD-vN 日期版本段:找 "-"+8 数字 后跟 "-v" 或结尾
    let bytes = no_colon.as_bytes();
    let mut cut = no_colon.len();
    for i in 0..bytes.len() {
        if bytes[i] != b'-' || i + 9 > bytes.len() {
            continue;
        }
        if bytes[i + 1..i + 9].iter().all(u8::is_ascii_digit)
            && (i + 9 == bytes.len() || bytes[i + 9..].starts_with(b"-v"))
        {
            cut = i;
            break;
        }
    }
    no_colon.truncate(cut);
    let out = no_colon.trim_matches(['-', '.']).to_string();
    if out.is_empty() || out == "auto" {
        "kiro-cli-agent".into()
    } else {
        out
    }
}

impl UsageSource for KiroSource {
    fn id(&self) -> &'static str {
        "kiro"
    }

    /// roots 直接返回 db 文件路径(§1)。
    fn discover_roots(&self) -> Vec<PathBuf> {
        let mut dbs = Vec::new();
        if let Ok(v) = std::env::var("KIRO_CLI_DB_PATH") {
            for seg in v.split(',') {
                let p = PathBuf::from(seg.trim());
                if p.is_file() {
                    dbs.push(p);
                }
            }
            return dbs;
        }
        let d = home().join("Library/Application Support/kiro-cli/data.sqlite3");
        if d.is_file() {
            dbs.push(d);
        }
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
                .push("未发现 kiro-cli/data.sqlite3 或 KIRO_CLI_DB_PATH".into());
            return (vec![], stats);
        }

        let mut records: Vec<UsageRecord> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        let mut no_token_counts = 0u64; // 真实计数缺失跳过(§2)
        let mut est_fallback_skipped = 0u64;

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

            let has_v2 = conn
                .query_row(
                    "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='conversations_v2'",
                    [],
                    |r| r.get::<_, i64>(0),
                )
                .unwrap_or(0)
                > 0;
            if !has_v2 {
                stats.notes.push(format!(
                    "{} 无 conversations_v2 表,忽略(旧版布局待适配)",
                    db.display()
                ));
                continue;
            }
            let sql = "SELECT conversation_id, value FROM conversations_v2";
            let Ok(mut stmt) = conn.prepare(sql) else {
                stats.notes.push("conversations_v2 查询构建失败".into());
                stats.failed = true;
                continue;
            };
            let Ok(mut rows) = stmt.query([]) else {
                stats.notes.push("conversations_v2 查询执行失败".into());
                stats.failed = true;
                continue;
            };
            stats.files_parsed += 1;
            while let Some(row) = crate::common::next_sqlite_row(&mut rows, "kiro", &mut stats) {
                let vals: Vec<SqlValue> = (0..2)
                    .filter_map(|i| row.get::<_, SqlValue>(i).ok())
                    .collect();
                if vals.len() < 2 {
                    stats.lines_bad_json += 1;
                    continue;
                }
                let conversation = match &vals[0] {
                    SqlValue::Text(s) => s.clone(),
                    _ => {
                        stats.lines_bad_json += 1;
                        continue;
                    }
                };
                let data = match &vals[1] {
                    SqlValue::Text(s) => s.clone(),
                    _ => {
                        stats.lines_bad_json += 1;
                        continue;
                    }
                };
                let Ok(doc) = serde_json::from_str::<serde_json::Value>(&data) else {
                    stats.lines_bad_json += 1;
                    continue;
                };
                // 会话级模型兜底(§2)
                let sess_model = doc
                    .pointer("/model_info/model_id")
                    .and_then(|x| x.as_str())
                    .map(norm_model);
                let Some(reqs) = doc
                    .pointer("/user_turn_metadata/requests")
                    .and_then(|x| x.as_array())
                else {
                    continue;
                };
                stats.lines_total += reqs.len() as u64;
                for req in reqs {
                    let i = json_u64(req.get("input_token_count"));
                    let o = json_u64(req.get("output_token_count"));
                    if i == 0 && o == 0 {
                        // char/4 估算兜底不采用(§2):无真实计数跳过
                        est_fallback_skipped += 1;
                        continue;
                    }
                    let ts_ms = json_u64(req.get("request_start_timestamp_ms")) as i64;
                    if ts_ms <= 0 {
                        no_token_counts += 1;
                        continue;
                    }
                    let model = req
                        .get("model_id")
                        .and_then(|x| x.as_str())
                        .map(norm_model)
                        .or_else(|| sess_model.clone())
                        .unwrap_or_else(|| "kiro-cli-agent".into());
                    // request_id 缺失用会话内定位键兜底(时间戳+计数位)
                    let key = match req.get("request_id").and_then(|x| x.as_str()) {
                        Some(rid) => format!("kiro:{rid}"),
                        None => format!("kiro:{}:{}:{}", conversation, ts_ms, records.len()),
                    };
                    if seen.insert(key.clone()) {
                        records.push(UsageRecord {
                            source: "kiro",
                            session_id: conversation.clone(),
                            agent: None,
                            model,
                            ts_ms,
                            caliber: Caliber::RequestDelta,
                            request_count: 1,
                            input_uncached: i,
                            cache_read: 0, // 库无缓存列(§4)
                            cache_write: 0,
                            cache_write_1h: 0,
                            output: o,
                            reasoning_output: 0,
                            cost_usd: None,
                            dedup_key: key,
                        });
                    }
                }
            }
        }

        if est_fallback_skipped > 0 {
            stats.notes.push(format!(
                "{} 个请求缺真实 token 计数,跳过(char/4 估算不采用,§2)",
                est_fallback_skipped
            ));
        }
        if no_token_counts > 0 {
            stats.notes.push(format!(
                "{} 个请求缺 request_start_timestamp_ms,跳过",
                no_token_counts
            ));
        }
        (records, stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kiro_model_norm() {
        assert_eq!(norm_model("Claude-Sonnet-4-5"), "claude-sonnet-4-5");
        assert_eq!(norm_model("anthropic.claude-opus-4"), "claude-opus-4");
        assert_eq!(norm_model("openai.gpt-5.2"), "gpt-5.2");
        assert_eq!(
            norm_model("arn:aws:bedrock:us-east-1:123:model/anthropic.claude-sonnet-4-5"),
            "claude-sonnet-4-5"
        );
        assert_eq!(
            norm_model("anthropic.claude-haiku-4-5:1"),
            "claude-haiku-4-5"
        );
        assert_eq!(
            norm_model("us.anthropic.claude-sonnet-4-5-20251001-v1:0"),
            {
                // us.anthropic. 前缀不在剥除清单,日期段剥离生效
                "us.anthropic.claude-sonnet-4-5".to_string()
            }
        );
        assert_eq!(norm_model(""), "kiro-cli-agent");
        assert_eq!(norm_model("auto"), "kiro-cli-agent");
    }
}
