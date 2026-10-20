//! Gemini CLI 适配器 —— 依据 docs/数据源档案/18-gemini.md(Community,本机未实证)。
//!
//! 根:~/.gemini/tmp;GEMINI_DATA_DIR(tmp 级直用)或 GEMINI_HOME(~/.gemini 级拼 tmp)(§1)
//! 文件:递归 *.jsonl + *.json 双格式(§2)
//! jsonl:type=="gemini"(逐字节精确)且带 tokens;同 id 后值覆盖前值(流式更新,§4)
//! inclusive 推断:total==input+output+thoughts+tool 且 cached>0 → input 减 cached(§3)
//! .json:messages 数组同 jsonl;stats.models 会话聚合 → 每模型一条 @startTime(§2b)
//! 时间戳:行 timestamp|created_at(ISO)> startTime > 文件 mtime(§5)
//! 去重:有 id 用 id;无 id 内容键;stats 用 文件名:模型(§4)

use crate::common::{env_roots, home, iso_to_ms, json_u64, walk_with};
use crate::model::{Caliber, ScanStats, UsageRecord, UsageSource};
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;

pub struct GeminiSource;

/// tokens 对象(别名组)→ [input_uncached, cache_read, output, reasoning](§2/§3)。
fn map_tokens(t: &Value) -> [u64; 4] {
    let g = |keys: &[&str]| {
        for k in keys {
            if let Some(x) = t.get(*k) {
                if !x.is_null() {
                    return json_u64(Some(x));
                }
            }
        }
        0
    };
    let input = g(&["input", "prompt", "input_tokens", "prompt_tokens"]);
    let output = g(&["output", "candidates", "output_tokens", "candidates_tokens"]);
    let cached = g(&["cached", "cached_tokens"]);
    let thoughts = g(&[
        "thoughts",
        "reasoning",
        "thoughts_tokens",
        "reasoning_tokens",
    ]);
    let tool = g(&["tool", "tool_tokens"]);
    let total = g(&["total", "total_tokens"]);
    // inclusive 推断(§3):total 恰为四项和且 cached>0 → input 含缓存,减除
    if cached > 0
        && total
            == input
                .saturating_add(output)
                .saturating_add(thoughts)
                .saturating_add(tool)
    {
        [
            input.saturating_sub(cached.min(input)),
            cached,
            output,
            thoughts,
        ]
    } else {
        [input, cached, output, thoughts]
    }
}

impl UsageSource for GeminiSource {
    fn id(&self) -> &'static str {
        "gemini"
    }

    fn discover_roots(&self) -> Vec<PathBuf> {
        // §7.1:真实变量为 GEMINI_CLI_HOME(替换 home);GEMINI_DATA_DIR/GEMINI_HOME 为
        // 第三方解析器约定,保留作兼容别名
        let mut roots = env_roots("GEMINI_CLI_HOME")
            .into_iter()
            .map(|p| p.join("tmp"))
            .collect::<Vec<_>>();
        roots.extend(env_roots("GEMINI_DATA_DIR"));
        for p in env_roots("GEMINI_HOME") {
            roots.push(p.join("tmp"));
        }
        roots.retain(|p| p.is_dir());
        if roots.is_empty() {
            let d = home().join(".gemini/tmp");
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
                .push("未发现 ~/.gemini/tmp 或 GEMINI_DATA_DIR/GEMINI_HOME".into());
            return (vec![], stats);
        }
        let mut files: Vec<PathBuf> = Vec::new();
        let mut per_root = Vec::new();
        for r in roots {
            let mut got = walk_with(r, None, &|p| {
                p.extension().is_some_and(|x| x == "jsonl" || x == "json")
            });
            got.sort();
            per_root.push((r.display().to_string(), got.len()));
            files.extend(got);
            if max_files.is_some_and(|m| files.len() >= m) {
                files.truncate(max_files.unwrap());
                break;
            }
        }
        stats.files_seen = files.len();

        let mut records: Vec<UsageRecord> = Vec::new();
        let mut last_wins: HashMap<String, UsageRecord> = HashMap::new(); // 同 id 流式更新,后值覆盖(§4)

        for path in &files {
            let mtime_ms = path
                .metadata()
                .ok()
                .and_then(|m| m.modified().ok())
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            let stem = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_string();
            let is_jsonl = path.extension().is_some_and(|x| x == "jsonl");

            // 逐行处理(jsonl)或 messages 数组处理(.json,§2)
            let feed_line =
                |v: &Value, sticky_model: &mut String, file_model: &str| -> Option<UsageRecord> {
                    let tokens = v.get("tokens").filter(|t| t.is_object())?;
                    let line_type = v.get("type").and_then(|x| x.as_str()).unwrap_or("");
                    if line_type != "gemini" {
                        return None; // 非用量行(首行会话元数据等)
                    }
                    let [i, cr, o, r] = map_tokens(tokens);
                    if i == 0 && cr == 0 && o == 0 && r == 0 {
                        return None;
                    }
                    let ts_ms = v
                        .get("timestamp")
                        .or_else(|| v.get("created_at"))
                        .and_then(|x| x.as_str())
                        .and_then(iso_to_ms)
                        .unwrap_or(mtime_ms);
                    let session_id = v
                        .get("sessionId")
                        .or_else(|| v.get("session_id"))
                        .and_then(|x| x.as_str())
                        .unwrap_or(&stem)
                        .to_string();
                    // 模型:行 > sticky 继承 > 文件级(§5)
                    let model = v
                        .get("model")
                        .and_then(|x| x.as_str())
                        .map(|m| {
                            *sticky_model = m.to_string();
                            m.to_string()
                        })
                        .unwrap_or_else(|| {
                            if !sticky_model.is_empty() {
                                sticky_model.clone()
                            } else if !file_model.is_empty() {
                                file_model.to_string()
                            } else {
                                "unknown".into()
                            }
                        });
                    let rec = UsageRecord {
                        source: "gemini",
                        session_id,
                        agent: None,
                        model,
                        ts_ms,
                        caliber: Caliber::RequestDelta,
                        request_count: 1,
                        input_uncached: i,
                        cache_read: cr,
                        cache_write: 0,
                        cache_write_1h: 0,
                        output: o,
                        reasoning_output: r,
                        cost_usd: None,
                        dedup_key: String::new(),
                    };
                    Some(rec)
                };

            if is_jsonl {
                stats.files_parsed += 1;
                let mut sticky = String::new();
                crate::common::for_each_line(path, |line| {
                    stats.lines_total += 1;
                    let Ok(v) = serde_json::from_str::<Value>(line) else {
                        stats.lines_bad_json += 1;
                        return;
                    };
                    let mut sticky_model = sticky.clone();
                    if let Some(mut rec) = feed_line(&v, &mut sticky_model, "") {
                        // 同 id 后值覆盖;无 id 内容键直接入队(§4)
                        match v.get("id").and_then(|x| x.as_str()) {
                            Some(id) => {
                                rec.dedup_key = format!("gemini:{}:{id}", rec.session_id);
                                last_wins.insert(rec.dedup_key.clone(), rec);
                            }
                            None => {
                                rec.dedup_key = format!(
                                    "gemini:{}:{}:{}:{}",
                                    rec.session_id, rec.ts_ms, rec.model, rec.input_uncached
                                );
                                last_wins.insert(rec.dedup_key.clone(), rec);
                            }
                        }
                    }
                    sticky = sticky_model;
                });
            } else {
                // .json(§2):messages 形状或 stats.models 会话聚合
                let doc = std::fs::read_to_string(path)
                    .ok()
                    .and_then(|t| serde_json::from_str::<Value>(&t).ok());
                let Some(doc) = doc else {
                    stats.lines_bad_json += 1;
                    continue;
                };
                stats.files_parsed += 1;
                // Gemini can initialize logs.json as an empty array before any
                // chat exists. This is recognized empty data, not a new usage shape.
                if path.file_name().is_some_and(|n| n == "logs.json")
                    && doc.as_array().is_some_and(|items| items.is_empty())
                {
                    continue;
                }
                let file_model = doc.get("model").and_then(|x| x.as_str()).unwrap_or("");
                let start_ms = doc
                    .get("startTime")
                    .or_else(|| doc.get("timestamp"))
                    .and_then(|x| x.as_str())
                    .and_then(iso_to_ms)
                    .unwrap_or(mtime_ms);
                let file_session = doc
                    .get("sessionId")
                    .or_else(|| doc.get("session_id"))
                    .and_then(|x| x.as_str())
                    .unwrap_or(&stem)
                    .to_string();
                if let Some(msgs) = doc.get("messages").and_then(|x| x.as_array()) {
                    stats.lines_total += msgs.len() as u64;
                    let mut sticky = String::new();
                    for v in msgs {
                        let mut sticky_model = sticky.clone();
                        if let Some(mut rec) = feed_line(v, &mut sticky_model, file_model) {
                            rec.session_id = file_session.clone();
                            match v.get("id").and_then(|x| x.as_str()) {
                                Some(id) => {
                                    rec.dedup_key = format!("gemini:{}:{id}", rec.session_id)
                                }
                                None => {
                                    rec.dedup_key = format!(
                                        "gemini:{}:{}:{}",
                                        rec.session_id, rec.ts_ms, rec.model
                                    )
                                }
                            }
                            last_wins.insert(rec.dedup_key.clone(), rec);
                        }
                        sticky = sticky_model;
                    }
                } else if let Some(models) =
                    doc.pointer("/stats/models").and_then(|x| x.as_object())
                {
                    // 会话级聚合(§2b):每模型一条 @startTime
                    stats.lines_total += models.len() as u64;
                    for (model, agg) in models {
                        let Some(tokens) = agg.get("tokens") else {
                            continue;
                        };
                        let [i, cr, o, r] = map_tokens(tokens);
                        if i == 0 && cr == 0 && o == 0 && r == 0 {
                            continue;
                        }
                        let key = format!("gemini:{}:{model}", stem);
                        last_wins.insert(
                            key.clone(),
                            UsageRecord {
                                source: "gemini",
                                session_id: file_session.clone(),
                                agent: None,
                                model: model.clone(),
                                ts_ms: start_ms,
                                caliber: Caliber::RequestDelta,
                                request_count: 1,
                                input_uncached: i,
                                cache_read: cr,
                                cache_write: 0,
                                cache_write_1h: 0,
                                output: o,
                                reasoning_output: r,
                                cost_usd: None,
                                dedup_key: key,
                            },
                        );
                    }
                } else {
                    stats.lines_unknown_type += 1;
                }
            }
        }

        let mut keys: Vec<String> = last_wins.keys().cloned().collect();
        keys.sort();
        for k in keys {
            if let Some(rec) = last_wins.remove(&k) {
                records.push(rec);
            }
        }

        for (root, n) in per_root {
            stats.notes.push(format!("根 {}:{n} 个 json/jsonl", root));
        }
        (records, stats)
    }
}
