//! Codebuff 适配器 —— 依据 docs/数据源档案/21-codebuff.md(Community,本机未实证)。
//!
//! 根:CODEBUFF_DATA_DIR(名叫 projects 直用,否则拼)或 ~/.config/{manicode,manicode-dev,
//!     manicode-staging}/projects 三 channel(§1)
//! 文件:递归 *.json 文件名精确 == chat-messages.json,整文件 JSON 数组(§2)
//! usage 三处嵌套按序 merge(仅补零字段):metadata.usage → metadata.codebuff.usage →
//!     runState…messageHistory 倒序 assistant 的 providerOptions.usage(§2)
//! 别名组取值 + totalTokens 差额兜底(output==0 时补 output,§3)
//! 时间戳:timestamp > createdAt > metadata.timestamp(ISO/秒) > chatId 目录名 > mtime(§4)
//! 去重键:codebuff:{chatId}:{message.id}(§4)

use crate::common::{env_roots, home, iso_to_ms, json_u64, walk_with};
use crate::model::{Caliber, ScanStats, UsageRecord, UsageSource};
use serde_json::Value;
use std::collections::HashSet;
use std::path::PathBuf;

pub struct CodebuffSource;

/// 别名组取值(§3):首个存在且非 null 的键。
fn alias(v: &Value, keys: &[&str]) -> u64 {
    for k in keys {
        if let Some(x) = v.get(*k) {
            if !x.is_null() {
                return json_u64(Some(x));
            }
        }
    }
    0
}

/// 三处嵌套 merge(§2):仅补零字段。
fn merge_usage(meta: &Value) -> Value {
    let mut acc = serde_json::Map::new();
    let mut sources: Vec<&Value> = Vec::new();
    if let Some(u) = meta.pointer("/usage") {
        sources.push(u);
    }
    if let Some(u) = meta.pointer("/codebuff/usage") {
        sources.push(u);
    }
    // messageHistory 倒序第一个 assistant 的 providerOptions.usage(§2)
    if let Some(hist) = meta
        .pointer("/runState/sessionState/mainAgentState/messageHistory")
        .and_then(|h| h.as_array())
    {
        for m in hist.iter().rev() {
            if m.get("role").and_then(|x| x.as_str()) != Some("assistant") {
                continue;
            }
            if let Some(u) = m.pointer("/providerOptions/usage") {
                sources.push(u);
            } else if let Some(u) = m.pointer("/providerOptions/codebuff/usage") {
                sources.push(u);
            }
            break; // 只取倒序第一个 assistant
        }
    }
    for s in sources {
        if let Some(obj) = s.as_object() {
            for (k, val) in obj {
                let is_zero = acc.get(k).is_none_or(|prev| json_u64(Some(prev)) == 0);
                if is_zero && !val.is_null() {
                    acc.insert(k.clone(), val.clone());
                }
            }
        }
    }
    Value::Object(acc)
}

impl UsageSource for CodebuffSource {
    fn id(&self) -> &'static str {
        "codebuff"
    }

    fn discover_roots(&self) -> Vec<PathBuf> {
        let mut roots: Vec<PathBuf> = env_roots("CODEBUFF_DATA_DIR")
            .into_iter()
            .map(|p| {
                if p.file_name().and_then(|s| s.to_str()) == Some("projects") {
                    p
                } else {
                    p.join("projects")
                }
            })
            .filter(|p| p.is_dir())
            .collect();
        if roots.is_empty() {
            for chan in ["manicode", "manicode-dev", "manicode-staging"] {
                let d = home().join(".config").join(chan).join("projects");
                if d.is_dir() {
                    roots.push(d);
                }
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
                .push("未发现 ~/.config/manicode*/projects 或 CODEBUFF_DATA_DIR".into());
            return (vec![], stats);
        }
        let mut files: Vec<PathBuf> = Vec::new();
        let mut per_root = Vec::new();
        for r in roots {
            let mut got = walk_with(r, None, &|p| {
                p.file_name().and_then(|n| n.to_str()) == Some("chat-messages.json")
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
        let mut seen: HashSet<String> = HashSet::new();
        let mut total_gap = 0u64; // totalTokens 差额未能归桶的行数(§3)
        let mut dup_dropped = 0u64;

        for path in &files {
            // chatId = 上级目录名;时间戳兜底源(§4)
            let chat_id = path
                .parent()
                .and_then(|p| p.file_name())
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_string();
            // chatId 目录名形如 2026-01-02T03-04-05.000Z:T 后前两个 - 换 : 再按 ISO 解析
            let chatid_ms = {
                let raw = chat_id.clone();
                let t = raw.split('T').nth(1).unwrap_or("").to_string();
                let mut parts = t.split('-');
                let fixed = format!(
                    "{}T{}:{}:{}",
                    raw.split('T').next().unwrap_or(""),
                    parts.next().unwrap_or(""),
                    parts.next().unwrap_or(""),
                    parts.next().unwrap_or("")
                );
                iso_to_ms(fixed.trim_end_matches('Z').to_string().as_str())
            };
            let mtime_ms = path
                .metadata()
                .ok()
                .and_then(|m| m.modified().ok())
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            let Some(doc) = std::fs::read_to_string(path)
                .ok()
                .and_then(|t| serde_json::from_str::<Value>(&t).ok())
            else {
                stats.lines_bad_json += 1;
                continue;
            };
            let Some(arr) = doc.as_array() else {
                stats.lines_unknown_type += 1;
                continue;
            };
            stats.files_parsed += 1;
            stats.lines_total += arr.len() as u64;
            for msg in arr {
                // 用量行判据(§2)
                let role = msg
                    .get("variant")
                    .or_else(|| msg.get("role"))
                    .and_then(|x| x.as_str())
                    .unwrap_or("");
                if !matches!(role, "ai" | "agent" | "assistant") {
                    continue;
                }
                let meta = msg.get("metadata").unwrap_or(&Value::Null);
                let u = merge_usage(meta);
                if u.as_object().is_none_or(|o| o.is_empty()) {
                    continue;
                }
                let i = alias(
                    &u,
                    &[
                        "inputTokens",
                        "input_tokens",
                        "promptTokens",
                        "prompt_tokens",
                    ],
                );
                let o = alias(
                    &u,
                    &[
                        "outputTokens",
                        "output_tokens",
                        "completionTokens",
                        "completion_tokens",
                    ],
                );
                let cr =
                    alias(&u, &["cacheReadInputTokens", "cache_read_input_tokens"]).max(json_u64(
                        u.pointer("/promptTokensDetails/cachedTokens")
                            .or_else(|| u.pointer("/prompt_tokens_details/cached_tokens")),
                    ));
                let cw = alias(
                    &u,
                    &[
                        "cacheCreationInputTokens",
                        "cache_creation_input_tokens",
                        "cacheCreationTokens",
                        "cache_creation_tokens",
                        "cachedTokensCreated",
                        "cached_tokens_created",
                    ],
                );
                if i == 0 && o == 0 && cr == 0 && cw == 0 {
                    continue;
                }
                // totalTokens 差额兜底(§3):output==0 时补 output,否则忽略计数
                let sum = i + o + cr + cw;
                let total = alias(&u, &["totalTokens", "total_tokens", "total"]);
                let output = if total > sum && o == 0 {
                    total - sum
                } else {
                    o
                };
                if total > sum && o != 0 {
                    total_gap += 1;
                }
                // 时间戳链(§4)
                let ts_ms = ["timestamp", "createdAt"]
                    .iter()
                    .find_map(|k| msg.get(*k))
                    .or_else(|| meta.get("timestamp"))
                    .and_then(|x| match x {
                        Value::String(s) => iso_to_ms(s.as_str()),
                        Value::Number(n) => n.as_i64().map(crate::common::secs_or_ms),
                        _ => None,
                    })
                    .or(chatid_ms)
                    .unwrap_or(mtime_ms);
                let model = meta
                    .get("model")
                    .or_else(|| meta.pointer("/codebuff/model"))
                    .and_then(|x| x.as_str())
                    .unwrap_or("codebuff-unknown")
                    .to_string();
                let key = match msg.get("id").and_then(|x| x.as_str()) {
                    Some(id) => format!("codebuff:{}:{id}", chat_id),
                    None => format!("codebuff:{}:{}:{}:{}", chat_id, ts_ms, model, i),
                };
                if !seen.insert(key.clone()) {
                    dup_dropped += 1;
                    continue;
                }
                records.push(UsageRecord {
                    source: "codebuff",
                    session_id: chat_id.clone(),
                    agent: None,
                    model,
                    ts_ms,
                    caliber: Caliber::RequestDelta,
                    input_uncached: i,
                    cache_read: cr,
                    cache_write: cw,
                    cache_write_1h: 0,
                    output,
                    reasoning_output: 0,
                    cost_usd: None, // credits 非美元(§5)
                    dedup_key: key,
                });
            }
        }

        for (root, n) in per_root {
            stats
                .notes
                .push(format!("根 {}:{n} 个 chat-messages.json", root));
        }
        if total_gap > 0 {
            stats.notes.push(format!(
                "totalTokens 差额超四桶之和且 output≠0,忽略差额 {total_gap} 行(§3)"
            ));
        }
        if dup_dropped > 0 {
            stats
                .notes
                .push(format!("去重键命中丢弃 {dup_dropped} 条(§4)"));
        }
        (records, stats)
    }
}
