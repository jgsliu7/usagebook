//! Amp 适配器 —— 依据 docs/数据源档案/16-amp.md(Community,本机未实证)。
//!
//! 根:~/.local/share/amp/threads 或 AMP_DATA_DIR(末段已叫 threads 则直用,否则拼 threads,§1)
//! 文件:threads/**/*.json,单 JSON 对象 {id, messages[], usageLedger{events[]}}(§2)
//! 双 schema:usageLedger.events 非空 → ledger(逐请求事件 + toMessageId 联表补缓存桶);
//!     否则回落 messages[](role=="assistant" 的 usage 直读,§3)
//! toMessageId 联表:先按 messages[].messageId 值匹配(ccusage),失败按下标(juejin),再失败缓存桶置 0
//! 去重:amp:{threadId}:ledger|msg:{index}(append-only 索引键天然稳定)
//! credits 是 amp 积分非美元,不进 cost_usd(§6)

use crate::common::{env_roots, home, iso_to_ms, json_u64, walk_with};
use crate::model::{Caliber, ScanStats, UsageRecord, UsageSource};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

pub struct AmpSource;

impl UsageSource for AmpSource {
    fn id(&self) -> &'static str {
        "amp"
    }

    fn discover_roots(&self) -> Vec<PathBuf> {
        // env 末段已叫 threads 则直用(juejin 语义),否则拼 threads(ccusage 语义,§1)
        let mut roots: Vec<PathBuf> = env_roots("AMP_DATA_DIR")
            .into_iter()
            .map(|p| {
                if p.file_name().and_then(|s| s.to_str()) == Some("threads") {
                    p
                } else {
                    p.join("threads")
                }
            })
            .filter(|p| p.is_dir())
            .collect();
        if roots.is_empty() {
            let d = home().join(".local/share/amp/threads");
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
                .push("未发现 ~/.local/share/amp/threads 或 AMP_DATA_DIR".into());
            return (vec![], stats);
        }
        let mut files: Vec<PathBuf> = Vec::new();
        let mut per_root = Vec::new();
        for r in roots {
            let mut got = walk_with(r, None, &|p| p.extension().is_some_and(|x| x == "json"));
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
        let mut ledger_files = 0u64;
        let mut msg_files = 0u64;
        let mut join_miss = 0u64;
        let mut dup_dropped = 0u64;

        for path in &files {
            let Some(text) = std::fs::read_to_string(path).ok() else {
                stats.lines_bad_json += 1;
                continue;
            };
            let Ok(doc) = serde_json::from_str::<Value>(&text) else {
                stats.lines_bad_json += 1; // 非 thread 文件(目录里的杂项 .json),按坏文件计数跳过
                continue;
            };
            let Some(messages) = doc.get("messages").and_then(|x| x.as_array()) else {
                stats.lines_unknown_type += 1;
                continue;
            };
            stats.files_parsed += 1;
            let thread_id = doc
                .get("id")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string();

            // messageId → assistant usage 的联表(ledger 路径补缓存桶用,§3)
            let mut by_msg_id: HashMap<String, &Value> = HashMap::new();
            for m in messages.iter() {
                if m.get("role").and_then(|x| x.as_str()) == Some("assistant") {
                    if let (Some(mid), Some(u)) =
                        (m.get("messageId").and_then(|x| x.as_i64()), m.get("usage"))
                    {
                        by_msg_id.insert(mid.to_string(), u);
                    }
                }
            }

            let events = doc
                .pointer("/usageLedger/events")
                .and_then(|x| x.as_array())
                .filter(|a| !a.is_empty());

            if let Some(events) = events {
                // —— ledger 路径:逐请求事件(§3)——
                ledger_files += 1;
                stats.lines_total += events.len() as u64;
                for (idx, e) in events.iter().enumerate() {
                    let Some(tokens) = e.get("tokens") else {
                        stats.lines_unknown_type += 1;
                        continue;
                    };
                    let input = json_u64(tokens.get("input"));
                    let output = json_u64(tokens.get("output"));
                    if input == 0 && output == 0 {
                        continue; // 全零事件不入账
                    }
                    // toMessageId 联表:值匹配优先,下标兜底(§3;两参考实现语义冲突,双兜底)
                    let join = e
                        .get("toMessageId")
                        .and_then(|x| x.as_i64())
                        .and_then(|mid| by_msg_id.get(&mid.to_string()).copied())
                        .or_else(|| {
                            e.get("toMessageId").and_then(|x| x.as_i64()).and_then(|i| {
                                messages
                                    .get(i as usize)
                                    .filter(|m| m.get("usage").is_some())
                                    .map(|m| m.get("usage").unwrap())
                            })
                        });
                    let (cr, cw) = match join {
                        Some(u) => (
                            json_u64(u.get("cacheReadInputTokens")),
                            json_u64(u.get("cacheCreationInputTokens")),
                        ),
                        None => {
                            join_miss += 1;
                            (0, 0)
                        }
                    };
                    let model = e
                        .get("model")
                        .and_then(|x| x.as_str())
                        .unwrap_or("unknown")
                        .to_string();
                    let ts_ms = e
                        .get("timestamp")
                        .and_then(|x| x.as_str())
                        .and_then(iso_to_ms)
                        .unwrap_or(0);
                    let key = format!("amp:{}:ledger:{idx}", thread_id);
                    if !seen.insert(key.clone()) {
                        dup_dropped += 1;
                        continue;
                    }
                    records.push(UsageRecord {
                        source: "amp",
                        session_id: thread_id.clone(),
                        agent: None,
                        model,
                        ts_ms,
                        caliber: Caliber::RequestDelta,
                        request_count: 1,
                        input_uncached: input,
                        cache_read: cr,
                        cache_write: cw,
                        cache_write_1h: 0,
                        output,
                        reasoning_output: 0,
                        cost_usd: None, // credits 非 USD,不进成本(§6)
                        dedup_key: key,
                    });
                }
            } else {
                // —— messages 回落:assistant.usage 直读(§3)——
                msg_files += 1;
                for (idx, m) in messages.iter().enumerate() {
                    if m.get("role").and_then(|x| x.as_str()) != Some("assistant") {
                        continue;
                    }
                    let Some(u) = m.get("usage") else { continue };
                    stats.lines_total += 1;
                    let g = |k: &str| json_u64(u.get(k));
                    let (i, o, cr, cw) = (
                        g("inputTokens"),
                        g("outputTokens"),
                        g("cacheReadInputTokens"),
                        g("cacheCreationInputTokens"),
                    );
                    if i == 0 && o == 0 && cr == 0 && cw == 0 {
                        continue;
                    }
                    let model = u
                        .get("model")
                        .and_then(|x| x.as_str())
                        .unwrap_or("unknown")
                        .to_string();
                    let ts_ms = u
                        .get("timestamp")
                        .and_then(|x| x.as_str())
                        .and_then(iso_to_ms)
                        .or_else(|| {
                            m.get("timestamp")
                                .and_then(|x| x.as_str())
                                .and_then(iso_to_ms)
                        })
                        .unwrap_or(0);
                    let key = format!("amp:{}:msg:{idx}", thread_id);
                    if !seen.insert(key.clone()) {
                        dup_dropped += 1;
                        continue;
                    }
                    records.push(UsageRecord {
                        source: "amp",
                        session_id: thread_id.clone(),
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
                }
            }
        }

        for (root, n) in per_root {
            stats.notes.push(format!("根 {}:{n} 个 json", root));
        }
        stats.notes.push(format!(
            "ledger 文件 {ledger_files} · messages 回落文件 {msg_files}"
        ));
        if join_miss > 0 {
            stats
                .notes
                .push(format!("toMessageId 联表未命中 {join_miss} 条(缓存桶置 0)"));
        }
        if dup_dropped > 0 {
            stats.notes.push(format!("重复索引丢弃 {dup_dropped} 条"));
        }
        (records, stats)
    }
}
