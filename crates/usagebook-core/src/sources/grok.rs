//! Grok Build 适配器 —— 依据 docs/数据源档案/15-grok.md(Community,本机未实证)。
//!
//! 根:~/.grok 或 GROK_HOME(逗号多根);文件名精确 == updates.jsonl(§1)
//! 行:sessionUpdate=="turn_completed" 且带 usage;modelUsage 存在时逐模型展开(§2)
//! 口径:input inclusive(含 cachedRead,减除);reasoning 是 output 子集,丢弃防双计(§3)
//! 成本:costUsdTicks × 1e-10 定点美元,账单权威值(§3)
//! 时间戳:_meta.agentTimestampMs(毫秒)> 行 timestamp(unix 秒,§4)
//! 去重键:grok:{eventId}:{model},无 eventId 用内容键(§4)

use crate::common::{env_roots, home, json_u64, walk_with};
use crate::model::{Caliber, ScanStats, UsageSource, UsageRecord};
use serde_json::Value;
use std::collections::HashSet;
use std::path::PathBuf;

pub struct GrokSource;

/// 单模型 usage 条目 → 五桶 + 成本(§3)。
fn map_usage(u: &Value) -> ([u64; 4], Option<f64>) {
    let g = |k: &str| json_u64(u.get(k));
    let (input, cr, cw, out) = (g("inputTokens"), g("cachedReadTokens"), g("cacheCreationTokens"), g("outputTokens"));
    let uncached = input.saturating_sub(cr); // inclusive 减除(§3)
    let cache_write = cw.min(uncached); // 钳制到剩余 uncached(ccusage)
    let ticks = u.get("costUsdTicks").and_then(|x| x.as_f64()).unwrap_or(0.0);
    let cost = if ticks != 0.0 { Some(ticks * 1e-10) } else { None }; // 1 tick = 1e-10 USD
    ([uncached, cr, cache_write, out], cost)
}

impl UsageSource for GrokSource {
    fn id(&self) -> &'static str {
        "grok"
    }

    fn discover_roots(&self) -> Vec<PathBuf> {
        let mut roots = env_roots("GROK_HOME");
        if roots.is_empty() {
            let d = home().join(".grok");
            if d.is_dir() {
                roots.push(d);
            }
        }
        roots
    }

    fn scan_roots(&self, roots: &[std::path::PathBuf], max_files: Option<usize>) -> (Vec<UsageRecord>, ScanStats) {
        let mut stats = ScanStats::default();
        if roots.is_empty() {
            stats.notes.push("未发现 ~/.grok 或 GROK_HOME".into());
            return (vec![], stats);
        }
        // —— 发现:sessions/** 下文件名精确 == updates.jsonl(§1)——
        let mut files: Vec<PathBuf> = Vec::new();
        let mut per_root = Vec::new();
        for r in roots {
            let mut got = walk_with(&r.join("sessions"), None, &|p| {
                p.file_name().and_then(|n| n.to_str()) == Some("updates.jsonl")
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
        let mut no_ts = 0u64;
        let mut dup_dropped = 0u64;

        for path in &files {
            // 会话兜底:目录名(§4)
            let dir_session = path
                .parent()
                .and_then(|p| p.file_name())
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_string();
            crate::common::for_each_line(path, |line| {
                stats.lines_total += 1;
                let Ok(v) = serde_json::from_str::<Value>(line) else {
                    stats.lines_bad_json += 1;
                    return;
                };
                let params = v.get("params").unwrap_or(&Value::Null);
                let update = params.get("update").unwrap_or(&Value::Null);
                // 用量行判据(§2)
                if update.get("sessionUpdate").and_then(|x| x.as_str()) != Some("turn_completed") {
                    stats.lines_unknown_type += 1;
                    return;
                }
                let Some(usage) = update.get("usage").filter(|u| u.is_object()) else {
                    stats.lines_unknown_type += 1;
                    return;
                };
                // 时间戳:agentTimestampMs(毫秒)> 行 timestamp(unix 秒,§4)
                let ts_ms = params
                    .pointer("/_meta/agentTimestampMs")
                    .and_then(|x| x.as_i64())
                    .or_else(|| v.get("timestamp").and_then(|x| x.as_i64()).map(|s| s.saturating_mul(1000)))
                    .unwrap_or(0);
                if ts_ms == 0 {
                    no_ts += 1;
                }
                let session_id = params
                    .get("sessionId")
                    .and_then(|x| x.as_str())
                    .unwrap_or(&dir_session)
                    .to_string();
                let event_id = params.pointer("/_meta/eventId").and_then(|x| x.as_str()).unwrap_or("");
                // 逐模型展开 or 单条(§2)
                let model_entries: Vec<(String, &Value)> =
                    match usage.get("modelUsage").and_then(|m| m.as_object()) {
                        Some(mu) if !mu.is_empty() => mu.iter().map(|(k, u)| (k.clone(), u)).collect(),
                        _ => vec![("unknown".into(), usage)],
                    };
                for (model, u) in model_entries {
                    let ([i, cr, cw, o], cost) = map_usage(u);
                    // reasoning 是 output 子集,丢弃防双计(§3)
                    let key = if event_id.is_empty() {
                        format!("grok:{session_id}:{ts_ms}:{model}:{}:{}:{}:{}", i, cr, cw, o)
                    } else {
                        format!("grok:{event_id}:{model}")
                    };
                    if !seen.insert(key.clone()) {
                        dup_dropped += 1;
                        continue;
                    }
                    records.push(UsageRecord {
                        source: "grok",
                        session_id: session_id.clone(),
                        agent: None,
                        model,
                        ts_ms,
                        caliber: Caliber::RequestDelta,
                        input_uncached: i,
                        cache_read: cr,
                        cache_write: cw,
                        cache_write_1h: 0,
                        output: o,
                        reasoning_output: 0,
                        cost_usd: cost,
                        dedup_key: key,
                    });
                }
            });
            stats.files_parsed += 1;
        }

        for (root, n) in per_root {
            stats.notes.push(format!("根 {}:{n} 个 updates.jsonl", root));
        }
        if no_ts > 0 {
            stats.notes.push(format!("无时间戳行 {no_ts} 条(ts=0 记账,§4)"));
        }
        if dup_dropped > 0 {
            stats.notes.push(format!("去重键命中丢弃 {dup_dropped} 条(§4)"));
        }
        (records, stats)
    }
}
