//! Qwen Code 适配器 —— 依据 docs/数据源档案/14-qwen.md(源码一手取证+实装对账)。
//!
//! 双通道(§7.2,月度文件首选):<runtimeBase>/usage/token-usage-YYYY-MM.jsonl
//!     (每 API 响应一条,含后台辅助 source 字段)存在则独占;否则回退
//!     projects/<sanitizeCwd>/chats/*.jsonl(tmp 布局已证伪,不扫,§7.1)
//! 根(§7.1):QWEN_RUNTIME_DIR > QWEN_HOME > ~/.qwen(⚠️ QWEN_DATA_DIR/QWEN_TMP_DIR 不存在,已删)
//! 月度语义(§7.3):input ⊇ cached → input_uncached=in−cached;authType ∈ {openai,qwen-oauth}
//!     时 candidates 含 thoughts → output=out−thoughts;其余协议 output 直取
//! chats 语义(§3):inclusive 减除 + 形状守卫(协议不可判,按 openai 兼容处理并计数)
//! 去重:月度 qwen:monthly:{id};chats qwen:{uuid},内容键兜底(§4)

use crate::common::{env_roots, home, iso_to_ms, walk_with};
use crate::model::{Caliber, ScanStats, UsageRecord, UsageSource};
use serde_json::Value;
use std::collections::HashSet;
use std::path::PathBuf;

pub struct QwenSource;

impl UsageSource for QwenSource {
    fn id(&self) -> &'static str {
        "qwen"
    }

    fn discover_roots(&self) -> Vec<PathBuf> {
        // §7.1:双根探测并集(QWEN_RUNTIME_DIR 运行时根 + QWEN_HOME 全局根 + ~/.qwen)——
        // 官方 rebuildFromSessionJsonl 硬编码全局根,设了 RUNTIME_DIR 的用户旧数据会留在
        // 全局根,单根探测会漏;两根都存在时都扫,canonicalize 去重。
        let mut roots = env_roots("QWEN_RUNTIME_DIR");
        roots.extend(env_roots("QWEN_HOME"));
        roots.push(home().join(".qwen"));
        let mut seen = HashSet::new();
        roots.retain(|p| p.is_dir() && p.canonicalize().ok().is_some_and(|c| seen.insert(c)));
        roots
    }

    fn scan_roots(
        &self,
        roots: &[std::path::PathBuf],
        max_files: Option<usize>,
    ) -> (Vec<UsageRecord>, ScanStats) {
        let mut stats = ScanStats::default();
        let mut records: Vec<UsageRecord> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        let mut no_ts = 0u64;
        if roots.is_empty() {
            stats
                .notes
                .push("未发现 ~/.qwen 或 QWEN_RUNTIME_DIR/QWEN_HOME".into());
            return (records, stats);
        }
        // —— 首选通道:usage/token-usage-*.jsonl 月度文件(§7.2,含后台辅助调用)——
        let mut monthly: Vec<PathBuf> = Vec::new();
        for r in roots {
            monthly.extend(walk_with(&r.join("usage"), None, &|p| {
                p.file_name().is_some_and(|n| {
                    n.to_string_lossy().starts_with("token-usage-")
                        && p.extension().is_some_and(|x| x == "jsonl")
                })
            }));
        }
        monthly.sort();
        if !monthly.is_empty() {
            stats.files_seen = monthly.len();
            for path in &monthly {
                scan_monthly_file(path, &mut records, &mut seen, &mut stats, &mut no_ts);
                stats.files_parsed += 1;
            }
            stats.notes.push(format!(
                "月度 token-usage 通道 {} 文件(§7.2,chats 通道让位防双计)",
                monthly.len()
            ));
            return (records, stats);
        }

        // —— 兜底通道:projects/**/*.jsonl(tmp 布局已证伪不扫,§7.1)——
        let mut files: Vec<PathBuf> = Vec::new();
        let mut per_root = Vec::new();
        for r in roots {
            let mut got = walk_with(&r.join("projects"), None, &|p| {
                p.extension().is_some_and(|x| x == "jsonl")
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

        let mut shape_guard = 0u64;
        let mut all_zero = 0u64;
        let mut dup_dropped = 0u64;
        // §7.4:同 uuid 多行 fragment 聚合 last-wins —— 用 map 后写覆盖,循环结束统一出账
        let mut pending: std::collections::HashMap<String, UsageRecord> =
            std::collections::HashMap::new();

        for path in &files {
            // 文件 mtime 兜底时间戳(§2)
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
            crate::common::for_each_line(path, |line| {
                stats.lines_total += 1;
                let Ok(v) = serde_json::from_str::<Value>(line) else {
                    stats.lines_bad_json += 1;
                    return;
                };
                // 用量行判据(§2)
                if v.get("type").and_then(|x| x.as_str()) != Some("assistant") {
                    stats.lines_unknown_type += 1;
                    return;
                }
                let Some(um) = v.get("usageMetadata") else {
                    stats.lines_unknown_type += 1;
                    return;
                };
                let g = |k: &str| crate::common::json_u64(um.get(k));
                let (prompt, cand, thoughts, cached) = (
                    g("promptTokenCount"),
                    g("candidatesTokenCount"),
                    g("thoughtsTokenCount"),
                    g("cachedContentTokenCount"),
                );
                if prompt == 0 && cand == 0 {
                    all_zero += 1;
                    return;
                }
                // inclusive 减除 + 形状守卫(§3/§6)
                let input_uncached = if cached > prompt {
                    shape_guard += 1;
                    prompt
                } else {
                    prompt - cached
                };
                let output = cand.saturating_sub(thoughts);
                let thoughts = thoughts.min(cand); // 估算值可能超过 output,同月度通道钳制(2026-10-17)
                let ts_ms = v
                    .get("timestamp")
                    .and_then(|x| x.as_str())
                    .and_then(iso_to_ms)
                    .unwrap_or(mtime_ms);
                if ts_ms == 0 {
                    no_ts += 1;
                }
                let session_id = v
                    .get("sessionId")
                    .and_then(|x| x.as_str())
                    .unwrap_or(&stem)
                    .to_string();
                let model = v
                    .get("model")
                    .and_then(|x| x.as_str())
                    .unwrap_or("unknown")
                    .to_string();
                // 去重键(§4):uuid 优先,内容键兜底
                let key = match v.get("uuid").and_then(|x| x.as_str()) {
                    Some(u) => format!("qwen:{u}"),
                    None => format!("qwen:{}:{}:{model}", session_id, ts_ms),
                };
                let record = UsageRecord {
                    source: "qwen",
                    session_id,
                    agent: None,
                    model,
                    ts_ms,
                    caliber: Caliber::RequestDelta,
                    request_count: 1,
                    input_uncached,
                    cache_read: cached,
                    cache_write: 0,
                    cache_write_1h: 0,
                    output,
                    reasoning_output: thoughts,
                    cost_usd: None,
                    dedup_key: key.clone(),
                };
                // last-wins(§7.4):后写覆盖前写
                if pending.insert(key, record).is_some() {
                    dup_dropped += 1;
                }
            });
            stats.files_parsed += 1;
        }

        // §7.4:fragment 聚合完成,统一出账(保持发现顺序已不可考,按 key 稳定序)
        let mut keys: Vec<String> = pending.keys().cloned().collect();
        keys.sort();
        for k in keys {
            if let Some(r) = pending.remove(&k) {
                records.push(r);
            }
        }

        for (root, n) in per_root {
            stats.notes.push(format!("根 {}:{n} 个 jsonl", root));
        }
        if all_zero > 0 {
            stats
                .notes
                .push(format!("全零 usageMetadata 跳过 {all_zero} 行(§2)"));
        }
        if shape_guard > 0 {
            stats.notes.push(format!(
                "cached>prompt 改判 exclusive {shape_guard} 行(形状守卫,§6)"
            ));
        }
        if no_ts > 0 {
            stats
                .notes
                .push(format!("无时间戳回退文件 mtime {no_ts} 行(§2)"));
        }
        if dup_dropped > 0 {
            stats
                .notes
                .push(format!("去重键命中丢弃 {dup_dropped} 行(§4)"));
        }
        (records, stats)
    }
}

/// 月度 token-usage 文件解析(§7.2):每行 = 一次 API 响应(含后台辅助 source)。
fn scan_monthly_file(
    path: &std::path::Path,
    records: &mut Vec<UsageRecord>,
    seen: &mut HashSet<String>,
    stats: &mut ScanStats,
    no_ts: &mut u64,
) {
    let mtime_ms = path
        .metadata()
        .ok()
        .and_then(|m| m.modified().ok())
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    crate::common::for_each_line(path, |line| {
        stats.lines_total += 1;
        let Ok(v) = serde_json::from_str::<Value>(line) else {
            stats.lines_bad_json += 1;
            return;
        };
        let g = |k: &str| crate::common::json_u64(v.get(k));
        let input = g("inputTokens");
        let output = g("outputTokens");
        let cached = g("cachedTokens");
        let thoughts = g("thoughtsTokens");
        if input == 0 && output == 0 && cached == 0 {
            return; // 全零跳过(三跳过)
        }
        // 五桶(§7.3):input ⊇ cached(inclusive);output 按 authType 分协议
        let input_uncached = input.saturating_sub(cached);
        let auth = v.get("authType").and_then(|x| x.as_str()).unwrap_or("");
        let (out_net, reasoning) = if auth.is_empty() || auth == "openai" || auth == "qwen-oauth" {
            // candidates 含 thoughts;thoughts 是估算值,可能超过 output——钳到 o 防虚增(与 pi 同族防御,2026-10-17)
            (output.saturating_sub(thoughts), thoughts.min(output))
        } else {
            (output, thoughts.min(output)) // Gemini 原生等:output 不含 thoughts,同样钳制
        };
        let ts_ms = v
            .get("timestamp")
            .and_then(|x| x.as_str())
            .and_then(iso_to_ms)
            .unwrap_or(mtime_ms);
        if ts_ms == 0 {
            *no_ts += 1;
        }
        let session_id = v
            .get("sessionId")
            .and_then(|x| x.as_str())
            .unwrap_or("unknown")
            .to_string();
        let model = v
            .get("model")
            .and_then(|x| x.as_str())
            .unwrap_or("unknown")
            .to_string();
        // 后台辅助调用归因(实装发现,§7.2):source != main 记入 agent
        let source_tag = v.get("source").and_then(|x| x.as_str()).unwrap_or("main");
        let agent = if source_tag.is_empty() || source_tag == "main" {
            None
        } else {
            Some(source_tag.to_string())
        };
        // 去重(§7.2):月度行自带 uuid id
        let key = match v.get("id").and_then(|x| x.as_str()) {
            Some(id) => format!("qwen:monthly:{id}"),
            None => format!(
                "qwen:monthly:{}:{}:{model}:{ts_ms}:{input}:{output}",
                session_id, cached
            ),
        };
        if !seen.insert(key.clone()) {
            return;
        }
        records.push(UsageRecord {
            source: "qwen",
            session_id,
            agent,
            model,
            ts_ms,
            caliber: Caliber::RequestDelta,
            request_count: 1,
            input_uncached,
            cache_read: cached,
            cache_write: 0,
            cache_write_1h: 0,
            output: out_net,
            reasoning_output: reasoning,
            cost_usd: None, // 无成本字段(§6)
            dedup_key: key,
        });
    });
}
