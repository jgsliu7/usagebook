//! WorkBuddy 适配器 —— 依据 docs/数据源档案/05-workbuddy.md v1.0。
//!
//! 根:WORKBUDDY_HOME → ~/.workbuddy(+ ~/.workbuddy-ai);projects/<编码路径>/<sessionId>.jsonl
//! 行:type=message role=assistant(**role 在顶层**)+ function_call(usage 常态挂此,DeepSeek 特例实为主流)
//! usage 优先级:providerData.rawUsage(OpenAI 原生)→ providerData.usage(camel)→ message.usage
//! 陷阱:message.usage 用 Anthropic 字段名但是 OpenAI 语义(input 含 cache,inclusive 减除,档案 §4)
//! 去重:workbuddy:{session}:{messageId|traceId|id},token 大者胜(档案 §5.1)
//!
//! v1.0 实现时本机新证据(待回写档案):
//! - 时间戳是 **epoch 毫秒数字**(非 ISO);status 仅 message 行有(completed/incomplete);
//! - 行类型全集:message(user/assistant)/function_call/function_call_result/reasoning/
//!   file-history-snapshot/session-meta/ai-title;子代理在 <sessionId>/subagents/agent-*.jsonl;
//! - rawUsage 别名族:cache_read 取 [prompt_tokens_details.cached_tokens, prompt_cache_hit_tokens,
//!   cache_read_input_tokens] 首个正值(末者实测常为 0 不可单独信);写桶 [prompt_cache_write_tokens,
//!   cache_creation_input_tokens];reasoning 取 [completion_tokens_details.reasoning_tokens,
//!   completion_thinking_tokens] 首个正值;另有 credit 字段(单位不明,不入 cost_usd);
//! - providerData.agent="cli" 为主代理,"general-purpose"+isSubAgent=true 为子代理。

use crate::common::*;
use crate::model::*;
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;

pub struct WorkbuddySource;

/// 五桶提取中间态(inclusive 语义未定,由各来源自行减除后填入)
#[derive(Default, Clone, Copy)]
struct Buckets {
    input_uncached: u64,
    cache_read: u64,
    cache_write: u64,
    output: u64,
    reasoning: u64,
}

impl Buckets {
    fn is_zero(&self) -> bool {
        self.input_uncached + self.cache_read + self.cache_write + self.output + self.reasoning == 0
    }
}

fn num(o: &Value, k: &str) -> u64 {
    o.get(k).and_then(|x| x.as_u64()).unwrap_or(0)
}

/// 别名族取首个正值(tokscale BuddyUsage 同义,档案 §3)
fn first_positive(o: &Value, keys: &[&str]) -> u64 {
    for k in keys {
        let v = num(o, k);
        if v > 0 {
            return v;
        }
    }
    0
}

/// providerData.usage 的 inputTokensDetails 是**数组**:[{cached_tokens}]
fn det_num(o: &Value, path: &str, key: &str) -> u64 {
    o.pointer(path)
        .and_then(|a| a.as_array())
        .and_then(|a| a.first())
        .map(|d| num(d, key))
        .unwrap_or(0)
}

/// epoch 数字/ISO 字符串 → 毫秒(本机实证为毫秒数字;<1e12 视为秒 ×1000 防御)
fn ts_to_ms(v: &Value) -> Option<i64> {
    match v {
        Value::Number(n) => {
            let t = n.as_i64().or_else(|| n.as_f64().map(|f| f as i64))?;
            Some(if t > 0 && t < 1_000_000_000_000 {
                t * 1000
            } else {
                t
            })
        }
        Value::String(s) => iso_to_ms(s),
        _ => None,
    }
}

/// inclusive 形状判定(档案 §4):total==input+output 且 cache_read>0 → input 含 cache,减除。
/// exclusive 形状(total==input+output−cache)则原样。
fn split_inclusive(input: u64, output: u64, total: u64, cache_read: u64) -> (u64, u64) {
    if cache_read > 0 && total == input.saturating_add(output) {
        (input.saturating_sub(cache_read), output)
    } else {
        (input, output)
    }
}

/// 来源 1(最优先):providerData.rawUsage —— OpenAI 原生,cached/reasoning 在 details。
fn from_raw_usage(u: &Value) -> Buckets {
    let prompt = num(u, "prompt_tokens");
    let completion = num(u, "completion_tokens");
    let det_cached = u
        .pointer("/prompt_tokens_details/cached_tokens")
        .and_then(|x| x.as_u64())
        .unwrap_or(0);
    let det_reason = u
        .pointer("/completion_tokens_details/reasoning_tokens")
        .and_then(|x| x.as_u64())
        .unwrap_or(0);
    let cache_read = [
        det_cached,
        num(u, "prompt_cache_hit_tokens"),
        num(u, "cache_read_input_tokens"),
    ]
    .into_iter()
    .find(|v| *v > 0)
    .unwrap_or(0);
    let cache_write = first_positive(
        u,
        &["prompt_cache_write_tokens", "cache_creation_input_tokens"],
    );
    let reasoning = [det_reason, num(u, "completion_thinking_tokens")]
        .into_iter()
        .find(|v| *v > 0)
        .unwrap_or(0);
    // reasoning 是 output 子集:上游偶发自相矛盾(reason>completion,本机实证 2 行),
    // 钳制保证 output+reasoning == completion_tokens
    let reasoning = reasoning.min(completion);
    Buckets {
        input_uncached: prompt.saturating_sub(cache_read),
        cache_read,
        cache_write,
        output: completion - reasoning,
        reasoning,
    }
}

/// 来源 2:providerData.usage —— camelCase,inputTokens 含 cache(details 是数组)
fn from_provider_usage(u: &Value) -> Buckets {
    let input = num(u, "inputTokens");
    let output = num(u, "outputTokens");
    let cache_read = [
        det_num(u, "/inputTokensDetails", "cached_tokens"),
        num(u, "cacheReadTokens"),
    ]
    .into_iter()
    .find(|v| *v > 0)
    .unwrap_or(0);
    let (in_unc, out) = split_inclusive(input, output, num(u, "totalTokens"), cache_read);
    let reasoning = det_num(u, "/outputTokensDetails", "reasoning_tokens").min(out);
    Buckets {
        input_uncached: in_unc,
        cache_read,
        cache_write: first_positive(u, &["cacheCreationTokens", "cacheWriteTokens"]),
        output: out - reasoning,
        reasoning,
    }
}

/// 来源 3(最后):message.usage —— Anthropic 字段名但 OpenAI 语义(input 含 cache,档案 §4 硬约束)
fn from_message_usage(u: &Value) -> Buckets {
    let input = num(u, "input_tokens");
    let output = num(u, "output_tokens");
    let cache_read = num(u, "cache_read_input_tokens");
    let (in_unc, out) = split_inclusive(input, output, num(u, "total_tokens"), cache_read);
    Buckets {
        input_uncached: in_unc,
        cache_read,
        cache_write: num(u, "cache_creation_input_tokens"),
        output: out,
        reasoning: 0,
    }
}

impl WorkbuddySource {
    fn roots(&self) -> Vec<PathBuf> {
        let mut roots = env_roots("WORKBUDDY_HOME");
        let h = home();
        for d in [".workbuddy", ".workbuddy-ai"] {
            let p = h.join(d);
            if p.is_dir() && !roots.contains(&p) {
                roots.push(p);
            }
        }
        roots
    }
}

impl UsageSource for WorkbuddySource {
    fn id(&self) -> &'static str {
        "workbuddy"
    }

    fn discover_roots(&self) -> Vec<PathBuf> {
        self.roots()
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
                .push("未发现 ~/.workbuddy 或 WORKBUDDY_HOME".into());
            return (vec![], stats);
        }
        let mut files = Vec::new();
        for r in roots {
            let d = r.join("projects");
            if d.is_dir() {
                files.extend(walk_jsonl(
                    &d,
                    max_files.map(|m| m.saturating_sub(files.len())),
                ));
            }
        }
        // 回滚日志非转录,防御排除(档案 §5.5;.ndjson 本就不入 walk_jsonl)
        files.retain(|p| {
            p.file_name()
                .map(|n| !n.to_string_lossy().ends_with(".file-rollback.ndjson"))
                .unwrap_or(true)
        });
        stats.files_seen = files.len();

        let mut dedup: HashMap<String, UsageRecord> = HashMap::new();
        let mut src_hits = [0u64; 3]; // rawUsage / providerData.usage / message.usage
        let mut status_missing = 0u64;
        let mut status_filtered = 0u64;
        let mut ts_missing = 0u64;

        for path in &files {
            let fallback_session = path
                .file_stem()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_default();
            let (total, bad) = for_each_line(path, |line| {
                let Ok(v) = serde_json::from_str::<Value>(line) else {
                    stats.lines_bad_json += 1;
                    return;
                };
                let t = v.get("type").and_then(|x| x.as_str()).unwrap_or("");
                // 用量行:assistant 消息 + function_call(DeepSeek 特例,档案 §4 末)
                let usage_row = match t {
                    "message" => v.get("role").and_then(|x| x.as_str()) == Some("assistant"),
                    "function_call" => true,
                    // 已知非用量行(本机全集实证):user 消息/工具结果/思考/快照/元数据/标题
                    "function_call_result"
                    | "reasoning"
                    | "file-history-snapshot"
                    | "session-meta"
                    | "ai-title" => return,
                    _ => {
                        stats.lines_unknown_type += 1;
                        return;
                    }
                };
                if !usage_row {
                    return; // role != assistant 的 message 行
                }
                // status 仅 message 行有;缺失视为 completed(档案 §8)
                match v.get("status").and_then(|x| x.as_str()) {
                    Some("completed") | None => {
                        if v.get("status").is_none() {
                            status_missing += 1;
                        }
                    }
                    Some(_) => {
                        status_filtered += 1;
                        return;
                    }
                }
                // usage 优先级(档案 §4 硬约束:rawUsage 最优先,防 Anthropic 名/OpenAI 义双计)
                let pd = v.get("providerData");
                let (buckets, which) = if let Some(raw) =
                    pd.and_then(|p| p.get("rawUsage")).filter(|u| u.is_object())
                {
                    (from_raw_usage(raw), 0)
                } else if let Some(u) = pd.and_then(|p| p.get("usage")).filter(|u| u.is_object()) {
                    (from_provider_usage(u), 1)
                } else if let Some(u) = v.pointer("/message/usage").filter(|u| u.is_object()) {
                    (from_message_usage(u), 2)
                } else {
                    return; // 无用量(多数 function_call/assistant 行是纯事件)
                };
                if buckets.is_zero() {
                    return;
                }
                src_hits[which] += 1;
                let Some(ts_ms) = v.get("timestamp").and_then(ts_to_ms) else {
                    ts_missing += 1;
                    return;
                };
                let session_id = v
                    .get("sessionId")
                    .and_then(|x| x.as_str())
                    .unwrap_or(&fallback_session)
                    .to_string();
                let model = pd
                    .and_then(|p| p.get("model"))
                    .or_else(|| pd.and_then(|p| p.get("requestModelId")))
                    .or_else(|| v.pointer("/message/model"))
                    .and_then(|x| x.as_str())
                    .unwrap_or("unknown")
                    .to_string();
                // 子代理:isSubAgent=true 时 agent 为子代理名,否则主代理(本机实证 cli/general-purpose)
                let agent = pd
                    .and_then(|p| p.get("isSubAgent"))
                    .and_then(|x| x.as_bool())
                    .filter(|b| *b)
                    .and_then(|_| pd.and_then(|p| p.get("agent")).and_then(|x| x.as_str()))
                    .map(|s| s.to_string());
                let msg_key = pd
                    .and_then(|p| p.get("messageId"))
                    .or_else(|| pd.and_then(|p| p.get("traceId")))
                    .or_else(|| v.get("id"))
                    .and_then(|x| x.as_str())
                    .unwrap_or("");
                let key = format!("workbuddy:{}:{}", session_id, msg_key);
                let rec = UsageRecord {
                    source: "workbuddy",
                    session_id: session_id.clone(),
                    agent,
                    model,
                    ts_ms,
                    caliber: Caliber::RequestDelta,
                    input_uncached: buckets.input_uncached,
                    cache_read: buckets.cache_read,
                    cache_write: buckets.cache_write,
                    cache_write_1h: 0,
                    output: buckets.output,
                    reasoning_output: buckets.reasoning,
                    cost_usd: None, // rawUsage.credit 单位不明,不冒认 USD
                    dedup_key: key.clone(),
                };
                // token 大者胜(claude.rs 同义;多 sink 镜像时五分量键天然塌缩)
                match dedup.get(&key) {
                    Some(old) if old.token_total() >= rec.token_total() => {}
                    _ => {
                        dedup.insert(key, rec);
                    }
                }
            });
            stats.lines_total += total;
            stats.lines_bad_json += bad;
            if total > 0 {
                stats.files_parsed += 1;
            }
        }
        stats.notes.push(format!(
            "usage 来源命中:rawUsage {} / providerData.usage {} / message.usage {}",
            src_hits[0], src_hits[1], src_hits[2]
        ));
        stats.notes.push(format!(
            "status 缺失 {} 行(视为 completed),非 completed 跳过 {} 行",
            status_missing, status_filtered
        ));
        if ts_missing > 0 {
            stats.notes.push(format!("{} 行缺时间戳跳过", ts_missing));
        }
        for r in roots {
            let db = r.join("workbuddy.db");
            if db.is_file() {
                stats.notes.push(format!(
                    "workbuddy.db 存在({},{},session_usage 校验源 v1.1)",
                    db.display(),
                    db.metadata().map(|m| m.len()).unwrap_or(0)
                ));
            }
        }
        (dedup.into_values().collect(), stats)
    }
}
