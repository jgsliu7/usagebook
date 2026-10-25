//! OMP(Oh My Pi)适配器 —— 依据 docs/数据源档案/09-omp.md(v1.0 冻结)。
//!
//! 属主纪律:扫描根固定 `~/.omp/agent/sessions`,
//!   **永不认 PI_CODING_AGENT_DIR**(omp 与 pi 共用该环境变量,认它会把两个客户端
//!   钉到同一棵树双计;tokscale clients.rs:993-1001 同款纪律)。
//! 文件(§2):`sessions/<编码cwd>/<ISO-ts>_<uuid>.jsonl` 主会话 +
//!   `sessions/<编码cwd>/<ISO-ts>_<uuid>/<AgentName>.jsonl` 子代理/顾问独立会话文件
//!   (自带 session 头,逐文件独立解析,§5.2);agent.db 为配额型账本(本机全空是
//!   常态),统计不依赖,存在性探测只看 sessions/ 目录(§5.7)。
//! 用量(§3/§4):type=="message" 且 message.role=="assistant" 的 message.usage
//!   (camelCase;exclusive:input 不含 cacheRead,无需 input 减除);
//!   r⊆o 须减除 → 输出桶 = output − reasoningTokens,r>o 时钳制 min(r,o)(§4/§8);
//!   失败请求(errorMessage 与 usage 并存)不过滤(§5.4);
//!   title_change.childUsage/aggregateUsage 聚合字段在场必须忽略(§5.2);
//!   全零 usage 行按横切三跳过纪律跳过并计数(00-总览 §五.5)。
//! 成本(§4):usage.cost.total 自带逐请求美元成本(provider 计价,官方口径)→
//!   cost_usd;缺失记 0 并标注(§8)。
//! 去重键(§5.3):omp:{message_id}(tokscale standard_deduped 跨会话键,
//!   responseId 仅辅助素材)。

use crate::common::*;
use crate::model::*;
use serde_json::Value;
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};

/// 已知非用量行类型(档案 §3 行型全集,本机 21 文件全量普查共 10 种):
/// message 的 user/toolResult 无用量;custom = tool_execution_start/session_exit;
/// session(头)/ session_init / title / title_change / model_change /
/// thinking_level_change / custom_message / reset_boundary 均无用量。
const KNOWN_TYPES: &[&str] = &[
    "message",
    "custom",
    "session",
    "session_init",
    "title",
    "title_change",
    "model_change",
    "thinking_level_change",
    "custom_message",
    "reset_boundary",
];

fn is_known_type(t: &str) -> bool {
    KNOWN_TYPES.contains(&t)
}

/// epoch 数值归一毫秒:<1e12 视为秒 ×1000(00-总览 §五.4)。
fn norm_ms(t: f64) -> i64 {
    if t > 0.0 && t < 1e12 {
        (t * 1000.0).round() as i64
    } else {
        t.round() as i64
    }
}

/// 从 `<ISO-ts>_<uuid>` 文件/目录名提取 uuid(ISO ts 不含下划线;
/// session 头缺失时的退化素材,§3)。
fn uuid_from_name(name: &str) -> String {
    match name.split_once('_') {
        Some((_, rest)) => rest.trim_end_matches(".jsonl").to_string(),
        None => name.trim_end_matches(".jsonl").to_string(),
    }
}

/// 按相对根深度归类(§2):根/<项目>/<文件> = 主会话(agent = None);
/// 根/<项目>/<会话目录>/<文件> = 子代理/顾问独立会话文件(agent = 文件名 stem)。
fn agent_of(root: &Path, file: &Path) -> Option<String> {
    let depth = file.strip_prefix(root).ok()?.components().count();
    if depth <= 2 {
        None
    } else {
        file.file_stem().map(|s| s.to_string_lossy().into_owned())
    }
}

/// session 头缺失时的退化 session id:主会话取文件名 uuid,
/// 子代理文件取父目录(会话目录)uuid。
fn fallback_session(root: &Path, file: &Path) -> String {
    let depth = file
        .strip_prefix(root)
        .map(|r| r.components().count())
        .unwrap_or(0);
    if depth <= 2 {
        file.file_name()
            .map(|s| uuid_from_name(&s.to_string_lossy()))
            .unwrap_or_else(|| "unknown".into())
    } else {
        file.parent()
            .and_then(|p| p.file_name())
            .map(|s| uuid_from_name(&s.to_string_lossy()))
            .unwrap_or_else(|| "unknown".into())
    }
}

pub struct OmpSource;

impl UsageSource for OmpSource {
    fn id(&self) -> &'static str {
        "omp"
    }

    fn discover_roots(&self) -> Vec<PathBuf> {
        // §5.1/跨源规范 §三:根固定 ~/.omp/agent/sessions,永不认 PI_CODING_AGENT_DIR
        let root = home().join(".omp").join("agent").join("sessions");
        if root.is_dir() {
            vec![root]
        } else {
            vec![]
        }
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
                .push("未发现 ~/.omp/agent/sessions(固定根,不认 PI_CODING_AGENT_DIR)".into());
            return (vec![], stats);
        }
        // 属主纪律显式上报(跨源规范 §三):该变量在场也不认,归 pi 适配器判定
        if let Ok(v) = std::env::var("PI_CODING_AGENT_DIR") {
            if !v.trim().is_empty() {
                stats.notes.push(format!(
                    "PI_CODING_AGENT_DIR 在场({})——按跨源规范 §三 omp 不认该变量,固定根扫描",
                    v
                ));
            }
        }

        let root = &roots[0]; // 固定单根(§5.1),无多根语义
        let files = walk_jsonl(root, max_files);
        stats.files_seen = files.len();
        let mut main_files = 0usize;
        let mut sub_files = 0usize;
        for f in &files {
            if agent_of(root, f).is_some() {
                sub_files += 1;
            } else {
                main_files += 1;
            }
        }

        let mut dedup: HashMap<String, UsageRecord> = HashMap::new();
        let mut models: BTreeSet<String> = BTreeSet::new();
        let mut sessions: BTreeSet<String> = BTreeSet::new();
        let mut zero_skipped = 0u64; // 全零 usage(三跳过纪律,00-总览 §五.5)
        let mut usage_missing = 0u64; // assistant 行无 usage 对象
        let mut no_time_skipped = 0u64;
        let mut ts_line_fallback = 0u64; // message.timestamp 缺失,退化行级时间戳
        let mut ident_violations = 0u64; // totalTokens != i+cr+cw+o(§8 逐行校验)
        let mut r_clamped = 0u64; // reasoning>output 钳制(§8 防御)
        let mut cost_missing = 0u64; // 非零行缺 cost.total → 记 0(§8)
        let mut title_change_seen = 0u64; // 聚合字段在场即忽略(§5.2)
        let mut error_billed = 0u64; // 失败请求带非零用量,计费不过滤(§5.4)
        let mut dedup_collisions = 0u64; // 跨会话键碰撞(§5.3)

        for path in &files {
            let agent = agent_of(root, path);
            let mut session_id = fallback_session(root, path);
            let mut line_no = 0u64;
            let mut parsed_any = false;
            let read_outcome = for_each_line(path, |line| {
                line_no += 1;
                let Ok(v) = serde_json::from_str::<Value>(line) else {
                    stats.lines_bad_json += 1;
                    return;
                };
                let t = v.get("type").and_then(|x| x.as_str()).unwrap_or("");
                if t == "session" {
                    // session 头 id 为会话权威标识(子代理文件也带头,§3)
                    if let Some(id) = v.get("id").and_then(|x| x.as_str()) {
                        session_id = id.to_string();
                    }
                    return;
                }
                if t == "title_change" {
                    // childUsage/aggregateUsage 聚合字段在场必须忽略(§5.2:
                    // 子代理文件已独立解析,再算聚合即双计)
                    title_change_seen += 1;
                    return;
                }
                if t != "message" {
                    if !is_known_type(t) {
                        stats.lines_unknown_type += 1;
                    }
                    return;
                }
                let Some(msg) = v.get("message") else {
                    stats.lines_unknown_type += 1;
                    return;
                };
                if msg.get("role").and_then(|x| x.as_str()) != Some("assistant") {
                    return; // user / toolResult 无用量(§3)
                }
                let Some(u) = msg.get("usage") else {
                    usage_missing += 1;
                    return;
                };
                let g = |k: &str| u.get(k).and_then(|x| x.as_u64()).unwrap_or(0);
                let (i, cr, cw, o, r_raw, tt) = (
                    g("input"),
                    g("cacheRead"),
                    g("cacheWrite"),
                    g("output"),
                    g("reasoningTokens"),
                    g("totalTokens"),
                );
                if i + cr + cw + o == 0 {
                    // 全零跳过(00-总览 §五.5 三跳过;非按 error 过滤——本机失败
                    // 请求(errorMessage 行)恰为此全零行,§5.4)
                    zero_skipped += 1;
                    return;
                }
                // 逐行恒等式校验 total==i+cr+cw+o(§4;违例不丢行只上报,§8)
                if tt != i + cr + cw + o {
                    ident_violations += 1;
                }
                // r⊆o 减除(§4):输出桶不含 reasoning;r>o 钳制防御(§8)
                let r = r_raw.min(o);
                if r_raw > o {
                    r_clamped += 1;
                }
                let out_excl_r = o - r;
                // 自带逐请求美元成本(§4);缺失记 0 并标注(§8)
                let cost = match u.pointer("/cost/total").and_then(|x| x.as_f64()) {
                    Some(c) => c,
                    None => {
                        cost_missing += 1;
                        0.0
                    }
                };
                // 失败请求也计费计数,不按 stopReason/error 过滤(§5.4)
                if msg.get("errorMessage").is_some() {
                    error_billed += 1;
                }
                // 时间戳(§5.6):message.timestamp 本机实证为 epoch 毫秒数值
                // (档案 §3 括注 ISO 字符串为上游形态,两种都认);缺失退化行级 ISO
                let ts_from_msg = match msg.get("timestamp") {
                    Some(Value::Number(n)) => n.as_f64().map(norm_ms),
                    Some(Value::String(s)) => iso_to_ms(s),
                    _ => None,
                };
                let ts = match ts_from_msg {
                    Some(t) => Some(t),
                    None => match v
                        .get("timestamp")
                        .and_then(|x| x.as_str())
                        .and_then(iso_to_ms)
                    {
                        Some(t) => {
                            ts_line_fallback += 1;
                            Some(t)
                        }
                        None => None,
                    },
                };
                let Some(ts) = ts else {
                    no_time_skipped += 1;
                    return;
                };
                let model = msg
                    .get("model")
                    .and_then(|x| x.as_str())
                    .unwrap_or("unknown")
                    .to_string();
                models.insert(model.clone());
                sessions.insert(session_id.clone());
                // 去重键:omp:{message_id}(§5.3 跨会话键;无 id 退化 session:行号)
                let key = match v.get("id").and_then(|x| x.as_str()) {
                    Some(id) => format!("omp:{}", id),
                    None => format!("omp:{}:{}", session_id, line_no),
                };
                let rec = UsageRecord {
                    source: "omp",
                    session_id: session_id.clone(),
                    agent: agent.clone(),
                    model,
                    ts_ms: ts,
                    caliber: Caliber::RequestDelta,
                    request_count: 1,
                    input_uncached: i,
                    cache_read: cr,
                    cache_write: cw,
                    cache_write_1h: 0,
                    output: out_excl_r,
                    reasoning_output: r,
                    cost_usd: Some(cost),
                    dedup_key: key.clone(),
                };
                // 键碰撞:token_total 大者胜(kimi.rs 同款替换规则)
                match dedup.get(&key) {
                    Some(old) => {
                        dedup_collisions += 1;
                        if old.token_total() < rec.token_total() {
                            dedup.insert(key, rec);
                        }
                    }
                    None => {
                        dedup.insert(key, rec);
                    }
                }
                parsed_any = true;
            });
            let total = read_outcome.lines_delivered;
            read_outcome.apply_to_stats(&mut stats);
            if parsed_any || total > 0 {
                stats.files_parsed += 1;
            }
        }

        stats.notes.push(format!(
            "根 {}:{} 个 jsonl(主会话 {} · 子代理 {}),子代理文件逐文件独立解析归 agent 字段",
            root.display(),
            files.len(),
            main_files,
            sub_files
        ));
        if title_change_seen > 0 {
            stats.notes.push(format!(
                "title_change {} 行(childUsage/aggregateUsage 聚合字段在场即忽略,§5.2)",
                title_change_seen
            ));
        }
        if zero_skipped > 0 {
            stats.notes.push(format!(
                "全零 usage 跳过 {} 条(00-总览 §五.5 三跳过;非按 error 过滤,§5.4)",
                zero_skipped
            ));
        }
        if error_billed > 0 {
            stats
                .notes
                .push(format!("失败请求计费 {} 条(§5.4 不过滤)", error_billed));
        }
        if ident_violations > 0 {
            stats.notes.push(format!(
                "totalTokens 恒等式违例 {} 行(§8 逐行校验,不丢行)",
                ident_violations
            ));
        }
        if r_clamped > 0 {
            stats
                .notes
                .push(format!("reasoning>output 钳制 {} 行(§8 防御)", r_clamped));
        }
        if cost_missing > 0 {
            stats
                .notes
                .push(format!("cost.total 缺失记 0 共 {} 条(§8)", cost_missing));
        }
        if usage_missing > 0 {
            stats.notes.push(format!(
                "assistant 行缺 usage 对象 {} 条(跳过)",
                usage_missing
            ));
        }
        if ts_line_fallback > 0 {
            stats.notes.push(format!(
                "message.timestamp 缺失退化行级时间戳 {} 条",
                ts_line_fallback
            ));
        }
        if no_time_skipped > 0 {
            stats
                .notes
                .push(format!("缺时间戳跳过 {} 条", no_time_skipped));
        }
        if dedup_collisions > 0 {
            stats.notes.push(format!(
                "跨会话键碰撞 {} 次(omp:{{message_id}} 键,大者胜,§5.3)",
                dedup_collisions
            ));
        }
        let cost_sum: f64 = dedup.values().filter_map(|r| r.cost_usd).sum();
        stats.notes.push(format!(
            "会话 {} · 模型 {} · 自带逐请求成本合计 ${:.6}(官方口径,§4)",
            sessions.len(),
            models.iter().cloned().collect::<Vec<_>>().join(", "),
            cost_sum
        ));
        (dedup.into_values().collect(), stats)
    }
}
