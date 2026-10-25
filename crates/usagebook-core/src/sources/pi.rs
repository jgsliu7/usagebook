//! Pi(pi coding agent)适配器 —— 依据 docs/数据源档案/10-pi.md(v1.0 冻结)。
//!
//! 根(§2):`PI_CODING_AGENT_DIR`(指到 agent 目录一级)否则 `~/.pi/agent`;与 omp 共享该
//!   环境变量,解析后逐根碰撞检查:根等于/位于 `~/.omp` 之下 → 让位 omp,跳过并 note 上报
//!   (pi 让位纪律)。
//! 文件(§2):`sessions/<编码cwd>/*.jsonl`,一文件=一会话;递归 walk 兜底(TokenTracker 同款)。
//! 行类型(§3 本机全集):session(首行,携带真实 id/cwd)/model_change/thinking_level_change/
//!   message(role ∈ user/assistant/toolResult,仅 assistant 有 usage)。
//! 五桶映射(§4):input/cacheRead/cacheWrite/output 直取(exclusive,零减除);
//!   reasoning 字段名分裂(§5.4):`reasoning ?? reasoningTokens ?? 0`(null 按 0,reasoning ⊂ output);
//!   cacheWrite1h 恒不并入 cache_write(§4,非 0 单独上报待考证)。
//! cost.total → cost_usd(客户端逐请求估算口径,非官方账单,§4);
//!   totalTokens 作恒等式校验(= input+cacheRead+cacheWrite+output,失配计数上报不静默,§8)。
//! 去重键(§5.1):`pi:{session文件名}:{entry_id}`(行级顶层 id);model 缺失兜底 `pi-unknown`(§5.7)。
//! 时间戳(§4):message.timestamp 优先、行级 entry.timestamp 兜底;数字 <1e12 视为秒 ×1000,
//!   ISO 字符串解析(横切规范 00-总览 §五.4)。

use crate::common::*;
use crate::model::*;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

pub struct PiSource;

/// Stage only identity, lineage and metering facts in memory, never message content.
fn project_usage(v: &Value) -> Option<Value> {
    Some(usage_projection(
        v,
        &[
            "/type",
            "/id",
            "/parentId",
            "/parentSession",
            "/timestamp",
            "/version",
            "/message/role",
            "/message/timestamp",
            "/message/model",
            "/message/provider",
            "/message/usage/input",
            "/message/usage/cacheRead",
            "/message/usage/cacheWrite",
            "/message/usage/output",
            "/message/usage/reasoning",
            "/message/usage/reasoningTokens",
            "/message/usage/totalTokens",
            "/message/usage/cacheWrite1h",
            "/message/usage/cost/total",
        ],
    ))
}

/// A child may copy any branch, not necessarily a byte prefix. Entry IDs are
/// scoped to a proven parent edge; independent sessions never share ownership.
fn copied_rows(
    files: &[PathBuf],
    aliases: &HashMap<PathBuf, Vec<usize>>,
    docs: &[std::sync::Arc<UsageLines>],
    stats: &mut ScanStats,
) -> (Vec<HashSet<u64>>, Vec<bool>) {
    use super::fork_lineage::{lexical, quarantine};
    let mut parents = vec![Vec::new(); files.len()];
    let mut bad = vec![false; files.len()];
    let mut missing_id = vec![false; files.len()];
    let mut entries: Vec<HashMap<String, Value>> = vec![HashMap::new(); files.len()];
    for (i, doc) in docs.iter().enumerate() {
        // Standalone sessions preserve their already-readable prefix. Read failures
        // quarantine a whole family only when copy ownership depends on this file.
        let headers: Vec<_> = doc
            .events
            .iter()
            .filter(|(_, v)| v["type"] == "session")
            .collect();
        if headers.len() > 1 {
            bad[i] = true;
        }
        if let Some((_, header)) = headers.first() {
            if let Some(parent) = header.get("parentSession").filter(|v| !v.is_null()) {
                if let Some(parent) = parent.as_str().filter(|s| !s.is_empty()) {
                    let path = Path::new(parent);
                    let resolved = if path.is_absolute() {
                        lexical(path)
                    } else {
                        lexical(&files[i].parent().unwrap_or(Path::new("")).join(path))
                    };
                    parents[i] = aliases.get(&resolved).cloned().unwrap_or_default();
                    if parents[i].len() != 1 {
                        bad[i] = true;
                    }
                } else {
                    bad[i] = true;
                }
            }
        }
        for (_, entry) in &doc.events {
            if entry["type"] == "session" || entry["type"] == "label" {
                continue;
            }
            let Some(id) = entry
                .get("id")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
            else {
                if entry.pointer("/message/usage").is_some() {
                    missing_id[i] = true;
                }
                continue;
            };
            let mut projection = entry.clone();
            projection.as_object_mut().unwrap().remove("parentId");
            if entries[i].insert(id.to_string(), projection).is_some() {
                bad[i] = true;
            }
        }
    }
    let mut skipped = vec![HashSet::new(); files.len()];
    for i in 0..files.len() {
        if parents[i].len() != 1 {
            continue;
        }
        let parent = parents[i][0];
        if missing_id[i] || missing_id[parent] {
            bad[i] = true;
        }
        let parent_header = docs[parent]
            .events
            .iter()
            .find(|(_, v)| v["type"] == "session");
        if parent_header.and_then(|(_, v)| v["id"].as_str()).is_none() {
            bad[i] = true;
        }
        let child_header = docs[i].events.iter().find(|(_, v)| v["type"] == "session");
        if child_header.and_then(|(_, v)| v["id"].as_str()).is_none()
            || child_header.map(|(_, v)| &v["id"]) == parent_header.map(|(_, v)| &v["id"])
        {
            bad[i] = true;
        }
        let mut witnessed = false;
        for (line, entry) in &docs[i].events {
            let Some(id) = entry["id"].as_str() else {
                continue;
            };
            if entry["type"] == "session" || entry["type"] == "label" {
                continue;
            }
            if let Some(original) = entries[parent].get(id) {
                let mut projection = entry.clone();
                projection.as_object_mut().unwrap().remove("parentId");
                if &projection != original {
                    bad[i] = true;
                } else {
                    witnessed = true;
                    skipped[i].insert(*line);
                }
            }
        }
        // A nonempty parent with no copied identity is not proof of a fork.
        if !entries[parent].is_empty() && !witnessed {
            bad[i] = true;
        }
    }
    for (child, ancestors) in parents.iter().enumerate() {
        if !ancestors.is_empty() {
            bad[child] |= !docs[child].readable || docs[child].bad > 0;
            for &parent in ancestors {
                bad[parent] |= !docs[parent].readable || docs[parent].bad > 0;
            }
        }
    }
    let bad = quarantine(&parents, bad);
    if bad.iter().any(|b| *b) {
        stats.failed = true;
        stats.notes.push(format!(
            "分叉谱系无法完整验证，隔离 {} 个相关会话",
            bad.iter().filter(|b| **b).count()
        ));
    }
    (skipped, bad)
}

/// 词法归一路径(去 `./`、消化 `../`;canonicalize 对不存在路径的兜底形态)。
fn normalize_lexical(p: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for c in p.components() {
        match c {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                if !out.pop() {
                    out.push("..");
                }
            }
            other => out.push(other.as_os_str()),
        }
    }
    out
}

/// A 类共享根碰撞判定(跨源规范 §三):root 等于/位于 omp 根(~/.omp 整树)之下即碰撞。
/// 词法与 canonicalize 两种形态任一命中即判碰撞(~/.omp 可能不存在,canonicalize 失败走词法)。
fn collides_with_omp(root: &Path, omp: &Path) -> bool {
    let forms = |p: &Path| -> Vec<PathBuf> {
        let mut v = vec![normalize_lexical(p)];
        if let Ok(c) = std::fs::canonicalize(p) {
            v.push(c);
        }
        v
    };
    let root_forms = forms(root);
    let omp_forms = forms(omp);
    root_forms
        .iter()
        .any(|r| omp_forms.iter().any(|o| r == o || r.starts_with(o)))
}

/// 时间戳归一(§4 + 横切规范 §五.4):数字 <1e12 视为秒 ×1000,否则毫秒;ISO 字符串解析。
fn coerce_ts(v: Option<&Value>) -> Option<i64> {
    match v {
        Some(Value::Number(n)) => {
            let t = n.as_f64()?;
            if t > 0.0 && t < 1e12 {
                Some((t * 1000.0).round() as i64)
            } else {
                Some(t.round() as i64)
            }
        }
        Some(Value::String(s)) => iso_to_ms(s),
        _ => None,
    }
}

/// 单文件解析状态:session 头真实 id 流式捕获(首行,§2"无损 cwd/id")。
struct FileCtx {
    header_id: Option<String>,
    root: String, // 发现根(键组成部分,防多根同 stem 碰撞)
    file_stem: String,
    line_no: u64,
}

impl UsageSource for PiSource {
    fn id(&self) -> &'static str {
        "pi"
    }

    fn discover_roots(&self) -> Vec<PathBuf> {
        // 根 = PI_CODING_AGENT_DIR(指到 agent 目录一级)否则 ~/.pi/agent(§2;engram 同款)。
        // 环境变量为覆盖语义:设置且有效时独占,未设/无效时落默认根(omp 永不认此变量,跨源规范 §三)。
        let mut roots = env_roots("PI_CODING_AGENT_DIR");
        if roots.is_empty() {
            let p = home().join(".pi").join("agent");
            if p.is_dir() {
                roots.push(p);
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
                .push("未发现 ~/.pi/agent 或 PI_CODING_AGENT_DIR".into());
            return (vec![], stats);
        }
        // —— A 类共享根属主判定(跨源规范 §三,逐根进行):碰撞根让位 omp ——
        let omp_base = home().join(".omp");
        let mut live_roots = Vec::new();
        for r in roots {
            if collides_with_omp(r, &omp_base) {
                stats.notes.push(format!(
                    "PI_CODING_AGENT_DIR 与 omp 碰撞,本根让位 omp,pi 跳过:{}",
                    r.display()
                ));
            } else {
                live_roots.push(r);
            }
        }
        if live_roots.is_empty() {
            stats
                .notes
                .push("全部根与 omp 碰撞让位,pi 无可扫描根".into());
            return (vec![], stats);
        }

        // —— 会话发现:sessions/<编码cwd>/*.jsonl,递归 walk 兜底(§2)——
        let mut files = Vec::new();
        let mut per_root = Vec::new();
        for r in live_roots {
            let scope = {
                let s = r.join("sessions");
                if s.is_dir() {
                    s
                } else {
                    r.clone()
                }
            };
            let got = walk_jsonl(&scope, max_files.map(|m| m.saturating_sub(files.len())));
            per_root.push((r.display().to_string(), got.len()));
            files.extend(got);
        }
        let aliases = super::fork_lineage::unique_files(&mut files);
        stats.files_seen = files.len();
        let docs: Vec<_> = files
            .iter()
            .map(|path| {
                let mut lines = UsageLines::default();
                // Keep Pi's established UTF-8 and final-line contract. Source-level
                // caching already covers this source; no lossy JSONL reader here.
                let outcome = for_each_line(path, |line| lines.push(line, project_usage));
                lines.readable = outcome.reached_eof
                    && outcome.error.is_none()
                    && outcome.invalid_utf8_lines == 0;
                stats.lines_bad_json += lines.bad;
                outcome.apply_to_stats(&mut stats);
                std::sync::Arc::new(lines)
            })
            .collect();
        let (copied, blocked) = copied_rows(&files, &aliases, &docs, &mut stats);

        let mut dedup: HashMap<String, UsageRecord> = HashMap::new();
        let mut models: HashSet<String> = HashSet::new();
        let mut providers: HashSet<String> = HashSet::new(); // v1 统一 pi 源,provider 记入扩展上报(§5.6)
        let mut zero_skipped = 0u64; // usage 缺失或五分量全 0(§5.3)
        let mut no_ts_skipped = 0u64;
        let mut no_entry_id = 0u64; // 行级顶层 id 缺失(去重键退化行号)
        let mut model_fallback = 0u64; // model 缺失兜底 pi-unknown(§5.7)
        let mut identity_checked = 0u64; // totalTokens 在场的恒等式校验行数
        let mut identity_ok = 0u64;
        let mut cw1h_nonzero = 0u64; // cacheWrite1h 非 0:单列观察不并入,上报待考证(§4/§8)

        for (file_index, path) in files.iter().enumerate() {
            if blocked[file_index] {
                continue;
            }
            let file_stem = path
                .file_stem()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_else(|| "unknown".into());
            let root = super::fork_lineage::physical(path)
                .to_string_lossy()
                .into_owned();
            let mut ctx = FileCtx {
                header_id: None,
                root,
                file_stem,
                line_no: 0,
            };
            let mut parsed_any = false;
            docs[file_index].events.iter().for_each(|(line_no, v)| {
                ctx.line_no = *line_no;
                if copied[file_index].contains(line_no) {
                    return;
                }
                let t = v.get("type").and_then(|x| x.as_str()).unwrap_or("");
                match t {
                    // 首行 session 头:捕获真实会话 id(cwd 不入用量模型,§2/§3)
                    "session" => {
                        if ctx.header_id.is_none() {
                            if let Some(id) = v.get("id").and_then(|x| x.as_str()) {
                                ctx.header_id = Some(id.to_string());
                            }
                        }
                    }
                    // 已知非用量类型(§3 本机全集)
                    "model_change"
                    | "thinking_level_change"
                    | "label"
                    | "compaction"
                    | "branch_summary"
                    | "custom"
                    | "custom_message"
                    | "session_info" => {}
                    "message" => {
                        let Some(m) = v.get("message").filter(|x| x.is_object()) else {
                            stats.lines_unknown_type += 1; // message 行但信封异常:计数上报不静默
                            return;
                        };
                        match m.get("role").and_then(|x| x.as_str()) {
                            // user/toolResult 无 usage(§3),已知非用量角色
                            Some("user") | Some("toolResult") => {}
                            Some("assistant") => {
                                let Some(u) = m.get("usage").filter(|x| x.is_object()) else {
                                    zero_skipped += 1; // usage 缺失 → 跳过(§5.3)
                                    return;
                                };
                                let g = |k: &str| u.get(k).and_then(|x| x.as_u64()).unwrap_or(0);
                                // 五桶直取:input 本就是 exclusive,零减除(§4)
                                let (i, cr, cw, o) =
                                    (g("input"), g("cacheRead"), g("cacheWrite"), g("output"));
                                // reasoning 字段名分裂(§5.4):reasoning ?? reasoningTokens ?? 0;
                                // r 不超过 o(与 omp/dsh 同族防御):上游异常时 r>o 会凭空虚增
                                // 总量,钳到 o(2026-10-17 修)
                                let r = u
                                    .get("reasoning")
                                    .and_then(|x| x.as_u64())
                                    .or_else(|| u.get("reasoningTokens").and_then(|x| x.as_u64()))
                                    .unwrap_or(0)
                                    .min(o);
                                // totalTokens 恒等式校验(= 前四桶和,reasoning 不进 total,§3/§4)
                                let sum4 =
                                    i.saturating_add(cr).saturating_add(cw).saturating_add(o);
                                match u.get("totalTokens").and_then(|x| x.as_u64()) {
                                    Some(t) => {
                                        identity_checked += 1;
                                        if t == sum4 {
                                            identity_ok += 1;
                                        }
                                        // 失配:计数上报不静默(§8),记录照常入账
                                    }
                                    None => {
                                        zero_skipped += 1; // 无 totalTokens 视同不可信信封,跳过上报
                                        return; // 跳过即不入账:此前只计数未 return,审计口径失真(2026-10-17 修)
                                    }
                                }
                                // cacheWrite1h:恒不并入 cache_write;非 0 单独上报待考证(§4/§8)
                                if u.get("cacheWrite1h").and_then(|x| x.as_u64()).unwrap_or(0) > 0 {
                                    cw1h_nonzero += 1;
                                }
                                if i + cr + cw + o + r == 0 {
                                    zero_skipped += 1; // 五分量全 0 → 跳过(§5.3)
                                    return;
                                }
                                // message.timestamp 优先,行级 entry.timestamp 兜底(§4)
                                let Some(ts) = coerce_ts(m.get("timestamp"))
                                    .or_else(|| coerce_ts(v.get("timestamp")))
                                else {
                                    no_ts_skipped += 1;
                                    return;
                                };
                                // model 缺失兜底 pi-unknown(§5.7)
                                let model = m
                                    .get("model")
                                    .and_then(|x| x.as_str())
                                    .unwrap_or("pi-unknown")
                                    .to_string();
                                if model == "pi-unknown" {
                                    model_fallback += 1;
                                }
                                if let Some(p) = m.get("provider").and_then(|x| x.as_str()) {
                                    providers.insert(p.to_string());
                                }
                                models.insert(model.clone());
                                // 去重键:行级顶层 entry.id(§5.1)
                                // 键掺入根路径(2026-10-17):多根(主目录+项目目录)同 stem 文件
                                // 此前跨根碰撞、大者胜丢记录
                                let key = match v.get("id").and_then(|x| x.as_str()) {
                                    Some(eid) => {
                                        format!("pi:{}:{}:{}", ctx.root, ctx.file_stem, eid)
                                    }
                                    None => {
                                        no_entry_id += 1;
                                        format!(
                                            "pi:{}:{}:L{}",
                                            ctx.root, ctx.file_stem, ctx.line_no
                                        )
                                    }
                                };
                                let rec = UsageRecord {
                                    source: "pi",
                                    session_id: ctx
                                        .header_id
                                        .clone()
                                        .unwrap_or_else(|| ctx.file_stem.clone()),
                                    agent: None,
                                    model,
                                    ts_ms: ts,
                                    caliber: Caliber::RequestDelta,
                                    request_count: 1,
                                    input_uncached: i,
                                    cache_read: cr,
                                    cache_write: cw,
                                    cache_write_1h: 0,
                                    output: o,
                                    reasoning_output: r,
                                    // 客户端逐请求估算成本,直取 cost.total(§4/§8)
                                    cost_usd: u
                                        .get("cost")
                                        .and_then(|c| c.get("total"))
                                        .and_then(|x| x.as_f64()),
                                    dedup_key: key.clone(),
                                };
                                // 命中大者胜(与 kimi/claude 替换规则一致)
                                match dedup.get(&key) {
                                    Some(old) if old.token_total() >= rec.token_total() => {}
                                    _ => {
                                        dedup.insert(key, rec);
                                    }
                                }
                                parsed_any = true;
                            }
                            _ => stats.lines_unknown_type += 1,
                        }
                    }
                    _ => stats.lines_unknown_type += 1,
                }
            });
            let total = docs[file_index].total;
            if parsed_any || total > 0 {
                stats.files_parsed += 1;
            }
        }

        // —— notes 上报(禁止静默,跨源规范 §二.3)——
        for (root, n) in per_root {
            stats.notes.push(format!("根 {}:{} 个会话 jsonl", root, n));
        }
        let sessions: HashSet<&str> = dedup.values().map(|r| r.session_id.as_str()).collect();
        stats.notes.push(format!(
            "会话 {} · 模型 {} · provider {}",
            sessions.len(),
            models.iter().cloned().collect::<Vec<_>>().join(", "),
            if providers.is_empty() {
                "-".into()
            } else {
                providers.iter().cloned().collect::<Vec<_>>().join(", ")
            }
        ));
        // 恒等式校验结果(§4:totalTokens = input+cacheRead+cacheWrite+output)
        stats.notes.push(format!(
            "totalTokens 恒等式校验 {}/{} 通过{}",
            identity_ok,
            identity_checked,
            if identity_ok == identity_checked {
                String::new()
            } else {
                format!("({} 失配,上报不静默)", identity_checked - identity_ok)
            }
        ));
        // 成本合计(客户端估算口径,非官方账单,§4)
        let cost_sum: f64 = dedup.values().filter_map(|r| r.cost_usd).sum();
        stats.notes.push(format!(
            "成本合计 ${:.8}(客户端估算口径,非官方账单)",
            cost_sum
        ));
        if zero_skipped > 0 {
            stats.notes.push(format!(
                "全零/缺 usage/缺 totalTokens 跳过 {} 条(§5.3)",
                zero_skipped
            ));
        }
        if cw1h_nonzero > 0 {
            stats.notes.push(format!(
                "cacheWrite1h 非 0 共 {} 条:不并入 cache_write,待考证(§4/§8)",
                cw1h_nonzero
            ));
        }
        if no_ts_skipped > 0 {
            stats
                .notes
                .push(format!("缺时间戳跳过 {} 条", no_ts_skipped));
        }
        if no_entry_id > 0 {
            stats
                .notes
                .push(format!("行级 id 缺失 {} 条(去重键退化为行号)", no_entry_id));
        }
        if model_fallback > 0 {
            stats.notes.push(format!(
                "model 缺失兜底 pi-unknown {} 条(§5.7)",
                model_fallback
            ));
        }
        (dedup.into_values().collect(), stats)
    }
}
