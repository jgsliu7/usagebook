//! Kimi Code 适配器 —— 依据 docs/数据源档案/03-kimi-code.md(v1.0 + §10 桌面版补记)。
//! 扫描核 scan_kimi_wire 与 Kimi Work 共用(桌面版内嵌同款 kimi-code 运行时,kimi_work.rs)。
//!
//! 双根:~/.kimi-code(KIMI_CODE_HOME)+ 旧根 ~/.kimi(本机实证并存,档案 §2)
//! 文件:仅 basename == "wire.jsonl";新布局判据 = 父目录的父目录 == agents(aiusage 同义)
//! 新格式(§3):type=="usage.record",usageScope=="turn" 才计(session 累计跳过);
//!   inputOther 已是扣除缓存的新输入(exclusive,与 Codex 相反,§4 映射表为唯一依据)
//! 旧格式(§3,本机嵌套校正):message.type=="StatusUpdate",用量在
//!   message.payload.token_usage(snake_case;档案所写 message.token_usage 一级位置不成立)
//! 真实模型:llm.request.model 优先(usage.record.model 是配置别名,§4)
//! 去重键:CodeBurn 五元组 {source_id}:{session}:{agent}:{line}:{ordinal}(§5.2;
//!   前缀随注入源 id,kimi / kimi-work 互不串扰)

use crate::common::*;
use crate::model::*;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

pub struct KimiSource;

fn project_usage(v: &Value) -> Option<Value> {
    match v.get("type").and_then(Value::as_str) {
        Some(_) => Some(usage_projection(
            v,
            &[
                "/type",
                "/id",
                "/parentId",
                "/agentId",
                "/time",
                "/created_at",
                "/protocolVersion",
                "/protocol_version",
                "/model",
                "/usageScope",
                "/turnId",
                "/turnStep",
                "/seq",
                "/promptId",
                "/messageId",
                "/branch",
                "/base/branch",
                "/base/line",
                "/turns",
                "/count",
                "/legacyUndoLine",
                "/fromTurnId",
                "/message/id",
                "/message/role",
                "/message/origin/kind",
                "/message/origin/trigger",
                "/message/origin/phase",
                "/origin/kind",
                "/origin/trigger",
                "/origin/phase",
                "/usage/inputOther",
                "/usage/inputCacheRead",
                "/usage/inputCacheCreation",
                "/usage/output",
            ],
        )),
        None => {
            if let Some(t) = v.pointer("/message/type").and_then(Value::as_str) {
                if KNOWN_OLD_MSG_TYPES.contains(&t) {
                    return None;
                }
            }
            let mut out = usage_projection(
                v,
                &[
                    "/timestamp",
                    "/message/type",
                    "/message/payload/token_usage/input_other",
                    "/message/payload/token_usage/input_cache_read",
                    "/message/payload/token_usage/input_cache_creation",
                    "/message/payload/token_usage/output",
                ],
            );
            if let Some(original) = v.pointer("/message/payload/token_usage") {
                if out.pointer("/message/payload/token_usage").is_none() {
                    if out.get("message").is_none() {
                        out["message"] = serde_json::json!({});
                    }
                    if out["message"].get("payload").is_none() {
                        out["message"]["payload"] = serde_json::json!({});
                    }
                    out["message"]["payload"]["token_usage"] = if original.is_null() {
                        Value::Null
                    } else {
                        serde_json::json!({})
                    };
                }
            }
            Some(out)
        }
    }
}

/// 从 wire.jsonl 路径提取 (session_id, agent)。
/// 新:.../{session_|ses_}<uuid>/agents/<agent>/wire.jsonl(两代目录命名并存,§2)
/// 旧:.../<uuid>/wire.jsonl(无 agents 层,归 main)
fn session_agent_from_path(path: &Path) -> (String, String) {
    let comps: Vec<String> = path
        .components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect();
    let strip = |s: &str| -> String {
        for p in ["session_", "ses_"] {
            if let Some(t) = s.strip_prefix(p) {
                return t.to_string();
            }
        }
        s.to_string()
    };
    let n = comps.len();
    if n >= 4 && comps[n - 3] == "agents" {
        (strip(&comps[n - 4]), comps[n - 2].clone())
    } else if n >= 2 {
        (strip(&comps[n - 2]), "main".into())
    } else {
        ("unknown".into(), "main".into())
    }
}

fn session_directory(path: &Path) -> PathBuf {
    if path
        .ancestors()
        .nth(2)
        .and_then(Path::file_name)
        .is_some_and(|n| n == "agents")
    {
        path.ancestors().nth(3).unwrap().to_path_buf()
    } else {
        path.parent().unwrap_or(path).to_path_buf()
    }
}

/// Return copied-prefix boundaries and quarantined files. Metadata is read only
/// beside already discovered wires; parent IDs are resolved in this snapshot,
/// never converted into a path to open.
fn fork_boundaries(
    files: &[PathBuf],
    docs: &[std::sync::Arc<UsageLines>],
    stats: &mut ScanStats,
) -> (Vec<Option<u64>>, Vec<bool>) {
    use super::fork_lineage::{physical, quarantine};
    use std::io::Read;
    struct Session {
        dir: PathBuf,
        id: String,
        parent: Option<String>,
        wires: HashMap<String, usize>,
        agents: HashMap<String, Value>,
        bad: bool,
        read_bad: bool,
    }
    let mut sessions: Vec<Session> = Vec::new();
    let mut by_dir = HashMap::new();
    let mut file_session = Vec::new();
    for (index, path) in files.iter().enumerate() {
        let dir = physical(&session_directory(path));
        let session = *by_dir.entry(dir.clone()).or_insert_with(|| {
            let mut s = Session {
                id: dir
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .into_owned(),
                dir: dir.clone(),
                parent: None,
                wires: HashMap::new(),
                agents: HashMap::new(),
                bad: false,
                read_bad: false,
            };
            let state = dir.join("state.json");
            match std::fs::symlink_metadata(&state) {
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => s.bad = true,
                Ok(_) => {
                    // Reject an escaped metadata symlink before opening it.
                    if physical(&state).parent() != Some(dir.as_path()) {
                        s.bad = true;
                    } else {
                        let stamp = file_stamp(&state);
                        let mut bytes = Vec::new();
                        let parsed = std::fs::File::open(&state).and_then(|file| {
                            file.take(16 * 1024 * 1024 + 1).read_to_end(&mut bytes)
                        });
                        if parsed.is_err()
                            || bytes.len() > 16 * 1024 * 1024
                            || stamp != file_stamp(&state)
                        {
                            s.bad = true;
                        } else if let Ok(meta) = serde_json::from_slice::<Value>(&bytes) {
                            if !meta.is_object() {
                                s.bad = true;
                            }
                            if let Some(id) = meta.get("id") {
                                let short =
                                    s.id.strip_prefix("ses_")
                                        .or_else(|| s.id.strip_prefix("session_"))
                                        .unwrap_or(&s.id);
                                match id.as_str() {
                                    Some(id) if id == s.id || id == short => s.id = id.to_string(),
                                    _ => s.bad = true,
                                }
                            }
                            if let Some(agents) = meta.get("agents") {
                                if let Some(agents) = agents.as_object() {
                                    s.agents = agents
                                        .iter()
                                        .map(|(id, meta)| {
                                            (
                                                id.clone(),
                                                usage_projection(
                                                    meta,
                                                    &[
                                                        "/type",
                                                        "/parentAgentId",
                                                        "/forkedFrom",
                                                        "/labels/parentAgentId",
                                                    ],
                                                ),
                                            )
                                        })
                                        .collect();
                                } else {
                                    s.bad = true;
                                }
                            }
                            if let Some(parent) = meta.get("forkedFrom").filter(|v| !v.is_null()) {
                                if let Some(parent) = parent.as_str().filter(|v| !v.is_empty()) {
                                    s.parent = Some(parent.to_string());
                                } else {
                                    s.bad = true;
                                }
                            }
                        } else {
                            s.bad = true;
                        }
                    }
                }
            }
            let i = sessions.len();
            sessions.push(s);
            i
        });
        let agent = session_agent_from_path(path).1;
        if sessions[session].wires.insert(agent, index).is_some() {
            sessions[session].bad = true;
        }
        sessions[session].read_bad |= !docs[index].readable || docs[index].bad > 0;
        file_session.push(session);
    }
    let mut identities: HashMap<(PathBuf, String), Vec<usize>> = HashMap::new();
    for (i, session) in sessions.iter().enumerate() {
        identities
            .entry((
                session.dir.parent().unwrap_or(Path::new("")).to_path_buf(),
                session.id.clone(),
            ))
            .or_default()
            .push(i);
    }
    let mut parents = vec![Vec::new(); sessions.len()];
    let mut bad: Vec<_> = sessions.iter().map(|s| s.bad).collect();
    let mut boundary = vec![None; files.len()];
    for (i, session) in sessions.iter().enumerate() {
        if let Some(parent) = &session.parent {
            parents[i] = identities
                .get(&(
                    session.dir.parent().unwrap_or(Path::new("")).to_path_buf(),
                    parent.clone(),
                ))
                .cloned()
                .unwrap_or_default();
            if parents[i].len() != 1 {
                bad[i] = true;
                continue;
            }
            let ancestor = &sessions[parents[i][0]];
            // The metadata roster is part of ancestry completeness, not an
            // instruction to open undeclared paths outside the discovery set.
            if !session.wires.contains_key("main")
                || !ancestor.wires.contains_key("main")
                || session
                    .agents
                    .keys()
                    .any(|agent| !session.wires.contains_key(agent))
                || ancestor
                    .agents
                    .keys()
                    .any(|agent| !ancestor.wires.contains_key(agent))
            {
                bad[i] = true;
            }
            let main_fork_time = session.wires.get("main").and_then(|&file| {
                docs[file]
                    .events
                    .iter()
                    .rev()
                    .find(|(_, v)| v["type"] == "forked" && v["agentId"] == "main")
                    .and_then(|(_, v)| v["time"].as_f64())
            });
            for (agent, &child_file) in &session.wires {
                let child = &docs[child_file].events;
                let Some(marker) = child.iter().rposition(|(_, v)| v["type"] == "forked") else {
                    // Published create() seals a fresh wire and registers a sub
                    // agent without forkedFrom; agent fork() always records it.
                    // Combined with the new wire boundary this proves an agent
                    // created after the session fork, even if the parent later
                    // creates an independent agent with the same local name.
                    let meta = session.agents.get(agent);
                    let fresh_time = child
                        .first()
                        .filter(|(_, v)| v["type"] == "metadata")
                        .and_then(|(_, v)| v["created_at"].as_f64());
                    let fresh = agent != "main"
                        && meta.is_some_and(|m| {
                            m["type"] == "sub"
                                && (m["parentAgentId"] == "main"
                                    || m.pointer("/labels/parentAgentId")
                                        .is_some_and(|v| v == "main"))
                                && m.get("forkedFrom").is_none_or(Value::is_null)
                        })
                        && main_fork_time
                            .zip(fresh_time)
                            .is_some_and(|(fork, created)| {
                                created > fork
                                    && fork > 0.0
                                    && child.iter().all(|(_, v)| {
                                        v["time"].as_f64().is_none_or(|time| time >= created)
                                    })
                            });
                    if !fresh {
                        bad[i] = true;
                    }
                    continue;
                };
                let Some(&parent_file) = ancestor.wires.get(agent) else {
                    bad[i] = true;
                    continue;
                };
                let (_, marker_value) = &child[marker];
                if marker_value["agentId"].as_str() != Some(agent.as_str())
                    || marker_value["time"]
                        .as_f64()
                        .is_none_or(|time| !time.is_finite() || time <= 0.0)
                {
                    bad[i] = true;
                    continue;
                }
                let original = &docs[parent_file].events;
                // Full copies and provable turn prefixes preserve exact ordering.
                // Flattened/reordered branches without a unique protocol mapping fail closed.
                if marker > original.len()
                    || child[..marker]
                        .iter()
                        .zip(original.iter())
                        .any(|(a, b)| a.1 != b.1)
                {
                    bad[i] = true;
                    continue;
                }
                if marker == 0 && !original.is_empty() {
                    bad[i] = true;
                    continue;
                }
                boundary[child_file] = Some(child[marker].0);
            }
        } else if session
            .wires
            .values()
            .any(|&file| docs[file].events.iter().any(|(_, v)| v["type"] == "forked"))
        {
            // The remaining copy may be the only historical evidence: never silently drop it.
            bad[i] = true;
        }
    }
    for (child, ancestors) in parents.iter().enumerate() {
        if !ancestors.is_empty() {
            bad[child] |= sessions[child].read_bad;
            for &parent in ancestors {
                bad[parent] |= sessions[parent].read_bad;
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
    (boundary, file_session.iter().map(|&s| bad[s]).collect())
}

/// epoch 值归一毫秒:<1e12 视为秒 ×1000(旧根 time/timestamp 为秒,§4)。
fn norm_ms(t: f64) -> i64 {
    if t > 0.0 && t < 1e12 {
        (t * 1000.0).round() as i64
    } else {
        t.round() as i64
    }
}

/// 档案 §3 已知非用量类型(实现时全语料普查补全:
/// tool.call/tool.result/content.part/think/step.begin/token_counting.*/turn.ended/
/// prompt.completed/llm.tools_snapshot/permission.set_mode/file_history.tracked,
/// 另 15 种:runtime.set_binding/profile.bind/prompt.accepted|aborted/
/// file_history.checkpoint/turn.steer|cancel/turn.step.retrying|interrupted/
/// full_compaction.begin|complete/context.apply_compaction/plugin.session_start/
/// permission.record_approval_result/token_counting.rebased —— 均非用量,终版未知行 0;
/// 桌面版语料(kimi-work,2026-10-18 实证)增补:tools.register_user_tool 亦非用量)
const KNOWN_TYPES: &[&str] = &[
    "metadata",
    "forked",
    "llm.request",
    "context.append_loop_event",
    "context.append_message",
    "context.apply_compaction",
    "turn.prompt",
    "turn.ended",
    "turn.steer",
    "turn.cancel",
    "turn.step.retrying",
    "turn.step.interrupted",
    "prompt.completed",
    "prompt.accepted",
    "prompt.aborted",
    "tools.update_store",
    "tools.set_active_tools",
    "tools.register_user_tool",
    "micro_compaction.apply",
    "full_compaction.begin",
    "full_compaction.complete",
    "config.update",
    "task.started",
    "task.terminated",
    "interaction.request",
    "interaction.resolved",
    "subagent.completed",
    "step.begin",
    "step.end",
    "permission.set_mode",
    "permission.record_approval_result",
    "llm.tools_snapshot",
    "file_history.tracked",
    "file_history.checkpoint",
    "runtime.set_binding",
    "profile.bind",
    "plugin.session_start",
    "token_counting.measured",
    "token_counting.turn_recorded",
    "token_counting.rebased",
    "tool.call",
    "tool.result",
    "content.part",
    "think",
    "plan.revision",
];

/// 已知非用量前缀族(goal.* / plan_mode.* / swarm_mode.*,档案 §3)。
const KNOWN_PREFIXES: &[&str] = &["goal.", "plan_mode.", "swarm_mode."];

/// 旧根已知 message.type(本机全语料普查:StatusUpdate 外皆非用量;
/// SubagentEvent 517/Notification/StepInterrupted/Compaction.* 为普查新增)。
const KNOWN_OLD_MSG_TYPES: &[&str] = &[
    "ToolResult",
    "ToolCall",
    "ToolCallPart",
    "ContentPart",
    "StepBegin",
    "TurnEnd",
    "TurnBegin",
    "SubagentEvent",
    "Notification",
    "StepInterrupted",
    "CompactionBegin",
    "CompactionEnd",
];

fn is_known_type(t: &str) -> bool {
    KNOWN_TYPES.contains(&t) || KNOWN_PREFIXES.iter().any(|p| t.starts_with(p))
}

/// 单文件解析状态(真实模型随 llm.request 流式推进,§4)。
struct FileCtx {
    session_id: String,
    agent: String,
    /// 最近一次 llm.request.model(真实模型;§4)
    last_llm_model: Option<String>,
    line_no: u64,
}

impl UsageSource for KimiSource {
    fn id(&self) -> &'static str {
        "kimi"
    }

    fn discover_roots(&self) -> Vec<PathBuf> {
        let mut roots = env_roots("KIMI_CODE_HOME");
        let h = home();
        for d in [".kimi-code", ".kimi"] {
            let p = h.join(d);
            if p.is_dir() && !roots.contains(&p) {
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
        scan_kimi_wire(
            "kimi",
            "未发现 ~/.kimi-code、~/.kimi 或 KIMI_CODE_HOME",
            roots,
            max_files,
        )
    }
}

/// 双源共用扫描核(kimi = 独立 CLI;kimi-work = Kimi 桌面版内嵌运行时,kimi_work.rs 注入)。
/// source_id 同时用作记录 source、增量缓存命名空间与去重前缀,两源互不串扰;
/// 解析语义与字段映射对两源完全一致(桌面版会话目录 conv-*/ctitle-* 无前缀可剥,原样保留)。
pub(crate) fn scan_kimi_wire(
    source_id: &'static str,
    empty_note: &str,
    roots: &[PathBuf],
    max_files: Option<usize>,
) -> (Vec<UsageRecord>, ScanStats) {
    let mut stats = ScanStats::default();
    if roots.is_empty() {
        prune_usage_cache(source_id, &[]);
        stats.notes.push(empty_note.to_string());
        return (vec![], stats);
    }
    // 收集 wire.jsonl(仅文件名匹配;user-history/*.jsonl 等自然排除,§2)
    let mut files = Vec::new();
    let mut per_root = Vec::new();
    for r in roots {
        let scope = {
            let s = r.join("sessions");
            if s.is_dir() {
                s
            } else {
                r.clone()
            }
        };
        let got: Vec<PathBuf> =
            walk_jsonl(&scope, max_files.map(|m| m.saturating_sub(files.len())))
                .into_iter()
                .filter(|p| p.file_name().is_some_and(|n| n == "wire.jsonl"))
                .collect();
        per_root.push((r.display().to_string(), got.len()));
        files.extend(got);
    }
    super::fork_lineage::unique_files(&mut files);
    stats.files_seen = files.len();
    prune_usage_cache(source_id, &files);

    let docs: Vec<_> = files
        .iter()
        .map(|path| {
            let (lines, outcome) = cached_usage_file(
                source_id,
                path,
                Some(FileParser {
                    project: project_usage,
                    keep: |_| true,
                }),
                || read_usage_jsonl_committed(path, project_usage, |_| true),
            );
            lines.account(&mut stats, outcome);
            lines
        })
        .collect();
    let (copied_before, blocked) = if source_id == "kimi" {
        fork_boundaries(&files, &docs, &mut stats)
    } else {
        (vec![None; files.len()], vec![false; files.len()])
    };

    let mut dedup: HashMap<String, UsageRecord> = HashMap::new();
    let mut clash = crate::common::DedupTracker::default();
    let mut models: HashSet<String> = HashSet::new();
    let mut sessions: HashSet<String> = HashSet::new();
    let mut scope_skipped = 0u64; // 非 turn 的 usage.record
    let mut old_null_usage = 0u64; // 旧根 StatusUpdate 但 token_usage 为 null
    let mut no_time_skipped = 0u64;

    for (file_index, path) in files.iter().enumerate() {
        if blocked[file_index] {
            continue;
        }
        let (session_id, agent) = session_agent_from_path(path);
        let source_path = super::fork_lineage::physical(path)
            .to_string_lossy()
            .into_owned();
        let mut ctx = FileCtx {
            session_id,
            agent,
            last_llm_model: None,
            line_no: 0,
        };
        let mut parsed_any = false;
        let lines = &docs[file_index];
        let mut consume = |line_no: u64, v: &Value| {
            ctx.line_no = line_no;
            // —— 新格式:顶层有 type ——
            if let Some(t) = v.get("type").and_then(|x| x.as_str()) {
                match t {
                    "usage.record" => {
                        if copied_before[file_index].is_some_and(|boundary| line_no < boundary) {
                            return;
                        }
                        // usageScope 双值:turn=增量计入,session=累计不计(§5.1)
                        let scope = v.get("usageScope").and_then(|x| x.as_str()).unwrap_or("");
                        if scope != "turn" {
                            scope_skipped += 1;
                            return;
                        }
                        let Some(u) = v.get("usage") else { return };
                        let g = |k: &str| u.get(k).and_then(|x| x.as_u64()).unwrap_or(0);
                        // inputOther 已是 exclusive 新输入(§4),四桶直取
                        let (i, cr, cw, o) = (
                            g("inputOther"),
                            g("inputCacheRead"),
                            g("inputCacheCreation"),
                            g("output"),
                        );
                        if i + cr + cw + o == 0 {
                            return;
                        }
                        let Some(ts) = v.get("time").and_then(|x| x.as_f64()).map(norm_ms) else {
                            no_time_skipped += 1;
                            return;
                        };
                        // 真实模型:最近一次 llm.request.model 优先,退化为配置别名(§4)
                        let model = ctx.last_llm_model.clone().unwrap_or_else(|| {
                            v.get("model")
                                .and_then(|x| x.as_str())
                                .unwrap_or("unknown")
                                .to_string()
                        });
                        models.insert(model.clone());
                        sessions.insert(ctx.session_id.clone());
                        // 五元组键:session+agent+行号+行内序数(§5.2;行内单 usage,ordinal 恒 1)
                        let key = format!(
                            "{}:{}:{}:{}:{}:{}",
                            source_id, ctx.session_id, ctx.agent, ctx.line_no, 1, source_path
                        );
                        let rec = UsageRecord {
                            source: source_id,
                            session_id: ctx.session_id.clone(),
                            agent: if ctx.agent == "main" {
                                None
                            } else {
                                Some(ctx.agent.clone())
                            },
                            model,
                            ts_ms: ts,
                            caliber: Caliber::RequestDelta,
                            request_count: 1,
                            input_uncached: i,
                            cache_read: cr,
                            cache_write: cw,
                            cache_write_1h: 0,
                            output: o,
                            reasoning_output: 0,
                            cost_usd: None,
                            dedup_key: key.clone(),
                        };
                        // 命中大者胜(与 claude.rs 替换规则一致)
                        if clash.track(&key, rec.ts_ms, &rec.model, rec.token_buckets()) {
                            stats.key_clash += 1;
                        }
                        match dedup.get(&key) {
                            Some(old) if old.token_total() >= rec.token_total() => {}
                            _ => {
                                dedup.insert(key, rec);
                            }
                        }
                        parsed_any = true;
                    }
                    "llm.request" => {
                        // 真实模型权威行(§4):kimi-for-coding 等
                        if let Some(m) = v.get("model").and_then(|x| x.as_str()) {
                            ctx.last_llm_model = Some(m.to_string());
                        }
                    }
                    t if is_known_type(t) => {}
                    _ => stats.lines_unknown_type += 1,
                }
                return;
            }
            // —— 旧格式:顶层 {message, timestamp},无 type(§3)——
            if let Some(m) = v.get("message") {
                match m.get("type").and_then(|x| x.as_str()) {
                    Some("StatusUpdate") => {
                        if copied_before[file_index].is_some_and(|boundary| line_no < boundary) {
                            return;
                        }
                        // 本机实证:用量在 message.payload.token_usage(可为 null)
                        let tu = v.pointer("/message/payload/token_usage");
                        if tu.is_none() || tu.is_some_and(|x| x.is_null()) {
                            old_null_usage += 1;
                            return;
                        }
                        let u = tu.unwrap();
                        let g = |k: &str| u.get(k).and_then(|x| x.as_u64()).unwrap_or(0);
                        let (i, cr, cw, o) = (
                            g("input_other"),
                            g("input_cache_read"),
                            g("input_cache_creation"),
                            g("output"),
                        );
                        if i + cr + cw + o == 0 {
                            return;
                        }
                        let Some(ts) = v.get("timestamp").and_then(|x| x.as_f64()).map(norm_ms)
                        else {
                            no_time_skipped += 1;
                            return;
                        };
                        models.insert("unknown".into());
                        sessions.insert(ctx.session_id.clone());
                        let key = format!(
                            "{}:{}:{}:{}:{}:{}",
                            source_id, ctx.session_id, ctx.agent, ctx.line_no, 1, source_path
                        );
                        let rec = UsageRecord {
                            source: source_id,
                            session_id: ctx.session_id.clone(),
                            agent: None,
                            model: "unknown".into(),
                            ts_ms: ts,
                            caliber: Caliber::RequestDelta,
                            request_count: 1,
                            input_uncached: i,
                            cache_read: cr,
                            cache_write: cw,
                            cache_write_1h: 0,
                            output: o,
                            reasoning_output: 0,
                            cost_usd: None,
                            dedup_key: key.clone(),
                        };
                        if clash.track(&key, rec.ts_ms, &rec.model, rec.token_buckets()) {
                            stats.key_clash += 1;
                        }
                        match dedup.get(&key) {
                            Some(old) if old.token_total() >= rec.token_total() => {}
                            _ => {
                                dedup.insert(key, rec);
                            }
                        }
                        parsed_any = true;
                    }
                    Some(t) if KNOWN_OLD_MSG_TYPES.contains(&t) => {}
                    _ => stats.lines_unknown_type += 1,
                }
                return;
            }
            stats.lines_unknown_type += 1;
        };
        for (line_no, value) in &lines.events {
            consume(*line_no, value);
        }
        if parsed_any || lines.total > 0 {
            stats.files_parsed += 1;
        }
    }

    for (root, n) in per_root {
        stats.notes.push(format!("根 {}:{} 个 wire.jsonl", root, n));
    }
    if scope_skipped > 0 {
        stats.notes.push(format!(
            "usageScope 非 turn 跳过 {} 条(累计口径,§5.1)",
            scope_skipped
        ));
    }
    if old_null_usage > 0 {
        stats.notes.push(format!(
            "旧根 StatusUpdate 的 token_usage 为 null {} 条(跳过)",
            old_null_usage
        ));
    }
    if no_time_skipped > 0 {
        stats
            .notes
            .push(format!("缺时间戳跳过 {} 条", no_time_skipped));
    }
    stats.notes.push(format!(
        "会话 {} · 模型 {} · 子代理文件归 agent 字段",
        sessions.len(),
        models.iter().cloned().collect::<Vec<_>>().join(", ")
    ));
    (dedup.into_values().collect(), stats)
}
