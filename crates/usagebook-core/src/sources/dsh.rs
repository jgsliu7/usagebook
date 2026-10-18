//! DeepSeek DSH(DeepSeek Harness)适配器 —— 依据 docs/数据源档案/07-deepseek-dsh.md v1.0。
//!
//! 根:`~/.dsh`(DSH_HOME 覆盖,档案 §1);只扫 sessions/,desktop Electron 目录不进(§8)
//! 发现:任意深度收集 `session[.vN].jsonl[.zstd]`(juejin 正则,§2);
//!       同目录最高版本 + zstd 优先(§8,juejin dsh.ts:164-166)
//! 解码:魔数 0x28B52FFD 分发、不靠扩展名(§2);zstd 流式逐帧(一 flush 一帧),
//!       撕裂尾帧保留已解前缀;读/解压双上限 64MiB 炸弹防御(§8,tokscale dsh.rs:44-76)
//! 计量:assistant/message 唯一计量点(§5.1;末条 assistant/chunk 是用量镜像,§3b.1 不计);
//!       assistant/attempt 取 data.stream 末条 usage chunk(§3b.2,全零自然跳过);
//!       compaction/summary 防御性支持(§3b.3,真实计费调用,identity 用 compactionId)
//! 五桶:inputTokens 已是 exclusive 勿再减(§4 硬约束一);cacheWriteTokens 恒缺记 0;
//!       output = outputTokens − reasoningTokens(reasoning 是 output 子集,§4 硬约束二)
//! 时间:time epoch 毫秒(§5.3),缺失或 ≤0 跳过计数;<1e12 视为秒 ×1000(总览 §五.4)
//! 去重:dsh:{kind}{identity}:{time}:{provider}:{model}:五桶 完整键形(§5.2,
//!       identity 主取 msg:{message.id};fork 拷贝全同 → 跨文件折叠,tokscale dsh.rs:345-442)
//! 口径:DSH 不落任何成本字段(§4),cost_usd 恒 None

use crate::common::{
    cached_usage_file, env_roots, home, prune_usage_cache, usage_projection, UsageLines,
};
use crate::model::{Caliber, ScanStats, UsageRecord, UsageSource};
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};
use std::io::Read;
use std::path::{Path, PathBuf};

/// zstd 帧魔数(RFC 8478 §3.1.1;tokscale dsh.rs:34)。
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];
/// 单文件读盘上限:64MiB(§8;tokscale dsh.rs:52-64)。
const MAX_READ_BYTES: usize = 64 * 1024 * 1024;
/// 单文件解压上限:64MiB(zstd 解压比无上限,双上限缺一不可;tokscale dsh.rs:66-76)。
const MAX_DECODED_BYTES: usize = 64 * 1024 * 1024;
/// 流式解码缓冲大小。
const DECODE_CHUNK: usize = 128 * 1024;

/// 已知非用量行类型(档案 §3 行类型普查 实现时本机全语料 68 文件复核,
/// 未知类型终版为 0)。assistant/chunk 是 assistant/message 的用量镜像(§3b.1),
/// 单独计数不计桶;web/deepseek-search-llm-request 无 usage 字段(§3),搜索内部调用不记账。
/// 另有 6 个类型被显式处理,不在此列:session / request/header / llm/retry-started /
/// assistant/message / assistant/attempt / compaction/summary。
const KNOWN_TYPES: &[&str] = &[
    "assistant/chunk",
    "reasoning-chunks",
    "tool-call-chunks",
    "text-chunks",
    "step/start",
    "step/end",
    "turn/start",
    "turn/end",
    "tool/call",
    "tool/result",
    "tool/code-dispatch",
    "tool/code-dispatch-start",
    "user/message",
    "web/deepseek-search-llm-request",
    "llm/retry",
    "session/title",
    "session/end-seed",
    "session/title-llm-request",
    "subagent/descriptor",
    "request/context",
    "agent/inbox/spliced",
    "sandbox/mode",
    "permission/preset",
    "approval/policy",
    "todo/write",
    "workspace/changes",
    "goal/change",
    "system/message",
    "model/selection",
    "deliverables/presented",
    "command/run",
    "command/done",
    "agent-preset/selected",
];

pub struct DshSource;

/// 单文件解析状态(session 头为首行,到达前以目录名兜底)。
struct FileCtx {
    session_id: String,
    /// 子代理判定:头 origin=="subagent" 为准(§2);缺头时按目录名防御
    /// (主会话目录 session-<uuid>,子代理目录裸 <uuid>)。
    is_subagent: bool,
    /// 子代理类型(如 "code";§3 session 头 agentPreset)
    agent_preset: Option<String>,
    /// fork 继承边界:seq < seedLength 的事件是父会话已完成前缀,必须跳过
    /// (§3b.5;本机 0 出现,防御性实现,tokscale dsh.rs:285-299)
    seed_length: i64,
    /// 最近一次请求路由(request/header 的 data.header.config),message 缺 source 时兜底
    fallback_provider: Option<String>,
    fallback_model: Option<String>,
    /// (turn,step) 替换槽:无 llm/retry-started 时同槽后到 settlement 替换先到的
    /// (latest-wins,§3b.4);retry-started 关槽 → 重试的两次调用各自累计
    last_settlement: Option<(i64, i64, usize)>,
}

/// trim 后非空才返回(tokscale non_empty_string 同义)。
fn non_empty_str(s: &str) -> Option<&str> {
    let t = s.trim();
    if t.is_empty() {
        None
    } else {
        Some(t)
    }
}

/// Value → trim 后非空的 &str。
fn value_str(v: &Value) -> Option<&str> {
    v.as_str().and_then(non_empty_str)
}

/// 解析会话文件名 → (版本号, 是否 zstd);匹配 juejin 正则
/// `^session(?:\.v(\d+))?\.jsonl(?:\.zstd)?$`(§2)。session.lock 等不匹配自然排除。
fn session_fname_info(name: &str) -> Option<(u32, bool)> {
    let (stem, zstd) = if let Some(s) = name.strip_suffix(".jsonl.zstd") {
        (s, true)
    } else {
        let s = name.strip_suffix(".jsonl")?;
        (s, false)
    };
    let version = if stem == "session" {
        0
    } else {
        let v = stem.strip_prefix("session.v")?;
        v.parse::<u32>().ok()?
    };
    Some((version, zstd))
}

/// 递归收集会话数据文件候选(不假设目录深度;排序保证确定性)。
fn walk_candidates(root: &Path, out: &mut Vec<PathBuf>) {
    let Ok(rd) = std::fs::read_dir(root) else {
        return;
    };
    for e in rd.flatten() {
        let p = e.path();
        let Ok(ft) = e.file_type() else { continue };
        if ft.is_dir() {
            walk_candidates(&p, out);
        } else if let Some(name) = p.file_name().and_then(|s| s.to_str()) {
            if session_fname_info(name).is_some() {
                out.push(p);
            }
        }
    }
}

/// 读一个会话文件:魔数分发(zstd 流式逐帧 / 裸 jsonl 直取)。
/// Ok((字节, 尾帧撕裂)):撕裂 = 活跃会话一 flush 一帧、末帧未写完,已保留可解前缀(§8);
/// Err = 不可读 / 超限(该会话记零,tokscale dsh.rs:78-90 同义)。
fn read_session_bytes(path: &Path) -> Result<(Vec<u8>, bool), String> {
    let fname = path
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    let file = std::fs::File::open(path).map_err(|e| format!("{fname}: 打开失败 {e}"))?;
    let mut raw = Vec::new();
    let mut bounded = file.take(MAX_READ_BYTES as u64 + 1);
    bounded
        .read_to_end(&mut raw)
        .map_err(|e| format!("{fname}: 读取失败 {e}"))?;
    if raw.len() > MAX_READ_BYTES {
        return Err(format!("{fname}: 读超 {} 字节上限(已中止)", MAX_READ_BYTES));
    }
    // compression:none 后端写同名裸 session.jsonl:非 zstd 魔数即已解文本(§2)
    if raw.len() < ZSTD_MAGIC.len() || raw[..ZSTD_MAGIC.len()] != ZSTD_MAGIC {
        if raw.len() > MAX_DECODED_BYTES {
            return Err(format!("{fname}: 裸文件超 {} 字节上限", MAX_DECODED_BYTES));
        }
        return Ok((raw, false));
    }
    // zstd crate 的流式 Decoder 默认连读拼接帧直至 EOF,正合"一 flush 一帧"多帧格式
    let mut decoder = zstd::stream::read::Decoder::new(raw.as_slice())
        .map_err(|e| format!("{fname}: zstd 初始化失败 {e}"))?;
    let mut decoded = Vec::new();
    let mut chunk = vec![0u8; DECODE_CHUNK];
    loop {
        // 恰好填满上限后仍多探 1 字节:超限帧在物化前拒绝(tokscale dsh.rs:148-160)
        let headroom = (MAX_DECODED_BYTES - decoded.len()).saturating_add(1);
        let want = headroom.min(chunk.len());
        let n = match decoder.read(&mut chunk[..want]) {
            Ok(0) => break,
            Ok(n) => n,
            // 撕裂尾帧:保留已解前缀,部分末行将按坏行计数(解析纪律)
            Err(_) => return Ok((decoded, true)),
        };
        if decoded.len().saturating_add(n) > MAX_DECODED_BYTES {
            return Err(format!(
                "{fname}: 解压超 {} 字节上限(已中止于 {} 字节)",
                MAX_DECODED_BYTES,
                decoded.len()
            ));
        }
        decoded.extend_from_slice(&chunk[..n]);
    }
    Ok((decoded, false))
}

/// 计量点 usage 定位(§5.1):assistant/message 取 data.usage
/// (本机全语料 933/933 位于 data.usage;档案 §3a 投影注记"顶层与 data 平级",
/// 两处都兼容,取 data.usage 优先),缺失退化为 stream 末条 usage chunk;
/// attempt 恒取 stream 末条 usage chunk(§3b.2,DSH token meter 同义);
/// summary 取 data.usage(§3b.3)。
fn usage_for_event<'a>(v: &'a Value, event_type: &str) -> Option<&'a Value> {
    if event_type == "assistant/message" {
        return v
            .pointer("/data/usage")
            .or_else(|| v.get("usage")) // 档案 §3a 所记顶层位(本机未出现,防御)
            .or_else(|| last_stream_usage(v));
    }
    if event_type == "assistant/attempt" {
        return last_stream_usage(v);
    }
    if event_type == "compaction/summary" {
        return v.pointer("/data/usage");
    }
    None
}

/// 紧凑 Assistant 流内的 usage chunk:只认末条(早期样本是中间快照,加上会双计;
/// tokscale dsh.rs:496-513)。
fn last_stream_usage(v: &Value) -> Option<&Value> {
    v.pointer("/data/stream")?
        .as_array()?
        .iter()
        .rev()
        .find_map(|rec| {
            let chunk = rec.get("chunk")?;
            (rec.get("type")?.as_str()? == "chunk" && chunk.get("type")?.as_str()? == "usage")
                .then(|| chunk.get("usage"))
                .flatten()
        })
}

fn project_usage(v: &Value) -> Option<Value> {
    let ev = v.get("type").and_then(Value::as_str).unwrap_or("");
    if KNOWN_TYPES.contains(&ev) && ev != "assistant/chunk" {
        return None;
    }
    let mut out = usage_projection(
        v,
        &[
            "/type",
            "/id",
            "/origin",
            "/agentPreset",
            "/seedLength",
            "/seq",
            "/time",
            "/surfaceOp",
            "/data/header/config/provider",
            "/data/header/config/model",
            "/data/turn",
            "/data/step",
            "/data/message/id",
            "/data/message/source/provider",
            "/data/message/source/model",
            "/data/message/source/replayState/response/responseModel",
            "/data/attemptId",
            "/data/retryId",
            "/data/compactionId",
        ],
    );
    if let Some(usage) = usage_for_event(v, ev) {
        out["usagebook_usage"] = usage_projection(
            usage,
            &[
                "/inputTokens",
                "/outputTokens",
                "/cacheReadTokens",
                "/cacheWriteTokens",
                "/reasoningTokens",
            ],
        );
    }
    Some(out)
}

impl UsageSource for DshSource {
    fn id(&self) -> &'static str {
        "dsh"
    }

    fn discover_roots(&self) -> Vec<PathBuf> {
        // DSH_HOME(§1:tokscale dsh.rs:4 + TokenTracker rollout.js:22496;默认 ~/.dsh)
        let mut roots = env_roots("DSH_HOME");
        let p = home().join(".dsh");
        if p.is_dir() && !roots.contains(&p) {
            roots.push(p);
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
            prune_usage_cache("dsh", &[]);
            stats.notes.push("未发现 ~/.dsh 或 DSH_HOME".into());
            return (vec![], stats);
        }

        // —— 发现:根/sessions 下任意深度收集候选,同目录最高版本 + zstd 优先(§8)——
        let mut candidates = Vec::new();
        for r in roots {
            let s = r.join("sessions");
            if s.is_dir() {
                walk_candidates(&s, &mut candidates);
            } else {
                stats
                    .notes
                    .push(format!("根 {} 无 sessions/ 子目录,跳过", r.display()));
            }
        }
        candidates.sort();
        let mut per_dir: BTreeMap<PathBuf, Vec<(u32, bool, PathBuf)>> = BTreeMap::new();
        for p in candidates {
            let Some(info) =
                session_fname_info(p.file_name().and_then(|s| s.to_str()).unwrap_or_default())
            else {
                continue; // 理论不可达(walk_candidates 同规则过滤);防御性跳过并计 unknown
            };
            per_dir
                .entry(p.parent().unwrap_or(Path::new("")).to_path_buf())
                .or_default()
                .push((info.0, info.1, p));
        }
        let mut files: Vec<PathBuf> = Vec::new();
        let mut alternates = 0usize;
        for (_, mut group) in per_dir {
            group.sort_by_key(|a| (a.0, a.1)); // (版本, zstd) 双键
            let best = group.pop().expect("分组非空");
            alternates += group.len();
            files.push(best.2);
        }
        files.sort();
        if let Some(m) = max_files {
            files.truncate(m);
        }
        stats.files_seen = files.len();
        prune_usage_cache("dsh", &files);

        let mut records: Vec<UsageRecord> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        let mut clash = crate::common::DedupTracker::default();
        let mut models: HashSet<String> = HashSet::new();
        let mut sessions: HashSet<String> = HashSet::new();
        let mut mirror_chunks = 0u64; // assistant/chunk 镜像行(§3b.1,不计)
        let mut attempt_rows = 0u64;
        let mut summary_rows = 0u64;
        let mut seed_skipped = 0u64; // fork 前缀(§3b.5,本机 0)
        let mut zero_skipped = 0u64; // 全零 usage(attempt 本机全零)
        let mut no_usage_skipped = 0u64;
        let mut no_time_skipped = 0u64; // §5.3
        let mut surface_skipped = 0u64; // surfaceOp 非 append(§5.4,防御)

        for path in &files {
            let dir_name = path
                .parent()
                .and_then(|p| p.file_name())
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_else(|| "unknown".into());
            let mut ctx = FileCtx {
                session_id: dir_name.clone(), // 目录名兜底;session 头到达后覆盖
                is_subagent: !dir_name.starts_with("session-"),
                agent_preset: None,
                seed_length: 0,
                fallback_provider: None,
                fallback_model: None,
                last_settlement: None,
            };
            let (lines, outcome) = cached_usage_file("dsh", path, None, || {
                let mut out = UsageLines::default();
                match read_session_bytes(path) {
                    Ok((bytes, torn)) => {
                        out.readable = true;
                        out.bytes = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
                        if torn {
                            out.notes
                                .push(format!("{dir_name}: 活跃写入尾帧撕裂,保留已解前缀(§8)"));
                        }
                        for raw in bytes.split(|&b| b == b'\n') {
                            out.push(&String::from_utf8_lossy(raw), project_usage);
                        }
                        // reload-on-change 契约:整文件视为已提交,边界随返回值携带。
                        out.committed_bytes = out.bytes;
                        out.committed_lines = out.total;
                        out.committed_bad = out.bad;
                    }
                    Err(e) => {
                        out.bad = 1;
                        out.notes.push(format!("跳过会话文件({e})"));
                    }
                }
                out
            });
            lines.account(&mut stats, outcome);
            if !lines.readable {
                continue;
            }
            stats.files_parsed += 1;
            for (_, v) in &lines.events {
                let Some(ev) = v.get("type").and_then(|x| x.as_str()) else {
                    stats.lines_unknown_type += 1;
                    continue;
                };
                match ev {
                    "session" => {
                        if let Some(id) = v.get("id").and_then(|x| x.as_str()) {
                            if !id.trim().is_empty() {
                                ctx.session_id = id.to_string();
                            }
                        }
                        ctx.is_subagent =
                            v.get("origin").and_then(|x| x.as_str()) == Some("subagent");
                        ctx.agent_preset = v
                            .get("agentPreset")
                            .and_then(|x| x.as_str())
                            .map(str::to_string);
                        ctx.seed_length = v
                            .get("seedLength")
                            .and_then(|x| x.as_i64())
                            .filter(|n| *n > 0)
                            .unwrap_or(0);
                    }
                    "request/header" => {
                        // 请求路由兜底(tokscale dsh.rs:231-243)
                        let cfg = v.pointer("/data/header/config");
                        ctx.fallback_provider = cfg
                            .and_then(|c| c.get("provider"))
                            .and_then(|x| x.as_str())
                            .map(str::to_string);
                        ctx.fallback_model = cfg
                            .and_then(|c| c.get("model"))
                            .and_then(value_str)
                            .map(str::to_string);
                    }
                    "llm/retry-started" => {
                        // 关闭同 (turn,step) 替换槽 → 重试的两次调用各自累计(§3b.4)
                        let rt = v.pointer("/data/turn").and_then(|x| x.as_i64());
                        let rs = v.pointer("/data/step").and_then(|x| x.as_i64());
                        if let Some((lt, ls, _)) = ctx.last_settlement {
                            if Some(lt) == rt && Some(ls) == rs {
                                ctx.last_settlement = None;
                            }
                        }
                    }
                    "assistant/message" | "assistant/attempt" | "compaction/summary" => {
                        let is_summary = ev == "compaction/summary";
                        let is_attempt = ev == "assistant/attempt";
                        if is_attempt {
                            attempt_rows += 1;
                        }
                        if is_summary {
                            summary_rows += 1;
                        }
                        // fork 前缀双计防御(§3b.5)
                        if ctx.seed_length > 0
                            && v.get("seq")
                                .and_then(|x| x.as_i64())
                                .is_some_and(|seq| seq < ctx.seed_length)
                        {
                            seed_skipped += 1;
                            continue;
                        }
                        let Some(usage) = v.get("usagebook_usage") else {
                            no_usage_skipped += 1;
                            continue;
                        };
                        // json_u64 宽松读(2026-10-17):as_i64 对浮点数(100.0)返回 None→丢量
                        let g = |k: &str| crate::common::json_u64(usage.get(k));
                        // §4 映射表:inputTokens(exclusive)/ cacheReadTokens /
                        // cacheWriteTokens(本机全语料恒缺记 0)/ outputTokens / reasoningTokens
                        let (i, o_gross, cr, cw, r) = (
                            g("inputTokens"),
                            g("outputTokens"),
                            g("cacheReadTokens"),
                            g("cacheWriteTokens"),
                            g("reasoningTokens"),
                        );
                        // 三跳过纪律之一:全零 usage(§5.4)
                        if i + cr + cw + o_gross + r == 0 {
                            zero_skipped += 1;
                            continue;
                        }
                        // surfaceOp 非 append 跳过(§5.4;本机只见 append)
                        if let Some(op) = v.get("surfaceOp").and_then(|x| x.as_str()) {
                            if op != "append" {
                                surface_skipped += 1;
                                continue;
                            }
                        }
                        // 时间:epoch 毫秒(§5.3);<1e12 视为秒 ×1000(总览 §五.4)
                        let ts_raw = match v.get("time").and_then(|x| x.as_i64()) {
                            Some(t) if t > 0 => t,
                            _ => {
                                no_time_skipped += 1;
                                continue;
                            }
                        };
                        let ts_ms = if ts_raw < 1_000_000_000_000 {
                            ts_raw * 1000
                        } else {
                            ts_raw
                        };

                        // 服役模型:replayState.response.responseModel 优先于配置
                        // source.model(§3a);再退 request/header 路由
                        let source = v.pointer("/data/message/source");
                        let model = source
                            .and_then(|s| s.pointer("/replayState/response/responseModel"))
                            .and_then(value_str)
                            .or_else(|| source.and_then(|s| s.get("model")).and_then(value_str))
                            .or(ctx.fallback_model.as_deref())
                            .unwrap_or("unknown")
                            .to_string();
                        // 归属按 source.provider,不按模型名猜厂商(§8)
                        let provider = source
                            .and_then(|s| s.get("provider"))
                            .and_then(|x| x.as_str())
                            .filter(|s| !s.trim().is_empty())
                            .or(ctx.fallback_provider.as_deref())
                            .unwrap_or("unknown")
                            .to_string();

                        // 去重身份(§5.2):msg:{message.id} 为主;
                        // summary 用 cmp:{compactionId}、attempt 用 attempt-id:{attemptId}
                        // 独立命名空间;seq 兜底(序列号每文件重启,仅末位退路)
                        let sid = ctx.session_id.clone();
                        let identity = if is_attempt {
                            v.pointer("/data/attemptId")
                                .or_else(|| v.pointer("/data/retryId"))
                                .and_then(|x| x.as_str())
                                .and_then(non_empty_str)
                                .map(|id| format!("attempt-id:{id}"))
                                .or_else(|| {
                                    v.get("seq")
                                        .and_then(|x| x.as_i64())
                                        .map(|seq| format!("attempt-session:{sid}:seq:{seq}"))
                                })
                                .or_else(|| {
                                    let turn = v.pointer("/data/turn").and_then(|x| x.as_i64())?;
                                    let step = v.pointer("/data/step").and_then(|x| x.as_i64())?;
                                    Some(format!(
                                        "attempt-session:{sid}:turn:{turn}:step:{step}:{ts_raw}"
                                    ))
                                })
                                .unwrap_or_else(|| format!("attempt-session:{sid}"))
                        } else {
                            v.pointer("/data/message/id")
                                .and_then(|x| x.as_str())
                                .and_then(non_empty_str)
                                .map(|id| format!("msg:{id}"))
                                .or_else(|| {
                                    if !is_summary {
                                        return None;
                                    }
                                    v.pointer("/data/compactionId")
                                        .and_then(|x| x.as_str())
                                        .and_then(non_empty_str)
                                        .map(|id| format!("cmp:{id}"))
                                })
                                .or_else(|| {
                                    v.get("seq")
                                        .and_then(|x| x.as_i64())
                                        .map(|seq| format!("seq:{seq}"))
                                })
                                .unwrap_or_else(|| format!("sid:{sid}"))
                        };
                        let kind = if is_summary {
                            "summary:"
                        } else if is_attempt {
                            "attempt:"
                        } else {
                            ""
                        };
                        // 硬约束二:reasoning 是 outputTokens 子集,减除后五桶可加
                        let output = o_gross.saturating_sub(r);
                        // 完整键形:fork 跨文件折叠需拼入 time/provider/model/五桶(§5.2)
                        let dedup_key = format!(
                            "dsh:{kind}{identity}:{ts_ms}:{provider}:{model}:{i}:{output}:{cr}:{cw}:{r}"
                        );
                        if clash.track(&dedup_key, ts_ms, &model, [i, cr, cw, output, r]) {
                            stats.key_clash += 1;
                        }
                        if !seen.insert(dedup_key.clone()) {
                            continue;
                        }

                        let agent = if ctx.is_subagent {
                            Some(
                                ctx.agent_preset
                                    .clone()
                                    .filter(|s| !s.trim().is_empty())
                                    .unwrap_or_else(|| "subagent".into()),
                            )
                        } else {
                            None
                        };
                        let rec = UsageRecord {
                            source: "dsh",
                            session_id: sid,
                            agent,
                            model: model.clone(),
                            ts_ms,
                            caliber: Caliber::RequestDelta, // request_delta(§4 映射表)
                            input_uncached: i,              // 硬约束一:已是 exclusive,直取勿减
                            cache_read: cr,
                            cache_write: cw,
                            cache_write_1h: 0,
                            output,
                            reasoning_output: r,
                            cost_usd: None, // DSH 不落成本字段(§4)
                            dedup_key,
                        };
                        models.insert(model);
                        sessions.insert(rec.session_id.clone());

                        // (turn,step) settlement(§3b.4):summary 非循环 step 直接计
                        if is_summary {
                            records.push(rec);
                            continue;
                        }
                        let step_pair = v
                            .pointer("/data/turn")
                            .and_then(|x| x.as_i64())
                            .zip(v.pointer("/data/step").and_then(|x| x.as_i64()));
                        match step_pair {
                            Some((turn, step)) => {
                                if let Some((lt, ls, idx)) = ctx.last_settlement {
                                    if lt == turn && ls == step {
                                        records[idx] = rec; // latest-wins 替换
                                        continue;
                                    }
                                }
                                let idx = records.len();
                                records.push(rec);
                                ctx.last_settlement = Some((turn, step, idx));
                            }
                            None => {
                                records.push(rec);
                                ctx.last_settlement = None;
                            }
                        }
                    }
                    "assistant/chunk" => {
                        mirror_chunks += 1; // 用量镜像,不计(§3b.1)
                    }
                    t if KNOWN_TYPES.contains(&t) => {}
                    _ => stats.lines_unknown_type += 1,
                }
            }
        }

        for r in roots {
            let n = files.iter().filter(|f| f.starts_with(r)).count();
            if n > 0 {
                stats
                    .notes
                    .push(format!("根 {}:{} 个会话文件", r.display(), n));
            }
        }
        if alternates > 0 {
            stats.notes.push(format!(
                "同目录低版本/裸文件候选跳过 {} 个(最高版本+zstd 优先,§8)",
                alternates
            ));
        }
        if mirror_chunks > 0 {
            stats.notes.push(format!(
                "assistant/chunk 用量镜像 {} 行不计(§3b.1)",
                mirror_chunks
            ));
        }
        if attempt_rows > 0 {
            stats.notes.push(format!(
                "assistant/attempt {} 行(失败重试日志,全零自然跳过)",
                attempt_rows
            ));
        }
        if summary_rows > 0 {
            stats.notes.push(format!(
                "compaction/summary {} 行(压缩摘要计费调用)",
                summary_rows
            ));
        }
        if seed_skipped > 0 {
            stats.notes.push(format!(
                "fork 继承前缀跳过 {} 条(seedLength,§3b.5)",
                seed_skipped
            ));
        }
        if zero_skipped > 0 {
            stats
                .notes
                .push(format!("全零 usage 跳过 {} 条(§5.4)", zero_skipped));
        }
        if no_usage_skipped > 0 {
            stats
                .notes
                .push(format!("缺 usage 跳过 {} 条", no_usage_skipped));
        }
        if no_time_skipped > 0 {
            stats
                .notes
                .push(format!("缺时间戳跳过 {} 条(§5.3)", no_time_skipped));
        }
        if surface_skipped > 0 {
            stats.notes.push(format!(
                "surfaceOp 非 append 跳过 {} 条(§5.4)",
                surface_skipped
            ));
        }
        stats.notes.push(format!(
            "会话 {} · 模型 {} · 子代理会话归 agent 字段",
            sessions.len(),
            models.iter().cloned().collect::<Vec<_>>().join(", ")
        ));
        (records, stats)
    }
}
