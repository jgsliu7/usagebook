//! Codex CLI 适配器 —— 依据 docs/数据源档案/02-codex.md v1.0。
//!
//! 根:~/.codex(CODEX_HOME)+ sessions/ 与 archived_sessions/ 双目录
//! 用量:event_msg.payload.type == "token_count";last_token_usage 为主增量源
//! 陷阱:total_token_usage 是会话累计(可变快照),只做基线/退化 delta;
//!       fork/重放同一前缀按 Usage 元组键塌缩(与 ccusage 同义)
//! 口径:input 含 cached+cache_write(inclusive,须减);output 含 reasoning

use crate::common::*;
use crate::model::*;
use serde_json::Value;
use std::collections::HashSet;
use std::path::PathBuf;

pub struct CodexSource;

fn project_usage(v: &Value) -> Option<Value> {
    match v.get("type").and_then(Value::as_str) {
        Some("session_meta") => Some(usage_projection(v, &["/type", "/payload/session_id", "/payload/id"])),
        Some("turn_context") => Some(usage_projection(v, &["/type", "/payload/model"])),
        Some("event_msg") if v.pointer("/payload/type").and_then(Value::as_str) == Some("token_count") => {
            let mut paths = vec!["/type".to_string(), "/timestamp".into(), "/payload/type".into()];
            for side in ["last_token_usage", "total_token_usage"] {
                for field in ["input_tokens", "cached_input_tokens", "cache_write_input_tokens", "output_tokens", "reasoning_output_tokens", "total_tokens"] {
                    paths.push(format!("/payload/info/{side}/{field}"));
                }
            }
            let mut out = usage_projection(v, &paths.iter().map(String::as_str).collect::<Vec<_>>());
            // Preserve null/missing/empty-object semantics used by the delta state machine.
            if let Some(info) = v.pointer("/payload/info") {
                if info.is_null() { out["payload"]["info"] = Value::Null; }
                else {
                    if out.pointer("/payload/info").is_none() { out["payload"]["info"] = serde_json::json!({}); }
                    for side in ["last_token_usage", "total_token_usage"] {
                        if let Some(original) = info.get(side) {
                            if original.is_null() { out["payload"]["info"][side] = Value::Null; }
                            else if original.is_object() && out["payload"]["info"].get(side).is_none() { out["payload"]["info"][side] = serde_json::json!({}); }
                        }
                    }
                }
            }
            Some(out)
        }
        Some("event_msg" | "response_item" | "compacted" | "ghost_snapshot" | "token_usage_record" | "inter_agent_communication_metadata" | "world_state") => None,
        _ => Some(usage_projection(v, &["/type"])),
    }
}


/// [input, cached, cache_write, output, reasoning]
type Buckets = [u64; 5];

#[derive(Default)]
struct FileState {
    session_id: String,
    model: String,
    /// 累计基线(总量 + 分量),仅用于"只有 total"时的退化 delta 与单调校验
    prev_total: Option<u64>,
    prev_split: Option<Buckets>,
    /// 文件内首个累计值:新会话=基线0+首turn增量;fork=父前缀累计+首turn(档案 §5.3)
    first_total: Option<u64>,
    /// 首个带 last 事件的 last.total_tokens:与 first_total 同事件时基线=0,否则=继承前缀
    first_last: Option<u64>,
    /// 双法互验:Σlast vs 末条 total
    sum_last: u64,
    last_total: Option<u64>,
}

fn usage_tokens(u: &Value) -> Buckets {
    [
        u.get("input_tokens").and_then(|x| x.as_u64()).unwrap_or(0),
        u.get("cached_input_tokens").and_then(|x| x.as_u64()).unwrap_or(0),
        u.get("cache_write_input_tokens").and_then(|x| x.as_u64()).unwrap_or(0),
        u.get("output_tokens").and_then(|x| x.as_u64()).unwrap_or(0),
        u.get("reasoning_output_tokens").and_then(|x| x.as_u64()).unwrap_or(0),
    ]
}

fn sum(b: &Buckets) -> u64 {
    b.iter().fold(0u64, |a, x| a.saturating_add(*x))
}

impl UsageSource for CodexSource {
    fn id(&self) -> &'static str {
        "codex"
    }

    fn discover_roots(&self) -> Vec<PathBuf> {
        let mut roots = env_roots("CODEX_HOME");
        if roots.is_empty() {
            let p = home().join(".codex");
            if p.is_dir() {
                roots.push(p);
            }
        }
        roots
    }

    fn scan_roots(&self, roots: &[std::path::PathBuf], max_files: Option<usize>) -> (Vec<UsageRecord>, ScanStats) {
        let mut stats = ScanStats::default();
        if roots.is_empty() {
            prune_usage_cache("codex", &[]);
            stats.notes.push("未发现 ~/.codex 或 CODEX_HOME".into());
            return (vec![], stats);
        }
        let mut files = Vec::new();
        for r in roots {
            for sub in ["sessions", "archived_sessions"] {
                let d = r.join(sub);
                if d.is_dir() {
                    files.extend(walk_jsonl(&d, max_files.map(|m| m.saturating_sub(files.len()))));
                }
            }
        }
        stats.files_seen = files.len();
        prune_usage_cache("codex", &files);
        let mut records: Vec<UsageRecord> = Vec::new();
        let mut caliber_suspect = 0usize;

        // Usage 元组键去重提升为 scan 级全局(修正,档案 §8:
        // 以 session_id 去重不按文件——resume/compact 的多文件会话不得重复入账)
        let mut seen_global: HashSet<String> = HashSet::new();
        let mut clash = crate::common::DedupTracker::default();
        for path in &files {
            let fname = path
                .file_name()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_default();
            // 文件名兜底(2026-10-17 复核):session_meta 缺失时键=文件名。注意文件名 uuid 是
            // payload.id 而非 payload.session_id,与有 meta 正本的键天然不同——无法仅凭文件名
            // 让 sessions/archived 副本塌缩;当前 archived 为 .jsonl.zst 不被扫描,该双计场景
            // 不可达。若未来支持 zst,须按内容指纹排重而非文件名
            let mut st = FileState {
                session_id: fname.clone(),
                model: "unknown".into(),
                ..Default::default()
            };
            let (lines, outcome) = cached_usage_file("codex", path,
                Some(FileParser { project: project_usage,
                    keep: |line| line.contains("token_count") || line.contains("session_meta") || line.contains("turn_context") }),
                || read_usage_jsonl_committed(path, project_usage,
                    |line| line.contains("token_count") || line.contains("session_meta") || line.contains("turn_context")));
            lines.account(&mut stats, outcome);
            let mut consume = |v: &Value| {
                let ts_ms = v
                    .get("timestamp")
                    .and_then(|x| x.as_str())
                    .and_then(iso_to_ms)
                    .unwrap_or(0);
                match v.get("type").and_then(|x| x.as_str()) {
                    Some("session_meta") => {
                        if let Some(id) = v.pointer("/payload/session_id").and_then(Value::as_str).filter(|s| !s.is_empty())
                            .or_else(|| v.pointer("/payload/id").and_then(Value::as_str).filter(|s| !s.is_empty())) {
                            st.session_id = id.to_string();
                        }
                    }
                    Some("turn_context") => {
                        // 模型权威来源(档案 §5.5)
                        if let Some(m) = v.pointer("/payload/model").and_then(|x| x.as_str()) {
                            st.model = m.to_string();
                        }
                    }
                    Some("event_msg") => {
                        if v.pointer("/payload/type").and_then(|x| x.as_str()) != Some("token_count") {
                            return;
                        }
                        // #9660:info 可为 null —— 跳过计数
                        let Some(info) = v.pointer("/payload/info").filter(|i| !i.is_null()) else {
                            return;
                        };
                        // 文件首事件常为空 info 引导行(last/total 字段为 null):
                        // 继承基线 = first_total − 首个 last(同事件 ⇒ 基线 0)
                        if st.first_last.is_none() {
                            st.first_last = info
                                .pointer("/last_token_usage/total_tokens")
                                .and_then(|x| x.as_u64());
                        }
                        let last_split = info.get("last_token_usage").filter(|v| v.is_object()).map(usage_tokens);
                        let total_split = info.get("total_token_usage").filter(|v| v.is_object()).map(usage_tokens);
                        // total 语义 = total_tokens 字段(= input 含缓存 + output 含推理);
                        // 五桶直接相加会把 cached 双计(inclusive),缺字段时退化为 b0+b3
                        let total_now = info
                            .pointer("/total_token_usage/total_tokens")
                            .and_then(|x| x.as_u64())
                            .or_else(|| total_split.map(|t| t[0].saturating_add(t[3])));

                        // —— 增量状态机(档案 §5.1,tokscale 同义)——
                        let delta: Option<Buckets> = match last_split {
                            Some(l) => Some(l), // last 为主增量源
                            None => match (total_split, st.prev_split, st.prev_total) {
                                // 只有 total:退化分量差;不前进/乱序回归则跳过
                                (Some(t), Some(prev_s), Some(prev_t))
                                    if total_now.is_some_and(|n| n > prev_t) && prev_t > 0 =>
                                {
                                    Some([
                                        t[0].saturating_sub(prev_s[0]),
                                        t[1].saturating_sub(prev_s[1]),
                                        t[2].saturating_sub(prev_s[2]),
                                        t[3].saturating_sub(prev_s[3]),
                                        t[4].saturating_sub(prev_s[4]),
                                    ])
                                }
                                _ => None,
                            },
                        };
                        // 基线推进:全零快照不推进;单调才更新(档案 §5.1)
                        if let Some(t) = total_now {
                            if st.first_total.is_none() {
                                st.first_total = Some(t);
                            }
                            if t > 0 && st.prev_total.is_none_or(|p| t >= p) {
                                st.prev_total = Some(t);
                                st.prev_split = total_split;
                            }
                            st.last_total = Some(t);
                        }
                        let Some(b) = delta else { return };
                        if sum(&b) == 0 {
                            return;
                        }
                        // inclusive 减除(档案 §4):input 含 cached+cache_write;output 含 reasoning
                        let in_unc = b[0].saturating_sub(b[1]).saturating_sub(b[2]);
                        let out_ex = b[3].saturating_sub(b[4]);
                        // 去重键不含时间戳:本机实证 token_count 成对双发(last/total 全同、ts 不同),
                        // ts 入键会双双入账;total_now 会话内单调,天然区分不同 turn(档案 §5.1b)
                        let key = format!(
                            "codex:{}:{}:{}:{}:{}:{}:{}:{}",
                            st.session_id, st.model, b[0], b[1], b[2], b[3], b[4],
                            total_now.unwrap_or(0)
                        );
                        // 去重命中(成对双发/重放前缀)不累入校验和;
                        // 校验口径 = total_tokens 语义 = input(含缓存)+output(含推理)= b0+b3,
                        // 直接累加五原始桶会把 cached 双计(inclusive 语义)
                        if clash.track(&key, ts_ms, &st.model, [in_unc, b[1], b[2], out_ex, b[4]]) {
                            stats.key_clash += 1;
                        }
                        if seen_global.insert(key.clone()) {
                            st.sum_last = st.sum_last.saturating_add(b[0]).saturating_add(b[3]);
                            records.push(UsageRecord {
                                source: "codex",
                                session_id: st.session_id.clone(),
                                agent: None,
                                model: st.model.clone(),
                                ts_ms,
                                caliber: Caliber::RequestDelta,
                                input_uncached: in_unc,
                                cache_read: b[1],
                                cache_write: b[2],
                                cache_write_1h: 0,
                                output: out_ex,
                                reasoning_output: b[4],
                                cost_usd: None,
                                dedup_key: key,
                            });
                        }
                    }
                    // response_item / compacted / ghost_snapshot:非用量行。
                    // 全量扫描新证(档案 02 §3):较新 build 另有三种顶层类型——
                    // token_usage_record(请求级用量,与 token_count.last 逐字段相等,双表示;
                    //   本机 852 个含它的文件全部同时含 token_count,故跳过不双计;若未来
                    //   build 只写它,须切换主源并按 response_id 去重)、
                    // inter_agent_communication_metadata / world_state(编排元数据,非用量)
                    Some("response_item") | Some("compacted") | Some("ghost_snapshot")
                    | Some("token_usage_record")
                    | Some("inter_agent_communication_metadata")
                    | Some("world_state") => {}
                    Some(_) => stats.lines_unknown_type += 1,
                    None => stats.lines_unknown_type += 1,
                }
            };
            for (_, value) in &lines.events { consume(value); }
            stats.files_parsed += usize::from(lines.readable);
            // 双法互验(档案 §5.1/§5.3):
            // 继承基线 = first_total − first_last(新会话=0,fork 子会话=父前缀累计);
            // Σlast ≈ last_total − 继承基线
            if let (Some(t), true) = (st.last_total, st.sum_last > 0) {
                let inherited =
                    st.first_total.unwrap_or(0).saturating_sub(st.first_last.unwrap_or(0));
                let expect = t.saturating_sub(inherited);
                if expect.abs_diff(st.sum_last).saturating_mul(100) > st.sum_last {
                    caliber_suspect += 1;
                }
            }
        }
        if caliber_suspect > 0 {
            stats
                .notes
                .push(format!("{} 个会话的会话合计与末条请求累计相差>1%,按末条累计计入(数值可能偏大)", caliber_suspect));
        }
        (records, stats)
    }
}
