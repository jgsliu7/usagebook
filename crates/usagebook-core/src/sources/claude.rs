//! Claude Code 适配器 —— 依据 docs/数据源档案/01-claude-code.md v1.0。
//!
//! 根:~/.claude/projects(CLAUDE_CONFIG_DIR 逗号多根 + XDG 回退)
//! 行:assistant.message.usage 四字段(exclusive,input 不含 cache)
//! 去重:精确键 message.id+requestId → 退化键 session+ts+msg_id;token 大者胜
//! 特殊:subagents/**/journal.jsonl 丢弃;<synthetic> 模型跳过;cost-state 作会话级基线

use crate::common::*;
use crate::model::*;
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;

pub struct ClaudeSource;

struct ClaudeCandidate {
    record: UsageRecord,
    /// 保留缺失与显式 0 的区别;直到五桶 winner 选出后才约束到 cw。
    ttl_1h: Option<u64>,
    exact_key: bool,
}

#[derive(Default)]
struct TtlEvidence {
    value: Option<u64>,
    conflict: bool,
}

impl TtlEvidence {
    fn observe(&mut self, value: Option<u64>) {
        if let Some(value) = value {
            match self.value {
                Some(previous) if previous != value => self.conflict = true,
                None => self.value = Some(value),
                _ => {}
            }
        }
    }
}

// 只补同一精确请求、model、输入拆分的明细,不跨拆分猜测流式语义。
type TtlGroup = (String, String, [u64; 3]);

fn project_usage(v: &Value) -> Option<Value> {
    match v.get("type").and_then(Value::as_str) {
        Some("assistant") => Some(usage_projection(
            v,
            &[
                "/type",
                "/sessionId",
                "/requestId",
                "/timestamp",
                "/message/id",
                "/message/model",
                "/message/usage/input_tokens",
                "/message/usage/cache_read_input_tokens",
                "/message/usage/cache_creation_input_tokens",
                "/message/usage/cache_creation/ephemeral_1h_input_tokens",
                "/message/usage/output_tokens",
            ],
        )),
        Some("cost-state") => {
            let mut out =
                usage_projection(v, &["/type", "/sessionId", "/timestamp", "/totalCostUSD"]);
            if let Some(models) = v.get("modelUsage").and_then(Value::as_object) {
                let mapped = models
                    .iter()
                    .filter(|(name, _)| name.len() <= 4096)
                    .map(|(name, u)| {
                        (
                            name.clone(),
                            usage_projection(
                                u,
                                &[
                                    "/inputTokens",
                                    "/cacheReadInputTokens",
                                    "/cacheCreationInputTokens",
                                    "/outputTokens",
                                ],
                            ),
                        )
                    })
                    .collect();
                out["modelUsage"] = Value::Object(mapped);
            }
            Some(out)
        }
        Some(
            "user"
            | "attachment"
            | "summary"
            | "file-history-snapshot"
            | "file-history-delta"
            | "permission-mode"
            | "mode"
            | "custom-title"
            | "agent-name"
            | "system"
            | "queue-operation"
            | "last-prompt"
            | "atis-latch"
            | "ai-title"
            | "started"
            | "result",
        ) => None,
        _ => Some(usage_projection(v, &["/type"])),
    }
}

impl ClaudeSource {
    fn roots(&self) -> Vec<PathBuf> {
        let mut roots = env_roots("CLAUDE_CONFIG_DIR");
        if roots.is_empty() {
            let home_root = home().join(".claude");
            if home_root.is_dir() {
                roots.push(home_root);
            }
            if let Ok(xdg) = std::env::var("XDG_CONFIG_HOME") {
                let p = PathBuf::from(xdg).join("claude");
                if p.is_dir() {
                    roots.push(p);
                }
            }
        }
        // 根须含 projects/ 子目录;尾段已是 projects 则上取父目录(与 ccusage 同义)
        roots
            .into_iter()
            .filter_map(|r| {
                if r.file_name().is_some_and(|n| n == "projects") {
                    r.parent().map(|p| p.to_path_buf())
                } else {
                    let pj = r.join("projects");
                    pj.is_dir().then_some(r)
                }
            })
            .collect()
    }
}

impl UsageSource for ClaudeSource {
    fn id(&self) -> &'static str {
        "claude"
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
            prune_usage_cache("claude", &[]);
            stats
                .notes
                .push("未发现 ~/.claude/projects 或 CLAUDE_CONFIG_DIR".into());
            return (vec![], stats);
        }
        let mut files = Vec::new();
        for r in roots {
            files.extend(walk_jsonl(
                &r.join("projects"),
                max_files.map(|m| m.saturating_sub(files.len())),
            ));
        }
        // 编排日志非转录,显式丢弃(档案 §2)
        files.retain(|p| {
            !(p.file_name().is_some_and(|n| n == "journal.jsonl")
                && p.components().any(|c| c.as_os_str() == "subagents"))
        });
        stats.files_seen = files.len();
        prune_usage_cache("claude", &files);

        // 五桶 winner 与 TTL 证据分开:明细补全不得改变既有替换裁决。
        let mut dedup: HashMap<String, ClaudeCandidate> = HashMap::new();
        let mut ttl_evidence: HashMap<TtlGroup, TtlEvidence> = HashMap::new();
        let mut clash = crate::common::DedupTracker::default();
        // cost-state 会话级基线:session → record(保留最后)
        let mut cost_state: HashMap<String, UsageRecord> = HashMap::new();

        for path in &files {
            let in_subagents = path.components().any(|c| c.as_os_str() == "subagents");
            let agent_name = if in_subagents {
                path.file_stem().map(|s| s.to_string_lossy().into_owned())
            } else {
                None
            };
            let mut parsed_any = false;
            let (lines, outcome) = cached_usage_file(
                "claude",
                path,
                Some(FileParser {
                    project: project_usage,
                    keep: |line| line.contains("assistant") || line.contains("cost-state"),
                }),
                || {
                    read_usage_jsonl_committed(path, project_usage, |line| {
                        line.contains("assistant") || line.contains("cost-state")
                    })
                },
            );
            lines.account(&mut stats, outcome);
            let mut consume = |v: &Value| {
                let Some(t) = v.get("type").and_then(|x| x.as_str()) else {
                    stats.lines_unknown_type += 1;
                    return;
                };
                match t {
                    "assistant" => {
                        let Some(msg) = v.get("message") else { return };
                        let model = msg
                            .get("model")
                            .and_then(|x| x.as_str())
                            .unwrap_or("unknown")
                            .to_string();
                        // 合成消息不带用量,跳过(档案 §3)
                        if model == "<synthetic>" {
                            return;
                        }
                        let Some(u) = msg.get("usage") else { return };
                        let g = |k: &str| u.get(k).and_then(|x| x.as_u64()).unwrap_or(0);
                        let input = g("input_tokens");
                        let cache_read = g("cache_read_input_tokens");
                        let cache_write = g("cache_creation_input_tokens");
                        let ttl_1h = u
                            .pointer("/cache_creation/ephemeral_1h_input_tokens")
                            .and_then(|x| x.as_u64());
                        let output = g("output_tokens");
                        // 全零跳过(stormzhang 同义)
                        if input + cache_read + cache_write + output == 0 {
                            return;
                        }
                        let session_id = v
                            .get("sessionId")
                            .and_then(|x| x.as_str())
                            .unwrap_or("")
                            .to_string();
                        let msg_id = msg.get("id").and_then(|x| x.as_str()).unwrap_or("");
                        let ts_ms = v
                            .get("timestamp")
                            .and_then(|x| x.as_str())
                            .and_then(iso_to_ms)
                            .unwrap_or(0);
                        let rec = UsageRecord {
                            source: "claude",
                            session_id: session_id.clone(),
                            agent: agent_name.clone(),
                            model,
                            ts_ms,
                            caliber: Caliber::RequestDelta,
                            input_uncached: input,
                            cache_read,
                            cache_write,
                            cache_write_1h: 0,
                            output,
                            reasoning_output: 0,
                            cost_usd: None,
                            dedup_key: String::new(),
                        };
                        // 三层键的 v1 简化:精确键优先,退化键兜底(档案 §5.2)
                        let request_id = v.get("requestId").and_then(|x| x.as_str());
                        let exact_key =
                            !msg_id.is_empty() && request_id.is_some_and(|r| !r.is_empty());
                        let key = match request_id {
                            Some(r) if !r.is_empty() => format!("{}:{}", msg_id, r),
                            _ => format!("{}:{}:{}", session_id, ts_ms, msg_id),
                        };
                        let rec = UsageRecord {
                            dedup_key: key.clone(),
                            ..rec
                        };
                        if clash.track(&key, ts_ms, &rec.model, rec.token_buckets()) {
                            stats.key_clash += 1;
                        }
                        if exact_key && ttl_1h.is_some() {
                            ttl_evidence
                                .entry((
                                    key.clone(),
                                    rec.model.clone(),
                                    [input, cache_read, cache_write],
                                ))
                                .or_default()
                                .observe(ttl_1h);
                        }
                        // token 大者胜(与 ccusage 同义)
                        match dedup.get(&key) {
                            Some(old) if old.record.token_total() >= rec.token_total() => {}
                            _ => {
                                dedup.insert(
                                    key,
                                    ClaudeCandidate {
                                        record: rec,
                                        ttl_1h,
                                        exact_key,
                                    },
                                );
                            }
                        }
                        parsed_any = true;
                    }
                    "cost-state" => {
                        // 会话级官方口径基线(档案 §3 金矿):totalCostUSD + modelUsage
                        let session_id = v
                            .get("sessionId")
                            .and_then(|x| x.as_str())
                            .unwrap_or("")
                            .to_string();
                        let cost = v.get("totalCostUSD").and_then(|x| x.as_f64());
                        let ts_ms = v
                            .get("timestamp")
                            .and_then(|x| x.as_str())
                            .and_then(iso_to_ms)
                            .unwrap_or(0);
                        if let Some(mu) = v.get("modelUsage").and_then(|x| x.as_object()) {
                            for (model, m) in mu {
                                let g = |k: &str| m.get(k).and_then(|x| x.as_u64()).unwrap_or(0);
                                // 键掺入 model(2026-10-17 修):多模型 cost-state 同键互相覆盖,
                                // 仅幸存 1 模型且成本为整会话总额挂错归属(实测 59 条中 42 条多模型)
                                cost_state.insert(
                                    format!("cs:{}:{}", session_id, model),
                                    UsageRecord {
                                        source: "claude",
                                        session_id: session_id.clone(),
                                        agent: None,
                                        model: model.clone(),
                                        ts_ms,
                                        caliber: Caliber::SessionTotal,
                                        input_uncached: g("inputTokens"),
                                        cache_read: g("cacheReadInputTokens"),
                                        cache_write: g("cacheCreationInputTokens"),
                                        cache_write_1h: 0,
                                        output: g("outputTokens"),
                                        reasoning_output: 0,
                                        cost_usd: cost,
                                        dedup_key: format!("cost-state:{}", session_id),
                                    },
                                );
                            }
                        }
                    }
                    // 已知非用量行(本机 60 文件全集实证,档案 01 §3)
                    "user"
                    | "attachment"
                    | "summary"
                    | "file-history-snapshot"
                    | "file-history-delta"
                    | "permission-mode"
                    | "mode"
                    | "custom-title"
                    | "agent-name"
                    | "system"
                    | "queue-operation"
                    | "last-prompt"
                    | "atis-latch"
                    | "ai-title"
                    | "started"
                    | "result" => {}
                    _ => {
                        stats.lines_unknown_type += 1;
                    }
                }
            };
            for (_, value) in &lines.events {
                consume(value);
            }
            if parsed_any || lines.total > 0 {
                stats.files_parsed += 1;
            }
        }
        stats.notes.push(format!(
            "cost-state 会话级基线 {} 条(口径 session_total,不并入增量求和)",
            cost_state.len()
        ));
        let conflicts = ttl_evidence.values().filter(|e| e.conflict).count();
        let mut unknown_ttl = 0usize;
        let mut records: Vec<UsageRecord> = dedup
            .into_values()
            .map(|candidate| {
                let mut record = candidate.record;
                let evidence = candidate
                    .exact_key
                    .then(|| {
                        ttl_evidence.get(&(
                            record.dedup_key.clone(),
                            record.model.clone(),
                            [record.input_uncached, record.cache_read, record.cache_write],
                        ))
                    })
                    .flatten();
                let ttl = candidate.ttl_1h.or_else(|| {
                    evidence.and_then(|e| {
                        if e.conflict {
                            unknown_ttl += 1;
                            None
                        } else {
                            e.value
                        }
                    })
                });
                record.cache_write_1h = ttl.unwrap_or(0).min(record.cache_write);
                record
            })
            .collect();
        if conflicts > 0 {
            stats.notes.push(format!(
                "Claude 1h TTL 明细冲突 {conflicts} 组:保留五桶胜出记录的显式值(含 0);{unknown_ttl} 条胜出记录缺失且明细冲突,1h 不可确定(兼容字段显示 0)"
            ));
        }
        records.extend(cost_state.into_values());
        (records, stats)
    }
}
