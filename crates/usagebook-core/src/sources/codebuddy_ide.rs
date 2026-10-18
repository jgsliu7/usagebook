//! CodeBuddy IDE 适配器 —— 依据 docs/数据源档案/30-codebuddy.md §8(扩展通道)。
//!
//! 覆盖 CodeBuddy CN 桌面 IDE("CodeBuddy Work" 可视化 IDE,host=CodeBuddyIDE)与
//! VS Code/Cursor 插件(host=VSCode)——两者会话均落 CodeBuddyExtension,host 无关遍历。
//! 根:CODEBUDDY_IDE_HOME → 平台 CodeBuddyExtension/Data 目录(juejin resolveCodebuddyExtensionRoots 同义,
//! macOS 实证:~/Library/Application Support/CodeBuddyExtension/Data)。
//! 文件:<Data>/…/history/<workspace>/<session>/messages/*.json(一消息一文件;workspace 为 md5(cwd)
//! 或 base64(cwd) 匿名树;vscdb/genie-history 仅是 cwd 归因源,本模型无项目维度,不读)。
//! usage:role=assistant 的消息,挂 extra(盘上为**序列化 JSON 字符串**):
//! ① statsSnapshot(whole-loop 总和,inputTokens 含 cached,新输入=cacheMissTokens 缺省回退 input−cached)
//! ② 旧记录仅 extra.lastStep*(末步口径,约为真实值 1/7,juejin codebuddy.ts:381-446 同判)。
//! 去重:codebuddy-ide:{session}:{messageId|文件名},token 大者胜。
//! 双计:与 codebuddy(CLI)/workbuddy 数据根及会话 id 空间互斥(档案 30 §5 双产品同机实证);
//! credit 字段单位不明不入 cost_usd。

use crate::common::*;
use crate::model::*;
use serde_json::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

pub struct CodebuddyIdeSource;

/// history 目录搜索深度上限(juejin EXT_HISTORY_SEARCH_DEPTH=6 同义:
/// Data/<userId>/<host>/<userId>/history 四层,匿名树更浅,6 层余量覆盖新 host)。
const HISTORY_SEARCH_DEPTH: usize = 6;

fn num(o: &Value, k: &str) -> u64 {
    o.get(k).and_then(|x| x.as_u64()).unwrap_or(0)
}

/// extra 在盘上是序列化 JSON 字符串(本机实证),亦容错已是对象。
fn extra_of(v: &Value) -> Value {
    match v.get("extra") {
        Some(Value::String(s)) => serde_json::from_str(s).unwrap_or(Value::Null),
        Some(o @ Value::Object(_)) => o.clone(),
        _ => Value::Null,
    }
}

/// ① statsSnapshot:一条 assistant 消息=完整 agent 循环,各步重发全上下文,快照即总和。
/// thinkingTokens 是 outputTokens 子集(2026-10-18 扩展日志硬证据:totalTokens==input+output
/// 在 thinking>0 的记录上精确成立),与 CLI 通道同款减除+钳制。
fn from_snapshot(s: &Value) -> Option<(u64, u64, u64, u64, u64)> {
    let input = num(s, "inputTokens");
    let output = num(s, "outputTokens");
    let cached = num(s, "cachedInputTokens");
    let cache_write = num(s, "cacheWriteTokens");
    let reasoning = num(s, "thinkingTokens").min(output);
    if input == 0 && output == 0 && cached == 0 && cache_write == 0 {
        return None; // 无模型调用(纯工具/中间态)
    }
    // cacheMissTokens=新计费输入;旧记录缺省回退 input−cached(同 CLI 通道语义)
    let miss = {
        let m = num(s, "cacheMissTokens");
        if m > 0 {
            m
        } else {
            input.saturating_sub(cached)
        }
    };
    Some((miss, cached, cache_write, output - reasoning, reasoning))
}

/// ② lastStep* 回退:仅旧记录存在,末步口径(低估,juejin 实测约 1/7)。
fn from_last_step(e: &Value) -> Option<(u64, u64, u64, u64, u64)> {
    let raw_input = num(e, "lastStepInputTokens");
    let output = num(e, "lastStepOutputTokens");
    let cached = num(e, "lastStepCachedInputTokens");
    if raw_input == 0 && output == 0 && cached == 0 {
        return None;
    }
    Some((raw_input.saturating_sub(cached), cached, 0, output, 0))
}

fn find_history_dirs(dir: &Path, depth: usize, out: &mut Vec<PathBuf>) {
    if depth > HISTORY_SEARCH_DEPTH {
        return;
    }
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for e in entries.flatten() {
        let Ok(ft) = e.file_type() else { continue };
        if !ft.is_dir() {
            continue;
        }
        let p = e.path();
        if e.file_name() == "history" {
            out.push(p);
        } else {
            find_history_dirs(&p, depth + 1, out);
        }
    }
}

impl UsageSource for CodebuddyIdeSource {
    fn id(&self) -> &'static str {
        "codebuddy-ide"
    }

    fn discover_roots(&self) -> Vec<PathBuf> {
        let mut roots = env_roots("CODEBUDDY_IDE_HOME");
        let h = home();
        let mut candidates = vec![h.join("Library/Application Support/CodeBuddyExtension/Data")];
        if let Some(ad) = std::env::var_os("APPDATA") {
            candidates.push(PathBuf::from(ad).join("CodeBuddyExtension/Data"));
        }
        if let Some(lad) = std::env::var_os("LOCALAPPDATA") {
            candidates.push(PathBuf::from(lad).join("CodeBuddyExtension/Data"));
        }
        let data_home = std::env::var_os("XDG_DATA_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| h.join(".local/share"));
        let cfg_home = std::env::var_os("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| h.join(".config"));
        candidates.push(data_home.join("CodeBuddyExtension/Data"));
        candidates.push(cfg_home.join("CodeBuddyExtension/Data"));
        for p in candidates {
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
        let mut stats = ScanStats::default();
        if roots.is_empty() {
            stats
                .notes
                .push("未发现 CodeBuddyExtension/Data 或 CODEBUDDY_IDE_HOME".into());
            return (vec![], stats);
        }
        // host 无关枚举 history/<workspace>/<session>/messages/*.json(juejin 同策,新 host 零改动)
        let mut files = Vec::new();
        for r in roots {
            let mut history_dirs = Vec::new();
            find_history_dirs(r, 0, &mut history_dirs);
            history_dirs.sort();
            for hd in history_dirs {
                files.extend(walk_with(
                    &hd,
                    max_files.map(|m| m.saturating_sub(files.len())),
                    &|p| {
                        p.extension().is_some_and(|x| x == "json")
                            && p.parent().is_some_and(|pp| {
                                pp.file_name() == Some(std::ffi::OsStr::new("messages"))
                            })
                    },
                ));
            }
        }
        stats.files_seen = files.len();

        let mut dedup: HashMap<String, UsageRecord> = HashMap::new();
        let mut snap_hits = 0u64;
        let mut last_step_hits = 0u64;
        let mut non_assistant = 0u64;
        let mut ts_missing = 0u64;

        for path in &files {
            let Ok(raw) = std::fs::read_to_string(path) else {
                stats.lines_bad_json += 1;
                continue;
            };
            let Ok(v) = serde_json::from_str::<Value>(&raw) else {
                stats.lines_bad_json += 1;
                continue;
            };
            stats.lines_total += 1;
            if v.get("role").and_then(|x| x.as_str()) != Some("assistant") {
                non_assistant += 1;
                continue;
            }
            let extra = extra_of(&v);
            let buckets = if let Some(s) = extra.get("statsSnapshot").filter(|s| s.is_object()) {
                from_snapshot(s).inspect(|_| snap_hits += 1)
            } else {
                from_last_step(&extra).inspect(|_| last_step_hits += 1)
            };
            let Some((input_uncached, cache_read, cache_write, output, reasoning)) = buckets else {
                continue; // assistant 但无模型调用(工具/中间态;不留 seen,juejin 同策防后续补写漏采)
            };
            if input_uncached + cache_read + cache_write + output + reasoning == 0 {
                continue;
            }
            // createdAt 本机实证为 ISO 字符串;epoch 数字形态做双兼容(与家族 ts_to_ms 同策)
            let Some(ts_ms) = v.get("createdAt").and_then(super::workbuddy::ts_to_ms) else {
                ts_missing += 1;
                continue;
            };
            // session = messages/ 的父目录名;消息 id 盘上为 hex32,缺省回退文件名
            let session_id = path
                .parent()
                .and_then(|p| p.parent())
                .and_then(|p| p.file_name())
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_default();
            let msg_key = v
                .get("id")
                .and_then(|x| x.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| {
                    path.file_stem()
                        .map(|s| s.to_string_lossy().into_owned())
                        .unwrap_or_default()
                });
            let model = ["modelId", "modelName"]
                .iter()
                .find_map(|k| extra.get(k).and_then(|x| x.as_str()))
                .unwrap_or("unknown")
                .to_string();
            let key = format!("codebuddy-ide:{}:{}", session_id, msg_key);
            let rec = UsageRecord {
                source: "codebuddy-ide",
                session_id: session_id.clone(),
                agent: None,
                model,
                ts_ms,
                caliber: Caliber::RequestDelta,
                input_uncached,
                cache_read,
                cache_write,
                cache_write_1h: 0,
                output,
                reasoning_output: reasoning,
                cost_usd: None,
                dedup_key: key.clone(),
            };
            match dedup.get(&key) {
                Some(old) if old.token_total() >= rec.token_total() => {}
                _ => {
                    dedup.insert(key, rec);
                }
            }
            stats.files_parsed += 1;
        }
        stats.notes.push(format!(
            "usage 来源命中:statsSnapshot {} / lastStep {} (非 assistant 消息 {} 个)",
            snap_hits, last_step_hits, non_assistant
        ));
        if ts_missing > 0 {
            stats
                .notes
                .push(format!("{} 条缺 createdAt 跳过", ts_missing));
        }
        (dedup.into_values().collect(), stats)
    }
}
