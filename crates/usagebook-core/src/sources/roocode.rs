//! Roo Code 适配器 —— 依据 docs/数据源档案/20-roocode.md(Community,本机未实证)。
//!
//! 根:AI_USAGE_VSCODE_ROOTS 或各 VS Code 宿主 globalStorage/rooveterinaryinc.roo-cline/tasks(§1)
//! 文件:tasks/<taskUuid>/ui_messages.json,JSON 数组(非 JSONL);
//!     用量行 say ∈ {api_req_started, api_req_deleted} 且 text 以 { 开头(§2)
//! 模型:同目录 api_conversation_history.json 正则 <model>X</model> 最后一处匹配
//!     (>1MB 只读末尾 1MB);无 → protocol:<slug>;再无 → unknown(§4)
//! 去重键:roocode:{taskUuid}:{ts}(§4)

use crate::common::{env_roots, json_u64};
use crate::model::{Caliber, ScanStats, UsageSource, UsageRecord};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

pub struct RoocodeSource;

/// <model>X</model> 最后一处匹配(§4;>1MB 只读末尾,前缀残缺匹配忽略)。
fn last_model_tag(path: &Path) -> Option<String> {
    let Ok(meta) = std::fs::metadata(path) else { return None };
    let buf = if meta.len() > 1_048_576 {
        // 只读末尾 1MB(§4)
        let mut f = std::fs::File::open(path).ok()?;
        use std::io::{Seek, SeekFrom};
        f.seek(SeekFrom::Start(meta.len() - 1_048_576)).ok()?;
        let mut b = Vec::new();
        use std::io::Read;
        f.read_to_end(&mut b).ok()?;
        b
    } else {
        std::fs::read(path).ok()?
    };
    let text = String::from_utf8_lossy(&buf);
    let mut last = None;
    let mut rest = text.as_ref();
    while let Some(p) = rest.find("<model>") {
        let after = &rest[p + 7..];
        if let Some(e) = after.find("</model>") {
            last = Some(after[..e].to_string());
            rest = &after[e..];
        } else {
            break;
        }
    }
    last.filter(|m| !m.is_empty())
}

impl UsageSource for RoocodeSource {
    fn id(&self) -> &'static str {
        "roocode"
    }

    fn discover_roots(&self) -> Vec<PathBuf> {
        let mut roots = env_roots("AI_USAGE_VSCODE_ROOTS");
        if roots.is_empty() {
            let hosts = [
                "Code",
                "Code - Insiders",
                "Cursor",
                "VSCodium",
                "Windsurf",
                "Trae",
                "Trae CN",
                "CodeBuddy",
            ];
            // VS Code globalStorage 跨平台布局:mac ~/Library/Application Support,
            // Windows %APPDATA%,Linux ~/.config(juejin windows 双源实证,§1)
            let mut bases = vec![crate::common::home().join("Library/Application Support")];
            if let Ok(appdata) = std::env::var("APPDATA") {
                if !appdata.trim().is_empty() {
                    bases.push(std::path::PathBuf::from(appdata.trim()));
                }
            }
            bases.push(crate::common::home().join(".config"));
            for base in bases {
                for h in hosts {
                    let d = base
                        .join(h)
                        .join("User/globalStorage/rooveterinaryinc.roo-cline/tasks");
                    if d.is_dir() {
                        roots.push(d);
                    }
                }
            }
        }
        roots
    }

    fn scan_roots(&self, roots: &[std::path::PathBuf], max_files: Option<usize>) -> (Vec<UsageRecord>, ScanStats) {
        let mut stats = ScanStats::default();
        if roots.is_empty() {
            stats
                .notes
                .push("未发现 Roo Code tasks 目录或 AI_USAGE_VSCODE_ROOTS".into());
            return (vec![], stats);
        }
        // —— 发现:<root>/<taskUuid>/ui_messages.json(§1)——
        let mut files: Vec<PathBuf> = Vec::new();
        let mut per_root = Vec::new();
        for r in roots {
            let mut got: Vec<PathBuf> = Vec::new();
            if let Ok(rd) = std::fs::read_dir(r) {
                for e in rd.flatten() {
                    let p = e.path().join("ui_messages.json");
                    if p.is_file() {
                        got.push(p);
                    }
                }
            }
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
        let mut dup_dropped = 0u64;
        // 模型标签按 history 文件路径 memo(2026-10-17):此前每条用量行都重读
        // api_conversation_history.json(≤1MB),大任务 N 请求 = N 次全文件 IO
        let mut model_tag_cache: HashMap<std::path::PathBuf, Option<String>> = HashMap::new();

        for path in &files {
            let task = path
                .parent()
                .and_then(|p| p.file_name())
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_string();
            let Some(doc) = std::fs::read_to_string(path)
                .ok()
                .and_then(|t| serde_json::from_str::<Value>(&t).ok())
            else {
                stats.lines_bad_json += 1;
                continue;
            };
            let Some(arr) = doc.as_array() else {
                stats.lines_unknown_type += 1;
                continue;
            };
            stats.files_parsed += 1;
            stats.lines_total += arr.len() as u64;
            for el in arr {
                let say = el.get("say").and_then(|x| x.as_str()).unwrap_or("");
                if say != "api_req_started" && say != "api_req_deleted" {
                    continue;
                }
                let Some(text) = el.get("text").and_then(|x| x.as_str()).filter(|t| t.starts_with('{')) else {
                    continue;
                };
                let Ok(u) = serde_json::from_str::<Value>(text) else {
                    stats.lines_bad_json += 1;
                    continue;
                };
                let g = |k: &str| json_u64(u.get(k));
                let (raw_i, o, cr, cw) = (g("tokensIn"), g("tokensOut"), g("cacheReads"), g("cacheWrites"));
                // tokensIn 是 inclusive(档案 §6.2 定稿,源码铁证):须减 cacheReads+cacheWrites
                // 得 exclusive 新输入,否则含缓存请求 input 侧系统性虚增(2026-10-17 修)
                let i = raw_i.saturating_sub(cr).saturating_sub(cw);
                if raw_i == 0 && o == 0 && cr == 0 && cw == 0 {
                    continue;
                }
                let ts_ms = el.get("ts").and_then(|x| x.as_i64()).unwrap_or(0);
                // 模型链(§4):history 正则 → protocol slug → unknown(经 memo,同文件只读一次)
                let hist_path = path.parent().unwrap_or(Path::new("")).join("api_conversation_history.json");
                let model = model_tag_cache
                    .entry(hist_path.clone())
                    .or_insert_with(|| last_model_tag(&hist_path))
                    .clone()
                    .unwrap_or_else(|| {
                        u.get("apiProtocol")
                            .and_then(|x| x.as_str())
                            .map(|p| {
                                let slug: String = p
                                    .to_lowercase()
                                    .chars()
                                    .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
                                    .collect();
                                format!("protocol:{}", slug.trim_matches('-'))
                            })
                            .unwrap_or_else(|| "unknown".into())
                    });
                let key = format!("roocode:{}:{}", task, ts_ms);
                if !seen.insert(key.clone()) {
                    dup_dropped += 1;
                    continue;
                }
                records.push(UsageRecord {
                    source: "roocode",
                    session_id: task.clone(),
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
                    cost_usd: None,
                    dedup_key: key,
                });
            }
        }

        for (root, n) in per_root {
            stats.notes.push(format!("根 {}:{n} 个 ui_messages.json", root));
        }
        if dup_dropped > 0 {
            stats.notes.push(format!("去重键命中丢弃 {dup_dropped} 条(§4)"));
        }
        (records, stats)
    }
}
