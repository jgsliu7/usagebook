//! Droid 适配器 —— 依据 docs/数据源档案/22-droid.md(Community,本机未实证)。
//!
//! 根:DROID_SESSIONS_DIR / FACTORY_DIR/sessions / 默认 ~/.factory/sessions(§1)
//! 文件:递归 *.settings.json,每文件 = 一个会话的累计快照(§2)
//! 会话级累计裁决(§3):整会话总量计入 providerLockTimestamp(锁定时刻,一次性写入稳定),
//!     防跨小时桶 UPSERT 双计;缺则回退 mtime 并计数上报
//! 五桶直取 + totalTokens 差额兜底(output==0 补 output,§4);模型名规范化(§4)
//! 去重:droid:{sessionId},同 id 多文件保总量最大者(§5)

use crate::common::{env_roots, home, iso_to_ms, json_u64, walk_with};
use crate::model::{Caliber, ScanStats, UsageRecord, UsageSource};
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;

pub struct DroidSource;

/// 模型名规范化(§4):剥 custom: 前缀与 [...] 段,小写,./空格→-,折叠 -。
fn norm_model(raw: &str) -> String {
    let stripped = raw.strip_prefix("custom:").unwrap_or(raw);
    let no_bracket = match stripped.find('[') {
        Some(p) => &stripped[..p],
        None => stripped,
    };
    let lowered: String = no_bracket
        .to_lowercase()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect();
    let mut out = String::new();
    let mut prev_dash = false;
    for c in lowered.chars() {
        if c == '-' {
            if !prev_dash && !out.is_empty() {
                out.push('-');
            }
            prev_dash = true;
        } else {
            out.push(c);
            prev_dash = false;
        }
    }
    out.trim_matches('-').to_string()
}

impl UsageSource for DroidSource {
    fn id(&self) -> &'static str {
        "droid"
    }

    fn discover_roots(&self) -> Vec<PathBuf> {
        let mut roots = env_roots("DROID_SESSIONS_DIR");
        for p in env_roots("FACTORY_DIR") {
            roots.push(p.join("sessions"));
        }
        roots.retain(|p| p.is_dir());
        if roots.is_empty() {
            let d = home().join(".factory/sessions");
            if d.is_dir() {
                roots.push(d);
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
                .push("未发现 ~/.factory/sessions 或 DROID_SESSIONS_DIR/FACTORY_DIR".into());
            return (vec![], stats);
        }
        let mut files: Vec<PathBuf> = Vec::new();
        let mut per_root = Vec::new();
        for r in roots {
            let mut got = walk_with(r, None, &|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.ends_with(".settings.json"))
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

        // (session_id) → 记录,同 id 保总量最大者(§5)
        let mut best: HashMap<String, UsageRecord> = HashMap::new();
        let mut mtime_fallback = 0u64;

        for path in &files {
            let session_id = path
                .file_stem()
                .and_then(|s| s.to_str())
                .map(|s| s.trim_end_matches(".settings").to_string())
                .unwrap_or_default();
            let Some(doc) = std::fs::read_to_string(path)
                .ok()
                .and_then(|t| serde_json::from_str::<Value>(&t).ok())
            else {
                stats.lines_bad_json += 1;
                continue;
            };
            let Some(tu) = doc.get("tokenUsage") else {
                stats.lines_unknown_type += 1;
                continue;
            };
            stats.files_parsed += 1;
            stats.lines_total += 1;
            let g = |k: &str| json_u64(tu.get(k));
            let (mut i, o, cw, cr, rs) = (
                g("inputTokens"),
                g("outputTokens"),
                g("cacheCreationTokens"),
                g("cacheReadTokens"),
                g("thinkingTokens"),
            );
            if i == 0 && o == 0 && cw == 0 && cr == 0 && rs == 0 {
                continue;
            }
            // totalTokens 差额兜底(§4)
            let sum = i + o + cw + cr + rs;
            let total = g("totalTokens");
            let output = if total > sum && o == 0 {
                total - sum
            } else {
                o
            };
            let _ = &mut i;
            // 时间戳(§3):providerLockTimestamp 稳定优先;mtime 回退有漂移风险,计数上报
            let ts_ms = doc
                .get("providerLockTimestamp")
                .and_then(|x| x.as_str())
                .and_then(iso_to_ms)
                .unwrap_or_else(|| {
                    mtime_fallback += 1;
                    path.metadata()
                        .ok()
                        .and_then(|m| m.modified().ok())
                        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                        .map(|d| d.as_millis() as i64)
                        .unwrap_or(0)
                });
            let model = doc
                .get("model")
                .and_then(|x| x.as_str())
                .map(norm_model)
                .filter(|m| !m.is_empty())
                .unwrap_or_else(|| "unknown".into());
            let total_buckets = i + output + cw + cr + rs;
            let key = format!("droid:{}", session_id);
            // 同 id 多文件保总量最大者(§5)
            let replace = match best.get(&session_id) {
                Some(prev) => {
                    prev.input_uncached
                        + prev.output
                        + prev.cache_read
                        + prev.cache_write
                        + prev.reasoning_output
                        <= total_buckets
                }
                None => true,
            };
            if replace {
                best.insert(
                    session_id.clone(),
                    UsageRecord {
                        source: "droid",
                        session_id,
                        agent: None,
                        model,
                        ts_ms,
                        caliber: Caliber::RequestDelta, // 会话终态一次性入账,重扫稳定(§5)
                        input_uncached: i,
                        cache_read: cr,
                        cache_write: cw,
                        cache_write_1h: 0,
                        output,
                        reasoning_output: rs,
                        cost_usd: None,
                        dedup_key: key,
                    },
                );
            }
        }

        let mut keys: Vec<String> = best.keys().cloned().collect();
        keys.sort();
        let records: Vec<UsageRecord> = keys.into_iter().filter_map(|k| best.remove(&k)).collect();

        for (root, n) in per_root {
            stats
                .notes
                .push(format!("根 {}:{n} 个 *.settings.json", root));
        }
        if mtime_fallback > 0 {
            stats.notes.push(format!(
                "缺 providerLockTimestamp 回退 mtime {mtime_fallback} 个(有跨小时漂移风险,§3)"
            ));
        }
        (records, stats)
    }
}
