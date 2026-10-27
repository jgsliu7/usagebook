//! Cline 适配器 —— 依据 docs/数据源档案/11-cline.md v1.0。
//!
//! 根:~/.cline/data/sessions,解析链逐级可覆盖 CLINE_SESSION_DATA_DIR →
//!     CLINE_DATA_DIR/sessions → CLINE_DIR/data/sessions → 默认根(§1)
//! 文件:sessions/<sessionId>/<id>.messages.json 为**整文件 JSON 非 JSONL**
//!     (发现逻辑自写,勿用 common::walk_jsonl——那只收 .jsonl);
//!     teams 子代理 agent_*.messages.json 同目录异名,必须全 glob *.messages.json,
//!     只取目录同名文件会漏(§2,本机漏 8 份/154 条)
//! 过滤:仅统计 native 会话(导入镜像不计的原判据)——导入镜像
//!     (mode=="import" 或 importedFrom 存在,本机 1218/1221)与 ~/.codex、~/.claude
//!     原文件数据等同,不过滤即双计 5.06B input;跳过并计数上报,禁止静默
//! 口径:metrics 为 request_delta,usageDelta diff 的 **inclusive** 总量(§4)——
//!     inputTokens 含 cacheRead+cacheWrite(须减)、outputTokens 含 reasoning;
//!     cr>i 与 inclusive 矛盾时该条改判 exclusive 直入(形状兜底,§8)
//! 模型:真实模型逐条在 messages[N].modelInfo.id(manifest.model 对导入是占位符,§5-2)
//! 去重键:cline:{sessionId}:{messageId},id 稳定、ts:index 兜底(§5-6)
//! db 不读:sessions 表无 token 列,文件即账本(§2)

use crate::common::{env_roots, file_stamp, home, FileStamp, SourceFingerprint};
use crate::model::{Caliber, PreparedSourceScan, ScanStats, UsageRecord, UsageSource};
use serde_json::Value;
use std::collections::{BTreeSet, HashSet};
use std::path::{Path, PathBuf};

pub struct ClineSource;

/// manifest 惰性信息(仅主会话的导入判定与模型回退链需要;子代理无独立 manifest,§2)。
struct ManifestInfo {
    is_import: bool,
    model: String,
    provider: String,
    invalid_json: bool,
}

const NO_MANIFEST: ManifestInfo = ManifestInfo {
    is_import: false,
    model: String::new(),
    provider: String::new(),
    invalid_json: false,
};

/// 读同目录 manifest `<sessionId>.json`(§3):
/// metadata.sessionHistoryOrigin.mode=="import" 或 importedFrom 存在 → 导入镜像(§5-1);
/// 缺失可选 manifest 保持兼容；损坏时仅允许消息本身能证明归属和模型的历史。
fn read_manifest(dir: &Path) -> std::io::Result<ManifestInfo> {
    let Some(name) = dir.file_name().and_then(|s| s.to_str()) else {
        return Ok(NO_MANIFEST);
    };
    let text = match std::fs::read_to_string(dir.join(format!("{name}.json"))) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(NO_MANIFEST),
        Err(error) => return Err(error),
    };
    let Ok(v) = serde_json::from_str::<Value>(&text) else {
        return Ok(ManifestInfo {
            invalid_json: true,
            ..NO_MANIFEST
        });
    };
    let is_import = v
        .pointer("/metadata/sessionHistoryOrigin/mode")
        .and_then(|x| x.as_str())
        == Some("import")
        || v.get("metadata")
            .is_some_and(|m| m.get("importedFrom").is_some_and(|x| !x.is_null()));
    Ok(ManifestInfo {
        is_import,
        invalid_json: false,
        model: v
            .get("model")
            .and_then(|x| x.as_str())
            .unwrap_or_default()
            .to_string(),
        provider: v
            .get("provider")
            .and_then(|x| x.as_str())
            .unwrap_or_default()
            .to_string(),
    })
}

impl UsageSource for ClineSource {
    fn id(&self) -> &'static str {
        "cline"
    }

    fn discover_roots(&self) -> Vec<PathBuf> {
        // 目录解析链逐级可覆盖(§1;juejin cline.ts:52-63、TokenTracker rollout.js:11752-11766);
        // env 支持逗号多根(00-总览 §三),首个命中层级生效
        let levels: Vec<Vec<PathBuf>> = vec![
            env_roots("CLINE_SESSION_DATA_DIR"),
            env_roots("CLINE_DATA_DIR")
                .into_iter()
                .map(|p| p.join("sessions"))
                .collect(),
            env_roots("CLINE_DIR")
                .into_iter()
                .map(|p| p.join("data").join("sessions"))
                .collect(),
            vec![home().join(".cline").join("data").join("sessions")],
        ];
        for lvl in levels {
            let hit: Vec<PathBuf> = lvl.into_iter().filter(|p| p.is_dir()).collect();
            if !hit.is_empty() {
                return hit;
            }
        }
        Vec::new()
    }

    fn prepare_scan(&self, roots: &[PathBuf]) -> Option<Box<dyn PreparedSourceScan>> {
        Some(Box::new(ClineInputs::discover(roots, None)))
    }

    fn scan_roots(
        &self,
        roots: &[PathBuf],
        max_files: Option<usize>,
    ) -> (Vec<UsageRecord>, ScanStats) {
        ClineInputs::discover(roots, max_files).scan()
    }
}

/// The cache and parser share one shallow discovery manifest. An incomplete
/// directory listing must never become a successful replacement of saved usage.
struct ClineInputs {
    files: Vec<PathBuf>,
    per_root: Vec<(String, usize)>,
    fingerprint: SourceFingerprint,
    stats: ScanStats,
}

#[cfg(feature = "regression-hooks")]
thread_local! {
    static ENTRY_ERROR: std::cell::RefCell<Option<PathBuf>> = const { std::cell::RefCell::new(None) };
    static TYPE_ERROR: std::cell::RefCell<Option<PathBuf>> = const { std::cell::RefCell::new(None) };
}

#[cfg(feature = "regression-hooks")]
#[doc(hidden)]
pub fn __fail_directory_entry_for_tests(path: PathBuf) {
    ENTRY_ERROR.with(|slot| *slot.borrow_mut() = Some(path));
}

#[cfg(feature = "regression-hooks")]
#[doc(hidden)]
pub fn __fail_directory_type_for_tests(path: PathBuf) {
    TYPE_ERROR.with(|slot| *slot.borrow_mut() = Some(path));
}

impl ClineInputs {
    fn failure(&mut self, path: &Path, error: impl std::fmt::Display) {
        self.stats.failed = true;
        self.stats
            .notes
            .push(format!("{} 用量输入读取失败: {error}", path.display()));
    }

    fn entries(&mut self, path: &Path) -> Vec<std::fs::DirEntry> {
        let mut entries = Vec::new();
        match std::fs::read_dir(path) {
            Ok(reader) => {
                #[cfg(feature = "regression-hooks")]
                let reader = {
                    let fault = ENTRY_ERROR.with(|slot| {
                        if slot.borrow().as_deref() == Some(path) {
                            slot.borrow_mut().take();
                            true
                        } else {
                            false
                        }
                    });
                    reader.chain(
                        fault.then(|| Err(std::io::Error::other("injected directory entry error"))),
                    )
                };
                for entry in reader {
                    match entry {
                        Ok(entry) => entries.push(entry),
                        Err(error) => self.failure(path, error.kind()),
                    }
                }
            }
            Err(error) => self.failure(path, error.kind()),
        }
        entries.sort_by_key(|entry| entry.path());
        entries
    }

    fn metadata(&mut self, path: &Path) -> Option<std::fs::Metadata> {
        #[cfg(feature = "regression-hooks")]
        let fault = TYPE_ERROR.with(|slot| {
            if slot.borrow().as_deref() == Some(path) {
                slot.borrow_mut().take();
                true
            } else {
                false
            }
        });
        #[cfg(feature = "regression-hooks")]
        if fault {
            self.failure(path, "injected directory type error");
            return None;
        }
        match std::fs::metadata(path) {
            Ok(metadata) => Some(metadata),
            Err(error) => {
                self.failure(path, error.kind());
                None
            }
        }
    }

    fn remember_file(&mut self, path: &Path) {
        match file_stamp(path) {
            Some(stamp) => self.fingerprint.push((path.to_path_buf(), stamp)),
            None => self.failure(path, "无法读取文件状态"),
        }
    }

    fn discover(roots: &[PathBuf], max_files: Option<usize>) -> Self {
        let mut out = Self {
            files: Vec::new(),
            per_root: Vec::new(),
            fingerprint: Vec::new(),
            stats: ScanStats::default(),
        };
        if roots.is_empty() {
            out.stats.notes.push(
                "未发现 ~/.cline/data/sessions 或 CLINE_SESSION_DATA_DIR/CLINE_DATA_DIR/CLINE_DIR"
                    .into(),
            );
        }
        for root in roots {
            let mut files = Vec::new();
            // A genuinely absent optional root remains a complete empty input.
            // A dangling link, wrong type or other I/O error is not absence.
            match std::fs::symlink_metadata(root) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => {
                    out.failure(root, error.kind());
                    continue;
                }
                Ok(_) => {}
            }
            match out.metadata(root) {
                Some(metadata) if metadata.is_dir() => {}
                Some(_) => {
                    out.failure(root, "预期会话目录");
                    continue;
                }
                None => continue,
            }
            out.fingerprint
                .push((root.clone(), FileStamp::membership()));
            for entry in out.entries(root) {
                let dir = entry.path();
                match out.metadata(&dir) {
                    Some(metadata) if metadata.is_dir() => {}
                    Some(_) => continue,
                    None => continue,
                }
                out.fingerprint.push((dir.clone(), FileStamp::membership()));
                for entry in out.entries(&dir) {
                    let path = entry.path();
                    let name = path.file_name().and_then(|name| name.to_str());
                    let is_messages = name.is_some_and(|name| name.ends_with(".messages.json"));
                    let is_manifest = dir
                        .file_name()
                        .and_then(|name| name.to_str())
                        .is_some_and(|session| name == Some(format!("{session}.json").as_str()));
                    if !is_messages && !is_manifest {
                        continue;
                    }
                    match out.metadata(&path) {
                        Some(metadata) if metadata.is_file() => {}
                        Some(_) => {
                            out.failure(&path, "预期会话文件");
                            continue;
                        }
                        None => continue,
                    }
                    out.remember_file(&path);
                    if is_messages {
                        files.push(path);
                    }
                }
            }
            files.sort();
            out.per_root.push((root.display().to_string(), files.len()));
            out.files.extend(files);
            if max_files.is_some_and(|limit| out.files.len() >= limit) {
                out.files.truncate(max_files.unwrap());
                break;
            }
        }
        out.stats.files_seen = out.files.len();
        out
    }
}

impl PreparedSourceScan for ClineInputs {
    fn fingerprint(&self) -> Option<SourceFingerprint> {
        (!self.stats.failed).then(|| self.fingerprint.clone())
    }

    fn scan(&self) -> (Vec<UsageRecord>, ScanStats) {
        let mut stats = self.stats.clone();
        let files = &self.files;
        let mut records: Vec<UsageRecord> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new(); // 幂等入队,同键不入(00-总览 §5-1)
        let mut native_sessions: HashSet<String> = HashSet::new();
        let mut models: BTreeSet<String> = BTreeSet::new();
        let mut import_files = 0u64; // 导入镜像文件(≡ 会话,导入无子代理文件,§3b)
        let mut version_skipped = 0u64;
        let mut agent_files = 0u64;
        let mut all_zero = 0u64;
        let mut excl_reclass = 0u64;
        let mut dup_dropped = 0u64;

        for path in files {
            let dir = path.parent().unwrap_or(Path::new(""));
            // 性能:先读小 manifest(<1KB)判导入,避免把 308MB 导入镜像整文件读入解析
            // (实测该源 RSS 664MB 的主因);manifest 缺失才落回读文件按 origin.mode 复核
            let pre_manifest = match read_manifest(dir) {
                Ok(manifest) => manifest,
                Err(error) => {
                    stats.failed = true;
                    stats.notes.push(format!(
                        "{} 会话信息读取失败: {}",
                        dir.display(),
                        error.kind()
                    ));
                    continue;
                }
            };
            if pre_manifest.is_import {
                import_files += 1;
                continue;
            }
            // 整文件 JSON 非 JSONL(§3);读取失败与 JSON 错误都阻止发布。
            let text = match std::fs::read_to_string(path) {
                Ok(text) => text,
                Err(error) => {
                    stats.failed = true;
                    stats.lines_bad_json += 1;
                    stats.notes.push(format!(
                        "{} 用量文件读取失败: {}",
                        path.display(),
                        error.kind()
                    ));
                    continue;
                }
            };
            let Ok(doc) = serde_json::from_str::<Value>(&text) else {
                stats.lines_bad_json += 1;
                continue;
            };
            // version != 1 整文件跳过 + 计数(§5-8;juejin cline.ts:300)
            if doc.get("version").and_then(|x| x.as_i64()) != Some(1) {
                version_skipped += 1;
                continue;
            }
            // —— 导入镜像过滤(§5-1 + 跨源去重规范 §四)——
            // 快路径:messages.json 自带 origin.mode(本机与 manifest 1218/1218 一致);
            // 主会话以 manifest 为正典复核;子代理 mode=="subagent" 即 native(§2)
            let own_mode = doc
                .pointer("/origin/mode")
                .and_then(|x| x.as_str())
                .unwrap_or("");
            let mf = pre_manifest;
            if mf.invalid_json && !matches!(own_mode, "native" | "subagent" | "import") {
                stats.failed = true;
                stats.notes.push(format!(
                    "{} 会话信息 JSON 不完整，无法确认导入归属",
                    dir.display()
                ));
                continue;
            }
            let is_import = match own_mode {
                "import" => true,
                "subagent" => false,
                _ => mf.is_import,
            };
            if is_import {
                import_files += 1;
                continue;
            }
            let Some(msgs) = doc.get("messages").and_then(|x| x.as_array()) else {
                stats.lines_unknown_type += 1; // 顶层缺 messages[]:形状未知,计数上报
                continue;
            };
            stats.files_parsed += 1;
            stats.lines_total += msgs.len() as u64;
            // 会话归属(§2):文件 sessionId(子代理为 parent__agent 复合 id,天然命名空间)
            // 作去重键;子代理 origin.parentThreadId 归主会话、origin.subagent 归 agent 字段
            let file_sid = doc
                .get("sessionId")
                .and_then(|x| x.as_str())
                .unwrap_or_default()
                .to_string();
            let origin = doc.get("origin");
            let session_id = origin
                .and_then(|o| o.get("parentThreadId"))
                .and_then(|x| x.as_str())
                .unwrap_or(&file_sid)
                .to_string();
            let agent = origin
                .and_then(|o| o.get("subagent"))
                .and_then(|x| x.as_str())
                .map(str::to_string);
            if own_mode == "subagent" {
                agent_files += 1;
            } else {
                native_sessions.insert(session_id.clone());
            }

            let mut invalid_model_reported = false;
            for (idx, m) in msgs.iter().enumerate() {
                // 已知角色仅 user/assistant(§3 本机全集);其余计未知类型
                match m.get("role").and_then(|x| x.as_str()) {
                    Some("user") | Some("assistant") => {}
                    _ => stats.lines_unknown_type += 1,
                }
                // metrics 只挂 assistant 消息,带 metrics 即一条 request_delta(§3/§5-8)
                let Some(mt) = m.get("metrics").filter(|x| !x.is_null()) else {
                    continue;
                };
                let g = |k: &str| mt.get(k).and_then(|x| x.as_u64()).unwrap_or(0);
                let (i_tok, cr, cw, o_tok, rs) = (
                    g("inputTokens"), // inclusive:含 cacheRead+cacheWrite(§4)
                    g("cacheReadTokens"),
                    g("cacheWriteTokens"),
                    g("outputTokens"),        // 含 reasoning(§4)
                    g("reasoningTokenCount"), // 本机无键,防御读取(§3)
                );
                // 服务端上报 USD 直入,缺 → None 不估算(cline-free 恒 0 属正常,§4)
                let cost = mt.get("cost").and_then(|x| x.as_f64());
                // 全零条目跳过 + 计数(§5-8/00-总览 §5-5;成本非 0 仍入账)
                if i_tok
                    .saturating_add(cr)
                    .saturating_add(cw)
                    .saturating_add(o_tok)
                    .saturating_add(rs)
                    == 0
                    && cost.is_none_or(|c| c == 0.0)
                {
                    all_zero += 1;
                    continue;
                }
                // inclusive 减除(§4);cr>i 与 inclusive 矛盾 → 该条改判 exclusive 直入(§8 形状兜底)
                let input_uncached = if cr > i_tok {
                    excl_reclass += 1;
                    i_tok
                } else {
                    i_tok.saturating_sub(cr).saturating_sub(cw)
                };
                let output = o_tok.saturating_sub(rs); // reasoning 为 output 子集(§4)
                                                       // 模型回退链(§5-2):per-message modelInfo.id → manifest.model → provider:xxx
                if mf.invalid_json && m.pointer("/modelInfo/id").and_then(Value::as_str).is_none() {
                    stats.failed = true;
                    if !invalid_model_reported {
                        stats.notes.push(format!(
                            "{} 会话信息 JSON 不完整，无法读取模型回退信息",
                            dir.display()
                        ));
                        invalid_model_reported = true;
                    }
                    continue;
                }
                let model = m
                    .pointer("/modelInfo/id")
                    .and_then(|x| x.as_str())
                    .map(str::to_string)
                    .unwrap_or_else(|| {
                        if !mf.model.is_empty() {
                            mf.model.clone()
                        } else if !mf.provider.is_empty() {
                            format!("provider:{}", mf.provider)
                        } else {
                            "unknown".into()
                        }
                    });
                // ts 已是 epoch 毫秒数字(§5-7,<1e12 ×1000 纪律不适用)
                let ts_ms = m.get("ts").and_then(|x| x.as_i64()).unwrap_or(0);
                // 去重键(§5-6):cline:{sessionId}:{messageId};id 缺失用 ts:index 兜底
                let mid = m.get("id").and_then(|x| x.as_str()).unwrap_or_default();
                let key = if mid.is_empty() {
                    format!("cline:{}:{}:{}", file_sid, ts_ms, idx)
                } else {
                    format!("cline:{}:{}", file_sid, mid)
                };
                if !seen.insert(key.clone()) {
                    dup_dropped += 1;
                    continue;
                }
                models.insert(model.clone());
                records.push(UsageRecord {
                    source: "cline",
                    session_id: session_id.clone(),
                    agent: agent.clone(),
                    model,
                    ts_ms,
                    caliber: Caliber::RequestDelta,
                    request_count: 1,
                    input_uncached,
                    cache_read: cr,
                    cache_write: cw,
                    cache_write_1h: 0,
                    output,
                    reasoning_output: rs,
                    cost_usd: cost,
                    dedup_key: key,
                });
            }
        }

        for (root, n) in &self.per_root {
            stats
                .notes
                .push(format!("根 {}:{n} 个 *.messages.json", root));
        }
        if import_files > 0 {
            stats.notes.push(format!(
                "导入镜像会话跳过 {import_files} 个(mode==import,与 ~/.codex、~/.claude 原文件重复,防双计)"
            ));
        }
        if version_skipped > 0 {
            stats.notes.push(format!(
                "version!=1 整文件跳过 {version_skipped} 个(未知布局)"
            ));
        }
        if all_zero > 0 {
            stats
                .notes
                .push(format!("全零 metrics 跳过 {all_zero} 条(无用量)"));
        }
        if excl_reclass > 0 {
            stats.notes.push(format!(
                "cacheRead>input 改判 exclusive {excl_reclass} 条(形状兜底,§8)"
            ));
        }
        if dup_dropped > 0 {
            stats
                .notes
                .push(format!("去重键命中丢弃 {dup_dropped} 条(§5-6)"));
        }
        stats.notes.push(format!(
            "native 主会话 {} · 子代理文件 {agent_files} · 模型 {}",
            native_sessions.len(),
            models.iter().cloned().collect::<Vec<_>>().join(", ")
        ));
        (records, stats)
    }
}
