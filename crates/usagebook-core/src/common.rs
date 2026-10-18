//! 跨源公共工具:环境变量多根解析、递归发现、时间解析、累计快照差分。

use serde_json::Value;
use std::path::{Path, PathBuf};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

// Only serve's snapshot scans opt in. One-shot CLI/fixture scans remain uncached.
thread_local! { static INCREMENTAL: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

pub fn with_incremental_cache<T>(f: impl FnOnce() -> T) -> T {
    struct Reset(bool);
    impl Drop for Reset {
        fn drop(&mut self) { INCREMENTAL.with(|v| v.set(self.0)); }
    }
    let _reset = Reset(INCREMENTAL.with(|v| v.replace(true)));
    f()
}

#[derive(Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FileStamp {
    len: u64,
    modified: std::time::SystemTime,
    #[serde(default)]
    identity: Option<String>,
}

impl FileStamp {
    pub fn size(&self) -> u64 {
        self.len
    }

    pub fn mtime_ns(&self) -> i64 {
        self.modified
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos().min(i64::MAX as u128) as i64)
            .unwrap_or(0)
    }

    /// The first two components identify the object; the remainder records its version.
    pub fn identity_key(&self) -> String {
        self.identity.clone().unwrap_or_default()
    }
}

pub fn file_stamp(path: &Path) -> Option<FileStamp> {
    let m = std::fs::metadata(path).ok()?;
    #[cfg(windows)]
    let file = std::fs::File::open(path).ok();
    metadata_stamp(&m, {
        #[cfg(windows)] { file.as_ref() }
        #[cfg(not(windows))] { None }
    })
}

fn metadata_stamp(m: &std::fs::Metadata, _file: Option<&std::fs::File>) -> Option<FileStamp> {
    #[cfg(unix)]
    use std::os::unix::fs::MetadataExt;
    #[cfg(unix)]
    let identity = Some(format!("{}/{}/{}/{}", m.dev(), m.ino(), m.ctime(), m.ctime_nsec()));
    #[cfg(windows)]
    let identity = _file.and_then(|file| {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Storage::FileSystem::{GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION};
        let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
        if unsafe { GetFileInformationByHandle(file.as_raw_handle(), &mut info) } == 0 { return None; }
        let index = ((info.nFileIndexHigh as u64) << 32) | info.nFileIndexLow as u64;
        let created = ((info.ftCreationTime.dwHighDateTime as u64) << 32) | info.ftCreationTime.dwLowDateTime as u64;
        Some(format!("{}/{}/{}", info.dwVolumeSerialNumber, index, created))
    });
    #[cfg(not(any(unix, windows)))]
    let identity = None;
    Some(FileStamp {
        len: m.len(), modified: m.modified().ok()?,
        identity,
    })
}

fn handle_stamp(file: &std::fs::File) -> Option<FileStamp> {
    metadata_stamp(&file.metadata().ok()?, Some(file))
}

/// Read provenance is transient: no handles or raw source bytes enter the cache.
pub struct ReadSnapshot {
    file: std::fs::File,
    stamp: FileStamp,
    committed_seal: i64,
}

impl ReadSnapshot {
    fn stable(&self, path: &Path, expected: &FileStamp) -> bool {
        self.stamp.identity.is_some() && self.stamp == *expected
            && handle_stamp(&self.file).as_ref() == Some(expected)
            && file_stamp(path).as_ref() == Some(expected)
    }
}

/// Cached values contain only allowlisted usage metadata, never prompt/tool text.
/// Replaying these events preserves each adapter's global dedup and settlement order.
#[derive(Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct UsageLines {
    pub events: Vec<(u64, Value)>,
    pub total: u64,
    pub bad: u64,
    pub bytes: u64,
    pub notes: Vec<String>,
    pub readable: bool,
    /// 已提交边界(最后一个以 `\n` 终结行; v3.1 载荷携带):
    /// committed_bytes=绝对字节位,committed_lines=已提交前缀非空行计数。
    /// 兼容机制=字段级 default(L2 为新建,现存磁盘无旧版载荷)。
    #[serde(default)]
    pub committed_bytes: u64,
    #[serde(default)]
    pub committed_lines: u64,
    /// 已提交前缀坏行计数(持久载荷截断用;不序列化——镜像行为由 bad 与计数规则保证)。
    #[serde(skip, default)]
    pub committed_bad: u64,
    #[serde(skip, default)]
    pub read_snapshot: Option<Arc<ReadSnapshot>>,
}

/// 本文件本轮的缓存结果;适配器调用点把它交给 UsageLines::account 统一记账。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheOutcome {
    /// 无 L1/L2 命中的全量读(含 off/Disabled 直读;§8:计 rewrote)。
    FreshLoad,
    /// L1a 或 L2 Unchanged 命中,零源盘读取。
    Reused,
    /// L2 AppendFrom 尾读(含尾读 Err 后的降级全量;§8:不计 rewrote/replayed)。
    Appended,
    /// Rewound 全量重读替换(§8:rewrote 与 replayed 各 +1)。
    Replay(&'static str),
}

/// 同键再见的"补全窗":model 相同且 ts 差在此窗内 → 视为同请求的多次记录
/// (流式逐块写/成对双发),不算键碰撞。
pub const KEY_CLASH_TS_WINDOW_MS: u64 = 60_000;

/// 键碰撞观察器(源内去重可见性):同键再次出现时判"同请求补全"还是"键碰撞"。
/// **只观察不裁决**——各适配器既有的替换(大者胜)/丢弃(先到先得)策略不变;
/// 碰撞经 ScanStats.key_clash 上报,禁止静默吞数据。
/// 判据(三态):
/// - 同 model 且 ts 在补全窗内 → 同请求多次记录(流式逐块/成对双发),非碰撞;
/// - 完整五桶全同且 ts 超窗 → 值全同的远距重投(重放/恢复再发),
///   键内嵌值的源(codex/dsh)命中即此类,去重语义正确,非碰撞;
/// - 其余(同键不同 model,或超窗且值不同)→ 键碰撞。
#[derive(Default)]
pub struct DedupTracker {
    seen: HashMap<String, (i64, String, [u64; 5])>,
}

impl DedupTracker {
    /// 记录一次键出现;返回 true = 判定键碰撞(不同请求撞同键)。
    pub fn track(&mut self, key: &str, ts_ms: i64, model: &str, value_sig: [u64; 5]) -> bool {
        match self.seen.get(key) {
            Some((ts, m, sig)) => {
                !(m == model && (ts.abs_diff(ts_ms) <= KEY_CLASH_TS_WINDOW_MS || *sig == value_sig))
            }
            None => {
                self.seen.insert(key.to_string(), (ts_ms, model.to_string(), value_sig));
                false
            }
        }
    }
}

/// 可寻址 JSONL 源的解析闭包对(L2 尾部读需要; FileParser)。
pub struct FileParser {
    pub project: fn(&Value) -> Option<Value>,
    pub keep: fn(&str) -> bool,
}

impl UsageLines {
    /// 统一记账 归属口径;原 bool 通道删除):
    /// Fresh→rewrote;Replay→rewrote+replayed;Appended/Reused→零计数;
    /// bytes 只对发生源盘读取的结果累计(Appended=尾读字节,不叠加前缀)。
    pub fn account(&self, stats: &mut crate::model::ScanStats, outcome: CacheOutcome) {
        stats.failed |= !self.readable;
        stats.lines_total += self.total;
        stats.lines_bad_json += self.bad;
        stats.notes.extend(self.notes.iter().cloned());
        match outcome {
            CacheOutcome::Reused => { stats.files_reused += 1; }
            CacheOutcome::FreshLoad => {
                stats.files_read += 1;
                stats.bytes_read += self.bytes;
                stats.rewrote += 1;
            }
            CacheOutcome::Appended => {
                stats.files_read += 1;
                stats.bytes_read += self.bytes;
            }
            CacheOutcome::Replay(_) => {
                stats.files_read += 1;
                stats.bytes_read += self.bytes;
                stats.rewrote += 1;
                stats.replayed += 1;
            }
        }
    }

    pub fn push(&mut self, line: &str, project: fn(&Value) -> Option<Value>) {
        let line = line.trim();
        if line.is_empty() { return; }
        self.total += 1;
        match serde_json::from_str::<Value>(line) {
            Ok(value) => {
                if let Some(value) = project(&value) { self.events.push((self.total, value)); }
            }
            Err(_) => self.bad += 1,
        }
    }
}

struct UsageEntry { stamp: FileStamp, payload: Arc<[u8]>, decoded_len: usize, weight: usize, touched: u64 }
#[derive(Default)]
struct UsageCache { entries: HashMap<(&'static str, PathBuf), UsageEntry>, weight: usize, clock: u64 }
static USAGE_CACHE: OnceLock<Mutex<UsageCache>> = OnceLock::new();
// Only compressed allowlisted metadata is retained. JSON trees are short-lived,
// avoiding cache thrash from allocator/node overhead on large session histories.
const USAGE_CACHE_BUDGET: usize = 96 * 1024 * 1024;
const MAX_CACHED_USAGE_BYTES: usize = 64 * 1024 * 1024;

pub fn prune_usage_cache(source: &'static str, files: &[PathBuf]) {
    if !INCREMENTAL.with(|v| v.get()) { return; }
    let live: std::collections::HashSet<&PathBuf> = files.iter().collect();
    let mut cache = USAGE_CACHE.get_or_init(Default::default).lock().unwrap_or_else(|e| e.into_inner());
    cache.entries.retain(|(s,p),_| *s != source || live.contains(p));
    cache.weight = cache.entries.values().map(|v| v.weight).sum();
    // L2 死行 GC v3.1-C N8:实现在本函数内部,8 个调用点自然全覆盖)。
    // 仅在本轮文件清单非空(发现后)时执行;扫描前的 &[] 清 L1a 调用不触 L2。
    if !files.is_empty() {
        crate::store::gc_source(source, files);
    }
}

/// L1a 回填公共路径(压缩载荷与预算纪律与既有实现一致;payload 可来自 L2 行,零重压缩)。
fn backfill_l1a(
    key: (&'static str, PathBuf),
    stamp: FileStamp,
    payload: Arc<[u8]>,
    decoded_len: usize,
    path: &Path,
) {
    let weight = payload.len() + path.as_os_str().len() * 2 + 256;
    if weight > USAGE_CACHE_BUDGET { return; }
    let mut cache = USAGE_CACHE.get_or_init(Default::default).lock().unwrap_or_else(|e| e.into_inner());
    if let Some(old) = cache.entries.remove(&key) { cache.weight -= old.weight; }
    while cache.weight + weight > USAGE_CACHE_BUDGET {
        let victim = cache.entries.iter().min_by_key(|(_,v)| v.touched).map(|(k,_)| k.clone());
        let Some(victim) = victim else { break };
        if let Some(old) = cache.entries.remove(&victim) { cache.weight -= old.weight; }
    }
    cache.clock += 1;
    let touched = cache.clock;
    cache.entries.insert(key, UsageEntry { stamp, payload, decoded_len, weight, touched });
    cache.weight += weight;
}

fn l1a_lookup(key: &(&'static str, PathBuf), stamp: &FileStamp) -> Option<(Arc<[u8]>, usize)> {
    let mut cache = USAGE_CACHE.get_or_init(Default::default).lock().unwrap_or_else(|e| e.into_inner());
    cache.clock += 1;
    let tick = cache.clock;
    let hit = cache.entries.get_mut(key).filter(|entry| &entry.stamp == stamp).map(|entry| {
        entry.touched = tick;
        (entry.payload.clone(), entry.decoded_len)
    });
    if hit.is_none() {
        if let Some(old) = cache.entries.remove(key) { cache.weight -= old.weight; }
    }
    hit
}

fn l1a_evict(key: &(&'static str, PathBuf)) {
    let mut cache = USAGE_CACHE.get_or_init(Default::default).lock().unwrap_or_else(|e| e.into_inner());
    if let Some(old) = cache.entries.remove(key) { cache.weight -= old.weight; }
}

fn compress_l1a(lines: &UsageLines, _path: &Path) -> Option<(Arc<[u8]>, usize)> {
    let bytes = serde_json::to_vec(lines).ok().filter(|b| b.len() <= MAX_CACHED_USAGE_BYTES)?;
    let payload: Arc<[u8]> = zstd::bulk::compress(&bytes, 1).ok()?.into_boxed_slice().into();
    Some((payload, bytes.len()))
}

/// 已提交前缀的持久化载荷 `\n` 终结行的事件,边界=lines.committed_*)。
fn committed_prefix_payload(head: &UsageLines, tail: &TailRead) -> UsageLines {
    let committed_events = tail.committed_events;
    let mut out = UsageLines {
        events: head.events.clone(),
        total: tail.committed_lines,
        bad: head.bad + tail.committed_bad,
        bytes: head.bytes + (tail.committed_bytes - tail.from_offset),
        notes: head.notes.clone(),
        readable: head.readable,
        committed_bytes: tail.committed_bytes,
        committed_lines: tail.committed_lines,
        committed_bad: head.bad + tail.committed_bad,
        read_snapshot: tail.lines.read_snapshot.clone(),
    };
    out.events.extend_from_slice(&tail.lines.events[..committed_events]);
    out
}

/// 文件级缓存缝(L1a + L2; v3.1 状态机)。
///
/// 裁决顺序:L1a 命中 → Reused;L2 judge → Unchanged(回放,零源盘读)/
/// AppendFrom(尾读+单调追加)/Rewound(全量重读替换)/Fresh(全量读插入);
/// store Disabled/off/故障 → 直读(FreshLoad)。
/// 尾读 Err → 降级 load() 全量,旧 L2 行不动、不落库,返回 Appended(§6.1 v3.1-C B2)。
pub fn cached_usage_file(
    source: &'static str,
    path: &Path,
    parser: Option<FileParser>,
    load: impl FnOnce() -> UsageLines,
) -> (Arc<UsageLines>, CacheOutcome) {
    let mut load = Some(load);
    if !INCREMENTAL.with(|v| v.get()) {
        return (Arc::new(load.take().unwrap()()), CacheOutcome::FreshLoad);
    }
    let before = file_stamp(path);
    let key = (source, path.to_path_buf());
    let cached = if let Some(stamp) = &before {
        l1a_lookup(&key, stamp)
    } else { None };
    // Decode outside the lock: independent sources remain parallel.
    if let Some((payload, decoded_len)) = cached {
        if let Ok(bytes) = zstd::bulk::decompress(&payload, decoded_len) {
            if let Ok(lines) = serde_json::from_slice::<UsageLines>(&bytes) {
                return (Arc::new(lines), CacheOutcome::Reused);
            }
        }
    }

    // —— L2(跨进程持久层)——
    if let Some(stamp) = &before {
        if crate::store::store_ready() {
            if let Some(outcome) = l2_attempt(source, path, stamp, parser, &mut load, &key) {
                return outcome;
            }
        }
    }

    // 直读(无元数据/store 故障/off):现状路径。
    let data = Arc::new(load.take().unwrap()());
    // Never freeze a failed read or a file that changed while being read.
    if let Some(stamp) = before.filter(|stamp| data.readable && Some(stamp) == file_stamp(path).as_ref()) {
        if let Some((payload, decoded_len)) = compress_l1a(&data, path) {
            backfill_l1a(key, stamp, payload, decoded_len, path);
        }
    }
    (data, CacheOutcome::FreshLoad)
}

/// Decode failures rebuild the observed row; all writes compare its revision.
fn l2_attempt(
    source: &'static str,
    path: &Path,
    stamp: &FileStamp,
    parser: Option<FileParser>,
    load: &mut Option<impl FnOnce() -> UsageLines>,
    key: &(&'static str, PathBuf),
) -> Option<(Arc<UsageLines>, CacheOutcome)> {
    let observed = crate::store::observe_file(source, path)?;
    let row = observed.row.as_ref();
    let cursor = row.map(|r| &r.cursor);
    // When judge needs a seal, retain this exact handle and the verified window.
    let mut append_input = None;
    let mut verdict = if parser.is_some() {
        crate::store::judge(stamp.mtime_ns(), stamp.size(), Some(&stamp.identity_key()), cursor, |offset| {
            let mut file = std::fs::File::open(path)?;
            if handle_stamp(&file).as_ref() != Some(stamp) {
                return Err(std::io::Error::other("file changed before tail verification"));
            }
            let window = read_boundary_window(&mut file, offset)?;
            let seal = crate::store::tail_seal(&window);
            append_input = Some((file, window));
            Ok(seal)
        })
    } else {
        crate::store::judge_reload(stamp.mtime_ns(), stamp.size(), Some(&stamp.identity_key()), cursor)
    };
    let head = if matches!(verdict, crate::store::Verdict::Unchanged | crate::store::Verdict::AppendFrom { .. }) {
        row.and_then(|r| crate::store::decode_lines(&r.events, r.events_len))
    } else { None };
    if head.is_none() && matches!(verdict, crate::store::Verdict::Unchanged | crate::store::Verdict::AppendFrom { .. }) {
        verdict = crate::store::Verdict::Fresh;
    }
    match verdict {
        crate::store::Verdict::Unchanged => {
            let row = row?;
            if Some(stamp) == file_stamp(path).as_ref() {
                backfill_l1a(key.clone(), stamp.clone(), row.events.clone(), row.events_len, path);
            }
            Some((Arc::new(head?), CacheOutcome::Reused))
        }
        crate::store::Verdict::AppendFrom { offset } => {
            let parser = parser?;
            let head = head?;
            let (file, window) = append_input?;
            let tail = read_tail_from_handle(path, file, stamp.clone(), offset,
                row?.lines_total as u64, window, parser.project, parser.keep);
            match tail {
                Ok(tail) => {
                    let mut merged = head.clone();
                    merged.events.extend_from_slice(&tail.lines.events);
                    merged.total = tail.lines.total;
                    merged.bad = head.bad + tail.lines.bad;
                    merged.bytes = tail.lines.bytes;
                    merged.readable = tail.lines.readable;
                    merged.notes.extend(tail.lines.notes.iter().cloned());
                    merged.committed_bytes = tail.committed_bytes;
                    merged.committed_lines = tail.committed_lines;
                    merged.read_snapshot = tail.lines.read_snapshot.clone();
                    let prefix = committed_prefix_payload(&head, &tail);
                    let result = persist_payload(source, path, stamp, &prefix, &observed.revision, true, true);
                    if result == crate::store::FileCommit::Conflict {
                        // Do not retry a stale head. A bounded full reread supplies this scan;
                        // the next scan observes the winning cache revision afresh.
                        l1a_evict(key);
                        let mut fresh = read_usage_jsonl_committed(path, parser.project, parser.keep);
                        fresh.bytes += merged.bytes;
                        fresh.notes.push("缓存版本已变化,本轮重新全读".into());
                        return Some((Arc::new(fresh), CacheOutcome::FreshLoad));
                    }
                    if merged.readable && snapshot_stable(&merged, path, stamp, true) {
                        if let Some((payload, len)) = compress_l1a(&merged, path)
                            .filter(|_| snapshot_stable(&merged, path, stamp, true)) {
                            backfill_l1a(key.clone(), stamp.clone(), payload, len, path);
                        }
                    }
                    Some((Arc::new(merged), CacheOutcome::Appended))
                }
                Err(err) => {
                    let mut data = load.take()?();
                    data.bytes += err.bytes;
                    data.notes.push("读取中断,本轮全读,下轮重新判定".into());
                    // Never freeze a failed tail attempt under the old observed stamp.
                    Some((Arc::new(data), CacheOutcome::Appended))
                }
            }
        }
        crate::store::Verdict::Fresh | crate::store::Verdict::Rewound(_) => {
            let mut data = load.take()?();
            let result = persist_full(source, path, stamp, &data, &observed.revision, parser.is_some());
            l1a_evict(key);
            if result == crate::store::FileCommit::Conflict {
                if let Some(parser) = parser {
                    let old_bytes = data.bytes;
                    data = read_usage_jsonl_committed(path, parser.project, parser.keep);
                    data.bytes += old_bytes;
                }
                data.notes.push("缓存版本已变化,下轮重新判定".into());
            } else if data.readable && snapshot_stable(&data, path, stamp, parser.is_some()) {
                if let Some((payload, len)) = compress_l1a(&data, path)
                    .filter(|_| snapshot_stable(&data, path, stamp, parser.is_some())) {
                    backfill_l1a(key.clone(), stamp.clone(), payload, len, path);
                }
            }
            let outcome = match verdict {
                crate::store::Verdict::Rewound(reason) => CacheOutcome::Replay(reason),
                _ => CacheOutcome::FreshLoad,
            };
            Some((Arc::new(data), outcome))
        }
    }
}

fn snapshot_stable(data: &UsageLines, path: &Path, stamp: &FileStamp, required: bool) -> bool {
    match &data.read_snapshot {
        Some(snapshot) => snapshot.stable(path, stamp),
        None => !required && file_stamp(path).as_ref() == Some(stamp),
    }
}

#[cfg(feature = "regression-hooks")]
thread_local! {
    static PERSIST_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = Default::default();
}

#[cfg(feature = "regression-hooks")]
#[doc(hidden)]
pub fn __set_persist_hook_for_tests(hook: impl FnOnce() + 'static) {
    PERSIST_HOOK.with(|slot| *slot.borrow_mut() = Some(Box::new(hook)));
}

fn persist_payload(source: &'static str, path: &Path, stamp: &FileStamp, data: &UsageLines,
    revision: &str, required: bool, append: bool) -> crate::store::FileCommit {
    use crate::store::FileCommit;
    if !data.readable { return FileCommit::Unavailable; }
    if !snapshot_stable(data, path, stamp, required) { return FileCommit::Conflict; }
    let Some((blob, len)) = crate::store::encode_lines(data) else { return FileCommit::Unavailable };
    if len > MAX_CACHED_USAGE_BYTES {
        crate::store::note_capped_file();
        return FileCommit::Unavailable;
    }
    #[cfg(feature = "regression-hooks")]
    {
        let hook = PERSIST_HOOK.with(|slot| slot.borrow_mut().take());
        if let Some(hook) = hook { hook(); }
    }
    // The seal was computed from consumed bytes, never by reopening the path here.
    if !snapshot_stable(data, path, stamp, required) { return FileCommit::Conflict; }
    let cursor = crate::store::FileCursor {
        mtime_ns: stamp.mtime_ns(), size: stamp.size(), byte_offset: data.committed_bytes,
        tail_seal: data.read_snapshot.as_ref().map_or(0, |s| s.committed_seal),
        identity: Some(stamp.identity_key()),
    };
    let write = crate::store::FileWrite { cursor: &cursor, events: &blob, events_len: len,
        lines_total: data.committed_lines as i64 };
    if append { crate::store::append_file(source, path, revision, write) }
    else { crate::store::replace_file(source, path, revision, write) }
}

fn persist_full(source: &'static str, path: &Path, stamp: &FileStamp, data: &UsageLines,
    revision: &str, required: bool) -> crate::store::FileCommit {
    let mut payload = data.clone();
    if data.committed_bytes < data.bytes && data.total > data.committed_lines
        && payload.events.last().is_some_and(|(ord, _)| *ord == data.total) {
        payload.events.pop();
    }
    payload.total = data.committed_lines;
    payload.bad = data.committed_bad;
    payload.bytes = data.committed_bytes;
    persist_payload(source, path, stamp, &payload, revision, required, false)
}

pub fn read_usage_jsonl(path: &Path, project: fn(&Value) -> Option<Value>, keep: fn(&str) -> bool) -> UsageLines {
    read_usage_jsonl_inner(path, project, keep, false)
}

pub fn read_usage_jsonl_committed(path: &Path, project: fn(&Value) -> Option<Value>, keep: fn(&str) -> bool) -> UsageLines {
    read_usage_jsonl_inner(path, project, keep, true)
}

pub struct TailRead {
    pub lines: UsageLines,
    pub from_offset: u64,
    pub committed_bytes: u64,
    pub committed_lines: u64,
    pub committed_bad: u64,
    pub committed_events: usize,
    pub pending_tail: bool,
}

pub struct TailError { pub error: std::io::Error, pub bytes: u64 }

fn read_boundary_window(file: &mut std::fs::File, end: u64) -> std::io::Result<Vec<u8>> {
    use std::io::{Read, Seek, SeekFrom};
    let len = end.min(crate::store::TAIL_SEAL_BYTES);
    file.seek(SeekFrom::Start(end - len))?;
    let mut window = vec![0; len as usize];
    file.read_exact(&mut window)?;
    Ok(window)
}

fn extend_window(window: &mut Vec<u8>, bytes: &[u8]) {
    let limit = crate::store::TAIL_SEAL_BYTES as usize;
    if bytes.len() >= limit {
        window.clear(); window.extend_from_slice(&bytes[bytes.len() - limit..]);
    } else {
        let excess = (window.len() + bytes.len()).saturating_sub(limit);
        window.drain(..excess);
        window.extend_from_slice(bytes);
    }
}

struct StreamRead { lines: UsageLines, seal: i64, committed_events: usize }

fn read_stream(reader: &mut impl std::io::BufRead, project: fn(&Value) -> Option<Value>,
    keep: fn(&str) -> bool, offset: u64, resume_lines: u64, mut window: Vec<u8>, track: bool) -> StreamRead {
    let mut out = UsageLines { readable: true, total: resume_lines,
        committed_bytes: offset, committed_lines: resume_lines, ..Default::default() };
    let mut committed_events = 0;
    let mut committed_window = window.clone();
    let mut buf = Vec::new();
    loop {
        buf.clear();
        match reader.read_until(b'\n', &mut buf) {
            Ok(0) => break,
            Ok(n) => {
                out.bytes += n as u64;
                let line = String::from_utf8_lossy(&buf);
                let line = line.trim_end_matches(['\n', '\r']);
                if keep(line) { out.push(line, project); }
                else if !line.trim().is_empty() { out.total += 1; }
                extend_window(&mut window, &buf);
                if buf.ends_with(b"\n") {
                    out.committed_bytes = offset + out.bytes;
                    out.committed_lines = out.total;
                    out.committed_bad = out.bad;
                    committed_events = out.events.len();
                    committed_window.clone_from(&window);
                }
            }
            Err(err) => {
                // read_until may have consumed a partial line before returning Err.
                // Retain only prior successful records and never retry permanent I/O errors.
                out.bytes += buf.len() as u64;
                out.bad += 1;
                out.readable = false;
                out.notes.push(format!("用量文件读取中断: {}", err.kind()));
                break;
            }
        }
    }
    if !track {
        out.committed_bytes = offset + out.bytes;
        out.committed_lines = out.total;
        out.committed_bad = out.bad;
        committed_events = out.events.len();
        committed_window = window;
    }
    StreamRead { lines: out, seal: crate::store::tail_seal(&committed_window), committed_events }
}

/// Streaming entry point for embedders and deterministic I/O fault injection.
#[doc(hidden)]
pub fn read_usage_jsonl_reader(reader: &mut impl std::io::BufRead,
    project: fn(&Value) -> Option<Value>, keep: fn(&str) -> bool) -> UsageLines {
    read_stream(reader, project, keep, 0, 0, Vec::new(), true).lines
}

fn read_usage_jsonl_inner(path: &Path, project: fn(&Value) -> Option<Value>, keep: fn(&str) -> bool,
    track: bool) -> UsageLines {
    use std::io::{BufReader, Read};
    let file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(e) => return UsageLines { bad: 1, notes: vec![format!("用量文件不可读: {}", e.kind())], ..Default::default() },
    };
    let Some(stamp) = handle_stamp(&file) else {
        return UsageLines { bad: 1, notes: vec!["无法读取文件元数据".into()], ..Default::default() };
    };
    let mut result = read_stream(&mut BufReader::new((&file).take(stamp.len)), project, keep, 0, 0, Vec::new(), track);
    let snapshot = Arc::new(ReadSnapshot { file, stamp: stamp.clone(), committed_seal: result.seal });
    if !snapshot.stable(path, &stamp) {
        result.lines.readable = false;
        result.lines.notes.push("读取期间文件发生变化,本轮结果不缓存".into());
    }
    result.lines.read_snapshot = Some(snapshot);
    result.lines
}

pub fn read_usage_jsonl_tail(path: &Path, from_offset: u64, resume_lines: u64,
    project: fn(&Value) -> Option<Value>, keep: fn(&str) -> bool) -> Result<TailRead, TailError> {
    let err = |error| TailError { error, bytes: 0 };
    let mut file = std::fs::File::open(path).map_err(err)?;
    let stamp = handle_stamp(&file).ok_or_else(|| err(std::io::Error::other("missing file metadata")))?;
    let window = read_boundary_window(&mut file, from_offset).map_err(err)?;
    read_tail_from_handle(path, file, stamp, from_offset, resume_lines, window, project, keep)
}

#[allow(clippy::too_many_arguments)]
fn read_tail_from_handle(path: &Path, file: std::fs::File, stamp: FileStamp, from_offset: u64,
    resume_lines: u64, window: Vec<u8>, project: fn(&Value) -> Option<Value>, keep: fn(&str) -> bool) -> Result<TailRead, TailError> {
    use std::io::{BufReader, Read};
    if from_offset > stamp.len {
        return Err(TailError { error: std::io::Error::other("file truncated"), bytes: 0 });
    }
    let mut result = read_stream(&mut BufReader::new((&file).take(stamp.len - from_offset)),
        project, keep, from_offset, resume_lines, window, true);
    let snapshot = Arc::new(ReadSnapshot { file, stamp: stamp.clone(), committed_seal: result.seal });
    if !result.lines.readable || !snapshot.stable(path, &stamp) {
        return Err(TailError { error: std::io::Error::other("incomplete or unstable tail read"), bytes: result.lines.bytes });
    }
    result.lines.read_snapshot = Some(snapshot);
    let lines = result.lines;
    Ok(TailRead { committed_bytes: lines.committed_bytes, committed_lines: lines.committed_lines,
        committed_bad: lines.committed_bad, committed_events: result.committed_events,
        pending_tail: lines.committed_bytes < from_offset + lines.bytes, lines, from_offset })
}

/// Copy selected JSON-pointer paths. Paths name scalar/token metadata only.
pub fn usage_projection(value: &Value, paths: &[&str]) -> Value {
    fn put(out: &mut Value, parts: &[&str], value: Value) {
        if parts.is_empty() { *out = value; return; }
        if !out.is_object() { *out = Value::Object(Default::default()); }
        let slot = out.as_object_mut().unwrap().entry(parts[0]).or_insert(Value::Null);
        put(slot, &parts[1..], value);
    }
    let mut out = Value::Object(Default::default());
    for path in paths {
        if let Some(v) = value.pointer(path) {
            // Even a malformed "model"/"id" field must not retain nested messages.
            // Legitimate token metadata is scalar and identifiers are short.
            if v.is_object() || v.is_array() || v.as_str().is_some_and(|s| s.len() > 4096) { continue; }
            put(&mut out, &path[1..].split('/').collect::<Vec<_>>(), v.clone());
        }
    }
    out
}

type SourceFingerprint = Vec<(PathBuf, FileStamp)>;
struct SourceEntry {
    fingerprint: SourceFingerprint,
    records: Vec<crate::model::UsageRecord>,
    stats: crate::model::ScanStats,
    weight: usize,
}
static SOURCE_CACHE: OnceLock<Mutex<HashMap<&'static str, SourceEntry>>> = OnceLock::new();
const SOURCE_CACHE_BUDGET: usize = 32 * 1024 * 1024;

/// Metadata only: includes SQLite databases AND adjacent WAL/SHM/journal files.
/// Any traversal error disables reuse, rather than turning inaccessible data into zero.
fn source_fingerprint(roots: &[PathBuf]) -> Option<SourceFingerprint> {
    fn visit(p: &Path, out: &mut SourceFingerprint, descend: bool) -> Option<()> {
        let md = std::fs::symlink_metadata(p).ok()?;
        out.push((p.to_path_buf(), file_stamp(p)?));
        if md.is_dir() || (descend && p.is_dir()) {
            let mut children = std::fs::read_dir(p).ok()?.collect::<Result<Vec<_>, _>>().ok()?;
            children.sort_by_key(|e| e.path());
            for e in children { visit(&e.path(), out, false)?; }
        }
        Some(())
    }
    let mut out = Vec::new();
    for root in roots {
        visit(root, &mut out, true)?;
        // Goose and other adapters can return a database FILE as their root.
        // A WAL commit need not change the main database's mtime or length.
        if root.is_file() {
            for suffix in ["-wal", "-shm", "-journal"] {
                let mut name = root.as_os_str().to_os_string();
                name.push(suffix);
                let sidecar = PathBuf::from(name);
                match std::fs::symlink_metadata(&sidecar) {
                    Ok(_) => visit(&sidecar, &mut out, false)?,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {},
                    Err(_) => return None,
                }
            }
        }
    }
    Some(out)
}

/// SOURCE_CACHE 条目权重(驻留内存预算口径;L2 命中回填与扫描路径共用)。
fn source_cache_weight(
    records: &[crate::model::UsageRecord],
    stats: &crate::model::ScanStats,
    fingerprint: &Option<SourceFingerprint>,
) -> usize {
    std::mem::size_of_val(records)
        + records.iter().map(|r| r.session_id.capacity() + r.model.capacity() + r.dedup_key.capacity()
            + r.agent.as_ref().map_or(0, String::capacity)).sum::<usize>()
        + stats.notes.capacity() * std::mem::size_of::<String>()
        + stats.notes.iter().map(String::capacity).sum::<usize>()
        + fingerprint.as_ref().map_or(0, |f| f.capacity() * std::mem::size_of::<(PathBuf, FileStamp)>()
            + f.iter().map(|(p,_)| p.as_os_str().len() * 2).sum::<usize>()) + 256
}

/// Snapshot-only entry point; adapters' scan()/scan_roots() retain one-shot behavior.
/// 非 JSONL 大源多一层 L2(source_state 指纹门;:L1b → L2 → 全量扫描,
/// 成功且非空的结果整行持久化(空记录成功轮删除旧行,镜像 L1b 不冻结语义)。
pub fn scan_source_cached(
    source: &dyn crate::model::UsageSource, roots: &[PathBuf],
) -> (Vec<crate::model::UsageRecord>, crate::model::ScanStats) {
    if matches!(source.id(), "claude" | "codex" | "kimi" | "dsh") {
        return with_incremental_cache(|| source.scan_roots(roots, None));
    }
    let before = source_fingerprint(roots);
    if let Some(fingerprint) = &before {
        let cache = SOURCE_CACHE.get_or_init(Default::default).lock().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = cache.get(source.id()) {
            if &entry.fingerprint == fingerprint {
                let mut stats = entry.stats.clone();
                stats.files_reused = stats.files_parsed;
                stats.files_read = 0;
                stats.bytes_read = 0;
                stats.rewrote = 0;
                stats.replayed = 0;
                return (entry.records.clone(), stats);
            }
        }
    }
    // —— L2:源级指纹门(跨进程)——
    if let (Some(fingerprint), true) = (&before, crate::store::store_ready()) {
        if let Some(row) = crate::store::source_row(source.id()) {
            let stored = crate::store::decode_fingerprint(&row.fingerprint);
            if let Some(stored) = stored {
                if stored == *fingerprint {
                    if let Some((records, mut stats)) = crate::store::decode_records(&row.records) {
                        stats.files_reused = stats.files_parsed;
                        stats.files_read = 0;
                        stats.bytes_read = 0;
                        stats.rewrote = 0;
                        stats.replayed = 0;
                        // 回填 L1b L1b"):进程内下轮免 SQLite 读行+解压。
                        let weight = source_cache_weight(&records, &stats, &before);
                        if weight <= SOURCE_CACHE_BUDGET {
                            let mut cache = SOURCE_CACHE.get_or_init(Default::default)
                                .lock().unwrap_or_else(|e| e.into_inner());
                            let occupied: usize = cache.values().map(|e| e.weight).sum();
                            if occupied + weight <= SOURCE_CACHE_BUDGET {
                                cache.insert(source.id(), SourceEntry {
                                    fingerprint: fingerprint.clone(),
                                    records: records.clone(),
                                    stats: stats.clone(),
                                    weight,
                                });
                            }
                        }
                        return (records, stats);
                    }
                }
            }
        }
    }
    // Remove stale state even if re-reading subsequently fails.
    SOURCE_CACHE.get_or_init(Default::default).lock().unwrap_or_else(|e| e.into_inner()).remove(source.id());
    let (records, mut stats) = source.scan_roots(roots, None);
    stats.files_read = stats.files_parsed;
    let weight = source_cache_weight(&records, &stats, &before);
    // 冻结契约:只认显式标志;notes 文本不参与机器判定(措辞润色不得改变缓存语义)。
    let failed = stats.failed || stats.lines_bad_json > 0;
    let stable = before.as_ref().is_some_and(|fingerprint|
        !records.is_empty() && !failed && Some(fingerprint) == source_fingerprint(roots).as_ref());
    if let Some(fingerprint) = before.filter(|_| stable) {
        let mut cache = SOURCE_CACHE.get_or_init(Default::default).lock().unwrap_or_else(|e| e.into_inner());
        let occupied: usize = cache.values().map(|e| e.weight).sum();
        // Prefer a bounded cache to retaining an unbounded copy of every source's history.
        if occupied + weight <= SOURCE_CACHE_BUDGET {
            cache.insert(source.id(), SourceEntry { fingerprint: fingerprint.clone(), records: records.clone(), stats: stats.clone(), weight });
        }
        // L2 持久化:成功且非空(空记录成功轮删除旧行,。
        if crate::store::store_ready() {
            if let Some(fp_blob) = crate::store::encode_fingerprint(&fingerprint) {
                if let Some(rec_blob) = crate::store::encode_records(&records, &stats) {
                    crate::store::replace_source(source.id(), &fp_blob, &rec_blob, rec_blob.len());
                }
            }
        }
    } else if !failed && records.is_empty() {
        crate::store::delete_source(source.id());
    }
    (records, stats)
}

/// 环境变量可含逗号分隔多根(档案 00-总览 §三);返回存在的根。
pub fn env_roots(var: &str) -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Ok(v) = std::env::var(var) {
        for seg in v.split(',') {
            let p = PathBuf::from(seg.trim());
            if !p.as_os_str().is_empty() && p.is_dir() {
                roots.push(p);
            }
        }
    }
    roots
}

/// usagebook 自己的数据目录(USAGEBOOK_DATA_DIR 覆盖;state.db/快照缓存/日志同源)。
pub fn data_dir() -> PathBuf {
    std::env::var_os("USAGEBOOK_DATA_DIR")
        .filter(|p| !p.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| home().join(".usagebook"))
}

pub fn home() -> PathBuf {
    // 原生 Windows(cmd/PowerShell)无 HOME,回退 USERPROFILE;再缺失才退 "/"
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/"))
}

/// 递归收集 *.jsonl(排序保证确定性;不假设目录深度)。
pub fn walk_jsonl(root: &Path, max_files: Option<usize>) -> Vec<PathBuf> {
    walk_with(root, max_files, &|p| p.extension().is_some_and(|x| x == "jsonl"))
}

/// 递归收集满足谓词的文件(排序保证确定性;不假设目录深度)。
/// community 源发现规则形态各异(.json/.jsonl 双扩展、固定文件名),统一走此入口。
pub fn walk_with(root: &Path, max_files: Option<usize>, keep: &dyn Fn(&Path) -> bool) -> Vec<PathBuf> {
    let mut out = Vec::new();
    walk_inner(root, max_files, keep, &mut out);
    out.sort();
    out
}

fn walk_inner(root: &Path, max_files: Option<usize>, keep: &dyn Fn(&Path) -> bool, out: &mut Vec<PathBuf>) {
    if let Some(m) = max_files {
        if out.len() >= m {
            return;
        }
    }
    let Ok(rd) = std::fs::read_dir(root) else { return };
    for e in rd.flatten() {
        let p = e.path();
        let Ok(ft) = e.file_type() else { continue };
        if ft.is_dir() {
            walk_inner(&p, max_files, keep, out);
        } else if keep(&p) {
            out.push(p);
            if let Some(m) = max_files {
                if out.len() >= m {
                    return;
                }
            }
        }
    }
}

/// 宽松数值提取:JSON 数字(u64 或 f64 截断)或数字字符串 → u64。
/// 新式格式(OTel/网关)数值常在数字与字符串双形态间漂移,统一在此容错。
pub fn json_u64(v: Option<&Value>) -> u64 {
    match v {
        Some(Value::Number(n)) => n.as_u64().unwrap_or_else(|| n.as_f64().map(|f| f.max(0.0) as u64).unwrap_or(0)),
        Some(Value::String(s)) => s.trim().parse().unwrap_or(0),
        _ => 0,
    }
}

/// 秒/毫秒分界(2026-10-17 统一):此值以下视为秒。此前 secs_or_ms 用 1e12 而
/// flex_to_ms 数值档用 1e11,同一概念两套判据、[1e11,1e12) 区间结论相反——统一为 1e11
/// (2013 年以前的秒与 5138 年以后的毫秒均不出现,分界取谁都正确,取严格侧)。
pub const TS_MS_FLOOR: i64 = 100_000_000_000;

/// 只读打开 SQLite 并统一施加 busy_timeout/query_only 纪律(2026-10-17 收编):
/// 此前 10 处适配器样板复制且失败语义漂移(部分吞掉/部分 notes+failed)。
/// 打开或 busy_timeout 失败均返回 Err,由调用方按各自渠道语义处置;
/// query_only 失败静默降级(个别平台不支持,只读 flags 已是主防线)。
pub fn open_sqlite_readonly(db: &std::path::Path) -> rusqlite::Result<rusqlite::Connection> {
    let conn = rusqlite::Connection::open_with_flags(
        db,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;
    conn.busy_timeout(std::time::Duration::from_secs(5))?;
    let _ = conn.pragma_update(None, "query_only", true);
    Ok(conn)
}

/// epoch 秒/毫秒自适应(<TS_MS_FLOOR 视为秒 ×1000;juejin 惯例,时间戳漂移防护)。
pub fn secs_or_ms(n: i64) -> i64 {
    if n.abs() < TS_MS_FLOOR {
        n.saturating_mul(1000)
    } else {
        n
    }
}

/// 多形态时间戳 → epoch 毫秒。按量级自适应:ISO 字符串 / [秒,纳秒] 二元组 /
/// 数值标量(≥1e17 纳秒、≥1e14 微秒、≥1e11 毫秒、否则秒)/ 数字字符串。
/// OTel 与新式网关格式的公共入口;失败返回 None。
pub fn flex_to_ms(v: Option<&Value>) -> Option<i64> {
    match v {
        Some(Value::String(s)) => {
            let t = s.trim();
            if let Ok(n) = t.parse::<i64>() {
                Some(secs_or_ms(n))
            } else {
                iso_to_ms(t)
            }
        }
        Some(Value::Array(a)) if a.len() == 2 => {
            // OTel hrTime:[秒, 纳秒]
            let sec = a.first()?.as_i64()?;
            let nanos = a.get(1)?.as_i64().unwrap_or(0);
            Some(sec.saturating_mul(1000).saturating_add(nanos / 1_000_000))
        }
        Some(Value::Number(n)) => {
            let f = n.as_f64()?;
            let ms = if f.abs() >= 1e17 {
                f / 1e6 // 纳秒
            } else if f.abs() >= 1e14 {
                f / 1e3 // 微秒
            } else if f.abs() >= TS_MS_FLOOR as f64 {
                f // 毫秒(与 secs_or_ms 共用分界,见 TS_MS_FLOOR 注释)
            } else {
                f * 1e3 // 秒
            };
            Some(ms as i64)
        }
        _ => None,
    }
}

/// 相邻差分跟踪器:累计快照 → 单次增量(gemini 会话快照 / copilot shutdown 区间 /
/// opencode 流式更新行共用语义)。总量较前值上升 → 各桶饱和差分;
/// 下降或首见 → 全量(会话重启/计数器重置语义,juejin diffGeminiTotals 同款)。
#[derive(Default)]
pub struct SnapshotDiff {
    prev: Option<[u64; 5]>,
}

impl SnapshotDiff {
    /// 推入当前快照,返回本次增量(全零增量由调用方决定是否入账)。
    /// 单调判据按**每桶各自**比较(2026-10-17 修):此前用五桶总和判断,总和不变而桶间
    /// 迁移(如 opencode 流式更新把 output 重分类为 reasoning)会凭空多计;总和升而某桶
    /// 降则该桶增量被饱和截断丢失——任一桶下降即视为计数器重置,走全量语义。
    pub fn push(&mut self, cur: [u64; 5]) -> [u64; 5] {
        let out = match self.prev {
            Some(p) if cur.iter().zip(p.iter()).all(|(c, p)| c >= p) => {
                [
                    cur[0].saturating_sub(p[0]),
                    cur[1].saturating_sub(p[1]),
                    cur[2].saturating_sub(p[2]),
                    cur[3].saturating_sub(p[3]),
                    cur[4].saturating_sub(p[4]),
                ]
            }
            _ => cur,
        };
        self.prev = Some(cur);
        out
    }
}


/// ISO 8601(Claude/Codex 转录时间戳)→ epoch 毫秒;失败返回 None(该行跳过计数)。
pub fn iso_to_ms(s: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|d| d.timestamp_millis())
}

/// epoch 毫秒 → 本地时区日期(聚合用)。
pub fn ms_to_local_date(ms: i64) -> String {
    use chrono::TimeZone;
    let t = chrono::Local.timestamp_millis_opt(ms);
    match t {
        chrono::LocalResult::Single(d) => d.format("%Y-%m-%d").to_string(),
        chrono::LocalResult::Ambiguous(a, _) => a.format("%Y-%m-%d").to_string(),
        chrono::LocalResult::None => "?".into(),
    }
}

/// epoch 毫秒 → 本地时区小时键 "YYYY-MM-DDTHH"(看板小时曲线用)。
pub fn ms_to_local_hour(ms: i64) -> String {
    use chrono::TimeZone;
    // DST 折叠/跳变时 single() 为 None → 取任一侧有效映射,避免数据落入 "?" 伪桶
    let t = chrono::Local.timestamp_millis_opt(ms);
    match t {
        chrono::LocalResult::Single(d) => d.format("%Y-%m-%dT%H").to_string(),
        chrono::LocalResult::Ambiguous(a, _) => a.format("%Y-%m-%dT%H").to_string(),
        chrono::LocalResult::None => "?".into(),
    }
}

/// 流式逐行读 jsonl,对每行调用 f(&str);返回(总行数,坏行数)。
/// 尾行不完整(append-only 崩溃残留)按坏行计数跳过——档案纪律。
pub fn for_each_line<P: AsRef<Path>, F: FnMut(&str)>(path: P, f: F) -> (u64, u64) {
    use std::io::BufReader;
    let file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(_) => return (0, 1),
    };
    for_each_line_reader(&mut BufReader::new(file), f)
}

/// Byte-level reading distinguishes consumed invalid UTF-8 from a permanent
/// I/O error with no progress. The tuple contract for legacy adapters is unchanged.
#[doc(hidden)]
pub fn for_each_line_reader(reader: &mut impl std::io::BufRead, mut f: impl FnMut(&str)) -> (u64, u64) {
    let (mut total, mut bad) = (0, 0);
    let mut buf = Vec::new();
    loop {
        buf.clear();
        match reader.read_until(b'\n', &mut buf) {
            Ok(0) => break,
            Ok(_) => match std::str::from_utf8(&buf) {
                Ok(line) => {
                    let line = line.trim();
                    if !line.is_empty() { total += 1; f(line); }
                }
                Err(_) => bad += 1,
            },
            // A partial line consumed before Err is not a complete record.
            Err(_) => { bad += 1; break; }
        }
    }
    (total, bad)
}

/// 清空进程内 L1a/L1b 缓存(仅测试;配合 store::reset_for_tests 模拟"新进程")。
#[doc(hidden)]
pub fn __reset_caches_for_tests() {
    if let Some(cache) = USAGE_CACHE.get() {
        let mut c = cache.lock().unwrap_or_else(|e| e.into_inner());
        c.entries.clear();
        c.weight = 0;
    }
    if let Some(cache) = SOURCE_CACHE.get() {
        let mut c = cache.lock().unwrap_or_else(|e| e.into_inner());
        c.clear();
    }
}


#[cfg(test)]
mod dedup_tests {
    use super::*;

    #[test]
    fn same_request_within_window_is_not_clash() {
        let mut t = DedupTracker::default();
        assert!(!t.track("k", 1_000, "m", [10, 0, 0, 0, 0]));
        // 同 model + 窗内 ts 差:流式逐块/成对双发 → 补全,非碰撞(值变化也视为补全)。
        assert!(!t.track("k", 1_000, "m", [20, 0, 0, 0, 0]));
        assert!(!t.track("k", 1_000 + KEY_CLASH_TS_WINDOW_MS as i64, "m", [30, 0, 0, 0, 0]));
    }

    #[test]
    fn different_model_or_far_ts_with_value_change_is_clash() {
        let mut t = DedupTracker::default();
        assert!(!t.track("k", 1_000, "m1", [100, 0, 0, 0, 0]));
        assert!(t.track("k", 1_000, "m2", [100, 0, 0, 0, 0]), "同键不同 model = 键碰撞");
        assert!(t.track("k", 1_000 + KEY_CLASH_TS_WINDOW_MS as i64 + 1, "m1", [200, 0, 0, 0, 0]),
            "同键超窗且值不同 = 键碰撞");
    }

    #[test]
    fn value_identical_redelivery_far_apart_is_not_clash() {
        // codex 成对双发/恢复重投:键内嵌值,同键 ⟹ 同值,远距重投是去重正确命中。
        let mut t = DedupTracker::default();
        assert!(!t.track("k", 1_000, "m", [777, 0, 0, 0, 0]));
        assert!(!t.track("k", 600_000, "m", [777, 0, 0, 0, 0]), "值全同的远距重投 ≠ 碰撞");
    }

    #[test]
    fn distinct_keys_never_clash() {
        let mut t = DedupTracker::default();
        assert!(!t.track("a", 1, "m", [1, 0, 0, 0, 0]));
        assert!(!t.track("b", 1, "m", [1, 0, 0, 0, 0]));
    }
}


