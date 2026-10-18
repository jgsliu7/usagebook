//! 运行日志:日期 + 大小双重滚动(设计裁决与理由见 docs/运行日志设计决策.md)。
//!
//! 分层约定:本模块只依赖 std/chrono/tracing(宏),不依赖 tracing-subscriber——
//! 订阅器组装在 cli(`logging_setup.rs`);本模块提供可独立测试的文件层设施:
//! - `FileSink`:双滚动写入器(本地零点缓存 + 内存字节计数;轮转永不停止);
//! - 清理 A/B/C(双上限,锁外执行,围栏正则式匹配);
//! - 简版目录锁(O_EXCL + pid;Unix kill(pid,0) 判陈旧,EPERM=存活;Windows 锁存在即降级);
//! - 独立 panic 通道(启动预建 panic.log,set_hook 替换默认 hook[不打原始 payload],同规脱敏);
//! - 净化(CWE-117 控制字符转义)与禁密替换(panic 通道用)。
//!
//! 写路径纪律(方案 §5):写入路径内部诊断一律走 `bypass_warn`(直写 stderr,限频),
//! 不得经 tracing——否则重入本模块的写锁造成递归/死锁。清理永远在锁外执行。

use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

/// 单文件大小上限(内置常量,不设环境变量)。
pub(crate) const MAX_FILE_BYTES: u64 = 20 * 1024 * 1024;
/// panic.log 单文件上限(超限改名 panic.log.old,单份归档;合计预算 10MiB)。
pub(crate) const PANIC_MAX_BYTES: u64 = 5 * 1024 * 1024;
const DEFAULT_KEEP_DAYS: u64 = 14;
const DEFAULT_MAX_TOTAL: u64 = 200 * 1024 * 1024;
const PREFIX: &str = "usagebook";
const LOCK_NAME: &str = ".lock";
const PANIC_NAME: &str = "panic.log";
const PANIC_OLD_NAME: &str = "panic.log.old";
/// 旁路告警与轮转重试的限频间隔(方案 §5)。
const RATE_LIMIT_MS: i64 = 30_000;
/// 连续写失败达到该次数 → 文件层降级禁用(方案 §5 边界三)。
const MAX_FAIL_STREAK: u32 = 3;

// ---------------------------------------------------------------------------
// 时钟注入
// ---------------------------------------------------------------------------

/// 可注入时钟:返回带本地时区偏移的时刻(生产 = Local::now;测试 = FixedOffset 构造)。
pub type Clock = Arc<dyn Fn() -> chrono::DateTime<chrono::FixedOffset> + Send + Sync>;

fn default_clock() -> Clock {
    Arc::new(|| chrono::Local::now().fixed_offset())
}

/// 下一个本地零点的 epoch 秒(与文件名日期同源;不使用 UTC epoch-day)。
fn next_local_midnight(now: chrono::DateTime<chrono::FixedOffset>) -> i64 {
    use chrono::{Datelike, TimeZone};
    let tomorrow = now.date_naive() + chrono::Days::new(1);
    match now
        .timezone()
        .with_ymd_and_hms(tomorrow.year(), tomorrow.month(), tomorrow.day(), 0, 0, 0)
        .single()
    {
        Some(dt) => dt.timestamp(),
        // DST 缺失小时的兜底:粗略 +86400,不阻塞轮转。
        None => now.timestamp() + 86_400,
    }
}

fn date_string(now: chrono::DateTime<chrono::FixedOffset>) -> String {
    now.format("%Y%m%d").to_string()
}

// ---------------------------------------------------------------------------
// 配置解析(方案 §8)
// ---------------------------------------------------------------------------

/// 日志目录:关闭哨兵或具体路径。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogDir {
    Off,
    At(PathBuf),
}

#[derive(Debug, Clone)]
pub struct LogConfig {
    pub dir: LogDir,
    pub keep_days: u64,
    pub max_total: u64,
}

/// 数据目录(与 serve 缓存、state.db 同源解析;实现收敛于 common::data_dir)。
pub fn data_dir() -> PathBuf {
    crate::common::data_dir()
}

fn is_off(v: &str) -> bool {
    matches!(v.trim().to_ascii_lowercase().as_str(), "off" | "0" | "false")
}

/// 静默解析目录(供 doctor 展示;不校验、不告警,一次性命令 stderr 零新增行)。
pub fn resolve_dir_quiet() -> LogDir {
    match std::env::var("USAGEBOOK_LOG_DIR") {
        Ok(v) if !v.trim().is_empty() => {
            if is_off(&v) { LogDir::Off } else { LogDir::At(PathBuf::from(v.trim())) }
        }
        _ => LogDir::At(data_dir().join("logs")),
    }
}

/// 完整配置解析(仅文件层激活时调用;非法值回退/钳制 + 旁路告警)。
/// off 哨兵早退:数值变量不解析、不告警(§8),清理用默认参数(§6)。
fn resolve_config() -> LogConfig {
    let dir = match std::env::var("USAGEBOOK_LOG_DIR") {
        Ok(v) if !v.trim().is_empty() => {
            if is_off(&v) {
                return LogConfig { dir: LogDir::Off, keep_days: DEFAULT_KEEP_DAYS, max_total: DEFAULT_MAX_TOTAL };
            }
            let p = PathBuf::from(v.trim());
            if !p.is_absolute() {
                bypass_warn("log-dir-relative",
                    format!("警告:USAGEBOOK_LOG_DIR 相对路径({}),已回退默认目录", p.display()));
                LogDir::At(data_dir().join("logs"))
            } else {
                LogDir::At(p)
            }
        }
        _ => LogDir::At(data_dir().join("logs")),
    };
    let keep_days = parse_env_u64("USAGEBOOK_LOG_KEEP_DAYS", DEFAULT_KEEP_DAYS, 0);
    let max_total = parse_env_u64("USAGEBOOK_LOG_MAX_TOTAL", DEFAULT_MAX_TOTAL, MAX_FILE_BYTES);
    LogConfig { dir, keep_days, max_total }
}

/// 环境变量 → u64:解析失败/低于下限 → 回退或钳制 + 旁路告警(方案 §8,不 fail-fast)。
fn parse_env_u64(key: &str, default: u64, min: u64) -> u64 {
    let Some(raw) = std::env::var(key).ok().filter(|v| !v.trim().is_empty()) else {
        return default;
    };
    match parse_u64_value(raw.trim(), default, min) {
        (v, None) => v,
        (v, Some(reason)) => {
            bypass_warn(key, format!("警告:{key}='{}' {reason}(采用 {v})", raw.trim()));
            v
        }
    }
}

/// 静默版值解析(off 清理路径用:采用与激活一致的钳制语义,但不产生告警)。
fn parse_env_u64_quiet(key: &str, default: u64, min: u64) -> u64 {
    match std::env::var(key).ok().filter(|v| !v.trim().is_empty()) {
        Some(raw) => parse_u64_value(raw.trim(), default, min).0,
        None => default,
    }
}

/// 纯函数值解析(可单测):返回(采用值, 告警原因)。
fn parse_u64_value(raw: &str, default: u64, min: u64) -> (u64, Option<String>) {
    match raw.parse::<u64>() {
        Ok(v) if v >= min => (v, None),
        Ok(v) => (min, Some(format!("低于下限 {min}(值 {v})"))),
        Err(_) => (default, Some("非法".to_string())),
    }
}

// ---------------------------------------------------------------------------
// 净化(CWE-117)与禁密替换
// ---------------------------------------------------------------------------

/// 控制字符转义:ASCII 控制字符(保留 TAB)、DEL、C1 控制区(U+0080-U+009F 的 UTF-8 双字节)
/// 转为可见字面;行尾原有的单个 `\n` 作为行终止符保留。
pub fn sanitize_line(buf: &[u8]) -> Vec<u8> {
    // 行尾终止符不参与转义(避免输出"\n"+真换行的双尾巴),转义其余部分后补回。
    let (body, ended_with_newline) = match buf.split_last() {
        Some((b'\n', head)) => (head, true),
        _ => (buf, false),
    };
    let buf = body;
    let mut out = Vec::with_capacity(buf.len() + 8);
    let mut i = 0;
    while i < buf.len() {
        let c = buf[i];
        match c {
            b'\t' => { out.push(c); i += 1; }
            b'\n' => { out.extend_from_slice(b"\\n"); i += 1; }
            b'\r' => { out.extend_from_slice(b"\\r"); i += 1; }
            0x00..=0x1f | 0x7f => {
                out.extend_from_slice(format!("\\x{c:02x}").as_bytes());
                i += 1;
            }
            // U+0080-U+009F(C1 控制区)在 UTF-8 中编码为 C2 80..C2 9F。
            0xc2 if i + 1 < buf.len() && (0x80..=0x9f).contains(&buf[i + 1]) => {
                out.extend_from_slice(format!("\\x{:02x}", buf[i + 1]).as_bytes());
                i += 2;
            }
            _ => { out.push(c); i += 1; }
        }
    }
    if ended_with_newline { out.push(b'\n'); }
    out
}

/// 禁密标记(小写;命中所在"词"整体替换为 [REDACTED];`-----BEGIN` 整行替换)。
const SECRET_MARKERS: &[&str] = &[
    "sk-", "ghp_", "github_pat_", "xoxb-", "akia", "-----begin",
    "bearer ", "authorization:", "token=", "session=", "code=",
];

/// 禁密替换 + 家目录缩略(panic 通道落盘前执行;主通道靠"只记白名单字段"纪律 + 测试断言)。
pub fn scrub_text(text: &str, home: Option<&Path>) -> String {
    let mut s = text.to_string();
    if let Some(h) = home.and_then(|p| p.to_str()).filter(|h| !h.is_empty()) {
        if s.contains(h) {
            s = s.replace(h, "~");
        }
    }
    loop {
        let lower = s.to_ascii_lowercase();
        let Some((marker, start)) = SECRET_MARKERS.iter().find_map(|m| lower.find(m).map(|at| (*m, at))) else {
            break;
        };
        let bytes = s.as_bytes();
        if marker == "-----begin" {
            let end = s[start..].find('\n').map(|i| start + i).unwrap_or(s.len());
            s.replace_range(start..end, "[REDACTED]");
        } else {
            // 词 = 起于词首(向前回退到空白),止于空白;标记带尾随空格/=/: 时连带其后一个词(值)。
            let mut start = start;
            while start > 0 && !bytes[start - 1].is_ascii_whitespace() { start -= 1; }
            let mut end = start;
            while end < bytes.len() && !bytes[end].is_ascii_whitespace() { end += 1; }
            if marker.ends_with(' ') || marker.ends_with('=') || marker.ends_with(':') {
                while end < bytes.len() && bytes[end].is_ascii_whitespace() { end += 1; }
                while end < bytes.len() && !bytes[end].is_ascii_whitespace() { end += 1; }
            }
            s.replace_range(start..end, "[REDACTED]");
        }
    }
    s
}

// ---------------------------------------------------------------------------
// 旁路告警(限频;不经 tracing——见模块头写路径纪律)
// ---------------------------------------------------------------------------

fn bypass_warn(key: &str, msg: String) {
    static LAST: OnceLock<Mutex<std::collections::HashMap<String, i64>>> = OnceLock::new();
    let map = LAST.get_or_init(|| Mutex::new(std::collections::HashMap::new()));
    let now = chrono::Local::now().timestamp_millis();
    let mut g = map.lock().unwrap_or_else(|e| e.into_inner());
    if g.get(key).and_then(|t| now.checked_sub(*t)).unwrap_or(i64::MAX) >= RATE_LIMIT_MS {
        g.insert(key.to_string(), now);
        // 容错写:stderr 破裂(管道关闭/满)时 eprintln! 会 panic,而本函数可能在持锁路径被调。
        let _ = writeln!(std::io::stderr(), "{msg}");
    }
}

// ---------------------------------------------------------------------------
// 文件打开(非常规文件防护,方案 §5)
// ---------------------------------------------------------------------------

/// append 打开 + 双重防护:路径级(symlink/fifo/目录拒绝)与 fd 级(打开后校验 regular file);
/// 新建时 0o600。返回 (句柄, 现有大小)。
fn open_append_guarded(path: &Path) -> io::Result<(std::fs::File, u64)> {
    if let Ok(md) = std::fs::symlink_metadata(path) {
        if !md.is_file() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "non-regular file"));
        }
    }
    let mut opts = std::fs::OpenOptions::new();
    opts.append(true).create(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let f = opts.open(path)?;
    let md = f.metadata()?;
    if !md.is_file() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "non-regular file"));
    }
    Ok((f, md.len()))
}

fn main_log_path(dir: &Path, date: &str) -> PathBuf {
    dir.join(format!("{PREFIX}.{date}.log"))
}

fn shard_log_path(dir: &Path, date: &str, n: u32) -> PathBuf {
    let nnn = if n < 1000 { format!("{n:03}") } else { n.to_string() };
    dir.join(format!("{PREFIX}.{date}.{nnn}.log"))
}

// ---------------------------------------------------------------------------
// 双滚动文件层
// ---------------------------------------------------------------------------

struct Inner {
    dir: PathBuf,
    file: std::fs::File,
    date: String,
    next_midnight: i64,
    bytes: u64,
    torn: bool,
    fail_streak: u32,
    high_water: u32,
    last_rotate_attempt: i64,
}

struct Shared {
    state: Mutex<Option<Inner>>,
    cfg: Mutex<Option<(PathBuf, LogConfig)>>, // (目录, 配置) — 清理用;None = 文件层未激活
    clock: Clock,
    max_bytes: AtomicU64,
    fail_inject: AtomicUsize,
    fail_rename: AtomicUsize,
}

/// 文件层句柄(cli 的 MakeWriter 持有克隆;`Write` 实现整行直达)。
pub struct FileSink(Arc<Shared>);

impl Clone for FileSink {
    fn clone(&self) -> Self { FileSink(Arc::clone(&self.0)) }
}

impl Write for FileSink {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> { self.write_line(buf) }
    fn flush(&mut self) -> io::Result<()> { Ok(()) }
}

impl FileSink {
    pub fn write_line(&self, buf: &[u8]) -> io::Result<usize> {
        let mut do_cleanup = false;
        {
            let mut guard = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
            let Some(inner) = guard.as_mut() else { return Ok(buf.len()) };
            let now = (self.0.clock)();
            let now_ts = now.timestamp();

            // 触发点一:日期变更(本地零点缓存)。失败不降级:保留句柄继续写旧日期文件,
            // 30s 后重试(与大小轮转的失败哲学一致,方案 §5"轮转永不停止")。
            if now_ts >= inner.next_midnight {
                let date = date_string(now);
                match open_append_guarded(&main_log_path(&inner.dir, &date)) {
                    Ok((f, size)) => {
                        inner.file = f;
                        inner.date = date;
                        inner.next_midnight = next_local_midnight(now);
                        inner.bytes = size;
                        inner.high_water = 0;
                        inner.torn = false;
                        do_cleanup = true;
                    }
                    Err(e) => {
                        bypass_warn("date-rollover", format!("警告:日志跨日轮转失败({e}),继续写当前文件,稍后重试"));
                        inner.next_midnight = now_ts + RATE_LIMIT_MS / 1000;
                    }
                }
            }

            // 边界三:撕裂修复。
            if inner.torn {
                let _ = inner.file.write_all(b"\n");
                inner.torn = false;
            }

            let max_bytes = self.0.max_bytes.load(Ordering::Relaxed);
            match do_write(&self.0.fail_inject, &mut inner.file, buf) {
                Ok(()) => {
                    inner.fail_streak = 0;
                    inner.bytes += buf.len() as u64;
                }
                Err(e) => {
                    inner.torn = true;
                    inner.fail_streak += 1;
                    bypass_warn("write-fail", format!("警告:日志写入失败({e}),已写条目不受影响"));
                    if inner.fail_streak >= MAX_FAIL_STREAK {
                        bypass_warn("write-fail", "警告:日志连续写入失败,本进程文件日志降级(console 不受影响)".into());
                        *guard = None;
                        return Ok(buf.len());
                    }
                    return Ok(buf.len());
                }
            }

            // 触发点二:大小超限(轮转永不停止;999 后自然进位 4 位号)。
            // 限频窗口对时钟回拨容错:now < last 说明系统时钟回拨,视为窗口已过
            // (否则回拨期间 20MiB 上限失效,主文件无界增长)。
            if inner.bytes >= max_bytes
                && rotate_window_elapsed(now_ts, inner.last_rotate_attempt)
            {
                inner.last_rotate_attempt = now_ts;
                let n = next_shard_no(&inner.dir, &inner.date, inner.high_water);
                let from = main_log_path(&inner.dir, &inner.date);
                let to = shard_log_path(&inner.dir, &inner.date, n);
                let rename_ret = {
                    let left = self.0.fail_rename.load(Ordering::Relaxed);
                    if left > 0 {
                        self.0.fail_rename.store(left - 1, Ordering::Relaxed);
                        Err(io::Error::new(io::ErrorKind::PermissionDenied, "injected rename failure"))
                    } else {
                        std::fs::rename(&from, &to)
                    }
                };
                match rename_ret {
                    Ok(()) => match open_append_guarded(&from) {
                        Ok((f, _)) => {
                            inner.file = f;
                            inner.bytes = 0;
                            inner.high_water = n;
                            inner.last_rotate_attempt = 0; // 成功轮转解除限频(限频只作用于失败重试)
                            do_cleanup = true;
                        }
                        Err(e) => {
                            bypass_warn("rotate", format!("警告:轮转后重开主文件失败({e}),文件日志降级"));
                            *guard = None;
                        }
                    },
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {
                        // 他方已轮转/移动:重开主文件继续。
                        match open_append_guarded(&from) {
                            Ok((f, _)) => { inner.file = f; inner.bytes = 0; }
                            Err(e2) => {
                                bypass_warn("rotate", format!("警告:轮转竞争后重开失败({e2}),文件日志降级"));
                                *guard = None;
                            }
                        }
                    }
                    // 边界二:rename 瞬态失败 → 继续写当前文件,30s 后重试(once 只管告警不管重试)。
                    Err(e) => {
                        bypass_warn("rotate", format!("警告:日志轮转失败({e}),稍后重试"));
                    }
                }
            }
        } // 锁在此释放;清理永远在锁外(方案 §5)。
        if do_cleanup {
            if let Some((dir, cfg)) = self.0.cfg.lock().unwrap_or_else(|e| e.into_inner()).clone() {
                let today = (self.0.clock)().date_naive();
                run_cleanup(&dir, cfg.keep_days, cfg.max_total, today);
            }
        }
        Ok(buf.len())
    }

    #[cfg(test)]
    fn is_active(&self) -> bool {
        self.0.state.lock().unwrap_or_else(|e| e.into_inner()).is_some()
    }

    #[cfg(test)]
    fn inject_write_failures(&self, n: u32) {
        self.0.fail_inject.store(n as usize, Ordering::Relaxed);
    }

    #[cfg(test)]
    fn inject_rename_failures(&self, n: u32) {
        self.0.fail_rename.store(n as usize, Ordering::Relaxed);
    }

    #[cfg(test)]
    fn set_max_bytes_for_test(&self, n: u64) {
        self.0.max_bytes.store(n, Ordering::Relaxed);
    }

    #[cfg(test)]
    fn set_max_total_for_test(&self, n: u64) {
        if let Some((_dir, cfg)) = self.0.cfg.lock().unwrap_or_else(|e| e.into_inner()).as_mut() {
            cfg.max_total = n;
        }
    }
}

/// 限频窗口是否已过:last==0 从未尝试;now >= last+30 窗口到点;now < last 时钟回拨(容错为已过)。
fn rotate_window_elapsed(now_ts: i64, last: i64) -> bool {
    last == 0 || now_ts >= last + RATE_LIMIT_MS / 1000 || now_ts < last
}

/// 写入(含测试失败注入;write_all 的 Err 不带已写字节数,失败路径由调用方统一处理)。
fn do_write(fail_inject: &AtomicUsize, f: &mut std::fs::File, buf: &[u8]) -> io::Result<()> {
    let left = fail_inject.load(Ordering::Relaxed);
    if left > 0 {
        fail_inject.store(left - 1, Ordering::Relaxed);
        return Err(io::Error::new(io::ErrorKind::WriteZero, "injected write failure"));
    }
    f.write_all(buf)
}

/// 当日下一个分片号 = max(目录中已存在最大号, 进程内高水位) + 1(单调递增,清理删号后高水位不回退)。
fn next_shard_no(dir: &Path, date: &str, high_water: u32) -> u32 {
    let prefix = format!("{PREFIX}.{date}.");
    let mut max = high_water;
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let name = e.file_name();
            let Some(name) = name.to_str() else { continue };
            if let Some(rest) = name.strip_prefix(&prefix) {
                if let Some(digits) = rest.strip_suffix(".log") {
                    if let Ok(n) = digits.parse::<u32>() {
                        max = max.max(n);
                    }
                }
            }
        }
    }
    max.saturating_add(1)
}

// ---------------------------------------------------------------------------
// 清理 A/B/C(方案 §6;锁外执行)
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct LogFile {
    path: PathBuf,
    size: u64,
    date: chrono::NaiveDate,
    shard: Option<u32>, // None = 当日主文件
}

fn parse_log_name(name: &str) -> Option<(chrono::NaiveDate, Option<u32>)> {
    let rest = name.strip_prefix(PREFIX)?.strip_prefix('.')?;
    let rest = rest.strip_suffix(".log")?;
    let (date_s, shard) = match rest.split_once('.') {
        Some((d, n)) => {
            if n.len() < 3 { return None; } // 围栏 \d{3,}:1-2 位号不是本程序产物,不删
            (d, Some(n.parse::<u32>().ok()?))
        }
        None => (rest, None),
    };
    let date = chrono::NaiveDate::parse_from_str(date_s, "%Y%m%d").ok()?;
    Some((date, shard))
}

/// 双上限清理:规则 A(保天数,活动主文件豁免)→ 规则 B(封总量,全序删除)→ 规则 C(围栏)。
fn run_cleanup(dir: &Path, keep_days: u64, max_total: u64, today: chrono::NaiveDate) {
    let mut files: Vec<LogFile> = Vec::new();
    let mut panic_old: Option<(PathBuf, u64)> = None;
    let mut panic_cur: Option<(PathBuf, u64)> = None;
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let name = e.file_name();
            let Some(name) = name.to_str() else { continue };
            // 只计 regular file,不跟随 symlink(规则 B)。
            let Ok(md) = std::fs::symlink_metadata(e.path()) else { continue };
            if !md.is_file() { continue; }
            if let Some((date, shard)) = parse_log_name(name) {
                files.push(LogFile { path: e.path(), size: md.len(), date, shard });
            } else if name == PANIC_OLD_NAME {
                panic_old = Some((e.path(), md.len()));
            } else if name == PANIC_NAME {
                panic_cur = Some((e.path(), md.len()));
            }
        }
    }

    let mut total: u64 = files.iter().map(|f| f.size).sum::<u64>()
        + panic_old.as_ref().map(|(_, s)| *s).unwrap_or(0)
        + panic_cur.as_ref().map(|(_, s)| *s).unwrap_or(0);

    // 规则 A:文件名日期 < today − keep_days → 删(当日主文件日期=today,天然豁免)。
    let cutoff = today - chrono::Duration::days(keep_days as i64);
    files.retain(|f| {
        if f.date < cutoff {
            delete_quiet(&f.path);
            total = total.saturating_sub(f.size);
            false
        } else {
            true
        }
    });

    // 规则 B:超封顶 → 先删最老日期整组;仍超才动当日分片(最小号先,最大号保底);
    // 再超删 panic 归档/正文。当前活动主文件(当日无分片号)永不删。
    if total > max_total {
        let by_date: Vec<chrono::NaiveDate> = files.iter().map(|f| f.date).collect::<std::collections::BTreeSet<_>>().into_iter().collect();
        for d in by_date {
            if total <= max_total { break; }
            if d >= today { continue; } // 当日组走下面的分片路径
            // 昨日组的无分片主文件豁免:另一实例的最后一次写在零点前时,其活动句柄
            // 正是昨日主文件(下一次写入即触发跨日轮转);删除它会造成一个刷新周期的
            // 静默丢失。更早的组不可能是活动句柄(写入前必先跨日轮转)。豁免代价 ≤ 单文件。
            let shield_yesterday_main = today - d == chrono::Duration::days(1);
            files.retain(|f| {
                if f.date == d {
                    if shield_yesterday_main && f.shard.is_none() { return true; }
                    delete_quiet(&f.path);
                    total = total.saturating_sub(f.size);
                    false
                } else { true }
            });
        }
        // 当日:分片从小号删起,最大号除非需清空不删。
        if total > max_total {
            let today_shards = files.iter()
                .filter(|f| f.date == today && f.shard.is_some())
                .map(|f| f.shard.unwrap())
                .collect::<std::collections::BTreeSet<_>>();
            if let Some(&max_n) = today_shards.iter().last() {
                let mut asc: Vec<u32> = today_shards.into_iter().collect();
                asc.retain(|n| *n != max_n);
                for n in asc {
                    if total <= max_total { break; }
                    if let Some(pos) = files.iter().position(|f| f.date == today && f.shard == Some(n)) {
                        delete_quiet(&files[pos].path);
                        total = total.saturating_sub(files[pos].size);
                        files.remove(pos);
                    }
                }
                if total > max_total { // 需清空:连最大号也删(活动主文件仍保留)
                    if let Some(pos) = files.iter().position(|f| f.date == today && f.shard == Some(max_n)) {
                        delete_quiet(&files[pos].path);
                        total = total.saturating_sub(files[pos].size);
                        files.remove(pos);
                    }
                }
            }
        }
        if total > max_total {
            if let Some((p, s)) = panic_old.take() {
                delete_quiet(&p);
                total = total.saturating_sub(s);
            }
        }
        if total > max_total {
            if let Some((p, _)) = panic_cur.take() {
                delete_quiet(&p);
            }
        }
        // 仍超封顶且只剩当日主文件 = 预期常态(活动文件不可删),不重复告警(方案 §5 边界三)。
    }
}

/// 删除(规则 C:仅本函数删文件且调用方已做围栏过滤;NotFound 容忍 = 他方并发清理)。
fn delete_quiet(p: &Path) {
    if let Err(e) = std::fs::remove_file(p) {
        if e.kind() != io::ErrorKind::NotFound {
            bypass_warn("cleanup", format!("警告:日志清理失败({} {})", e, p.display()));
        }
    }
}

// ---------------------------------------------------------------------------
// 目录锁(方案 §5 简版)
// ---------------------------------------------------------------------------

fn acquire_lock(dir: &Path) -> bool {
    let lp = dir.join(LOCK_NAME);
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    for _ in 0..3 {
        match opts.open(&lp) {
            Ok(mut f) => {
                let _ = write!(f, "{}", std::process::id());
                return true;
            }
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                if !stale_lock_recoverable(&lp) { return false; }
                // 陈旧锁已回收,重试创建。
            }
            Err(_) => return false,
        }
    }
    false
}

/// 锁文件年龄超过该阈值视为陈旧可回收(Windows 唯一依据;Unix 对不可解析锁的兜底)。
const STALE_LOCK_SECS: u64 = 3600;

fn lock_age_secs(lp: &Path) -> Option<u64> {
    let md = std::fs::symlink_metadata(lp).ok()?;
    let modified = md.modified().ok()?;
    match std::time::SystemTime::now().duration_since(modified) {
        Ok(age) => Some(age.as_secs()),
        // mtime 在未来(时钟回拨产物):按"足够陈旧"处理,Windows 死锁自愈不被推迟。
        Err(_) => Some(u64::MAX),
    }
}

/// 陈旧判定:Unix kill(pid,0) — Ok=存活,EPERM=他户进程(视为存活),ESRCH=陈旧可回收;
/// Windows 无零依赖 pid 查活 —— 以 mtime 年龄兜底(>1h 回收;serve 进程不刷 mtime,
/// 死锁最迟 1 小时后自愈);Unix 对不可解析的锁同样走年龄兜底。
/// Unix 锁判定(纯决策,可单测):kill(pid,0) 结果 → 是否可回收。
/// Ok=存活;EPERM=他户进程(视为存活,方案 §5 裁决);ESRCH=陈旧可回收;其他错误按占用。
#[cfg(unix)]
fn unix_lock_stale(kill_errno: Option<i32>) -> bool {
    match kill_errno {
        None => false,                       // rc == 0,存活
        Some(code) if code == libc::ESRCH => true,
        Some(_) => false,                    // EPERM 等:占用
    }
}

#[cfg(unix)]
fn stale_lock_recoverable(lp: &Path) -> bool {
    let Ok(buf) = std::fs::read_to_string(lp) else {
        return aged_out(lp);
    };
    let Ok(pid) = buf.trim().parse::<i32>() else {
        return aged_out(lp);
    };
    if pid <= 0 {
        return aged_out(lp);
    }
    // SAFETY: 信号 0 仅做存在性/权限检查,不发送任何信号。
    let rc = unsafe { libc::kill(pid, 0) };
    let errno = if rc == 0 { None } else { std::io::Error::last_os_error().raw_os_error() };
    unix_lock_stale(errno) && std::fs::remove_file(lp).is_ok()
}

#[cfg(unix)]
fn aged_out(lp: &Path) -> bool {
    match lock_age_secs(lp) {
        Some(age) if age > STALE_LOCK_SECS => std::fs::remove_file(lp).is_ok(),
        _ => false,
    }
}

#[cfg(not(unix))]
fn stale_lock_recoverable(lp: &Path) -> bool {
    match lock_age_secs(lp) {
        Some(age) if age > STALE_LOCK_SECS => std::fs::remove_file(lp).is_ok(),
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// panic 通道(方案 §7:预建、独立锁、同规脱敏、catch_unwind 包裹)
// ---------------------------------------------------------------------------

struct PanicFile {
    f: std::fs::File,
    size: u64,
}

struct PanicChannel {
    file: Mutex<Option<PanicFile>>,
    home: PathBuf,
    dir: Mutex<PathBuf>,
}

static PANIC: OnceLock<PanicChannel> = OnceLock::new();
static PANIC_INSTALLED: AtomicBool = AtomicBool::new(false);

fn install_panic_channel(dir: &Path) -> bool {
    let path = dir.join(PANIC_NAME);
    let (f, size) = match open_append_guarded(&path) {
        Ok(v) => v,
        Err(e) => {
            bypass_warn("panic-log", format!("警告:panic 留档文件不可用({e}),崩溃仅输出 stderr"));
            return false;
        }
    };
    let pf = PanicFile { f, size };
    let ch = PANIC.get_or_init(|| PanicChannel {
        file: Mutex::new(None),
        home: crate::common::home(),
        dir: Mutex::new(dir.to_path_buf()),
    });
    // 重复激活时真正更新句柄与目录(activate 的幂等承诺)。
    *ch.file.lock().unwrap_or_else(|e| e.into_inner()) = Some(pf);
    *ch.dir.lock().unwrap_or_else(|e| e.into_inner()) = dir.to_path_buf();
    if PANIC_INSTALLED.swap(true, Ordering::SeqCst) {
        return true;
    }
    std::panic::set_hook(Box::new(|info| {
        // hook 内不可 panic:格式化与写入全程 catch_unwind 包裹。
        // stderr 副本同规脱敏(不调默认 hook——它会打原始 payload,见方案 §7 禁密同规条款)。
        let entry = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| format_panic_entry(info)))
            .unwrap_or_else(|_| "panic entry formatting failed".to_string());
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| write_panic_entry(&entry)));
        let brief = entry.lines().take(2).collect::<Vec<_>>().join("\n\t");
        let _ = writeln!(std::io::stderr(), "{brief}\n(详情见 panic.log)");
    }));
    true
}

fn format_panic_entry(info: &std::panic::PanicHookInfo<'_>) -> String {
    let msg = info.payload().downcast_ref::<&str>().map(|s| s.to_string())
        .or_else(|| info.payload().downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "non-string panic payload".to_string());
    let loc = info.location().map(|l| format!("{}:{}", l.file(), l.line())).unwrap_or_else(|| "?".into());
    let now = chrono::Local::now().format("%Y-%m-%dT%H:%M:%S%.3f%:z");
    let home = PANIC.get().map(|c| c.home.clone());
    // 逐段"脱敏+净化"再以结构性换行拼接:段内(消息/位置/backtrace)的换行与控制
    // 字符一律转义为可见字面——panic 消息无法伪造 panic.log 的行结构(CWE-117)。
    let clean = |part: &str| -> String {
        let scrubbed = scrub_text(part, home.as_deref());
        String::from_utf8_lossy(&sanitize_line(scrubbed.as_bytes())).into_owned()
    };
    let mut lines = vec![
        format!("{now} PANIC usagebook v{} ({}/{}, pid {})",
            env!("CARGO_PKG_VERSION"),
            std::env::consts::OS, std::env::consts::ARCH, std::process::id()),
        format!("  message: {}", clean(&msg)),
        format!("  location: {}", clean(&loc)),
    ];
    if std::env::var("RUST_BACKTRACE").map(|v| v != "0").unwrap_or(false) {
        lines.push(format!("  backtrace: {}", clean(&format!("{:?}", std::backtrace::Backtrace::force_capture()))));
    }
    lines.join("\n") + "\n"
}

fn write_panic_entry(entry: &str) {
    let Some(ch) = PANIC.get() else { return };
    let dir = ch.dir.lock().unwrap_or_else(|e| e.into_inner()).clone();
    let mut guard = ch.file.lock().unwrap_or_else(|e| e.into_inner());
    let Some(pf) = guard.as_mut() else { return };
    let len = entry.len() as u64;
    if pf.size + len > PANIC_MAX_BYTES {
        let _ = std::fs::remove_file(dir.join(PANIC_OLD_NAME));
        let _ = std::fs::rename(dir.join(PANIC_NAME), dir.join(PANIC_OLD_NAME));
        match open_append_guarded(&dir.join(PANIC_NAME)) {
            Ok((f, sz)) => { pf.f = f; pf.size = sz; }
            Err(_) => { *guard = None; return; }
        }
    }
    if pf.f.write_all(entry.as_bytes()).is_ok() {
        pf.size += len;
    }
}

// ---------------------------------------------------------------------------
// Hub:进程级单例,cli 经 global() 取 sink;serve 绑定端口后调用 activate
// ---------------------------------------------------------------------------

pub enum Activation {
    /// 文件层已激活(含目录;启动标记已写入文件层)。
    Active { dir: PathBuf },
    /// LOG_DIR=off:文件层关闭(清理 .bad 残片由调用方无条件执行)。
    Off,
    /// 降级:文件层禁用,console 不受影响(含原因,已旁路告警)。
    Degraded(String),
}

pub struct LoggingHub {
    shared: Arc<Shared>,
}

impl LoggingHub {
    pub fn new(clock: Clock) -> Self {
        LoggingHub {
            shared: Arc::new(Shared {
                state: Mutex::new(None),
                cfg: Mutex::new(None),
                clock,
                max_bytes: AtomicU64::new(MAX_FILE_BYTES),
                fail_inject: AtomicUsize::new(0),
                fail_rename: AtomicUsize::new(0),
            }),
        }
    }

    pub fn sink(&self) -> FileSink {
        FileSink(Arc::clone(&self.shared))
    }

    /// 文件层激活(serve 绑定端口成功后调用;一次性命令不调用)。幂等:重复调用重开。
    pub fn activate(&self, command: &str) -> Activation {
        let cfg = resolve_config();
        let dir = match &cfg.dir {
            LogDir::Off => {
                // off 用户同样执行启动清理(§6/§8):默认目录 + 静默解析的用户参数
                // (尊重主实例的保留策略;数值告警仅文件层激活时发出,off 不告警)。
                let default_dir = data_dir().join("logs");
                if default_dir.is_dir() {
                    let keep = parse_env_u64_quiet("USAGEBOOK_LOG_KEEP_DAYS", DEFAULT_KEEP_DAYS, 0);
                    let total = parse_env_u64_quiet("USAGEBOOK_LOG_MAX_TOTAL", DEFAULT_MAX_TOTAL, MAX_FILE_BYTES);
                    run_cleanup(&default_dir, keep, total, (self.shared.clock)().date_naive());
                }
                return Activation::Off;
            }
            LogDir::At(d) => d.clone(),
        };
        if let Err(e) = create_dir_private(&dir) {
            bypass_warn("log-dir", format!("警告:日志目录不可用({} {}),文件日志降级为仅 console", e, dir.display()));
            return Activation::Degraded(format!("目录不可用:{e}"));
        }
        if !acquire_lock(&dir) {
            let _ = writeln!(std::io::stderr(), "提示:另一个 serve 实例已持有日志目录锁,本进程文件日志降级为仅 console(看板不受影响)。");
            return Activation::Degraded("日志目录锁被持有".into());
        }
        let now = (self.shared.clock)();
        let date = date_string(now);
        let (f, size) = match open_append_guarded(&main_log_path(&dir, &date)) {
            Ok(v) => v,
            Err(e) => {
                bypass_warn("log-dir", format!("警告:日志文件不可用({e}),文件日志降级为仅 console"));
                return Activation::Degraded(format!("日志文件不可用:{e}"));
            }
        };
        let panic_ok = install_panic_channel(&dir);
        *self.shared.cfg.lock().unwrap_or_else(|e| e.into_inner()) =
            Some((dir.clone(), LogConfig { dir: cfg.dir.clone(), keep_days: cfg.keep_days, max_total: cfg.max_total }));
        *self.shared.state.lock().unwrap_or_else(|e| e.into_inner()) = Some(Inner {
            dir: dir.clone(),
            file: f,
            date,
            next_midnight: next_local_midnight(now),
            bytes: size,
            torn: false,
            fail_streak: 0,
            high_water: 0,
            last_rotate_attempt: 0,
        });
        run_cleanup(&dir, cfg.keep_days, cfg.max_total, now.date_naive());
        // 启动标记(运行边界,方案 §7):info 级 → 文件层可见;console 默认 warn 不可见。
        tracing::info!(
            "启动标记 usagebook v{} {}({}/{}, pid {}) · 日志目录 {} · {}",
            env!("CARGO_PKG_VERSION"), command,
            std::env::consts::OS, std::env::consts::ARCH, std::process::id(),
            abbrev_home(&dir),
            startup_config_summary(),
        );
        if !panic_ok {
            tracing::warn!("panic 留档通道不可用,崩溃信息仅输出 stderr");
        }
        Activation::Active { dir }
    }
}

/// 目录创建(0o700;已存在且为目录则通过,符号链接目录拒绝)。
fn create_dir_private(dir: &Path) -> io::Result<()> {
    if let Ok(md) = std::fs::symlink_metadata(dir) {
        return if md.is_dir() { Ok(()) } else { Err(io::Error::new(io::ErrorKind::InvalidInput, "path is not a directory")) };
    }
    std::fs::create_dir_all(dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700));
    }
    Ok(())
}

/// 路径的 ~ 缩略(启动摘要/诊断消息统一使用,降低用户贴日志时的泄露面)。
pub fn abbrev_home(p: &Path) -> String {
    let home = crate::common::home();
    if let Ok(rest) = p.strip_prefix(&home) {
        return format!("~/{}", rest.display());
    }
    p.display().to_string()
}

/// 启动配置摘要(白名单变量;数值只打解析值;未知 USAGEBOOK_* 只记"已设置"不记值,方案 §7)。
fn startup_config_summary() -> String {
    let mut parts = Vec::new();
    for (k, v) in [
        ("USAGEBOOK_DATA_DIR", std::env::var("USAGEBOOK_DATA_DIR").ok()),
        ("USAGEBOOK_LOG_DIR", std::env::var("USAGEBOOK_LOG_DIR").ok()),
        ("USAGEBOOK_LOG_KEEP_DAYS", std::env::var("USAGEBOOK_LOG_KEEP_DAYS").ok()),
        ("USAGEBOOK_LOG_MAX_TOTAL", std::env::var("USAGEBOOK_LOG_MAX_TOTAL").ok()),
    ] {
        if let Some(v) = v.filter(|v| !v.trim().is_empty()) {
            let shown = match k {
                "USAGEBOOK_DATA_DIR" | "USAGEBOOK_LOG_DIR" => abbrev_home(&PathBuf::from(v.trim())),
                _ => v.trim().parse::<u64>().map(|n| n.to_string()).unwrap_or_else(|_| "<invalid,回退默认>".into()),
            };
            parts.push(format!("{k}={shown}"));
        }
    }
    let unknown = std::env::vars().filter(|(k, _)| k.starts_with("USAGEBOOK_")
        && !matches!(k.as_str(), "USAGEBOOK_DATA_DIR" | "USAGEBOOK_LOG_DIR" | "USAGEBOOK_LOG_KEEP_DAYS" | "USAGEBOOK_LOG_MAX_TOTAL"
            | "USAGEBOOK_STATE_DB" | "USAGEBOOK_INCREMENTAL" | "USAGEBOOK_TEST_PANIC")).count();
    if unknown > 0 { parts.push(format!("其他 USAGEBOOK_* 已设置 {unknown} 项(值不记录)")); }
    parts.join(" · ")
}

/// 进程级单例(默认本地时钟;测试用 `LoggingHub::new(注入时钟)` 自建实例)。
pub fn global() -> &'static LoggingHub {
    static HUB: OnceLock<LoggingHub> = OnceLock::new();
    HUB.get_or_init(|| LoggingHub::new(default_clock()))
}

// ---------------------------------------------------------------------------
// 测试
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{FixedOffset, TimeZone};

    /// 临时目录守卫:Drop 时递归清理(测试不再残留 ub-log-test-* 目录)。
    struct TmpGuard(PathBuf);
    impl std::ops::Deref for TmpGuard {
        type Target = Path;
        fn deref(&self) -> &Path { &self.0 }
    }
    impl AsRef<Path> for TmpGuard {
        fn as_ref(&self) -> &Path { &self.0 }
    }
    impl Drop for TmpGuard {
        fn drop(&mut self) { let _ = std::fs::remove_dir_all(&self.0); }
    }
    fn tmpdir(tag: &str) -> TmpGuard {
        let d = std::env::temp_dir().join(format!("ub-log-test-{tag}-{}-{}", std::process::id(), std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().subsec_nanos()));
        std::fs::create_dir_all(&d).unwrap();
        TmpGuard(d)
    }

    /// 可变时钟:base + 原子偏移秒(测试推进时间用)。
    fn mutable_clock(base: chrono::DateTime<FixedOffset>) -> (Clock, Arc<std::sync::atomic::AtomicI64>) {
        use chrono::Offset;
        let base_ts = base.timestamp();
        let off = Arc::new(std::sync::atomic::AtomicI64::new(0));
        let tz = base.offset().fix();
        let off2 = off.clone();
        let clock: Clock = Arc::new(move || {
            let ts = base_ts + off2.load(Ordering::Relaxed);
            tz.timestamp_opt(ts, 0).unwrap()
        });
        (clock, off)
    }

    /// 直接装配 Inner(不经 activate),便于单测滚动/清理核心。
    fn armed_sink(clock: Clock, dir: &Path, max_bytes: u64) -> FileSink {
        let hub = LoggingHub::new(clock.clone());
        let sink = hub.sink();
        sink.set_max_bytes_for_test(max_bytes);
        let now = (clock)();
        std::fs::create_dir_all(dir).unwrap();
        let (f, size) = open_append_guarded(&main_log_path(dir, &date_string(now))).unwrap();
        *hub.shared_state().lock().unwrap_or_else(|e| e.into_inner()) = Some(Inner {
            dir: dir.to_path_buf(),
            file: f,
            date: date_string(now),
            next_midnight: next_local_midnight(now),
            bytes: size,
            torn: false,
            fail_streak: 0,
            high_water: 0,
            last_rotate_attempt: 0,
        });
        *hub.shared_cfg().lock().unwrap_or_else(|e| e.into_inner()) = Some((dir.to_path_buf(), LogConfig { dir: LogDir::At(dir.to_path_buf()), keep_days: 14, max_total: DEFAULT_MAX_TOTAL }));
        sink
    }

    // 让测试模块能装配 hub 内部状态。
    impl LoggingHub {
        #[cfg(test)]
        fn shared_state(&self) -> &Mutex<Option<Inner>> { &self.shared.state }
        #[cfg(test)]
        fn shared_cfg(&self) -> &Mutex<Option<(PathBuf, LogConfig)>> { &self.shared.cfg }
    }

    use std::sync::atomic::Ordering;

    #[test]
    fn sanitize_escapes_controls_keeps_utf8_and_terminator() {
        let input = b"a\nb\x1b[31mc\td\xe4\xb8\xad\n";
        let out = sanitize_line(input);
        let s = String::from_utf8(out.clone()).unwrap();
        assert!(s.contains("a\\nb"));
        assert!(s.contains("\\x1b[31m") || s.contains("\\x1b"));
        assert!(s.contains('\t'), "TAB 保留");
        assert!(s.contains('中'), "UTF-8 中文不破坏");
        assert_eq!(*out.last().unwrap(), b'\n', "行终止符保留");
        assert!(!s.ends_with("\\n\n"), "行尾不得出现字面反斜杠n双尾巴");
        // C1 控制区(U+0085 = C2 85)
        let c1 = "x\u{85}y".as_bytes();
        assert!(String::from_utf8(sanitize_line(c1)).unwrap().contains("\\x85"));
    }

    #[test]
    fn scrub_redacts_marker_words_and_home() {
        let home = Path::new("/Users/tester");
        let text = "key sk-abc123 and token=zzz plus UBCANARY-sk-canary tail /Users/tester/proj/x.json";
        let out = scrub_text(text, Some(home));
        assert!(!out.contains("sk-abc123") && !out.contains("token=zzz") && !out.contains("UBCANARY"));
        assert!(out.contains("[REDACTED]"));
        assert!(out.contains("~/proj/x.json"), "家目录缩略: {out}");
        let pem = "line1\n-----BEGIN PRIVATE KEY-----\nabc\nend";
        let out2 = scrub_text(pem, None);
        assert!(!out2.contains("PRIVATE"));
    }

    #[test]
    fn rotation_creates_shards_and_monotonic_after_cleanup() {
        let dir = tmpdir("rotate");
        let tz = FixedOffset::east_opt(8 * 3600).unwrap();
        let base = tz.with_ymd_and_hms(2026, 10, 14, 10, 0, 0).unwrap();
        let (clock, _off) = mutable_clock(base);
        let sink = armed_sink(clock.clone(), &dir, 200);
        let line = format!("2026-10-14T10:00:00.000+08:00 INFO x {}\n", "z".repeat(50));
        // 6 次 × 76B:第 3、6 次写后触发轮转 → 分片 1、2,高水位=2。
        for _ in 0..6 { sink.write_line(line.as_bytes()).unwrap(); }
        assert!(shard_log_path(&dir, "20261014", 1).exists(), "首分片");
        assert!(shard_log_path(&dir, "20261014", 2).exists(), "第二分片");
        // 清理删掉分片后,进程内高水位不回退:新分片号仍递增(不复用 1/2)。
        std::fs::remove_file(shard_log_path(&dir, "20261014", 1)).unwrap();
        std::fs::remove_file(shard_log_path(&dir, "20261014", 2)).unwrap();
        for _ in 0..6 { sink.write_line(line.as_bytes()).unwrap(); }
        assert!(shard_log_path(&dir, "20261014", 3).exists(), "删号后新号从 3 继续(高水位不回退)");
        assert!(!shard_log_path(&dir, "20261014", 1).exists(), "不复用已删号");
    }

    #[test]
    fn rotation_crosses_midnight_by_local_not_utc() {
        let dir = tmpdir("midnight");
        // UTC+14:本地 23:58 == UTC 09:58;本地跨零点轮转必须在本地零点。
        let tz = FixedOffset::east_opt(14 * 3600).unwrap();
        let base = tz.with_ymd_and_hms(2026, 10, 14, 23, 58, 0).unwrap();
        let (clock, off) = mutable_clock(base);
        let sink = armed_sink(clock.clone(), &dir, 100 * 1024 * 1024);
        sink.write_line(b"before midnight\n").unwrap();
        assert!(main_log_path(&dir, "20261014").exists());
        off.store(180, Ordering::Relaxed); // +3min → 本地 2026-10-15 00:01
        sink.write_line(b"after midnight\n").unwrap();
        assert!(main_log_path(&dir, "20261015").exists(), "本地零点后开新日期文件");
        let old = std::fs::read_to_string(main_log_path(&dir, "20261014")).unwrap();
        assert!(old.contains("before") && !old.contains("after"));
    }

    #[test]
    fn write_failure_torn_repair_and_degrade() {
        let dir = tmpdir("enospc");
        let tz = FixedOffset::east_opt(8 * 3600).unwrap();
        let base = tz.with_ymd_and_hms(2026, 10, 14, 12, 0, 0).unwrap();
        let (clock, _off) = mutable_clock(base);
        let sink = armed_sink(clock.clone(), &dir, 100 * 1024 * 1024);
        sink.write_line(b"line one\n").unwrap();
        sink.inject_write_failures(1); // 一次失败 → 撕裂(半行丢失不可知,下次写前补 \n)
        sink.write_line(b"line two PART").unwrap();
        assert!(sink.is_active());
        sink.write_line(b"line three\n").unwrap();
        let content = std::fs::read_to_string(main_log_path(&dir, "20261014")).unwrap();
        // 补行修复后每行都以 \n 结尾。
        for l in content.split_inclusive('\n') {
            assert!(l.ends_with('\n'), "无撕裂半行残留: {l:?}");
        }
        assert!(content.contains("line one"));
        // 连续 3 次失败(每次 write_line 消耗一次注入)→ 降级(文件层禁用,后续写静默丢弃)。
        sink.inject_write_failures(3);
        for _ in 0..3 { sink.write_line(b"x\n").unwrap(); }
        assert!(!sink.is_active(), "连续失败后文件层降级");
        sink.write_line(b"y\n").unwrap(); // 降级后写入不 panic
    }

    #[test]
    fn cleanup_rules_abc() {
        let dir = tmpdir("cleanup");
        let mk = |name: &str, size: u64| {
            std::fs::write(dir.join(name), vec![b'x'; size as usize]).unwrap();
        };
        let today = chrono::NaiveDate::from_ymd_opt(2026, 10, 14).unwrap();
        // keep=14 → cutoff=09-30:< cutoff 删。
        mk("usagebook.20260929.log", 10); // 15 天前 → 删
        mk("usagebook.20260930.log", 10); // 恰 14 天前 → 留(边界)
        mk("usagebook.20260925.log", 10); // 19 天前整组 → 删
        mk("usagebook.20260925.001.log", 10);
        mk("usagebook.20260925.002.log", 10);
        mk("usagebook.20261014.log", 10); // 当日主文件
        mk("usagebook.20261014.001.log", 30);
        mk("usagebook.20261014.002.log", 10);
        mk("panic.log", 5);
        mk("panic.log.old", 5);
        mk("userfile.txt", 999); // 围栏外 → 不动
        mk(".lock", 1); // 围栏外 → 不动
        run_cleanup(&dir, 14, 100_000_000, today);
        assert!(!dir.join("usagebook.20260929.log").exists());
        assert!(!dir.join("usagebook.20260925.002.log").exists(), "超期整组删");
        assert!(dir.join("usagebook.20260930.log").exists(), "恰 14 天边界保留");
        assert!(dir.join("userfile.txt").exists() && dir.join(".lock").exists(), "围栏外不动");
        // 规则 B(封顶 45,当前总量 70):先删最老历史组(0930),再当日分片最小号先、最大号保留。
        run_cleanup(&dir, 14, 45, today);
        assert!(!dir.join("usagebook.20260930.log").exists(), "最老历史组先删");
        assert!(!dir.join("usagebook.20261014.001.log").exists(), "当日小号分片次之");
        assert!(dir.join("usagebook.20261014.002.log").exists(), "当日最大号保留");
        assert!(dir.join("usagebook.20261014.log").exists(), "活动主文件永不删");
        assert!(dir.join("panic.log").exists(), "未到极端不删 panic");
        // 极端(封顶 1):连最大号与 panic 归档/正文都可删,主文件仍在(预期常态)。
        run_cleanup(&dir, 14, 1, today);
        assert!(dir.join("usagebook.20261014.log").exists());
        assert!(!dir.join("usagebook.20261014.002.log").exists());
        assert!(!dir.join("panic.log").exists());
    }

    #[cfg(unix)]
    #[test]
    fn cleanup_counts_regular_files_only() {
        let dir = tmpdir("symlinkcount");
        let today = chrono::NaiveDate::from_ymd_opt(2026, 10, 14).unwrap();
        std::os::unix::fs::symlink("/etc/hosts", dir.join("usagebook.20261013.log")).unwrap();
        mkfile(&dir, "usagebook.20261014.log", 10);
        run_cleanup(&dir, 14, 1, today); // 封顶 1:能删的只有当日主文件——但它豁免 → 全保留
        assert!(dir.join("usagebook.20261013.log").symlink_metadata().unwrap().is_symlink(), "symlink 不计不删");
        assert!(dir.join("usagebook.20261014.log").exists());
    }

    fn mkfile(dir: &Path, name: &str, size: u64) {
        std::fs::write(dir.join(name), vec![b'x'; size as usize]).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn lockfile_acquire_stale_and_conflict() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tmpdir("lock");
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert!(acquire_lock(&dir), "首次取锁");
        assert!(!acquire_lock(&dir), "持锁期间二次取锁失败");
        let lp = dir.join(LOCK_NAME);
        assert_eq!(lp.symlink_metadata().unwrap().permissions().mode() & 0o777, 0o600, ".lock 0600");
        std::fs::remove_file(&lp).unwrap();
        // 陈旧锁:pid 必然不存在(> pid_max 量级)→ 回收后取锁成功。
        std::fs::write(&lp, "999999999").unwrap();
        assert!(acquire_lock(&dir), "陈旧锁回收");
        std::fs::remove_file(&lp).unwrap();
        // 存活 pid(本进程)→ 占用。
        std::fs::write(&lp, std::process::id().to_string()).unwrap();
        assert!(!acquire_lock(&dir), "活锁不回收");
    }

    #[test]
    fn config_off_sentinel_and_clamps() {
        // off 归一化(不真实设置进程 env,直接测 is_off)。
        for v in ["off", "OFF", " Off ", "0", "false"] {
            assert!(is_off(v), "{v} 应识别为 off");
        }
        assert!(!is_off("on") && !is_off(""));
        assert!(!is_off("/tmp/logs"));
    }

    #[test]
    fn shard_numbering_past_999() {
        let dir = tmpdir("nnn");
        mkfile(&dir, "usagebook.20261014.999.log", 1);
        mkfile(&dir, "usagebook.20261014.1000.log", 1);
        let n = next_shard_no(&dir, "20261014", 0);
        assert_eq!(n, 1001, "999 之后自然进位,不封顶");
        let p = shard_log_path(&dir, "20261014", 1001);
        assert!(p.to_str().unwrap().ends_with(".1001.log"));
        let p3 = shard_log_path(&dir, "20261014", 3);
        assert!(p3.to_str().unwrap().ends_with(".003.log"), "3 位零补齐");
    }

    #[test]
    fn next_midnight_uses_local_offset() {
        let tz = FixedOffset::east_opt(14 * 3600).unwrap();
        let now = tz.with_ymd_and_hms(2026, 10, 14, 9, 0, 0).unwrap(); // 本地上午 9 点 = UTC 前日 19:00
        let nm = next_local_midnight(now);
        let expected = tz.with_ymd_and_hms(2026, 10, 15, 0, 0, 0).unwrap().timestamp();
        assert_eq!(nm, expected, "UTC+14 的本地零点,不是 UTC 零点");
    }

    #[test]
    fn rename_failure_keeps_writing_and_retries_after_window() {
        let dir = tmpdir("renamefail");
        let tz = FixedOffset::east_opt(8 * 3600).unwrap();
        let base = tz.with_ymd_and_hms(2026, 10, 14, 12, 0, 0).unwrap();
        let (clock, off) = mutable_clock(base);
        let sink = armed_sink(clock.clone(), &dir, 150);
        let line = |i: u8| format!("2026-10-14T12:00:00.000+08:00 INFO line{i} padding padding\n");
        // 写满触发轮转,rename 注入失败 → 继续写当前文件,文件层不降级。
        sink.inject_rename_failures(1);
        for i in 0..5 { sink.write_line(line(i).as_bytes()).unwrap(); }
        assert!(sink.is_active(), "rename 失败不得降级文件层");
        assert!(!shard_log_path(&dir, "20261014", 1).exists(), "失败期间无分片");
        // 30s 窗口后重试成功(限频只作用于失败重试)。
        off.store(31, Ordering::Relaxed);
        sink.write_line(line(9).as_bytes()).unwrap();
        assert!(shard_log_path(&dir, "20261014", 1).exists(), "窗口后重试轮转成功");
        // line9 写入后才触发轮转,内容随旧主文件进入分片——失败期间与重试后的内容都不丢。
        let shard = std::fs::read_to_string(shard_log_path(&dir, "20261014", 1)).unwrap();
        for i in 0..5 { assert!(shard.contains(&format!("line{i}")), "失败期间内容不丢: line{i}"); }
        assert!(shard.contains("line9"), "重试后内容不丢");
    }

    #[test]
    fn concurrent_lines_never_interleave() {
        let dir = tmpdir("concurrent");
        let tz = FixedOffset::east_opt(8 * 3600).unwrap();
        let base = tz.with_ymd_and_hms(2026, 10, 14, 12, 0, 0).unwrap();
        let (clock, _off) = mutable_clock(base);
        let sink = armed_sink(clock.clone(), &dir, 100 * 1024 * 1024);
        let sink = std::sync::Arc::new(sink);
        let mut handles = Vec::new();
        for t in 0..4u8 {
            let sink = sink.clone();
            handles.push(std::thread::spawn(move || {
                for i in 0..200u16 {
                    let body = format!("T{t}-L{i:04} {}", "x".repeat(40));
                    let line = format!("2026-10-14T12:00:00.000+08:00 INFO {body}\n");
                    sink.write_line(line.as_bytes()).unwrap();
                }
            }));
        }
        for h in handles { h.join().unwrap(); }
        let content = std::fs::read_to_string(main_log_path(&dir, "20261014")).unwrap();
        let mut seen = std::collections::HashSet::new();
        for l in content.lines() {
            let marker = l.split_whitespace().nth(2).unwrap_or("");
            assert!(marker.starts_with('T') && marker.contains('-'),
                "行结构完整无交错: {l}");
            assert!(seen.insert(marker.to_string()), "每行唯一: {marker}");
        }
        assert_eq!(seen.len(), 800, "四线程×200 行全部落盘");
    }

    /// 确定性序列回归(替代 proptest,方案 §11):多组写序列后断言不变量。
    #[test]
    fn deterministic_sequences_invariants() {
        let tz = FixedOffset::east_opt(8 * 3600).unwrap();
        // 序列 1:风暴写 + 频繁清理触发(小 max_total)——总量有界、活动文件存在、NNN 单调。
        {
            let dir = tmpdir("seq-storm");
            let base = tz.with_ymd_and_hms(2026, 10, 14, 8, 0, 0).unwrap();
            let (clock, _off) = mutable_clock(base);
            let sink = armed_sink(clock.clone(), &dir, 100);
            sink.set_max_total_for_test(400);
            let line = |n: u8| format!("2026-10-14T08:00:00.000+08:00 INFO seq{n} {}\n", "y".repeat(40));
            for i in 0..60 { sink.write_line(line(i % 10).as_bytes()).unwrap(); }
            assert!(main_log_path(&dir, "20261014").exists(), "活动文件存在");
            let total: u64 = std::fs::read_dir(&dir).unwrap().flatten()
                .filter_map(|e| std::fs::symlink_metadata(e.path()).ok().filter(|m| m.is_file()).map(|m| m.len()))
                .sum();
            assert!(total <= 400 + 100, "总量 ≤ 封顶+单文件: {total}");
            let shards: Vec<u32> = std::fs::read_dir(&dir).unwrap().flatten()
                .filter_map(|e| e.file_name().to_str().and_then(parse_shard_no).map(|(_, n)| n))
                .collect();
            let mut sorted = shards.clone();
            sorted.sort_unstable();
            sorted.dedup();
            assert_eq!(shards.len(), sorted.len(), "分片号无重复");
        }
        // 序列 2:写→跨日→写→风暴,两日文件并存且日期正确。
        {
            let dir = tmpdir("seq-crossday");
            let base = tz.with_ymd_and_hms(2026, 10, 14, 23, 58, 0).unwrap();
            let (clock, off) = mutable_clock(base);
            let sink = armed_sink(clock.clone(), &dir, 100 * 1024 * 1024);
            sink.write_line(b"d1\n").unwrap();
            off.store(180, Ordering::Relaxed);
            for i in 0..3 { sink.write_line(format!("d2-{i}\n").as_bytes()).unwrap(); }
            assert!(main_log_path(&dir, "20261014").exists() && main_log_path(&dir, "20261015").exists());
        }
    }

    fn parse_shard_no(name: &str) -> Option<(String, u32)> {
        let rest = name.strip_prefix("usagebook.")?;
        let rest = rest.strip_suffix(".log")?;
        let (d, n) = rest.split_once('.')?;
        Some((d.to_string(), n.parse().ok()?))
    }

    #[test]
    fn parse_u64_value_clamps() {
        assert_eq!(parse_u64_value("14", 14, 0), (14, None));
        assert_eq!(parse_u64_value("0", 14, 0), (0, None), "0 对 KEEP_DAYS 合法");
        assert!(matches!(parse_u64_value("-1", 14, 0), (14, Some(_))), "负数非法回退默认");
        assert!(matches!(parse_u64_value("abc", 14, 0), (14, Some(_))));
        assert_eq!(parse_u64_value("1024", 20 * 1024 * 1024, 20 * 1024 * 1024).0, 20 * 1024 * 1024, "低于下限钳制");
        assert_eq!(parse_u64_value("999", 7, 5), (999, None));
    }

    #[test]
    fn clock_rollback_keeps_size_rotation_working() {
        let dir = tmpdir("rollback");
        let tz = FixedOffset::east_opt(8 * 3600).unwrap();
        let base = tz.with_ymd_and_hms(2026, 10, 14, 12, 0, 0).unwrap();
        let (clock, off) = mutable_clock(base);
        let sink = armed_sink(clock.clone(), &dir, 150);
        let line = |i: u8| format!("2026-10-14T12:00:00.000+08:00 INFO r{i} {}\n", "k".repeat(40));
        sink.inject_rename_failures(1);
        for i in 0..5 { sink.write_line(line(i).as_bytes()).unwrap(); } // 首次轮转失败,last=T
        assert!(!shard_log_path(&dir, "20261014", 1).exists());
        off.store(-3600, Ordering::Relaxed); // 时钟回拨 1h → now < last
        for i in 5..8 { sink.write_line(line(i).as_bytes()).unwrap(); }
        assert!(shard_log_path(&dir, "20261014", 1).exists(), "回拨期间窗口视为已过,轮转不停止");
    }

    #[test]
    fn cleanup_shields_yesterday_main_from_external_owner() {
        let dir = tmpdir("shield");
        let mk = |name: &str, size: u64| std::fs::write(dir.join(name), vec![b'x'; size as usize]).unwrap();
        let today = chrono::NaiveDate::from_ymd_opt(2026, 10, 14).unwrap();
        mk("usagebook.20261012.log", 50);   // 前天:不可能是活动句柄 → 可删
        mk("usagebook.20261013.log", 50);   // 昨日无分片主文件:另一实例的跨零点活动句柄 → 豁免
        mk("usagebook.20261013.001.log", 50);
        run_cleanup(&dir, 14, 50, today);   // 总量 150 > 50
        assert!(!dir.join("usagebook.20261012.log").exists(), "前天组先删");
        assert!(!dir.join("usagebook.20261013.001.log").exists(), "昨日分片可删");
        assert!(dir.join("usagebook.20261013.log").exists(), "昨日无分片主文件豁免(活动句柄保护)");
    }

    #[cfg(unix)]
    #[test]
    fn unix_lock_decision_matrix() {
        // 纯决策函数:EPERM 分支(进程外行为)的表驱动覆盖。
        assert!(!unix_lock_stale(None), "rc==0 存活");
        assert!(unix_lock_stale(Some(libc::ESRCH)), "ESRCH 陈旧");
        assert!(!unix_lock_stale(Some(libc::EPERM)), "EPERM=他户进程,视为存活(§5 裁决)");
        assert!(!unix_lock_stale(Some(libc::EINVAL)), "其他错误按占用");
    }

    #[cfg(unix)]
    #[test]
    fn future_mtime_lock_treated_as_stale() {
        let dir = tmpdir("futurelock");
        let lp = dir.join(LOCK_NAME);
        std::fs::write(&lp, "999999999").unwrap();
        if let Ok(f) = std::fs::OpenOptions::new().write(true).open(&lp) {
            let future = std::time::SystemTime::now() + std::time::Duration::from_secs(3600);
            let _ = f.set_modified(future);
        }
        assert_eq!(lock_age_secs(&lp), Some(u64::MAX), "未来 mtime 视为足够陈旧");
    }

    #[test]
    fn state_mutex_poisoning_does_not_break_writer() {
        let dir = tmpdir("poison");
        let tz = FixedOffset::east_opt(8 * 3600).unwrap();
        let base = tz.with_ymd_and_hms(2026, 10, 14, 12, 0, 0).unwrap();
        let (clock, _off) = mutable_clock(base);
        let hub = LoggingHub::new(clock.clone());
        let sink = hub.sink();
        sink.set_max_bytes_for_test(100 * 1024 * 1024);
        // 毒化状态锁:持锁 panic(模拟格式化路径 panic)。
        let state = hub.shared_state();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _g = state.lock().unwrap();
            panic!("poison the writer state");
        }));
        assert!(state.is_poisoned());
        // into_inner 恢复:毒化后仍可武装并写入。
        let now = (clock)();
        std::fs::create_dir_all(&*dir).unwrap();
        let (f, size) = open_append_guarded(&main_log_path(&dir, &date_string(now))).unwrap();
        *hub.shared_state().lock().unwrap_or_else(|e| e.into_inner()) = Some(Inner {
            dir: dir.to_path_buf(),
            file: f,
            date: date_string(now),
            next_midnight: next_local_midnight(now),
            bytes: size,
            torn: false,
            fail_streak: 0,
            high_water: 0,
            last_rotate_attempt: 0,
        });
        sink.write_line(b"after poison\n").unwrap();
        assert!(std::fs::read_to_string(main_log_path(&dir, &date_string(now))).unwrap().contains("after poison"));
    }

    #[test]
    fn cleanup_exactly_at_cap_deletes_nothing() {
        let dir = tmpdir("capexact");
        let mk = |name: &str, size: u64| std::fs::write(dir.join(name), vec![b'x'; size as usize]).unwrap();
        let today = chrono::NaiveDate::from_ymd_opt(2026, 10, 14).unwrap();
        mk("usagebook.20261012.log", 50); // 前天(不豁免)
        mk("usagebook.20261013.log", 50); // 昨日(活动句柄豁免)
        mk("usagebook.20261014.log", 50); // 当日(豁免)
        run_cleanup(&dir, 14, 150, today); // 恰好 = 封顶,不删
        assert!(dir.join("usagebook.20261012.log").exists());
        run_cleanup(&dir, 14, 149, today); // 超 1 字节 → 删最老可删组(前天)
        assert!(!dir.join("usagebook.20261012.log").exists());
        assert!(dir.join("usagebook.20261013.log").exists(), "昨日主文件仍豁免");
    }
}
