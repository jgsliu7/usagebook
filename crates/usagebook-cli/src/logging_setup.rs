//! 订阅器组装(tracing-subscriber 落 cli;文件层设施在 usagebook-core::logging)。
//!
//! 契约(方案 §3/§5):
//! - console 层**必须**写 stderr(fmt 层默认 stdout,会污染 `--json` 管道);
//! - console 与文件层共用净化转义(CWE-117):行缓冲 + 单次 write_all;
//! - 时间戳 RFC3339 带本地时区偏移与毫秒(tracing-subscriber 默认计时器是 UTC);
//! - `RUST_LOG` 显式设置时完全接管(此时不附加 usagebook_cli 的 console 抑制);
//! - 文件层写入器持有 core 的 FileSink,未激活时写入静默丢弃(serve 激活前零副作用)。
//!
//! 初始化时序:main() 中必须在 `Args::parse()` **之后**调用(clap 对
//! `--version`/`--help` 先退出,初始化抢跑会破坏 `--version` 全等输出契约)。

use std::io::Write;
use tracing_subscriber::fmt::time::FormatTime;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer;
use tracing_subscriber::{filter::LevelFilter, fmt, registry::Registry, EnvFilter};
use usagebook_core::logging::FileSink;

/// RFC3339 + 本地时区偏移 + 毫秒(文件层与 console 层共用)。
struct LocalTimer;

impl FormatTime for LocalTimer {
    fn format_time(&self, w: &mut fmt::format::Writer<'_>) -> std::fmt::Result {
        write!(
            w,
            "{}",
            chrono::Local::now().format("%Y-%m-%dT%H:%M:%S%.3f%:z")
        )
    }
}

/// 行缓冲 + 净化:攒到行尾(\n)才整体转义并单次写出(进程内行完整,多进程下行内不交错)。
struct SanitizedLineWriter<W: Write> {
    inner: W,
    buf: Vec<u8>,
}

impl<W: Write> SanitizedLineWriter<W> {
    fn new(inner: W) -> Self {
        SanitizedLineWriter {
            inner,
            buf: Vec::new(),
        }
    }

    fn flush_complete_lines(&mut self) -> std::io::Result<()> {
        while let Some(pos) = self.buf.iter().position(|&b| b == b'\n') {
            let line: Vec<u8> = self.buf.drain(..=pos).collect();
            let sanitized = usagebook_core::logging::sanitize_line(&line);
            self.inner.write_all(&sanitized)?;
        }
        Ok(())
    }
}

impl<W: Write> Write for SanitizedLineWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buf.extend_from_slice(buf);
        self.flush_complete_lines()?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if !self.buf.is_empty() {
            let rest = std::mem::take(&mut self.buf);
            let sanitized = usagebook_core::logging::sanitize_line(&rest);
            self.inner.write_all(&sanitized)?;
        }
        self.inner.flush()
    }
}

/// console 层 MakeWriter → stderr(净化 + 行缓冲)。
#[derive(Clone, Default)]
struct SanitizedStderr;

impl<'a> MakeWriter<'a> for SanitizedStderr {
    type Writer = SanitizedLineWriter<std::io::Stderr>;

    fn make_writer(&'a self) -> Self::Writer {
        SanitizedLineWriter::new(std::io::stderr())
    }
}

/// 文件层 MakeWriter → core FileSink(净化 + 行缓冲;未激活时写入丢弃)。
#[derive(Clone)]
struct FileWriter(FileSink);

impl<'a> MakeWriter<'a> for FileWriter {
    type Writer = SanitizedLineWriter<FileSink>;

    fn make_writer(&'a self) -> Self::Writer {
        SanitizedLineWriter::new(self.0.clone())
    }
}

fn level_str(verbosity: u8) -> &'static str {
    match verbosity {
        0 => "warn",
        1 => "debug",
        _ => "trace",
    }
}

/// 组装并安装全局订阅器(一次性幂等:`set_global_default` 仅首次生效)。
///
/// - 未设 `RUST_LOG`:console 层 = `{warn|debug|trace},-usagebook_cli`
///   (`-usagebook_cli` 抑制双轨站点的 console 重复:人机消息已直出 stderr);
///   文件层固定 INFO(serve 激活后生效),`-v/-vv` 只影响 console。
/// - 设了 `RUST_LOG`:两层同用该 EnvFilter,完全接管(重复输出由用户自担,README 注明)。
pub fn init(verbosity: u8) {
    // console 与文件层对 RUST_LOG 的决策必须同源:非法指令时两层都回退默认,
    // 不得出现"console 回退而文件层失去级别上限"的分叉。
    let env_filter = EnvFilter::try_from_default_env().ok();
    let console_filter = match &env_filter {
        Some(f) => f.clone(),
        None => EnvFilter::new(format!("{},-usagebook_cli", level_str(verbosity))),
    };
    let file_filter = if env_filter.is_some() {
        None // RUST_LOG 已接管全局;文件层不再叠加级别上限。
    } else {
        Some(LevelFilter::INFO)
    };

    let console_layer = fmt::layer()
        .with_writer(SanitizedStderr)
        // 无条件关 ANSI:净化器会把 ESC 序列转义成字面 \x1b 垃圾(TTY 下必现),
        // 且放行 CSI 会重新打开终端注入面(ADR D8)——console 一律纯文本。
        .with_ansi(false)
        .with_timer(LocalTimer)
        .with_filter(console_filter);

    let file_layer = fmt::layer()
        .with_writer(FileWriter(usagebook_core::logging::global().sink()))
        .with_ansi(false)
        .with_timer(LocalTimer);
    let file_layer = match file_filter {
        Some(lf) => file_layer.with_filter(lf).boxed(),
        None => file_layer.boxed(),
    };

    let _ = Registry::default()
        .with(console_layer)
        .with(file_layer)
        .try_init();
}
