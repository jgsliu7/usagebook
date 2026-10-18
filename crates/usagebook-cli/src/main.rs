//! usagebook —— 各 AI 编程工具 token 用量统计(macOS 优先,Windows 实验性支持)。
//! 数据源规范见 docs/数据源档案/(先档案、后代码)。
//! 三种用法:
//!   usagebook            终端报表扫描(--json 机器可读)
//!   usagebook serve      本地看板(自动刷新,曲线/来源/模型)
//!   usagebook doctor     本机数据源自检(哪些源会生效/在哪/多大)

use usagebook_core::{registered_sources, report};
use clap::{Parser, Subcommand};

mod serve;
mod logging_setup;

#[derive(Subcommand)]
enum Command {
    /// 启动本地看板(HTTP,自动刷新)
    Serve {
        /// 监听端口
        #[arg(long, default_value_t = DEFAULT_SERVE_PORT)]
        port: u16,
        /// 无前端驱动时的重扫回退间隔(秒,最小 5;看板轮询 ?arf= 声明时按其档位)
        #[arg(long, default_value_t = 60)]
        refresh_secs: u64,
        /// 服务就绪后打开本机看板
        #[arg(long)]
        open: bool,
    },
    /// 本机数据源自检:发现目录；--check 检查实际解析结果(退出码:0 正常,2 参数错误,3 数据性失败)
    Doctor {
        #[arg(long)]
        check: bool,
    },
}

#[derive(Parser)]
#[command(name = "usagebook", version, about = "Mac/Windows 上各 AI 编程工具 token 用量统计")]
struct Args {
    /// 只统计指定数据源(源 id 见 usagebook doctor;仅终端报表生效,看板恒为全部)
    #[arg(long, default_value = "all")]
    source: String,
    /// 只看最近 N 天
    #[arg(long)]
    days: Option<usize>,
    /// 抽样调试:每源最多解析 N 个文件
    #[arg(long)]
    max_files: Option<usize>,
    /// 以 JSON 输出(与终端报表同口径,便于脚本消费)
    #[arg(long, global = true)]
    json: bool,
    /// 提升诊断输出级别:一次 debug,两次 trace(仅未设 RUST_LOG 时生效)
    #[arg(short = 'v', long, action = clap::ArgAction::Count, global = true)]
    verbose: u8,
    #[command(subcommand)]
    command: Option<Command>,
}

/// serve 的默认端口;doctor 的看板在跑探测也用同一常量(自定义 --port 时该探测不适用)
const DEFAULT_SERVE_PORT: u16 = 7878;

/// 各源认的环境变量(以各源 discover_roots 实际读取为准;omp 固定根不认环境变量)。
/// 仅供 doctor 展示,错列/漏列不影响扫描本身;改各源发现变量时须同步此表。
const ENV_OVERRIDES: &[&str] = &[
    "XDG_CONFIG_HOME",
    "CLAUDE_CONFIG_DIR",
    "CODEX_HOME",
    "KIMI_CODE_HOME",
    "ZCODE_HOME",
    "WORKBUDDY_HOME",
    "DSH_HOME",
    "MINIMAX_CODE_HOME",
    "MINIMAX_DATA_DIR",
    "MAVIS_DATA_DIR",
    "PI_CODING_AGENT_DIR",
    "CLINE_SESSION_DATA_DIR",
    "CLINE_DATA_DIR",
    "CLINE_DIR",
    // —— Community 源 ——
    "QWEN_RUNTIME_DIR",
    "QWEN_HOME",
    "GROK_HOME",
    "AMP_DATA_DIR",
    "OPENCLAW_STATE_DIR",
    "GEMINI_DATA_DIR",
    "GEMINI_HOME",
    "COPILOT_HOME",
    "AI_USAGE_VSCODE_ROOTS",
    "CODEBUFF_DATA_DIR",
    "DROID_SESSIONS_DIR",
    "FACTORY_DIR",
    "AI_USAGE_GOOSE_DB",
    "GOOSE_PATH_ROOT",
    "KILO_DATA_DIR",
    "KILO_HOME",
    "HERMES_HOME",
    "KIRO_CLI_DB_PATH",
    "OPENCODE_DATA_DIR",
];

/// 运行日志的环境变量(单列小节展示;数字/字节/单目录语义,不支持多根)。
const LOG_ENV_OVERRIDES: &[&str] = &[
    "USAGEBOOK_LOG_DIR",
    "USAGEBOOK_LOG_KEEP_DAYS",
    "USAGEBOOK_LOG_MAX_TOTAL",
];

fn main() {
    let args = Args::parse();
    // 订阅器初始化必须在 parse 之后:clap 对 --version/--help 先退出,抢跑会污染其全等输出。
    logging_setup::init(args.verbose);
    if let Some(cmd) = args.command {
        match cmd {
            Command::Doctor { check } => {
                doctor(check, args.json);
                return;
            }
            Command::Serve { port, refresh_secs, open } => {
                if args.source != "all"
                    || args.days.is_some()
                    || args.max_files.is_some()
                {
                    eprintln!(
                        "错误:--source / --days / --max-files 与 serve 不兼容(看板始终统计全部来源),\n请去掉这些参数后重试,例:usagebook serve --port 8080"
                    );
                    std::process::exit(2);
                }
                serve::run(port, refresh_secs, open);
                return;
            }
        }
    }
    let adapters = registered_sources();
    // --source 动态校验:源 id 集随注册表增长,不再维护枚举(doctor 输出全集)
    if args.source != "all" && !adapters.iter().any(|a| a.id() == args.source) {
        eprintln!(
            "错误:未知源 '{}';可用值:all 或 {}",
            args.source,
            adapters.iter().map(|a| a.id()).collect::<Vec<_>>().join(" / ")
        );
        std::process::exit(2);
    }
    let mut all_records = Vec::new();

    if !args.json {
        println!("usagebook v{} —— 数据源扫描\n", env!("CARGO_PKG_VERSION"));
    }
    for a in &adapters {
        if args.source != "all" && a.id() != args.source {
            continue;
        }
        let roots = a.discover_roots();
        let roots_disp = if roots.is_empty() {
            "(未发现)".to_string()
        } else {
            roots
                .iter()
                .map(|p| p.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        };
        if !args.json {
            println!("[{}] 根: {}", a.id(), roots_disp);
        }
        let t0 = std::time::Instant::now();
        // 抽样调试(--max-files)保持一次性直读;其余走统一缓存缝(L1+L2;。
        let (records, stats) = if args.max_files.is_some() {
            a.scan(args.max_files)
        } else {
            usagebook_core::common::scan_source_cached(a.as_ref(), &roots)
        };
        if !args.json {
            let replay = if stats.replayed > 0 {
                format!(" · 重绕 {}", stats.replayed)
            } else {
                String::new()
            };
            let clash = if stats.key_clash > 0 {
                format!(" · 键碰撞 {}", stats.key_clash)
            } else {
                String::new()
            };
            let suffix = replay + &clash;
            println!(
                "    文件 {}/{} · 行 {} · 未知类型 {} · 坏行 {} · 用量记录 {} · {}ms{}",
                stats.files_parsed,
                stats.files_seen,
                stats.lines_total,
                stats.lines_unknown_type,
                stats.lines_bad_json,
                records.len(),
                t0.elapsed().as_millis(),
                suffix
            );
            for n in &stats.notes {
                println!("    · {}", n);
            }
        }
        all_records.extend(records);
    }

    if all_records.is_empty() {
        eprintln!(
            "\n未发现用量记录。可运行 `usagebook doctor --check` 检查数据源。\n反馈问题时请提供版本和诊断计数；不要粘贴原始对话或密钥。"
        );
    }
    if args.json {
        println!("{}", report::json_report(&all_records, args.days));
    } else {
        report::print_report(&all_records, args.days);
    }
}

/// 增量状态库面板 只读,不写 store)。
/// 接收已有快照构建 JSON(2026-10-17):doctor 终端路径曾两次调用 doctor_snapshot,
/// quick_check 全库校验重复执行,大库 doctor 慢一倍——快照一次,JSON/终端共用
fn store_panel_json(s: &usagebook_core::store::DoctorSnapshot) -> serde_json::Value {
    serde_json::json!({
        "mode": s.mode,
        "path": s.db_path.display().to_string(),
        "size_bytes": s.db_bytes,
        "quick_check_ok": s.quick_check_ok,
        "rotations": s.rotations,
        "capped_files": s.capped_files,
        "gc_files": s.gc_files,
        "cursor_lag_ms": s.cursor_lag_ms,
        "files_by_source": s.files_by_source.iter()
            .map(|(src, n, ms)| serde_json::json!({"source": src, "files": n, "updated_ms": ms}))
            .collect::<Vec<_>>(),
        "sources": s.sources.iter()
            .map(|(src, ms)| serde_json::json!({"source": src, "updated_ms": ms}))
            .collect::<Vec<_>>(),
    })
}

fn print_store_panel(s: &usagebook_core::store::DoctorSnapshot) {
    println!("\n增量状态库 —— mode={} · {} ({})",
        s.mode, s.db_path.display(), human_size(s.db_bytes));
    match s.quick_check_ok {
        Some(true) => println!("  quick_check: ok"),
        Some(false) if s.mode == "failed" =>
            println!("  quick_check: 未执行(本进程已降级直读;此前轮转未成功,检查磁盘/权限,下次启动将重试)"),
        Some(false) => println!("  quick_check: 异常(库将自动轮转重建;真相在源文件)"),
        None if s.mode == "future-schema" =>
            println!("  quick_check: 未执行(状态库由更新版本的 usagebook 创建,本进程停用增量缓存)"),
        None => println!("  quick_check: 未执行(off 模式)"),
    }
    println!("  轮转 {} · 超限全读 {} · GC 清理 {} · 游标滞后 {}",
        s.rotations, s.capped_files, s.gc_files,
        s.cursor_lag_ms.map(|ms| format!("{ms}ms")).unwrap_or_else(|| "—".into()));
    for (src, n, ms) in &s.files_by_source {
        println!("  file_state  {:<10} {} 个游标 · 最近更新 {}", src, n, ms);
    }
    for (src, ms) in &s.sources {
        println!("  source_state {:<10} 最近更新 {}", src, ms);
    }
}

/// 本机数据源自检:逐源列出生效根与体积,附环境变量覆盖一览。
fn doctor(check: bool, json: bool) {
    if json || check {
        let mut rows = Vec::new();
        for a in registered_sources() {
            let roots = a.discover_roots();
            let tier = usagebook_core::SOURCE_CATALOG.iter().find(|s| s.id == a.id()).map(|s| s.tier);
            let suspended = tier == Some(usagebook_core::SourceTier::Suspended);
            let mut row = serde_json::json!({"source": a.id(), "tier": tier,
                "discovered": !roots.is_empty(), "roots": roots, "checked": false});
            if check && !roots.is_empty() && !suspended {
                let t0 = std::time::Instant::now();
                let parsed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| a.scan_roots(&roots, None)));
                let (records, stats) = match parsed {
                    Ok(result) => result,
                    Err(_) => {
                        row["checked"] = true.into();
                        row["status"] = "failed".into();
                        row["notes"] = serde_json::json!(["解析器异常；其他数据源继续检查"]);
                        rows.push(row);
                        continue;
                    }
                };
                row["checked"] = true.into();
                row["records"] = records.len().into();
                row["files_seen"] = stats.files_seen.into();
                row["files_parsed"] = stats.files_parsed.into();
                row["lines_total"] = stats.lines_total.into();
                row["unknown_lines"] = stats.lines_unknown_type.into();
                row["bad_lines"] = stats.lines_bad_json.into();
                // 增量层与去重观察计数入机器可读通道(禁止静默;含 0 值便于脚本消费)
                // 注意:doctor --check 直连 scan_roots(只读承诺),而 rewrote/replayed 仅在
                // serve 增量层(scan_source_cached)递增——doctor 下这两个字段恒 0,保留仅为
                // JSON 结构稳定;脚本监控增量重绕请看 serve 日志(2026-10-17 注)
                row["rewrote"] = stats.rewrote.into();
                row["replayed"] = stats.replayed.into();
                row["key_clash"] = stats.key_clash.into();
                row["failed"] = stats.failed.into();
                row["notes"] = serde_json::json!(stats.notes);
                row["elapsed_ms"] = (t0.elapsed().as_millis() as u64).into();
                row["status"] = if stats.failed { "failed" } else if stats.lines_bad_json > 0 { "partial" } else if records.is_empty() { "no_usage" } else { "parsed" }.into();
            } else {
                row["status"] = if suspended { "unsupported_usage" } else if roots.is_empty() { "not_found" } else { "discovered" }.into();
            }
            rows.push(row);
        }
        let snap = usagebook_core::store::doctor_snapshot();
        let store = store_panel_json(&snap);
        // 数据性失败(退出码 3):任一源 failed / store 真损坏 / 本进程打开两连败 / 库由更新版本创建
        let store_failed = store["quick_check_ok"].as_bool() == Some(false)
            || store["mode"].as_str().is_some_and(|m| m == "failed" || m == "future-schema");
        let any_failed = rows.iter().any(|r| r["status"] == "failed");
        if json {
            println!("{}", serde_json::json!({
                "version": env!("CARGO_PKG_VERSION"),
                "sources": rows,
                "store": store,
            }));
        } else {
            println!("usagebook 解析检查（只读本机记录）");
            for row in rows {
                let status = match row["status"].as_str().unwrap_or_default() {
                    "parsed" => "已解析", "partial" => "已解析，部分行无法读取",
                    "no_usage" => "未读到用量", "not_found" => "未发现目录",
                    "unsupported_usage" => "上游无可用 token 数据", "failed" => "扫描失败(详情见 notes)", _ => "已发现目录",
                };
                println!("{:<10} {} · 记录 {} · 坏行 {} · 未识别事件 {}",
                    row["source"].as_str().unwrap_or_default(), status,
                    row["records"].as_u64().unwrap_or(0), row["bad_lines"].as_u64().unwrap_or(0),
                    row["unknown_lines"].as_u64().unwrap_or(0));
                if let Some(notes) = row["notes"].as_array() {
                    for note in notes { println!("  {}", note.as_str().unwrap_or_default()); }
                }
            }
            println!("未识别事件不一定含用量；坏行可能是尚未写完或损坏的记录。反馈时只提供诊断计数，不提供原始对话。");
            print_store_panel(&snap);
        }
        if any_failed || store_failed {
            std::process::exit(3);
        }
        return;
    }
    print_store_panel(&usagebook_core::store::doctor_snapshot());
    println!("usagebook doctor —— 本机数据源自检\n图例:✅ 已发现目录  — 未发现（未安装属正常）\n");
    let mut found = 0usize;
    for a in registered_sources() {
        let roots = a.discover_roots();
        if roots.is_empty() {
            println!("—  {:<10} 未发现", a.id());
            continue;
        }
        found += 1;
        for p in &roots {
            println!("✅ {:<10} {:>8}  {}", a.id(), human_size(dir_size(p)), p.display());
        }
    }
    println!("\n发现 {} 个数据源目录；运行 `usagebook doctor --check` 检查实际解析结果。", found);
    if found == 0 {
        println!("本机没有任何受支持的数据源,统计将无结果;参见 README 的支持列表。");
    }
    // 看板进程探测:重复 serve 是高频误操作,TCP 探测跨平台且零依赖
    if std::net::TcpStream::connect(("127.0.0.1", DEFAULT_SERVE_PORT)).is_ok() {
        println!("\n提示:看板疑似已在默认端口运行 → http://127.0.0.1:{DEFAULT_SERVE_PORT}(自定义 --port 时以实际端口为准;再次 serve 会提示端口占用)");
    }
    // 目录体积仅用于排查，不用未经实测的吞吐率推断耗时或内存。
    let mut total_bytes: u64 = 0u64;
    for a in registered_sources() {
        for p in a.discover_roots() {
            total_bytes += dir_size(&p);
        }
    }
    if total_bytes > 0 {
        println!(
            "\n已发现目录体积 {}。首次扫描耗时取决于文件数量与格式；看板后续检查会复用未变化的数据。",
            human_size(total_bytes)
        );
    }
    println!("\n环境变量覆盖一览(仅显式设置过才显示;均支持逗号分隔多根):");
    let set: Vec<&str> = ENV_OVERRIDES
        .iter()
        .filter(|k| std::env::var(k).map(|v| !v.is_empty()).unwrap_or(false))
        .copied()
        .collect();
    if set.is_empty() {
        println!("(无,全部使用官方默认根)");
    } else {
        for k in set {
            println!("  {} = {}", k, std::env::var(k).unwrap_or_default());
        }
    }
    // 日志变量单列(数字/字节/单目录语义,与"逗号分隔多根"不同,不得混入上表)。
    println!("\n日志变量(仅显式设置过才显示;USAGEBOOK_LOG_DIR=off 关闭文件日志,另两个为数值):");
    let log_set: Vec<&str> = LOG_ENV_OVERRIDES
        .iter()
        .filter(|k| std::env::var(k).map(|v| !v.is_empty()).unwrap_or(false))
        .copied()
        .collect();
    if log_set.is_empty() {
        println!("(无,全部使用默认值)");
    } else {
        for k in log_set {
            println!("  {} = {}", k, std::env::var(k).unwrap_or_default());
        }
    }
    // 运行日志体检(最小版):目录路径 + 当前总量 + 报障指引。
    match usagebook_core::logging::resolve_dir_quiet() {
        usagebook_core::logging::LogDir::Off => println!("\n运行日志:已关闭(USAGEBOOK_LOG_DIR=off)"),
        usagebook_core::logging::LogDir::At(dir) => {
            let size = if dir.is_dir() { dir_size(&dir) } else { 0 };
            println!("\n运行日志:{} · 当前 {}", dir.display(), human_size(size));
            println!("反馈问题时可附该目录(仅含路径/计数/耗时,不含对话内容);查看/调级/关闭见 README「运行日志」一节。");
        }
    }
}

/// 递归统计路径体积(符号链接不跟随,错误静默计 0)。
fn dir_size(p: &std::path::Path) -> u64 {
    let md = match std::fs::symlink_metadata(p) {
        Ok(m) => m,
        Err(_) => return 0,
    };
    if md.is_file() {
        return md.len();
    }
    if !md.is_dir() {
        return 0;
    }
    let mut total = 0u64;
    if let Ok(rd) = std::fs::read_dir(p) {
        for e in rd.flatten() {
            total += dir_size(&e.path());
        }
    }
    total
}

fn human_size(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "K", "M", "G", "T"];
    let mut v = n as f64;
    let mut i = 0usize;
    while v >= 1024.0 && i < UNITS.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{}{}", n, UNITS[0])
    } else {
        format!("{:.1}{}", v, UNITS[i])
    }
}
