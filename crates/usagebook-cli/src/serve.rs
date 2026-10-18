//! 本地看板服务：只读采集、快照缓存和回环 HTTP 服务。

use usagebook_core::snapshot::{build_snapshot, Snapshot, SnapshotDump};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

type SharedBody = Arc<Mutex<Option<Arc<Vec<u8>>>>>;

fn cache_path() -> std::path::PathBuf {
    usagebook_core::common::data_dir().join("snapshot-cache.json")
}

fn save_cache(snap: &Snapshot) {
    // 缓存含会话标识等白名单元数据,收紧到属主读写(0600 惯例)。
    let Ok(bytes) = serde_json::to_vec(snap) else { return };
    let p = cache_path();
    if let Some(dir) = p.parent() { let _ = std::fs::create_dir_all(dir); }
    let tmp = p.with_extension(format!("json.{}.tmp", std::process::id()));
    static WARNED: AtomicBool = AtomicBool::new(false);
    #[cfg(unix)]
    let ok = {
        use std::os::unix::fs::PermissionsExt;
        std::fs::write(&tmp, &bytes).is_ok()
            && std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600)).is_ok()
            && std::fs::rename(&tmp, &p).is_ok()
    };
    #[cfg(not(unix))]
    let ok = std::fs::write(&tmp, &bytes).is_ok() && std::fs::rename(&tmp, &p).is_ok();
    if !ok && !WARNED.swap(true, Ordering::Relaxed) {
        // 双轨:人机警告直出(console 可见,不受 -usagebook_cli 抑制),文件层另记 warn。
        let shown = usagebook_core::logging::abbrev_home(&p);
        eprintln!("警告:快照缓存写入失败({shown}),热启动不可用,稍后重试");
        tracing::warn!("快照缓存写入失败({shown}),热启动不可用,稍后重试");
    } else if ok { WARNED.store(false, Ordering::Relaxed); }
}

fn load_cache() -> Option<Snapshot> {
    // 清理崩溃残留的 tmp(含旧 pid 与本 pid):rename 前崩溃的 *.tmp 会永久累积(2026-10-17 加固)
    if let Some(dir) = cache_path().parent() {
        if let Ok(entries) = std::fs::read_dir(dir) {
            let prefix = cache_path().file_name()
                .and_then(|n| n.to_str())
                .map(|n| format!("{}.", n))
                .unwrap_or_default();
            for e in entries.flatten() {
                let name = e.file_name();
                if let Some(n) = name.to_str() {
                    if n.starts_with(&prefix) && n.ends_with(".tmp") {
                        let _ = std::fs::remove_file(e.path());
                    }
                }
            }
        }
    }
    let _ = std::fs::remove_file(cache_path().with_extension(format!("json.{}.tmp", std::process::id())));
    let bytes = std::fs::read(cache_path()).ok()?;
    let dump: SnapshotDump = serde_json::from_slice(&bytes).ok()?;
    Some(Snapshot::from(dump))
}

fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64).unwrap_or(0)
}

/// 解析查询串中的 arf 声明(?arf=30):键与 "arf" 全等(大小写敏感)、重复键取首个、
/// 值按 u64 十进制语义(允许前导 + 与前导零);任何失败返回 None=无效声明,不动租约。
fn parse_arf(url: &str) -> Option<u64> {
    let query = url.split_once('?')?.1;
    for seg in query.split('&') {
        let (k, v) = match seg.split_once('=') { Some(kv) => kv, None => continue };
        if k == "arf" { return v.parse::<u64>().ok(); }
    }
    None
}

/// 仅打开已绑定的本机地址，不接受来自配置文件的 URL。
fn open_dashboard(port: u16) {
    let url = format!("http://127.0.0.1:{port}");
    #[cfg(target_os = "macos")]
    let result = std::process::Command::new("open").arg(&url).spawn();
    #[cfg(target_os = "windows")]
    let result = std::process::Command::new("rundll32.exe").arg("url.dll,FileProtocolHandler").arg(&url).spawn();
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    let result = std::process::Command::new("xdg-open").arg(&url).spawn();
    match result {
        Ok(mut child) => { std::thread::spawn(move || { let _ = child.wait(); }); }
        Err(e) => eprintln!("无法自动打开浏览器({e})，请打开 {url}"),
    }
}

fn local_request(req: &tiny_http::Request, port: u16) -> bool {
    let authority = format!("127.0.0.1:{port}");
    let localhost = format!("localhost:{port}");
    let mut has_host = false;
    for h in req.headers() {
        if h.field.equiv("Host") {
            has_host = true;
            let value = h.value.as_str();
            if value != authority && value != localhost
                && !(port == 80 && (value == "127.0.0.1" || value == "localhost")) { return false; }
        }
        if h.field.equiv("Origin") {
            let value = h.value.as_str();
            if value != format!("http://{authority}") && value != format!("http://{localhost}")
                && !(port == 80 && (value == "http://127.0.0.1" || value == "http://localhost")) { return false; }
        }
        if h.field.equiv("Sec-Fetch-Site") && h.value.as_str() == "cross-site" { return false; }
    }
    has_host   // 无 Host(HTTP/1.0 式)直接拒:现代浏览器恒带,缺失即非预期来源(2026-10-17 加固)
}

/// 先绑定本机端口，成功后才启动扫描，避免第二个实例覆盖缓存。
pub fn run(port: u16, refresh_secs: u64, open: bool) {
    if refresh_secs < 5 || port == 0 {
        eprintln!("错误:端口须为 1–65535，--refresh-secs 最小为 5 秒");
        std::process::exit(2);
    }
    let server = match tiny_http::Server::http(("127.0.0.1", port)) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("端口 {port} 无法绑定:{e}\n若看板已启动，请打开 http://127.0.0.1:{port}\n或换端口: usagebook serve --port 7879");
            std::process::exit(1);
        }
    };
    // 端口绑定成功后才激活文件日志(先绑端口再取锁,方案 §5):
    // 端口失败保持纯 exit(1) 语义;双实例取不到锁则文件层降级,看板照常服务。
    // .bad 残片清理与文件层是否激活无关(off 用户同样执行,方案 §9)。
    usagebook_core::store::prune_bad_backups();
    match usagebook_core::logging::global().activate("serve") {
        usagebook_core::logging::Activation::Active { .. } | usagebook_core::logging::Activation::Off => {}
        usagebook_core::logging::Activation::Degraded(_) => {
            // 原因已在 activate 内旁路告警,此处不再重复。
        }
    }
    let cached_body: SharedBody = Arc::new(Mutex::new(None));
    let trigger = Arc::new(AtomicBool::new(false));
    let last_trigger_ms = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let scanning = Arc::new(AtomicBool::new(false));
    let last_scan_failed = Arc::new(AtomicBool::new(false));
    let last_request_ms = Arc::new(AtomicU64::new(0));
    let last_scan_started_ms = Arc::new(AtomicU64::new(0));
    // 前端驱动租约(20261014-刷新节奏前端驱动):看板轮询 ?arf=N 声明重扫节奏,
    // desired_ms=0 表示无声明;租约到期或无声明时回退 --refresh_secs
    let desired_ms = Arc::new(AtomicU64::new(0));
    let lease_until_ms = Arc::new(AtomicU64::new(0));

    if let Some(snap) = load_cache() {
        eprintln!("已加载本地快照，正在检查更新…");
        if let Ok(body) = serde_json::to_vec(&snap) {
            *cached_body.lock().unwrap_or_else(|e| e.into_inner()) = Some(Arc::new(body));
        }
    } else { println!("首次扫描中，完成后自动显示；数据较多时请稍候…"); }

    {
        let cached_body = cached_body.clone();
        let trigger = trigger.clone();
        let scanning = scanning.clone();
        let last_scan_failed = last_scan_failed.clone();
        let last_request_ms = last_request_ms.clone();
        let last_scan_started_ms = last_scan_started_ms.clone();
        let desired_ms = desired_ms.clone();
        let lease_until_ms = lease_until_ms.clone();
        std::thread::spawn(move || {
            let mut last = Instant::now();
            // 墙钟参照:Instant 单调钟不含系统休眠时间,唤醒后 elapsed 未走满 interval 会把
            // due 推迟一个周期(页面倒计时照走但数据旧)——兼看墙钟跳变,取二者较大者(2026-10-17)
            let mut last_wall_ms = now_ms();
            let mut first = true;
            loop {
                let forced = trigger.swap(false, Ordering::Relaxed);
                // 有效间隔:租约内的前端声明值,否则回退 refresh_secs
                let desired = desired_ms.load(Ordering::Relaxed);
                let driven = desired > 0 && now_ms() < lease_until_ms.load(Ordering::Relaxed);
                let interval_ms = if driven { desired } else { refresh_secs * 1000 };
                let wall_elapsed = now_ms().saturating_sub(last_wall_ms);
                let due = last.elapsed() >= Duration::from_millis(interval_ms)
                    || wall_elapsed >= interval_ms;
                let empty = cached_body.lock().unwrap_or_else(|e| e.into_inner()).is_none();
                let viewed = last_request_ms.load(Ordering::Relaxed) >= last_scan_started_ms.load(Ordering::Relaxed);
                // 冷启动失败也须按间隔重试，不能因 empty 进入持续扫描。
                if (first || due || forced) && (first || empty || forced || viewed) {
                    let verbose = first;
                    first = false;
                    scanning.store(true, Ordering::Relaxed);
                    last_scan_started_ms.store(now_ms(), Ordering::Relaxed);
                    // 测试钩子(仅集成测试使用):注入含禁密标记的 panic,
                    // 验证 panic 通道留档与 stderr/文件双路脱敏。panic 在 catch_unwind
                    // 闭包内触发——扫描线程由既有兜底接住,scanning 标志正常复位。
                    let test_panic = std::env::var("USAGEBOOK_TEST_PANIC").map(|v| v == "1").unwrap_or(false);
                    let snap = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        if test_panic {
                            panic!("injected test panic\nsecond line \u{1b}[31mUBCANARY-sk-testsecret123");
                        }
                        build_snapshot(refresh_secs, verbose)
                    }));
                    match snap {
                        Ok(s) => {
                            // 每轮 summary(§7 必记事件 3,字段集钉死:来源名+计数+耗时+错误码):
                            // info 一行总量,debug 逐源(仅 -v/显式 RUST_LOG 时可见)。
                            let failed = s.source_scans.iter().filter(|sc| sc.stats.failed).count();
                            let records: usize = s.source_scans.iter().map(|sc| sc.records).sum();
                            tracing::info!(
                                "本轮扫描 · 来源 {} · 记录 {records} · 耗时 {}ms · 失败源 {failed}",
                                s.source_scans.len(), s.scan_ms
                            );
                            for sc in &s.source_scans {
                                tracing::debug!(
                                    "来源 {} · 记录 {} · 坏行 {} · 失败 {}",
                                    sc.source, sc.records, sc.stats.lines_bad_json, sc.stats.failed as u8
                                );
                            }
                            if let Ok(body) = serde_json::to_vec(&s) {
                                save_cache(&s);
                                *cached_body.lock().unwrap_or_else(|e| e.into_inner()) = Some(Arc::new(body));
                                last_scan_failed.store(false, Ordering::Relaxed);
                            } else { last_scan_failed.store(true, Ordering::Relaxed); }
                        }
                        Err(_) => {
                            last_scan_failed.store(true, Ordering::Relaxed);
                            eprintln!("本轮扫描失败，保留上次结果；稍后重试。可运行 usagebook doctor --check 排查。");
                            tracing::warn!("本轮扫描失败,保留上次结果,稍后重试(可运行 usagebook doctor --check 排查)");
                        }
                    }
                    scanning.store(false, Ordering::Relaxed);
                    last = Instant::now();
                    last_wall_ms = now_ms();
                }
                std::thread::sleep(Duration::from_millis(500));
            }
        });
    }
    println!("usagebook 看板: http://127.0.0.1:{port} （无前端驱动时每 {refresh_secs}s 检查更新，Ctrl-C 退出）\n纯本地运行，数据不离开本机。");
    if open { open_dashboard(port); }
    // 内联源留在单文件便于维护；响应时提取成同源脚本，CSP 不允许注入的事件处理器执行。
    // i18n 字典以独立文件维护(翻译者不碰 dashboard.html)，编译期拼接为 dashboard.js 前缀；
    // 'use strict' 必须是拼接结果首条语句(脚本体自带的会退化为普通表达式)。
    const I18N_JS: &str = concat!("'use strict';const I18N={};",
        include_str!("i18n/zh-CN.js"), include_str!("i18n/en-US.js"));
    let (prefix, tail) = include_str!("dashboard.html").split_once("<script>").expect("dashboard script");
    let (script_body, suffix) = tail.split_once("</script>").expect("dashboard script end");
    let index_html = format!("{prefix}<script src=\"/dashboard.js\"></script>{suffix}");
    let dashboard_js = format!("{I18N_JS}{script_body}");
    let chart_js = include_str!("vendor/chart.umd.min.js");
    for req in server.incoming_requests() {
        let path = req.url().split('?').next().unwrap_or("/");
        let (body, ctype, code) = if !local_request(&req, port) {
            (r#"{"error":"local origin required"}"#.to_string(), "application/json; charset=utf-8", 403)
        } else if req.method() != &tiny_http::Method::Get {
            (r#"{"error":"method not allowed"}"#.to_string(), "application/json; charset=utf-8", 405)
        } else {
            match path {
                "/" | "/index.html" => (index_html.to_string(), "text/html; charset=utf-8", 200),
                "/dashboard.js" => (dashboard_js.to_string(), "application/javascript; charset=utf-8", 200),
                "/vendor/chart.umd.min.js" => (chart_js.to_string(), "application/javascript; charset=utf-8", 200),
                "/api/data" => {
                    // 任何 /api/data 都续 viewed;仅有效 arf 才写租约(先写租约、后取 eff):
                    // arf=0 显式关闭,其余夹紧 [5,86400] 后按 3 倍周期续租
                    last_request_ms.store(now_ms(), Ordering::Relaxed);
                    if let Some(n) = parse_arf(req.url()) {
                        if n == 0 {
                            desired_ms.store(0, Ordering::Relaxed);
                            lease_until_ms.store(0, Ordering::Relaxed);
                        } else {
                            let clamped = n.clamp(5, 86400);
                            desired_ms.store(clamped * 1000, Ordering::Relaxed);
                            lease_until_ms.store(now_ms() + 3 * clamped * 1000, Ordering::Relaxed);
                        }
                    }
                    let desired = desired_ms.load(Ordering::Relaxed);
                    let eff = if desired > 0 && now_ms() < lease_until_ms.load(Ordering::Relaxed)
                        { desired / 1000 } else { refresh_secs };
                    let cached = cached_body.lock().unwrap_or_else(|e| e.into_inner()).clone();
                    let busy = scanning.load(Ordering::Relaxed);
                    let failed = last_scan_failed.load(Ordering::Relaxed);
                    // refresh_secs=CLI 配置回退值(烤入快照),eff_refresh_secs=当前生效值(此处现算)
                    let body = if let Some(bytes) = cached {
                        let mut out = bytes[..bytes.len() - 1].to_vec();
                        out.extend_from_slice(format!(r#", "version":"{}","scanning":{},"last_scan_failed":{},"eff_refresh_secs":{}}}"#,
                            env!("CARGO_PKG_VERSION"), busy, failed, eff).as_bytes());
                        String::from_utf8_lossy(&out).into_owned()
                    } else {
                        serde_json::json!({"ready":false,"version":env!("CARGO_PKG_VERSION"),"scanning":busy,"last_scan_failed":failed,"eff_refresh_secs":eff}).to_string()
                    };
                    (body, "application/json; charset=utf-8", 200)
                }
                "/api/trigger" => {
                    // 最小触发间隔 2s(2026-10-17 安全加固):防本地页面循环触发重扫的 CPU DoS;
                    // 正常用户点击节奏远低于此,节流静默放行(响应不变)
                    let now = now_ms();
                    let last = last_trigger_ms.swap(now, Ordering::Relaxed);
                    if now - last >= 2000 { trigger.store(true, Ordering::Relaxed); }
                    (serde_json::json!({"ok":true,"scanning":scanning.load(Ordering::Relaxed)}).to_string(), "application/json; charset=utf-8", 200)
                }
                _ => (r#"{"error":"not found"}"#.to_string(), "application/json; charset=utf-8", 404),
            }
        };
        let ct = tiny_http::Header::from_bytes("Content-Type", ctype).expect("valid header");
        let cache = if path == "/vendor/chart.umd.min.js" { "public, max-age=86400" } else { "no-store" };
        let resp = tiny_http::Response::from_string(body).with_status_code(code)
            .with_header(ct)
            .with_header(tiny_http::Header::from_bytes("Cache-Control", cache).expect("valid header"))
            .with_header(tiny_http::Header::from_bytes("X-Content-Type-Options", "nosniff").expect("valid header"))
            .with_header(tiny_http::Header::from_bytes("Content-Security-Policy", "default-src 'none'; script-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'self'; img-src 'self' data:; font-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'").expect("valid header"));
        // respond 丢独立线程且不 join(2026-10-17 加固):tiny_http 无写超时,完全不读响应的
        // 慢客户端会把单线程主循环卡死(看板假死);后台线程慢写只影响该连接自身。
        // 本地单用户场景连接数有限,线程开销可忽略;若 spawn 失败等同响应失败
        let _ = std::thread::spawn(move || { let _ = req.respond(resp); });
    }
}
