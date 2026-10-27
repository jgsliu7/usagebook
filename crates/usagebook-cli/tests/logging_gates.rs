//! 运行日志集成测试(方案 §11;子进程拉起被测二进制,规避全局 subscriber/hook 单例)。
//!
//! 契约 gate(--json 纯净、--version 全等、双实例降级)+ 禁密断言(标记 + 诱饵 env)
//! + panic 通道脱敏 + .bad 清理 + 轮转触发配方 + 权限断言。

use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_usagebook")
}

/// 临时目录守卫:Drop 时递归清理(测试不再残留 ub-log-it-* 目录)。
struct TmpGuard(PathBuf);
impl std::ops::Deref for TmpGuard {
    type Target = Path;
    fn deref(&self) -> &Path {
        &self.0
    }
}
impl AsRef<Path> for TmpGuard {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}
impl Drop for TmpGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn tmpdir(tag: &str) -> TmpGuard {
    let d = std::env::temp_dir().join(format!(
        "ub-log-it-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos()
    ));
    std::fs::create_dir_all(&d).unwrap();
    TmpGuard(d)
}

/// serve 进程守卫:Drop 必杀，所有日志测试均使用隔离 HOME 与临时统计。
struct ServeGuard(Child);
impl ServeGuard {
    /// 结束进程并收集 stdout/stderr。
    fn finish(&mut self) -> (String, String) {
        let _ = self.0.kill();
        let mut out = String::new();
        let mut err = String::new();
        if let Some(mut o) = self.0.stdout.take() {
            o.read_to_string(&mut out).ok();
        }
        if let Some(mut e) = self.0.stderr.take() {
            e.read_to_string(&mut err).ok();
        }
        let _ = self.0.wait();
        (out, err)
    }
}
impl Drop for ServeGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn free_port() -> u16 {
    TcpListener::bind(("127.0.0.1", 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// 消除 free_port 竞态:两个测试并行时可能分到同一端口,输家秒退。
/// 起后确认 500ms 内未退出即视为存活,否则换端口重试。
fn spawn_serve_alive(data_dir: &Path, extra_env: &[(&str, &str)]) -> (ServeGuard, u16) {
    for _ in 0..5 {
        let port = free_port();
        let mut child = spawn_serve(port, data_dir, extra_env);
        std::thread::sleep(Duration::from_millis(500));
        match child.try_wait() {
            Ok(Some(_)) => continue, // 端口冲突等立即退出 → 换端口
            _ => return (ServeGuard(child), port),
        }
    }
    panic!("5 次尝试均未能启动存活的 serve(端口竞争)");
}

fn spawn_serve(port: u16, data_dir: &Path, extra_env: &[(&str, &str)]) -> Child {
    let mut cmd = Command::new(bin());
    cmd.args(["serve", "--temporary", "--port", &port.to_string()])
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", data_dir.join("home"))
        .env("XDG_CONFIG_HOME", data_dir.join("home/.config"))
        .env("XDG_DATA_HOME", data_dir.join("home/.local/share"))
        .env("USAGEBOOK_DATA_DIR", data_dir.as_os_str())
        .env("USAGEBOOK_STATE_DB", data_dir.join("state.db"))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (k, v) in extra_env {
        cmd.env(k, v);
    }
    cmd.spawn().expect("spawn usagebook serve")
}

/// Fixture for the current compatibility generation; legacy paths remain untouched.
fn cache_dir(data: &Path) -> PathBuf {
    data.join(format!(
        "state.db.cache-p{}-s2-l1",
        usagebook_core::store::PARSER_VERSION
    ))
}

fn backup_dir(data: &Path, timestamp: i64) -> PathBuf {
    let mut name = cache_dir(data).into_os_string();
    name.push(format!(".bad-{timestamp}-0"));
    PathBuf::from(name)
}

fn write_backup(data: &Path, timestamp: i64) {
    let dir = backup_dir(data, timestamp);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("state.db"), b"fixture database").unwrap();
    std::fs::write(dir.join("state.db-wal"), b"fixture sidecar").unwrap();
}

fn isolated_report(data: &Path, claude: &Path) -> Command {
    std::fs::create_dir_all(data).unwrap();
    std::fs::create_dir_all(claude.join("projects")).unwrap();
    let mut cmd = Command::new(bin());
    cmd.args(["--json", "--source", "claude"])
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", data.join("home"))
        .env("XDG_CONFIG_HOME", data.join("home/.config"))
        .env("XDG_DATA_HOME", data.join("home/.local/share"))
        .env("USAGEBOOK_LEDGER_PATH", data.join("ledger.db"))
        .env("USAGEBOOK_DATA_DIR", data)
        .env("USAGEBOOK_STATE_DB", data.join("state.db"))
        .env("CLAUDE_CONFIG_DIR", claude);
    cmd
}

/// 等待端口可连(serve 完成绑定 + 文件日志激活)。
fn wait_port(port: u16, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(150));
    }
    false
}

fn kill(child: &mut ServeGuard) -> (String, String) {
    child.finish()
}

/// 当日主日志(取目录内日期最大的 usagebook.YYYYMMDD.log——紧邻午夜运行也不 flake)。
fn today_log(dir: &Path) -> PathBuf {
    let mut best: Option<(String, PathBuf)> = None;
    for e in std::fs::read_dir(dir).unwrap().flatten() {
        let name = e.file_name().to_string_lossy().to_string();
        let Some(rest) = name.strip_prefix("usagebook.") else {
            continue;
        };
        let Some(date) = rest.strip_suffix(".log") else {
            continue;
        };
        if date.len() == 8
            && date.bytes().all(|b| b.is_ascii_digit())
            && best.as_ref().map(|(d, _)| date > d).unwrap_or(true)
        {
            best = Some((date.to_string(), e.path()));
        }
    }
    best.expect("至少一个主日志文件").1
}

/// 等待 panic.log 出现实际条目(激活即预建空文件,等待 exists 会竞态)。
fn wait_panic_entry(logs: &Path) -> String {
    let panic_log = logs.join("panic.log");
    let deadline = Instant::now() + Duration::from_secs(25);
    while Instant::now() < deadline {
        if let Ok(c) = std::fs::read_to_string(&panic_log) {
            if c.contains("PANIC") {
                return c;
            }
        }
        std::thread::sleep(Duration::from_millis(300));
    }
    panic!("25s 内 panic.log 未出现条目");
}

fn http_get(port: u16, path: &str) -> Option<String> {
    let mut s = std::net::TcpStream::connect(("127.0.0.1", port)).ok()?;
    write!(s, "GET {path} HTTP/1.0\r\nHost: 127.0.0.1:{port}\r\n\r\n").ok()?;
    let mut buf = String::new();
    s.read_to_string(&mut buf).ok()?;
    Some(buf)
}

#[cfg(unix)]
fn mode_of(p: &Path) -> u32 {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(p).unwrap().permissions().mode() & 0o777
}

// ---------------------------------------------------------------------------
// 契约 gate
// ---------------------------------------------------------------------------

#[test]
fn version_output_exact() {
    let out = Command::new(bin()).arg("--version").output().unwrap();
    assert_eq!(
        String::from_utf8_lossy(&out.stdout).trim(),
        "usagebook 0.2.0"
    );
    assert!(
        out.stderr.is_empty(),
        "--version 的 stderr 必须为空(初始化抢跑即破坏契约),实为: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// 非法日志环境变量 + 一次性命令:不解析不警告(stderr 与基线一致,--json 管道零污染)。
#[test]
fn invalid_log_env_silent_for_onetshot() {
    let dir = tmpdir("oneshot-log-env");
    let base = isolated_report(&dir.join("base-data"), &dir.join("base-claude"))
        .output()
        .unwrap();
    let with_env = isolated_report(&dir.join("env-data"), &dir.join("env-claude"))
        .env("USAGEBOOK_LOG_KEEP_DAYS", "abc")
        .env("USAGEBOOK_LOG_MAX_TOTAL", "xyz")
        .output()
        .unwrap();
    assert_eq!(
        base.stderr, with_env.stderr,
        "非法日志变量不得给一次性命令的 stderr 增加任何行"
    );
    // Ledger v2 streams one bounded JSON page per line.
    for output in [&base.stdout, &with_env.stdout] {
        let text = std::str::from_utf8(output).expect("stdout 必须为 UTF-8");
        assert!(!text.is_empty());
        for line in text.lines() {
            let page: serde_json::Value =
                serde_json::from_str(line).expect("NDJSON 每行必须完整可解析，不能夹带日志");
            assert!(page.get("stamp").is_some() && page.get("payload").is_some());
        }
    }
}

/// store 降级 fixture(cfg(unix)):垃圾库 + 目录只读 → 轮转失败 → "降级直读"进 stderr;
/// 同场景下 --json 的 stdout 仍完整可解析(console 钉死 stderr 的核心契约)。
#[cfg(unix)]
#[test]
fn json_stdout_pure_with_store_degrade() {
    let dir = tmpdir("degrade");
    let data = dir.join("data");
    let claude = dir.join("claude");
    let mut cmd = isolated_report(&data, &claude);
    std::fs::write(claude.join("projects/fixture.jsonl"),
        b"{\"type\":\"assistant\",\"sessionId\":\"fixture\",\"requestId\":\"r\",\"message\":{\"id\":\"m\",\"model\":\"fixture\",\"usage\":{\"input_tokens\":1}}}\n").unwrap();
    let cache = cache_dir(&data);
    std::fs::create_dir_all(&cache).unwrap();
    std::fs::write(cache.join("state.db"), b"garbage not a sqlite db").unwrap();
    let mut lock = cache.into_os_string();
    lock.push(".lock");
    // Permit lease acquisition, then fail the directory rename under this parent.
    std::fs::write(PathBuf::from(lock), b"").unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&data, std::fs::Permissions::from_mode(0o555)).unwrap();
    let out = cmd.output().unwrap();
    std::fs::set_permissions(&data, std::fs::Permissions::from_mode(0o755)).unwrap();
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("降级"), "store 降级信息必须留在 stderr: {err}");
    let text = std::str::from_utf8(&out.stdout).expect("stdout 必须为 UTF-8");
    assert!(!text.is_empty());
    for line in text.lines() {
        let page: serde_json::Value = serde_json::from_str(line)
            .expect("--json 的每个 NDJSON 页必须纯净可解析(store 降级不得污染管道)");
        assert!(page.get("stamp").is_some() && page.get("payload").is_some());
    }
}

// ---------------------------------------------------------------------------
// serve 文件层
// ---------------------------------------------------------------------------

#[test]
fn serve_startup_marker_rotation_and_permissions() {
    let data = tmpdir("serve-basic");
    let logs = data.join("logs");
    let (mut child, port) = spawn_serve_alive(&data, &[]);
    assert!(wait_port(port, Duration::from_secs(15)), "serve 未就绪");
    std::thread::sleep(Duration::from_millis(500)); // 启动标记落盘
    let (out, err) = kill(&mut child);
    let _ = (out, err);
    let main = today_log(&logs);
    assert!(main.exists(), "当日主日志文件必须存在");
    let content = std::fs::read_to_string(&main).unwrap();
    assert!(
        content.contains("启动标记"),
        "启动标记(运行边界)必须落盘: {content}"
    );
    assert!(content.contains(env!("CARGO_PKG_VERSION")));
    // 时间戳规范:每行首字段可被 RFC3339 严格解析(必含时区偏移)。
    for line in content.lines().filter(|l| !l.trim().is_empty()) {
        let ts = line.split_whitespace().next().unwrap_or("");
        chrono::DateTime::parse_from_rfc3339(ts)
            .unwrap_or_else(|e| panic!("行首应为 RFC3339 时间戳({e}): {line}"));
    }
    // 禁密断言(回归下限):主日志不得含凭据标记。
    for marker in [
        "sk-",
        "ghp_",
        "github_pat_",
        "Bearer ",
        "Authorization:",
        "token=",
        "session=",
        "-----BEGIN",
    ] {
        assert!(
            !content.contains(marker),
            "日志含禁密标记 {marker}: {content}"
        );
    }
    // "code=" 出现在 panic 留档断言中;主日志单独排除 code= (启动摘要不含)。
    assert!(!content.contains("code="));
    #[cfg(unix)]
    {
        assert_eq!(mode_of(&logs), 0o700, "logs/ 目录 0700");
        assert_eq!(mode_of(&main), 0o600, "主日志 0600");
        assert_eq!(mode_of(&logs.join(".lock")), 0o600, ".lock 0600");
    }
}

/// 诱饵环境变量:启动摘要只记白名单,未知 USAGEBOOK_* 只记"已设置"不记值。
#[test]
fn startup_summary_never_logs_unknown_env_values() {
    let data = tmpdir("serve-decoy");
    let logs = data.join("logs");
    let (mut child, port) = spawn_serve_alive(
        &data,
        &[("USAGEBOOK_DECOY_SECRET", "decoy-secret-value-42")],
    );
    assert!(wait_port(port, Duration::from_secs(15)));
    std::thread::sleep(Duration::from_millis(400));
    kill(&mut child);
    let content = std::fs::read_to_string(today_log(&logs)).unwrap();
    assert!(
        !content.contains("decoy-secret-value-42"),
        "未知 USAGEBOOK_* 的值不得入日志: {content}"
    );
}

/// 双实例(不同端口 + 同一 DATA_DIR):第二实例文件层降级,两个看板都可用。
#[test]
fn dual_instance_second_degrades_file_layer() {
    let data = tmpdir("serve-dual");
    let (mut a, port_a) = spawn_serve_alive(&data, &[]);
    assert!(wait_port(port_a, Duration::from_secs(15)));
    let (mut b, port_b) = spawn_serve_alive(&data, &[]);
    assert!(
        wait_port(port_b, Duration::from_secs(15)),
        "第二实例看板必须照常服务"
    );
    std::thread::sleep(Duration::from_millis(400));
    let resp = http_get(port_a, "/api/v2/status").expect("第一实例仍响应");
    assert!(resp.contains("200"), "第一实例 /api/v2/status: {resp}");
    let (_bo, be) = kill(&mut b);
    assert!(
        be.contains("文件日志降级") || be.contains("日志目录锁"),
        "第二实例 stderr 应含降级提示: {be}"
    );
    kill(&mut a);
}

/// panic 通道:注入的 panic(含 canary 与 sk- 标记)留档且被脱敏;看板进程存活。
#[test]
fn panic_channel_scrubs_and_persists() {
    let data = tmpdir("serve-panic");
    let logs = data.join("logs");
    let (mut child, port) = spawn_serve_alive(&data, &[("USAGEBOOK_TEST_PANIC", "1")]);
    assert!(wait_port(port, Duration::from_secs(15)));
    let content = wait_panic_entry(&logs);
    let (_out, err) = kill(&mut child);
    assert!(content.contains("test panic"), "panic 条目存在: {content}");
    assert!(
        !content.contains("UBCANARY"),
        "canary 必须被脱敏: {content}"
    );
    assert!(
        !content.contains("sk-testsecret"),
        "密钥标记词必须整体脱敏: {content}"
    );
    // stderr 副本同规脱敏(不再调默认 hook 打原始 payload)。
    assert!(
        !err.contains("UBCANARY") && !err.contains("sk-testsecret"),
        "stderr 副本必须同规脱敏: {err}"
    );
    assert!(
        err.contains("test panic") || err.contains("panic"),
        "stderr 应有净化后的崩溃摘要: {err}"
    );
    #[cfg(unix)]
    assert_eq!(mode_of(&logs.join("panic.log")), 0o600);
}

/// .bad 残片清理:预置 5 组,serve 启动后(与文件层无关)仅剩最新 3 组。
#[test]
fn bad_backups_pruned_to_three_groups() {
    let data = tmpdir("serve-bad");
    for ms in [1000i64, 2000, 3000, 4000, 5000] {
        write_backup(&data, ms);
    }
    std::fs::write(data.join("state.db.bad-1000"), b"legacy").unwrap();
    let other_generation = data.join("state.db.cache-p4-s1-l1.bad-1000-0");
    std::fs::create_dir_all(&other_generation).unwrap();
    std::fs::write(other_generation.join("state.db"), b"other generation").unwrap();
    let mut unrelated = cache_dir(&data).into_os_string();
    unrelated.push(".bad-9999-notnum");
    let unrelated = PathBuf::from(unrelated);
    std::fs::write(&unrelated, b"x").unwrap(); // 围栏外
    let (mut child, port) = spawn_serve_alive(&data, &[]);
    assert!(wait_port(port, Duration::from_secs(15)));
    std::thread::sleep(Duration::from_millis(300));
    kill(&mut child);
    for ms in [1000, 2000] {
        assert!(!backup_dir(&data, ms).exists(), "最老两组应被清理");
    }
    for ms in [3000, 4000, 5000] {
        assert!(
            backup_dir(&data, ms).join("state.db").exists(),
            "最新 3 组保留"
        );
        assert!(
            backup_dir(&data, ms).join("state.db-wal").exists(),
            "整组保留(含侧车)"
        );
    }
    assert!(unrelated.exists(), "围栏外文件不动");
    assert_eq!(
        std::fs::read(data.join("state.db.bad-1000")).unwrap(),
        b"legacy"
    );
    assert_eq!(
        std::fs::read(other_generation.join("state.db")).unwrap(),
        b"other generation"
    );
}

/// LOG_DIR=off:文件层关闭(无 logs/ 目录),看板照常服务,.bad 清理仍执行。
#[test]
fn log_dir_off_disables_file_layer_but_prunes() {
    let data = tmpdir("serve-off");
    for ms in [100i64, 200, 300, 400, 500] {
        write_backup(&data, ms);
    }
    std::fs::write(data.join("state.db.bad-100"), b"legacy").unwrap();
    let (mut child, port) = spawn_serve_alive(&data, &[("USAGEBOOK_LOG_DIR", "off")]);
    assert!(
        wait_port(port, Duration::from_secs(15)),
        "off 模式看板照常服务"
    );
    std::thread::sleep(Duration::from_millis(300));
    kill(&mut child);
    assert!(!data.join("logs").exists(), "off 模式不得创建日志目录");
    assert!(
        !backup_dir(&data, 100).exists() && !backup_dir(&data, 200).exists(),
        "off 用户同样执行 .bad 清理(最老两组删除)"
    );
    for ms in [300i64, 400, 500] {
        assert!(
            backup_dir(&data, ms).join("state.db").exists(),
            "最新 3 组保留(keep=3 语义)"
        );
        assert!(
            backup_dir(&data, ms).join("state.db-wal").exists(),
            "侧车随目录保留"
        );
    }
    assert_eq!(
        std::fs::read(data.join("state.db.bad-100")).unwrap(),
        b"legacy"
    );
}

/// 轮转触发配方(20MiB 内置常量无法自然写满):预置超限当日主文件 → 启动 stat 校准
/// → 首次写入后轮转出 .001 分片。
#[test]
fn rotation_triggered_by_startup_calibration() {
    let data = tmpdir("serve-rot");
    let logs = data.join("logs");
    std::fs::create_dir_all(&logs).unwrap();
    let today = chrono::Local::now().format("%Y%m%d").to_string();
    std::fs::write(
        logs.join(format!("usagebook.{today}.log")),
        vec![b'x'; 21 * 1024 * 1024],
    )
    .unwrap();
    let (mut child, port) = spawn_serve_alive(&data, &[]);
    assert!(wait_port(port, Duration::from_secs(15)));
    std::thread::sleep(Duration::from_millis(500));
    kill(&mut child);
    let shard = logs.join(format!("usagebook.{today}.001.log"));
    assert!(shard.exists(), "超限主文件应轮转出 .001 分片");
    let new_main_len = std::fs::metadata(today_log(&logs)).unwrap().len();
    assert!(
        new_main_len < 1024 * 1024,
        "新主文件应为小文件(仅含启动后的新条目): {new_main_len}"
    );
}

/// 两轮 serve(顺序起停):追加模式 + 两个启动标记(运行边界可数)。
#[test]
fn two_rounds_two_startup_markers() {
    let data = tmpdir("serve-twice");
    let logs = data.join("logs");
    for round in 0..2 {
        let (mut child, port) = spawn_serve_alive(&data, &[]);
        assert!(wait_port(port, Duration::from_secs(15)));
        std::thread::sleep(Duration::from_millis(400));
        kill(&mut child);
        if round == 0 {
            // 前一轮被 kill 留下死锁:把 mtime 回拨 2h,验证锁年龄回退(Windows 唯一自愈路径,
            // Unix 上 pid 判活本就能回收,此步幂等无害)。
            let lp = logs.join(".lock");
            if lp.exists() {
                if let Ok(f) = std::fs::OpenOptions::new().write(true).open(&lp) {
                    let past = std::time::SystemTime::now() - Duration::from_secs(7200);
                    let _ = f.set_modified(past);
                }
            }
        }
    }
    let content = std::fs::read_to_string(today_log(&logs)).unwrap();
    assert_eq!(
        content.matches("启动标记").count(),
        2,
        "两轮两个启动标记(锁年龄回退后二次取锁): {content}"
    );
}

/// panic.log 超 5MiB 时改名归档(panic.log.old,0600),新文件重建。
#[test]
fn panic_archive_rollover_at_5mib() {
    let data = tmpdir("serve-p5");
    let logs = data.join("logs");
    std::fs::create_dir_all(&logs).unwrap();
    std::fs::write(logs.join("panic.log"), vec![b'x'; 5 * 1024 * 1024 + 1]).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        // 以本程序产物的权限预建(0600);归档保留原权限,外来文件不强制收紧(与目录同款语义)。
        std::fs::set_permissions(
            logs.join("panic.log"),
            std::fs::Permissions::from_mode(0o600),
        )
        .unwrap();
    }
    let (mut child, port) = spawn_serve_alive(&data, &[("USAGEBOOK_TEST_PANIC", "1")]);
    assert!(wait_port(port, Duration::from_secs(15)));
    let old = logs.join("panic.log.old");
    let deadline = Instant::now() + Duration::from_secs(25);
    while !old.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(300));
    }
    kill(&mut child);
    assert!(old.exists(), "5MiB 超限后改名归档 panic.log.old");
    let cur_len = std::fs::metadata(logs.join("panic.log")).unwrap().len();
    assert!(
        cur_len < 1024 * 1024,
        "重建后的 panic.log 为小文件: {cur_len}"
    );
    #[cfg(unix)]
    {
        assert_eq!(mode_of(&old), 0o600);
        assert_eq!(mode_of(&logs.join("panic.log")), 0o600);
    }
}

/// LOG_DIR 指向不可用路径(已存在的文件):文件层降级 + stderr 提示,看板照常服务。
#[test]
fn unwritable_log_dir_degrades_console_warns() {
    let data = tmpdir("serve-baddir");
    let blocker = data.join("not-a-dir"); // 已存在的文件 → create_dir 失败
    std::fs::write(&blocker, b"x").unwrap();
    let (mut child, port) =
        spawn_serve_alive(&data, &[("USAGEBOOK_LOG_DIR", blocker.to_str().unwrap())]);
    assert!(wait_port(port, Duration::from_secs(15)), "降级不阻断看板");
    std::thread::sleep(Duration::from_millis(400));
    let (_o, e) = kill(&mut child);
    assert!(
        e.contains("文件日志降级") || e.contains("不可用"),
        "stderr 应含降级提示: {e}"
    );
}

/// 主日志不出现未缩略的家目录路径(日志路径统一 ~ 缩略)。
#[test]
fn home_path_never_appears_unabbreviated() {
    let data = tmpdir("serve-home");
    let logs = data.join("logs");
    let (mut child, port) = spawn_serve_alive(&data, &[]);
    assert!(wait_port(port, Duration::from_secs(15)));
    std::thread::sleep(Duration::from_millis(400));
    kill(&mut child);
    let content = std::fs::read_to_string(today_log(&logs)).unwrap();
    if let Ok(home) = std::env::var("HOME") {
        assert!(
            !content.contains(&home),
            "日志中的路径必须已做 ~ 缩略: {content}"
        );
    }
}

/// -v 与 --version 组合:clap 先退出,初始化零影响。
#[test]
fn verbose_flag_keeps_version_exact() {
    let out = Command::new(bin())
        .arg("-v")
        .arg("--version")
        .output()
        .unwrap();
    assert_eq!(
        String::from_utf8_lossy(&out.stdout).trim(),
        "usagebook 0.2.0"
    );
    assert!(out.stderr.is_empty());
}

/// 相对路径 LOG_DIR → 旁路告警回退默认目录(§8),默认目录正常落日志。
#[test]
fn relative_log_dir_falls_back_with_warning() {
    let data = tmpdir("serve-rel");
    let logs = data.join("logs");
    let (mut child, port) = spawn_serve_alive(&data, &[("USAGEBOOK_LOG_DIR", "./rellogs")]);
    assert!(wait_port(port, Duration::from_secs(15)), "回退后看板照常");
    std::thread::sleep(Duration::from_millis(400));
    let (_o, e) = kill(&mut child);
    assert!(e.contains("相对路径"), "stderr 应含回退告警: {e}");
    assert!(today_log(&logs).exists(), "日志落在默认目录");
}

/// 预置 symlink 主日志 → 非常规文件防护(§5):文件层降级,看板照常(端到端)。
#[cfg(unix)]
#[test]
fn symlink_main_log_degrades_end_to_end() {
    let data = tmpdir("serve-symlink");
    let logs = data.join("logs");
    std::fs::create_dir_all(&logs).unwrap();
    let target = data.join("victim.txt");
    std::fs::write(&target, b"do not touch").unwrap();
    let today = chrono::Local::now().format("%Y%m%d").to_string();
    std::os::unix::fs::symlink(&target, logs.join(format!("usagebook.{today}.log"))).unwrap();
    let (mut child, port) = spawn_serve_alive(&data, &[]);
    assert!(wait_port(port, Duration::from_secs(15)), "降级不阻断看板");
    std::thread::sleep(Duration::from_millis(400));
    let (_o, e) = kill(&mut child);
    assert!(
        e.contains("文件日志降级") || e.contains("不可用"),
        "stderr 应含降级提示: {e}"
    );
    assert_eq!(
        std::fs::read_to_string(&target).unwrap(),
        "do not touch",
        "symlink 目标零写入"
    );
}

/// panic 注入消息含换行与 ESC:端到端净化(文件层与 stderr 双路转义为可见字面)。
#[test]
fn panic_message_control_chars_sanitized_end_to_end() {
    let data = tmpdir("serve-esc");
    let logs = data.join("logs");
    let (mut child, port) = spawn_serve_alive(&data, &[("USAGEBOOK_TEST_PANIC", "1")]);
    assert!(wait_port(port, Duration::from_secs(15)));
    let content = wait_panic_entry(&logs);
    let (_o, err) = kill(&mut child);
    assert!(content.contains("test panic"));
    // 内容换行转义为可见字面;ESC 随禁密词整体脱敏(词级 [REDACTED] 会连带吞掉)——
    // 安全属性 = 无原始控制字节 + 无 canary。
    assert!(content.contains("\\n"), "内容换行转义为字面: {content}");
    assert!(!content.contains('\u{1b}'), "不得残留原始 ESC");
    assert!(!content.contains("UBCANARY") && !content.contains("sk-testsecret"));
    assert!(
        !err.contains('\u{1b}') && !err.contains("UBCANARY"),
        "stderr 同规: {err}"
    );
}

/// RUST_BACKTRACE=1 时 backtrace 落盘且同样过脱敏。
#[test]
fn panic_backtrace_recorded_and_scrubbed() {
    let data = tmpdir("serve-bt");
    let logs = data.join("logs");
    let (mut child, port) = spawn_serve_alive(
        &data,
        &[("USAGEBOOK_TEST_PANIC", "1"), ("RUST_BACKTRACE", "1")],
    );
    assert!(wait_port(port, Duration::from_secs(15)));
    let content = wait_panic_entry(&logs);
    let (_o, _e) = kill(&mut child);
    assert!(
        content.contains("backtrace"),
        "RUST_BACKTRACE=1 时应有 backtrace 段: {content}"
    );
    assert!(!content.contains("UBCANARY") && !content.contains("sk-testsecret"));
}
