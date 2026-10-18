//! 增量持久层 fixture 测试 test/)。
//! 测试手法 注):L1a/L1b 为进程级静态,以 __reset_caches_for_tests + store::reset_for_tests
//! 模拟"新进程";env 全局 → 全部用例串行(ENV_LOCK)。

use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{Duration, SystemTime};
use usagebook_core::common::{
    cached_usage_file, with_incremental_cache, CacheOutcome, FileParser, UsageLines,
};
use usagebook_core::model::ScanStats;
use usagebook_core::store;

static ENV_LOCK: Mutex<()> = Mutex::new(());

/// 记录集合的顺序无关比较(适配器去重容器为 HashMap,记录顺序非稳定契约)。
fn sorted_dbg(rs: &[usagebook_core::UsageRecord]) -> Vec<String> {
    let mut out: Vec<String> = rs.iter().map(|r| format!("{r:?}")).collect();
    out.sort();
    out
}

fn temp_dir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("usagebook-incr-{}-{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn project_identity(v: &serde_json::Value) -> Option<serde_json::Value> {
    Some(v.clone())
}

fn parser() -> FileParser {
    FileParser {
        project: project_identity,
        keep: |_| true,
    }
}

/// 单轮调用:重置进程级状态(=新进程),设 env,跑一次缝并记账。
fn round(
    state_db: &std::path::Path,
    file: &std::path::Path,
) -> (ArcProxy, CacheOutcome, ScanStats) {
    std::env::set_var("USAGEBOOK_STATE_DB", state_db);
    std::env::remove_var("USAGEBOOK_INCREMENTAL");
    store::reset_for_tests();
    usagebook_core::common::__reset_caches_for_tests();
    run_once(file)
}

fn round_off(
    state_db: &std::path::Path,
    file: &std::path::Path,
) -> (ArcProxy, CacheOutcome, ScanStats) {
    std::env::set_var("USAGEBOOK_STATE_DB", state_db);
    std::env::set_var("USAGEBOOK_INCREMENTAL", "off");
    store::reset_for_tests();
    usagebook_core::common::__reset_caches_for_tests();
    run_once(file)
}

struct ArcProxy(std::sync::Arc<UsageLines>);
impl ArcProxy {
    fn events(&self) -> &[(u64, serde_json::Value)] {
        &self.0.events
    }
}

fn run_once(file: &std::path::Path) -> (ArcProxy, CacheOutcome, ScanStats) {
    let p = parser();
    let (lines, outcome) = with_incremental_cache(|| {
        cached_usage_file(
            "tfx",
            file,
            Some(FileParser {
                project: p.project,
                keep: p.keep,
            }),
            || usagebook_core::common::read_usage_jsonl_committed(file, p.project, p.keep),
        )
    });
    let mut stats = ScanStats::default();
    lines.account(&mut stats, outcome);
    (ArcProxy(lines), outcome, stats)
}

fn bump_mtime(path: &std::path::Path) {
    let later = SystemTime::now() + Duration::from_secs(2);
    let f = std::fs::OpenOptions::new().append(true).open(path).unwrap();
    f.set_times(std::fs::FileTimes::new().set_modified(later))
        .unwrap();
}

fn set_mtime(path: &std::path::Path, t: SystemTime) {
    let f = std::fs::OpenOptions::new().append(true).open(path).unwrap();
    f.set_times(std::fs::FileTimes::new().set_modified(t))
        .unwrap();
}

fn write_lines(path: &std::path::Path, lines: &[&str], terminated: bool) {
    let mut s = String::new();
    for (i, l) in lines.iter().enumerate() {
        s.push_str(l);
        if terminated || i + 1 < lines.len() {
            s.push('\n');
        }
    }
    std::fs::write(path, s).unwrap();
    bump_mtime(path);
}

fn append_lines(path: &std::path::Path, lines: &[&str]) {
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new().append(true).open(path).unwrap();
    for l in lines {
        writeln!(f, "{l}").unwrap();
    }
    drop(f);
    bump_mtime(path);
}

// —— 1. fresh_insert_persists_cursor:全量读落游标;新进程轮零读复用 ——
#[test]
fn fresh_insert_persists_cursor() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let dir = temp_dir("f1");
    let file = dir.join("s.jsonl");
    write_lines(&file, &[r#"{"a":1}"#, r#"{"a":2}"#], true);
    let db = dir.join("state.db");

    let (_, outcome, stats) = round(&db, &file);
    assert_eq!(outcome, CacheOutcome::FreshLoad);
    assert_eq!(stats.rewrote, 1);
    let row = store::file_row("tfx", &file).expect("游标必须落库");
    assert_eq!(
        row.cursor.byte_offset as u64,
        std::fs::metadata(&file).unwrap().len()
    );

    // 新进程语义(L1/L2 皆冷)且无变更 → Unchanged 零读。
    let (_, outcome2, stats2) = round(&db, &file);
    assert_eq!(outcome2, CacheOutcome::Reused);
    assert_eq!(stats2.bytes_read, 0);
    assert_eq!(stats2.files_reused, 1);
}

// —— 2. tail_append_only_reads_new_bytes:追加只读尾部 ——
#[test]
fn tail_append_only_reads_new_bytes() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let dir = temp_dir("f2");
    let file = dir.join("s.jsonl");
    write_lines(&file, &[r#"{"a":1}"#], true);
    let db = dir.join("state.db");
    let (_, o1, _) = round(&db, &file);
    assert_eq!(o1, CacheOutcome::FreshLoad);

    append_lines(&file, &[r#"{"a":2}"#, r#"{"a":3}"#]);
    let (lines, outcome, stats) = round(&db, &file);
    assert_eq!(outcome, CacheOutcome::Appended);
    let tail_bytes = 2 * (r#"{"a":2}"#.len() as u64 + 1);
    assert!(
        stats.bytes_read <= tail_bytes + 4096,
        "bytes_read={} 应≤尾部+窗口",
        stats.bytes_read
    );
    assert_eq!(lines.events().len(), 3);
    // 载荷推进:下一轮(无变更)Unchanged 且事件数=3。
    let (lines2, o2, s2) = round(&db, &file);
    assert_eq!(o2, CacheOutcome::Reused);
    assert_eq!(s2.bytes_read, 0);
    assert_eq!(lines2.events().len(), 3);
}

// —— 3. pending_tail_not_committed:无 \n 末行不持久;无变更轮走 AppendFrom 不丢行;补 \n 恰好提交一次 ——
#[test]
fn pending_tail_not_committed() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let dir = temp_dir("f3");
    let file = dir.join("s.jsonl");
    write_lines(&file, &[r#"{"a":1}"#, r#"{"a":2}"#], false); // 末行无 \n
    let db = dir.join("state.db");

    let (l1, o1, _) = round(&db, &file);
    assert_eq!(o1, CacheOutcome::FreshLoad);
    assert_eq!(l1.events().len(), 2, "无换行末行本轮可见");
    let row = store::file_row("tfx", &file).unwrap();
    assert!(
        row.cursor.byte_offset < std::fs::metadata(&file).unwrap().len(),
        "游标不得越过未终结尾段"
    );
    let stored = store::decode_lines(&row.events, row.events_len).unwrap();
    assert_eq!(stored.events.len(), 1, "载荷只含已提交前缀");

    // 无任何变更的下一轮(mtime/size 均不变):判 AppendFrom 续读,记录不消失 核心断言)。
    let old_mtime = std::fs::metadata(&file).unwrap().modified().unwrap();
    let _ = old_mtime;
    let (l2, o2, _) = round(&db, &file);
    assert_eq!(
        o2,
        CacheOutcome::Appended,
        "pending 残留必须走 AppendFrom 而非 Unchanged"
    );
    assert_eq!(l2.events().len(), 2, "pending 行不得消失");

    // —— P0 形态:pending 行不产出事件(撕裂写/坏行)不得下溢/丢事件 ——
    {
        let dir2 = temp_dir("f3-torn");
        let file2 = dir2.join("s.jsonl");
        // 末行为无 \n 的半行坏 JSON(撕裂写残留, 场景族)。
        std::fs::write(
            &file2,
            concat!(
                r#"{"a":1}"#,
                "\n",
                r#"{"a":2}"#,
                "\n",
                r#"{"type":"assistant","broken"#,
            ),
        )
        .unwrap();
        bump_mtime(&file2);
        let db2 = dir2.join("state.db");
        let (l1, o1, _) = round(&db2, &file2);
        assert_eq!(o1, CacheOutcome::FreshLoad);
        assert_eq!(
            l1.events().len(),
            2,
            "坏 pending 行不产出事件,已提交 2 事件"
        );
        // 无变更下一轮:判 AppendFrom 续读 pending 坏行——不得 panic、事件数不变。
        let (l2, o2, s2) = round(&db2, &file2);
        assert_eq!(o2, CacheOutcome::Appended);
        assert_eq!(l2.events().len(), 2, "已提交事件一个不丢(R5)");
        assert_eq!(s2.lines_bad_json, 1, "pending 坏行按坏行计数可见");
    }

    // 补 \n 终结上轮尾行,再追加一行新事件:恰好提交一次,总数无重复。
    {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&file)
            .unwrap();
        write!(f, "\n{{\"a\":3}}\n").unwrap();
    }
    bump_mtime(&file);
    let (l3, o3, _) = round(&db, &file);
    assert_eq!(o3, CacheOutcome::Appended);
    assert_eq!(l3.events().len(), 3, "提交恰好一次,无重复事件");
}

// —— 4. truncation_replays_full_and_selfheals:截断全量重放;第二轮必须 Unchanged(自愈) ——
#[test]
fn truncation_replays_full_and_selfheals() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let dir = temp_dir("f4");
    let file = dir.join("s.jsonl");
    write_lines(&file, &[r#"{"a":1}"#, r#"{"a":2}"#], true);
    let db = dir.join("state.db");
    round(&db, &file);

    write_lines(&file, &[r#"{"a":1}"#], true); // 截断为更短
    let (l2, o2, s2) = round(&db, &file);
    assert_eq!(o2, CacheOutcome::Replay("truncated"));
    assert_eq!(s2.replayed, 1);
    assert_eq!(s2.rewrote, 1);
    assert_eq!(l2.events().len(), 1);

    let (_, o3, s3) = round(&db, &file); // 无再变更
    assert_eq!(o3, CacheOutcome::Reused, "截断重放后 L2 必须自愈");
    assert_eq!(s3.bytes_read, 0);
}

// —— 5. same_size_rewrite_detected:等长改写靠 seal 检出 ——
#[test]
fn same_size_rewrite_detected() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let dir = temp_dir("f5");
    let file = dir.join("s.jsonl");
    write_lines(&file, &[r#"{"a":1}"#], true);
    let db = dir.join("state.db");
    round(&db, &file);

    write_lines(&file, &[r#"{"b":1}"#], true); // 等长不同内容
    assert_eq!(
        std::fs::metadata(&file).unwrap().len(),
        r#"{"a":1}"#.len() as u64 + 1
    );
    let (_, o2, s2) = round(&db, &file);
    assert!(matches!(
        o2,
        CacheOutcome::Replay("identity" | "same-size-change")
    ));
    assert_eq!(s2.replayed, 1);
}

// —— 6. prefix_window_limitation:seal 窗口盲区按平台分层固化——
// unix:任何真实改写(即使等长+复原 mtime)都会改变 ctime → identity 捕获(强于 seal 窗口检出);
// Windows/identity 缺失:窗口外前缀改写不可检出 —— 以 judge 纯函数固化(identity=None)。
#[test]
fn prefix_window_limitation() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let dir = temp_dir("f6");
    let file = dir.join("s.jsonl");
    let filler = format!("{{\"pad\":\"{}\"}}", "x".repeat(8192));
    write_lines(&file, &[&filler, r#"{"a":1}"#], true);
    let db = dir.join("state.db");
    round(&db, &file);
    let old_mtime = std::fs::metadata(&file).unwrap().modified().unwrap();

    // 改写窗口之外的前缀(等长),再显式复原 mtime 与 size(fixture 6 手法)。
    let filler2 = format!("{{\"pad\":\"{}\"}}", "y".repeat(8192));
    write_lines(&file, &[&filler2, r#"{"a":1}"#], true);
    set_mtime(&file, old_mtime);

    // unix 分层:identity(ctime)捕获改写 → 重放(数据自愈,优于"判 Unchanged")。
    #[cfg(unix)]
    {
        let (_, o2, _) = round(&db, &file);
        assert_eq!(
            o2,
            CacheOutcome::Replay("identity"),
            "unix:窗口外前缀改写被 identity 捕获(identity 分支)"
        );
    }

    // 窗口盲区本体(Windows/identity 缺失语义):judge 纯函数,seal 窗口外不可检出。
    let stamp = usagebook_core::common::file_stamp(&file).unwrap();
    // 构造:mtime/size 相等但 seal 失配(窗口外改写的可检出面=窗口内;窗口外 seal 仍匹配旧值)。
    // 直接证明"identity=None 时 step3 通过 → Unchanged":局限固化。
    let verdict = usagebook_core::store::judge(
        stamp.mtime_ns(),
        stamp.size(),
        None,
        Some(&usagebook_core::store::FileCursor {
            mtime_ns: stamp.mtime_ns(),
            size: stamp.size(),
            byte_offset: stamp.size(),
            tail_seal: usagebook_core::store::tail_seal(&[]),
            identity: None,
        }),
        |_| unreachable!("Unchanged 不得读窗口"),
    );
    assert_eq!(
        verdict,
        usagebook_core::store::Verdict::Unchanged,
        "identity 缺失时窗口外前缀改写判 Unchanged(已知局限,Windows 面)"
    );
}

// —— 7. corrupt_db_rotates:坏库轮转自愈 ——
#[test]
fn corrupt_db_rotates() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let dir = temp_dir("f7");
    let file = dir.join("s.jsonl");
    write_lines(&file, &[r#"{"a":1}"#], true);
    let db = dir.join("state.db");
    std::env::set_var("USAGEBOOK_STATE_DB", &db);
    let actual = store::state_db_path();
    std::fs::create_dir_all(actual.parent().unwrap()).unwrap();
    std::fs::write(&actual, b"this is not a sqlite database at all").unwrap();

    let (_, o1, _) = round(&db, &file);
    assert_eq!(o1, CacheOutcome::FreshLoad, "坏库轮转后正常全量读");
    let bad: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().contains(".bad-"))
        .collect();
    assert!(!bad.is_empty(), "必须存在 .bad-<ms> 轮转文件");
    let snap = store::doctor_snapshot();
    assert_eq!(snap.rotations, 1);
    assert_eq!(
        store::file_row("tfx", &file).map(|_| ()),
        Some(()),
        "重建后游标照常落库"
    );
}

// —— 8. incremental_off_switch:off 时库零写入(不建库) ——
#[test]
fn incremental_off_switch() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let dir = temp_dir("f8");
    let file = dir.join("s.jsonl");
    write_lines(&file, &[r#"{"a":1}"#], true);
    let db = dir.join("state.db");

    let (_, o1, s1) = round_off(&db, &file);
    assert_eq!(o1, CacheOutcome::FreshLoad);
    assert!(!db.exists(), "off 模式严禁建库");
    assert_eq!(s1.rewrote, 1);
    assert_eq!(s1.bytes_read, std::fs::metadata(&file).unwrap().len());
    std::env::remove_var("USAGEBOOK_INCREMENTAL");
}

// —— 9. kimi_ordinal_continues:行号续算连续无重复无跳号 ——
#[test]
fn kimi_ordinal_continues() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let dir = temp_dir("f9");
    let file = dir.join("s.jsonl");
    // 空行不计 total/ordinal(与 read_usage_jsonl 口径逐位一致;fixture 9 补断言)。
    std::fs::write(&file, "\n{\"a\":1}\n\n{\"a\":2}\n\n").unwrap();
    bump_mtime(&file);
    let db = dir.join("state.db");
    round(&db, &file);

    append_lines(&file, &[r#"{"a":3}"#, "", r#"{"a":4}"#]);
    let (l2, o2, _) = round(&db, &file);
    assert_eq!(o2, CacheOutcome::Appended);
    let ordinals: Vec<u64> = l2.events().iter().map(|(n, _)| *n).collect();
    assert_eq!(
        ordinals,
        vec![1, 2, 3, 4],
        "ordinal 续算必须连续无重复无跳号(空行不计)"
    );
    let stored = store::decode_lines(
        &store::file_row("tfx", &file).unwrap().events,
        store::file_row("tfx", &file).unwrap().events_len,
    )
    .unwrap();
    assert_eq!(stored.total, 4, "空行不计 total(kimi line_no 口径)");
}

// —— 13. parser_version_wipe:版本不匹配整体重建 ——
#[test]
fn parser_namespace_mismatch_does_not_wipe() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let dir = temp_dir("f13");
    let file = dir.join("s.jsonl");
    write_lines(&file, &[r#"{"a":1}"#], true);
    let db = dir.join("state.db");
    round(&db, &file);
    assert!(store::file_row("tfx", &file).is_some());

    store::set_kv_for_tests("parser_version", "1");
    let (_, o2, s2) = round(&db, &file);
    assert_eq!(o2, CacheOutcome::FreshLoad, "命名空间标记异常时旁路全读");
    assert_eq!(s2.rewrote, 1);
    assert_eq!(store::doctor_snapshot().mode, "incompatible-cache");
    let conn = rusqlite::Connection::open(store::state_db_path()).unwrap();
    let count: i64 = conn
        .query_row("SELECT count(*) FROM file_state", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 1, "不得清空不匹配的命名空间");
}

// —— 16. prefix_rewrite_with_growth:窗口内前缀改写+尺寸增长 → seal 失配重放 ——
#[test]
fn prefix_rewrite_with_growth() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let dir = temp_dir("f16");
    let file = dir.join("s.jsonl");
    write_lines(&file, &[r#"{"a":1}"#, r#"{"a":2}"#], true);
    let db = dir.join("state.db");
    round(&db, &file);

    // 第二行处于旧游标窗口内:等长改写 + 追加一行(尺寸增长)。
    write_lines(&file, &[r#"{"a":1}"#, r#"{"b":2}"#, r#"{"a":3}"#], true);
    let (_, o2, s2) = round(&db, &file);
    assert_eq!(o2, CacheOutcome::Replay("seal-mismatch"));
    assert_eq!(s2.replayed, 1);
}

// —— 12lite. semantics_parity_synthetic:合成语料 on/off 逐字段全等(完整 25 源对拍在根仓) ——
#[test]
fn semantics_parity_synthetic() {
    use usagebook_core::UsageSource;
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let dir = temp_dir("f12lite");
    let db = dir.join("state.db");

    // claude 布局:roots 下 projects/<p>/<s>.jsonl;含 pending 尾行与跨文件同 message.id 的对抗形态。
    let proj = dir.join("projects/-tmp-demo");
    std::fs::create_dir_all(&proj).unwrap();
    std::fs::write(
        proj.join("s1.jsonl"),
        concat!(
            r#"{"type":"assistant","sessionId":"s1","requestId":"r1","timestamp":"2026-10-12T10:00:00+08:00","message":{"id":"msg_x","model":"claude-opus-4-8","usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":100,"cache_creation_input_tokens":50}},"stop_reason":"end_turn"}"#, "\n",
            r#"{"type":"assistant","sessionId":"s1","requestId":"r2","timestamp":"2026-10-12T10:01:00+08:00","message":{"id":"msg_y","model":"claude-opus-4-8","usage":{"input_tokens":3,"output_tokens":9,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}},"stop_reason":"end_turn"}"#, "\n",
            r#"{"type":"assistant","sessionId":"s1","requestId":"r3","timestamp":"2026-10-12T10:02:00+08:00","message":{"id":"msg_z","model":"claude-opus-4-8","usage":{"input_tokens":1,"output_tokens":1,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}}"#,
        ),
    )
    .unwrap();
    // 跨文件同 msg_id:claude 的 scan 级去重应与现状一致地收敛。
    std::fs::write(
        proj.join("s2.jsonl"),
        concat!(
            r#"{"type":"assistant","sessionId":"s1","requestId":"r1","timestamp":"2026-10-12T10:00:00+08:00","message":{"id":"msg_x","model":"claude-opus-4-8","usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":100,"cache_creation_input_tokens":50}},"stop_reason":"end_turn"}"#, "\n",
        ),
    )
    .unwrap();
    let roots = vec![dir.clone()];

    let src = usagebook_core::sources::claude::ClaudeSource;
    // off:一次性直读。
    std::env::set_var("USAGEBOOK_STATE_DB", &db);
    std::env::set_var("USAGEBOOK_INCREMENTAL", "off");
    store::reset_for_tests();
    usagebook_core::common::__reset_caches_for_tests();
    let (records_off, _) = src.scan_roots(&roots, None);

    // on:首轮(Fresh+pending)+ 追加后(Replay/Append)再跑,与 off 全等。
    std::env::remove_var("USAGEBOOK_INCREMENTAL");
    store::reset_for_tests();
    usagebook_core::common::__reset_caches_for_tests();
    let (records_on, _) =
        usagebook_core::common::with_incremental_cache(|| src.scan_roots(&roots, None));
    assert_eq!(sorted_dbg(&records_off), sorted_dbg(&records_on));

    // 追加一行后第二轮(on)仍与 off 全等(覆盖 AppendFrom 合并路径)。
    append_lines(
        &proj.join("s1.jsonl"),
        &[
            r#"{"type":"assistant","sessionId":"s1","requestId":"r4","timestamp":"2026-10-12T10:03:00+08:00","message":{"id":"msg_w","model":"claude-opus-4-8","usage":{"input_tokens":7,"output_tokens":7,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}},"stop_reason":"end_turn"}"#,
        ],
    );
    usagebook_core::common::__reset_caches_for_tests();
    let (records_on2, _) =
        usagebook_core::common::with_incremental_cache(|| src.scan_roots(&roots, None));

    std::env::set_var("USAGEBOOK_INCREMENTAL", "off");
    store::reset_for_tests();
    usagebook_core::common::__reset_caches_for_tests();
    let (records_off2, _) = src.scan_roots(&roots, None);
    assert_eq!(sorted_dbg(&records_off2), sorted_dbg(&records_on2));
    std::env::remove_var("USAGEBOOK_INCREMENTAL");
}

// —— 15. cd_source_l2_reuse:非 JSONL4 源跨"进程"复用(qwen 布局) ——
#[test]
fn cd_source_l2_reuse() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let dir = temp_dir("f15");
    // state.db 必须在被扫描根之外(指纹遍历整棵根树;库自身写入会使指纹永不匹配)。
    let db_dir = temp_dir("f15-db");
    let db = db_dir.join("state.db");
    let usage_dir = dir.join("usage");
    std::fs::create_dir_all(&usage_dir).unwrap();
    std::fs::write(
        usage_dir.join("token-usage-2026-10.jsonl"),
        concat!(
            r#"{"schemaVersion":1,"id":"u1","timestamp":"2026-10-07T01:59:02.949Z","localDate":"2026-10-07","sessionId":"s1","model":"deepseek-v4-pro","authType":"openai","source":"main","inputTokens":26244,"outputTokens":98,"cachedTokens":25984,"thoughtsTokens":17,"totalTokens":26342}"#, "\n",
        ),
    )
    .unwrap();
    let roots = vec![dir.clone()];
    let src = usagebook_core::sources::qwen::QwenSource;

    std::env::set_var("USAGEBOOK_STATE_DB", &db);
    std::env::remove_var("USAGEBOOK_INCREMENTAL");
    store::reset_for_tests();
    usagebook_core::common::__reset_caches_for_tests();
    let (recs1, st1) = usagebook_core::common::scan_source_cached(&src, &roots);
    assert!(!recs1.is_empty());
    assert!(st1.files_read > 0, "首轮必须真实读取");

    // 模拟新进程:清 L1b + 重开 store → 命中 L2:零读、records 全等。
    store::reset_for_tests();
    usagebook_core::common::__reset_caches_for_tests();
    let (recs2, st2) = usagebook_core::common::scan_source_cached(&src, &roots);
    assert_eq!(st2.bytes_read, 0);
    assert_eq!(st2.files_reused, st2.files_parsed);
    assert_eq!(format!("{recs1:?}"), format!("{recs2:?}"));

    // 失配(新增文件)→ 重扫并更新 L2。
    std::fs::write(
        usage_dir.join("token-usage-2026-11.jsonl"),
        concat!(
            r#"{"schemaVersion":1,"id":"u2","timestamp":"2026-11-07T01:59:02.949Z","localDate":"2026-11-07","sessionId":"s1","model":"deepseek-v4-pro","authType":"openai","source":"main","inputTokens":100,"outputTokens":9,"cachedTokens":25,"thoughtsTokens":1,"totalTokens":110}"#, "\n",
        ),
    )
    .unwrap();
    store::reset_for_tests();
    usagebook_core::common::__reset_caches_for_tests();
    let (recs3, st3) = usagebook_core::common::scan_source_cached(&src, &roots);
    assert!(st3.files_read > 0);
    assert!(recs3.len() > recs1.len());
}

// —— 2b. dsh_reload_on_change:reload 源的缝级集成 ——
#[test]
fn dsh_reload_on_change() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let dir = temp_dir("f2b");
    let file = dir.join("session.bin"); // 二进制形态无关紧要:reload 模式 load 闭包自定义
    std::fs::write(&file, b"{\"a\":1}\n").unwrap();
    bump_mtime(&file);
    let db = dir.join("state.db");

    // 自定义 load(模拟 dsh 闭包:整读+填 committed_*=整文件)。
    fn reload_load(path: &std::path::Path) -> UsageLines {
        let mut out = UsageLines::default();
        if let Ok(bytes) = std::fs::read(path) {
            out.readable = true;
            out.bytes = bytes.len() as u64;
            for raw in bytes.split(|&b| b == b'\n') {
                out.push(&String::from_utf8_lossy(raw), project_identity);
            }
            out.committed_bytes = out.bytes;
            out.committed_lines = out.total;
            out.committed_bad = out.bad;
        } else {
            out.bad = 1;
        }
        out
    }

    std::env::set_var("USAGEBOOK_STATE_DB", &db);
    std::env::remove_var("USAGEBOOK_INCREMENTAL");
    store::reset_for_tests();
    usagebook_core::common::__reset_caches_for_tests();
    let (l1, o1) = {
        let (l, o) = with_incremental_cache(|| {
            cached_usage_file("tfx2", &file, None, || reload_load(&file))
        });
        (ArcProxy(l), o)
    };
    assert_eq!(o1, CacheOutcome::FreshLoad);
    assert_eq!(l1.events().len(), 1);

    // 无变更 → Unchanged(reload 判定,零读)。
    store::reset_for_tests();
    usagebook_core::common::__reset_caches_for_tests();
    let (l2, o2, s2) = {
        let (l, o) = with_incremental_cache(|| {
            cached_usage_file("tfx2", &file, None, || reload_load(&file))
        });
        let mut st = ScanStats::default();
        l.account(&mut st, o);
        (ArcProxy(l), o, st)
    };
    assert_eq!(o2, CacheOutcome::Reused, "reload 源无变更轮零读复用");
    assert_eq!(s2.bytes_read, 0);
    assert_eq!(l2.events().len(), 1);

    // 任何失配(mtime 变)→ 整文件重载替换,禁用尾读。
    bump_mtime(&file);
    std::fs::write(&file, b"{\"a\":1}\n{\"a\":2}").unwrap();
    bump_mtime(&file);
    store::reset_for_tests();
    usagebook_core::common::__reset_caches_for_tests();
    let (l3, o3, s3) = {
        let (l, o) = with_incremental_cache(|| {
            cached_usage_file("tfx2", &file, None, || reload_load(&file))
        });
        let mut st = ScanStats::default();
        l.account(&mut st, o);
        (ArcProxy(l), o, st)
    };
    assert_eq!(
        o3,
        CacheOutcome::Replay("reload"),
        "失配→Rewound(reload),不走 AppendFrom"
    );
    assert_eq!(s3.replayed, 1);
    assert!(s3.bytes_read >= 15, "整文件重读");
    assert_eq!(l3.events().len(), 2);
    let _ = &l1;
}

// —— 17. gc_dead_rows:扫描前 &[] 不触 L2;发现后清单轮删除死行 ——
#[test]
fn gc_dead_rows() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let dir = temp_dir("f17");
    let db = dir.join("state.db");
    let files: Vec<_> = (0..4).map(|i| dir.join(format!("s{i}.jsonl"))).collect();
    for f in &files {
        write_lines(f, &[r#"{"a":1}"#], true);
    }
    // 首轮:4 文件全落库。
    for f in &files {
        round(&db, f);
    }
    let count = |src: &str| -> usize {
        store::doctor_snapshot()
            .files_by_source
            .iter()
            .filter(|(s, _, _)| s == src)
            .map(|(_, n, _)| *n as usize)
            .sum()
    };
    assert_eq!(count("tfx"), 4, "4 文件 4 游标");

    // 删除 2 个文件(模拟源工具清理会话)→ 剩余文件再扫一轮(其 prune 带非空清单)→ 死行被 GC。
    std::fs::remove_file(&files[0]).unwrap();
    std::fs::remove_file(&files[1]).unwrap();
    round(&db, &files[2]);
    round(&db, &files[3]);
    // 适配器在发现文件清单后调用 prune(L2 GC 的挂载点);fixture 直调缝,须手动模拟。
    with_incremental_cache(|| {
        usagebook_core::common::prune_usage_cache("tfx", &[files[2].clone(), files[3].clone()]);
    });
    assert_eq!(count("tfx"), 2, "已删文件的游标必须被 GC");
    let snap = store::doctor_snapshot();
    assert!(snap.gc_files >= 2, "GC 计数可见(实际 {})", snap.gc_files);
}
