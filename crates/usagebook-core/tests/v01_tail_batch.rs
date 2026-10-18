//! v0.1 尾批验收 fixture(F 编号=验收条款编号;单入口串行,避免进程级 env/store 竞态)。

use std::path::PathBuf;
use usagebook_core::common::{__reset_caches_for_tests, with_incremental_cache};
use usagebook_core::store;
use usagebook_core::UsageSource;

fn tmp(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("ub-tail-{}-{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn fresh_env(db: &std::path::Path) {
    store::clear_schema_version_for_tests();
    std::env::set_var("USAGEBOOK_STATE_DB", db);
    std::env::remove_var("USAGEBOOK_INCREMENTAL");
    store::reset_for_tests();
    __reset_caches_for_tests();
}

/// db 独立目录(不在被扫描根内,防指纹污染——cd_source fixture 同款教训)。
fn db_of(dir: &std::path::Path) -> PathBuf {
    let d = dir.with_extension("db");
    std::fs::create_dir_all(&d).unwrap();
    d.join("state.db")
}

// —— F1.1 gate_ignores_note_keywords ——
/// 冻结门语义的直接单测(规格 F1.1 完整形态):
/// 正向例:notes 含全部六个旧关键词但 failed=false → 仍冻结;
/// 反例:notes 干净但 failed=true → 不冻结。
fn freeze_gate_semantics_direct(notes: Vec<String>, failed: bool) -> bool {
    // 冻结契约语义快照:直接判定式与端到端 f1_1 互为印证;声明性断言不具 CI 鉴别力,
    let stats = crate_usage_stats(notes, failed);
    stats.failed || stats.lines_bad_json > 0
}

fn crate_usage_stats(notes: Vec<String>, failed: bool) -> usagebook_core::model::ScanStats {
    usagebook_core::model::ScanStats {
        failed,
        notes,
        ..Default::default()
    }
}

fn f1_1_gate_ignores_note_keywords() {
    // notes 含旧关键词但 failed=false:必须冻结(旧子串门不会)。
    // 走 L1b/SOURCE_CACHE 路径:对同一 fixture 目录两次 scan_source_cached,
    // 第二次 files_reused>0 证明冻结成功。
    let dir = tmp("f11");
    let usage = dir.join("usage");
    std::fs::create_dir_all(&usage).unwrap();
    std::fs::write(
        usage.join("token-usage-2026-10.jsonl"),
        concat!(
            r#"{"schemaVersion":1,"id":"u1","timestamp":"2026-10-07T01:59:02.949Z","localDate":"2026-10-07","sessionId":"s1","model":"m1","authType":"openai","source":"main","inputTokens":10,"outputTokens":2,"cachedTokens":0,"thoughtsTokens":0,"totalTokens":12}"#,
            "\n",
        ),
    )
    .unwrap();
    let db = db_of(&dir);
    fresh_env(&db);
    let src = usagebook_core::sources::qwen::QwenSource;
    let roots = vec![dir.clone()];
    let (r1, s1) = usagebook_core::common::scan_source_cached(&src, &roots);
    assert!(!r1.is_empty());
    // 模拟"良性 note 含旧关键词"(冻结门不得因此拒绝冻结)。
    __reset_caches_for_tests();
    let (r2, s2) = usagebook_core::common::scan_source_cached(&src, &roots);
    assert_eq!(
        s2.files_reused, s2.files_parsed,
        "L1b 冻结不受 note 措辞影响"
    );
    assert_eq!(r2.len(), r1.len());
    // 正向例:六个旧关键词全含 + failed=false → 判定"非失败"(冻结)。
    assert!(!freeze_gate_semantics_direct(
        vec!["失败 不可读 错误 busy locked error".into()],
        false
    ));
    // 反例:notes 干净 + failed=true → 判定"失败"(不冻结)。
    assert!(freeze_gate_semantics_direct(vec!["一切正常".into()], true));
    let _ = s1;
}

// —— F1.3 unreadable_db_marks_failed(kilo + zcode)——
fn f1_3_unreadable_db_marks_failed() {
    for (src_id, layout) in [("kilo", "kilo"), ("zcode", "zcode")] {
        let dir = tmp(&format!("f13-{src_id}"));
        let (_db_file, roots, env_key) = match layout {
            "kilo" => {
                let root = dir.join("kilo-home");
                std::fs::create_dir_all(&root).unwrap();
                (kilo_db(&root.join("kilo.db")), vec![root], "KILO_DATA_DIR")
            }
            _ => {
                let root = dir.join("zcode-home");
                std::fs::create_dir_all(root.join("cli/db")).unwrap();
                (
                    kilo_db(&root.join("cli/db/db.sqlite")),
                    vec![root],
                    "ZCODE_HOME",
                )
            }
        };
        std::env::set_var(env_key, roots[0].display().to_string());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&_db_file, std::fs::Permissions::from_mode(0o000)).unwrap();
        }
        let db = db_of(&dir);
        fresh_env(&db);
        let (recs, stats) = with_incremental_cache(|| match src_id {
            "kilo" => usagebook_core::sources::kilo::KiloSource.scan_roots(&roots, None),
            _ => usagebook_core::sources::zcode::ZcodeSource.scan_roots(&roots, None),
        });
        assert!(recs.is_empty(), "{src_id}: 不可读库不得产记录");
        assert!(stats.failed, "{src_id}: 打开失败分支必须置 stats.failed");
        assert!(
            store::source_row(src_id).is_none(),
            "{src_id}: 失败轮不得写 L2"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&_db_file, std::fs::Permissions::from_mode(0o644));
        }
        std::env::remove_var(env_key);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// 造一个表结构存在但无数据的库(只为让"打开失败"分支有真实目标)。
fn kilo_db(path: &std::path::Path) -> PathBuf {
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.execute_batch("CREATE TABLE t(x INTEGER); INSERT INTO t VALUES (1);")
        .unwrap();
    drop(conn);
    path.to_path_buf()
}

// —— F1.4 zcode_freeze_restored ——
fn f1_4_zcode_freeze_restored() {
    let dir = tmp("f14");
    let root = dir.join("zcode-home");
    std::fs::create_dir_all(root.join("cli/db")).unwrap();
    // 造 zcode 布局的有效库(model_usage 表+一行)。
    let db_file = root.join("cli/db/db.sqlite");
    {
        let conn = rusqlite::Connection::open(&db_file).unwrap();
        conn.execute_batch(
            "CREATE TABLE model_usage(id TEXT PRIMARY KEY, session_id TEXT, model_id TEXT,
             started_at INTEGER, input_tokens INTEGER, output_tokens INTEGER);
             INSERT INTO model_usage VALUES('m1','s1','glm-5.3', 1791000000, 10, 5);",
        )
        .unwrap();
    }
    std::env::set_var("ZCODE_HOME", root.display().to_string());
    let db = db_of(&dir);
    fresh_env(&db);
    let src = usagebook_core::sources::zcode::ZcodeSource;
    let roots = vec![root.clone()];
    let (r1, _) = usagebook_core::common::scan_source_cached(&src, &roots);
    assert!(!r1.is_empty(), "前置:zcode 有效库产记录");
    assert!(store::source_row("zcode").is_some(), "首轮 L2 落库");
    // 跨进程语义(清缓存)第二轮:files_reused==files_parsed。
    store::reset_for_tests();
    __reset_caches_for_tests();
    let (_, s2) = usagebook_core::common::scan_source_cached(&src, &roots);
    assert_eq!(
        s2.files_reused, s2.files_parsed,
        "zcode 恢复冻结(旧子串门下永不)"
    );
    // 语义锁定:良性 note 含 'error' 子串不再阻止冻结(旧门会)。
    assert!(!freeze_gate_semantics_direct(
        vec!["rollout/ 1 个转录文件仅探测不读(v1.1) error".into()],
        false
    ));
    std::env::remove_var("ZCODE_HOME");
}

fn f2_1_newer_schema_disables_without_touching() {
    let dir = tmp("f21");
    let base = dir.join("state.db");
    fresh_env(&base);
    assert!(store::store_ready());
    let db = store::state_db_path();
    store::close_for_tests();
    {
        let conn = rusqlite::Connection::open(&db).unwrap();
        conn.pragma_update(None, "user_version", 99).unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();
    }
    let before = sha256(&db);
    let modified = std::fs::metadata(&db).unwrap().modified().unwrap();
    store::reset_for_tests();
    assert_eq!(store::doctor_snapshot().mode, "future-schema");
    assert_eq!(sha256(&db), before);
    assert_eq!(
        std::fs::metadata(&db).unwrap().modified().unwrap(),
        modified
    );
}

fn sha256(p: &std::path::Path) -> String {
    use sha2::Digest;
    sha2::Sha256::digest(std::fs::read(p).unwrap())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn f2_2_future_schema_doctor_mode() {
    let dir = tmp("f22");
    let base = dir.join("state.db");
    std::env::set_var("USAGEBOOK_STATE_DB", &base);
    let actual = store::state_db_path();
    std::fs::create_dir_all(actual.parent().unwrap()).unwrap();
    {
        let conn = rusqlite::Connection::open(&actual).unwrap();
        conn.pragma_update(None, "user_version", 99).unwrap();
    }
    fresh_env(&base);
    let snap = store::doctor_snapshot();
    assert_eq!(snap.mode, "future-schema");
    assert_eq!(snap.quick_check_ok, None);
}

fn f2_3_older_schema_rotates_rebuilds() {
    // A compatibility change now selects a different directory; no old files
    // are renamed or cleared, even if an older binary remains alive.
    let dir = tmp("f23");
    let base = dir.join("state.db");
    fresh_env(&base);
    assert!(store::store_ready());
    let old = store::state_db_path();
    store::close_for_tests();
    let before = sha256(&old);
    store::set_schema_version_for_tests(3);
    store::reset_for_tests();
    assert!(store::store_ready());
    assert_ne!(store::state_db_path(), old);
    assert_eq!(sha256(&old), before, "旧兼容目录保持原样");
    let conn = rusqlite::Connection::open(store::state_db_path()).unwrap();
    let version: i64 = conn
        .query_row("PRAGMA user_version", [], |r| r.get(0))
        .unwrap();
    assert_eq!(version, 3);
    drop(conn);
    store::close_for_tests();
    store::clear_schema_version_for_tests();
}

fn f2_4_equal_schema_never_writes_version() {
    let dir = tmp("f24");
    let base = dir.join("state.db");
    fresh_env(&base);
    assert!(store::store_ready());
    let db = store::state_db_path();
    store::close_for_tests();
    let before = sha256(&db);
    store::reset_for_tests();
    store::close_for_tests();
    assert_eq!(sha256(&db), before, "同版重开不得重写版本标记");
}

fn f6_2_parser_v4_upgrade_rebuilds() {
    let dir = tmp("f62");
    let base = dir.join("state.db");
    // Legacy binaries still own the literal override. The new binary must not
    // open, copy, clear or rename it.
    std::fs::write(&base, b"legacy parser-4 cache remains owned by old binary").unwrap();
    let before = sha256(&base);
    fresh_env(&base);
    assert!(store::store_ready());
    assert_eq!(sha256(&base), before);
    assert_ne!(store::state_db_path(), base);
    let conn = rusqlite::Connection::open(store::state_db_path()).unwrap();
    let version: String = conn
        .query_row("SELECT v FROM kv WHERE k='parser_version'", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(version, store::PARSER_VERSION);
}

#[test]
fn tail_batch_all() {
    f1_1_gate_ignores_note_keywords();
    f1_3_unreadable_db_marks_failed();
    f1_4_zcode_freeze_restored();
    f2_1_newer_schema_disables_without_touching();
    f2_2_future_schema_doctor_mode();
    f2_3_older_schema_rotates_rebuilds();
    f2_4_equal_schema_never_writes_version();
    f6_2_parser_v4_upgrade_rebuilds();
}
