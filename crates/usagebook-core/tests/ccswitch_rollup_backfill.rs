//! ccswitch-rollup 回填回归(2026-10-19):
//! 整日替换(原生覆盖日跳过)、请求计数随记录携带、六元组同日多行归并、
//! 未映射 app_type 不回填;快照聚合 n 按 request_count 累加。

use std::collections::HashSet;

fn temp_db(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("usagebook-ccsw-{}-{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d.join("cc-switch.db")
}

const ROLLUP_DDL: &str = "CREATE TABLE usage_daily_rollups (
    date TEXT NOT NULL, app_type TEXT NOT NULL, provider_id TEXT NOT NULL,
    model TEXT NOT NULL, request_model TEXT NOT NULL DEFAULT '', pricing_model TEXT NOT NULL DEFAULT '',
    request_count INTEGER NOT NULL DEFAULT 0, success_count INTEGER NOT NULL DEFAULT 0,
    input_tokens INTEGER NOT NULL DEFAULT 0, output_tokens INTEGER NOT NULL DEFAULT 0,
    cache_read_tokens INTEGER NOT NULL DEFAULT 0, cache_creation_tokens INTEGER NOT NULL DEFAULT 0,
    total_cost_usd TEXT NOT NULL DEFAULT '0', avg_latency_ms INTEGER NOT NULL DEFAULT 0,
    input_token_semantics INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (date, app_type, provider_id, model, request_model, pricing_model))";

// Keep each fixture's SQL column values visible at its call site.
#[allow(clippy::too_many_arguments)]
fn insert(
    conn: &rusqlite::Connection,
    d: &str,
    app: &str,
    model: &str,
    i: i64,
    cr: i64,
    o: i64,
    n: i64,
) {
    conn.execute(
        "INSERT INTO usage_daily_rollups (date, app_type, provider_id, model, request_count, input_tokens, output_tokens, cache_read_tokens) VALUES (?1, ?2, '_x', ?3, ?4, ?5, ?6, ?7)",
        rusqlite::params![d, app, model, n, i, o, cr],
    )
    .unwrap();
}

/// 带 cache_creation 与 input_token_semantics 的夹具(0=legacy/1=total/2=fresh,cc-switch 常量)。
#[allow(clippy::too_many_arguments)]
fn insert_sem(
    conn: &rusqlite::Connection,
    d: &str,
    app: &str,
    model: &str,
    i: i64,
    cr: i64,
    cw: i64,
    o: i64,
    n: i64,
    sem: i64,
) {
    conn.execute(
        "INSERT INTO usage_daily_rollups (date, app_type, provider_id, model, request_count, input_tokens, output_tokens, cache_read_tokens, cache_creation_tokens, input_token_semantics) VALUES (?1, ?2, '_x', ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        rusqlite::params![d, app, model, n, i, o, cr, cw, sem],
    )
    .unwrap();
}

#[test]
fn backfill_day_replacement_and_counts() {
    let db = temp_db("main");
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.execute_batch(ROLLUP_DDL).unwrap();
    // 09-01:原生无覆盖 → 回填;六元组同日两模型两行 → 两条记录
    insert(
        &conn,
        "2026-09-01",
        "claude",
        "claude-opus-4-8",
        100,
        900,
        50,
        10,
    );
    insert(
        &conn,
        "2026-09-01",
        "claude",
        "claude-sonnet-4-8",
        20,
        180,
        10,
        2,
    );
    // 09-12:原生已覆盖(磁盘残段)→ 整日跳过,不得混采
    insert(
        &conn,
        "2026-09-12",
        "claude",
        "claude-opus-4-8",
        500,
        5000,
        300,
        40,
    );
    // codex 空窗日 → 回填;新版 rollup(semantics=2)input 已归一 fresh 直取
    insert_sem(
        &conn,
        "2026-08-01",
        "codex",
        "gpt-6-astra",
        700,
        2100,
        0,
        90,
        7,
        2,
    );
    // 未映射 app_type → 不回填
    insert(&conn, "2026-07-01", "mcode", "x", 10, 10, 10, 1);
    // 全零行 → 跳过
    insert(&conn, "2026-06-01", "claude", "claude-opus-4-8", 0, 0, 0, 0);
    drop(conn);

    let mut covered = HashSet::new();
    covered.insert(("claude".to_string(), "2026-09-12".to_string()));
    let (records, stats) = usagebook_core::sources::ccswitch_rollup::backfill_at(&db, &covered);

    assert_eq!(
        records.len(),
        3,
        "2(claude 09-01 双模型) + 1(codex 08-01): {records:?}"
    );
    let c1 = records
        .iter()
        .find(|r| r.model == "claude-opus-4-8")
        .expect("claude-opus 行");
    assert_eq!(
        (c1.input_uncached, c1.cache_read, c1.output),
        (100, 900, 50)
    );
    assert_eq!(c1.request_count, 10, "请求数随日聚合行携带");
    assert_eq!(c1.session_id, "2026-09-01", "会话位=日期(会话数≈回填天数)");
    assert!(
        c1.dedup_key.contains("claude:2026-09-01"),
        "键含原生app+日期"
    );
    let cx = records
        .iter()
        .find(|r| r.source == "ccswitch-rollup" && r.model == "gpt-6-astra")
        .unwrap();
    assert_eq!(
        (cx.input_uncached, cx.cache_read),
        (700, 2100),
        "rollup fresh input 直取"
    );
    assert!(records.iter().all(|r| r.source == "ccswitch-rollup"));
    assert!(!stats.notes.is_empty());
    // 去重键两两互异(同日双模型不塌缩)
    let keys: HashSet<&str> = records.iter().map(|r| r.dedup_key.as_str()).collect();
    assert_eq!(keys.len(), 3);

    // 全覆盖 → 零回填(整日替换的空集侧)
    let mut full = HashSet::new();
    full.insert(("claude".to_string(), "2026-09-01".to_string()));
    full.insert(("claude".to_string(), "2026-09-12".to_string()));
    full.insert(("codex".to_string(), "2026-08-01".to_string()));
    let (none, _) = usagebook_core::sources::ccswitch_rollup::backfill_at(&db, &full);
    assert!(none.is_empty());
}

#[test]
fn snapshot_n_accumulates_request_count() {
    use usagebook_core::model::{Caliber, UsageRecord};
    let rec = UsageRecord {
        source: "ccswitch-rollup",
        session_id: "2026-09-01".into(),
        agent: None,
        model: "m".into(),
        ts_ms: 1_789_000_000_000,
        caliber: Caliber::RequestDelta,
        input_uncached: 10,
        cache_read: 0,
        cache_write: 0,
        cache_write_1h: 0,
        output: 5,
        reasoning_output: 0,
        cost_usd: None,
        request_count: 5744,
        dedup_key: "k".into(),
    };
    let snap =
        usagebook_core::snapshot::snapshot_from_records(&[rec], 60, 1_789_050_000_000, vec![]);
    assert_eq!(snap.totals.b.n, 5744, "n 按记录携带的请求数累加");
    assert_eq!(snap.totals.b.i, 10);
}

#[test]
fn legacy_semantics_rows_follow_ccswitch_fresh_rule() {
    // 镜像 cc-switch sql_helpers::fresh_input_sql:只有"存储 input 含缓存"的 app(codex)的
    // 0(legacy)/1(total)行要减缓存;2(fresh)与非含缓存 app(claude/opencode/pi)一律直取。
    let db = temp_db("sem");
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.execute_batch(ROLLUP_DDL).unwrap();
    insert_sem(&conn, "2026-05-01", "codex", "m", 1000, 300, 0, 10, 1, 0); // legacy:1000-300
    insert_sem(&conn, "2026-05-02", "codex", "m", 1000, 300, 100, 10, 1, 1); // total:1000-300-100
    insert_sem(&conn, "2026-05-03", "codex", "m", 100, 300, 100, 10, 1, 1); // 不够减 → 直取
    insert_sem(&conn, "2026-05-04", "codex", "m", 1000, 300, 100, 10, 1, 2); // fresh → 直取
    insert_sem(&conn, "2026-05-05", "claude", "m", 500, 200, 0, 10, 1, 0); // 非含缓存 app → 直取
    insert_sem(&conn, "2026-05-06", "opencode", "m", 400, 100, 0, 10, 1, 0); // 非含缓存 app → 直取
    drop(conn);

    let (records, stats) =
        usagebook_core::sources::ccswitch_rollup::backfill_at(&db, &HashSet::new());
    let input_of = |d: &str| {
        records
            .iter()
            .find(|r| r.session_id == d)
            .unwrap_or_else(|| panic!("{d} 行缺失: {records:?}"))
            .input_uncached
    };
    assert_eq!(input_of("2026-05-01"), 700, "legacy 减 cache_read");
    assert_eq!(
        input_of("2026-05-02"),
        600,
        "total 减 cache_read+cache_creation"
    );
    assert_eq!(
        input_of("2026-05-03"),
        100,
        "不够减时直取,不得出现负数/0 清零"
    );
    assert_eq!(input_of("2026-05-04"), 1000, "fresh 直取");
    assert_eq!(
        input_of("2026-05-05"),
        500,
        "claude 存储即 exclusive,semantics 无关"
    );
    assert_eq!(
        input_of("2026-05-06"),
        400,
        "opencode 不在含缓存清单,semantics 无关"
    );
    let c1 = records
        .iter()
        .find(|r| r.session_id == "2026-05-01")
        .unwrap();
    assert_eq!(
        (c1.cache_read, c1.cache_write),
        (300, 0),
        "缓存列本身不受归一影响"
    );
    assert!(
        stats.notes.iter().any(|n| n.contains("老口径 rollup 行 3")),
        "3 行 codex 老口径(05-01/02/03)应计数上报: {:?}",
        stats.notes
    );
}

#[test]
fn missing_semantics_column_is_treated_as_legacy() {
    // v13 迁移前的老库没有 input_token_semantics 列:查询不得整体失败,整体按 legacy(0)处理。
    let ddl = ROLLUP_DDL.replace(
        "\n    input_token_semantics INTEGER NOT NULL DEFAULT 0,",
        "",
    );
    assert_ne!(ddl, ROLLUP_DDL, "夹具 DDL 必须真的去掉了 semantics 列");
    let db = temp_db("nocol");
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.execute_batch(&ddl).unwrap();
    insert(&conn, "2026-04-01", "codex", "m", 1000, 300, 10, 1); // legacy:1000-300
    insert(&conn, "2026-04-02", "claude", "m", 500, 200, 10, 1); // 直取
    drop(conn);

    let (records, _) = usagebook_core::sources::ccswitch_rollup::backfill_at(&db, &HashSet::new());
    assert_eq!(records.len(), 2, "{records:?}");
    let codex = records
        .iter()
        .find(|r| r.session_id == "2026-04-01")
        .unwrap();
    let claude = records
        .iter()
        .find(|r| r.session_id == "2026-04-02")
        .unwrap();
    assert_eq!(codex.input_uncached, 700);
    assert_eq!(claude.input_uncached, 500);
}

#[test]
fn native_source_list_is_single_source_of_truth() {
    // snapshot.rs 覆盖集过滤与 native_of 共用此常量;改动需同步档案 13 §9 与横切规范 §五。
    assert_eq!(
        usagebook_core::sources::ccswitch_rollup::BACKFILL_NATIVE_SOURCES,
        &["claude", "codex", "opencode", "pi"]
    );
}
