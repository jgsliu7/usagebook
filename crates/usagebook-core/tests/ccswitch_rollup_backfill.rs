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
    // codex 空窗日 → 回填;input 已归一 fresh 直取
    insert(
        &conn,
        "2026-08-01",
        "codex",
        "gpt-6-astra",
        700,
        2100,
        90,
        7,
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
