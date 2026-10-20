//! minimax model 回填回归(2026-10-19):
//! 新版 runtime(M3.1-Flash 起)写 local_runtime_token_usage 不落 model 列,
//! NULL 行从 v2/sessions/**/messages.jsonl 的 assistant 行按 turn_id/ts 双锚点回填,
//! 兜底会话内最高频模型;回填索引按会话隔离(同 turn_id 不跨会话串);
//! 模型维度统一裸名(native "minimax/MiniMax-M3" 与回填 "custom_provider:deepseek/
//! deepseek-flash" 一并去前缀);全部未命中才记 unknown。

use usagebook_core::UsageSource;

const DDL: &str = "CREATE TABLE local_runtime_token_usage (
  id INTEGER PRIMARY KEY AUTOINCREMENT, session_id TEXT NOT NULL, agent_name TEXT NOT NULL,
  framework_type TEXT NOT NULL, turn_id TEXT, model TEXT, ts INTEGER NOT NULL,
  input_tokens INTEGER DEFAULT 0, output_tokens INTEGER DEFAULT 0,
  reasoning_tokens INTEGER DEFAULT 0, cache_read_tokens INTEGER DEFAULT 0,
  cache_write_tokens INTEGER DEFAULT 0, cost_usd REAL DEFAULT 0, raw TEXT);
CREATE TABLE local_runtime_sessions (
  session_id TEXT PRIMARY KEY, record_json TEXT NOT NULL, updated_at_ms INTEGER NOT NULL);";

fn insert_usage(
    conn: &rusqlite::Connection,
    id: i64,
    sid: &str,
    turn: Option<&str>,
    model: Option<&str>,
    ts: i64,
    input: i64,
    output: i64,
) {
    conn.execute(
        "INSERT INTO local_runtime_token_usage (id, session_id, agent_name, framework_type, \
         turn_id, model, ts, input_tokens, output_tokens) \
         VALUES (?1, ?2, 'mavis', 'pi-agent', ?3, ?4, ?5, ?6, ?7)",
        rusqlite::params![id, sid, turn, model, ts, input, output],
    )
    .unwrap();
}

#[test]
fn model_backfill_turn_ts_session_and_isolation() {
    let root = std::env::temp_dir().join(format!("usagebook-minimax-bf-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let sess_dir = root.join("v2/sessions/2026/10/19/12-25-10-100-session_fixture");
    std::fs::create_dir_all(root.join("v2/sqlite")).unwrap();
    std::fs::create_dir_all(&sess_dir).unwrap();

    // 主库:两代数据并存 —— 老行 model 有值,新版行 model NULL
    let db = root.join("v2/sqlite/runtime-state.sqlite");
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.execute_batch(DDL).unwrap();
    conn.execute(
        "INSERT INTO local_runtime_sessions VALUES (?1, ?2, 0)",
        rusqlite::params![
            "mvs_aaaa",
            r#"{"sessionId":"mvs_aaaa","workspaceDir":"/tmp/proj","runtime":"pi-agent"}"#
        ],
    )
    .unwrap();
    const TS_TURN_HIT: i64 = 1_792_412_710_631; // assistant t-a 的 timestamp
    const TS_TS_HIT: i64 = 1_792_412_713_627; // assistant t-b 的 timestamp
    insert_usage(
        &conn,
        1,
        "mvs_aaaa",
        Some("t-old"),
        Some("minimax/MiniMax-M3"),
        TS_TURN_HIT,
        100,
        10,
    ); // native 带前缀 → 裸名
    insert_usage(
        &conn,
        2,
        "mvs_aaaa",
        Some("t-a"),
        None,
        1_792_412_700_000,
        200,
        10,
    ); // turn 锚(ts 故意不匹配)
    insert_usage(&conn, 3, "mvs_aaaa", Some("t-zz"), None, TS_TS_HIT, 300, 10); // turn 未命中 → ts 锚
    insert_usage(&conn, 4, "mvs_aaaa", None, None, 9_999_999_999_999, 400, 10); // 双锚皆空 → 会话频次
    insert_usage(
        &conn,
        5,
        "mvs_bbbb",
        Some("t-a"),
        None,
        TS_TURN_HIT,
        500,
        10,
    ); // 他会话同 turn → 必须隔离成 unknown
    insert_usage(
        &conn,
        6,
        "mvs_aaaa",
        Some("t-c"),
        None,
        1_792_412_700_001,
        600,
        10,
    ); // 回填值本身带前缀 → 裸名
    insert_usage(&conn, 7, "mvs_aaaa", None, None, 0, 700, 10); // 无时间戳 → 跳过
    insert_usage(&conn, 8, "mvs_aaaa", Some("t-a"), None, TS_TURN_HIT, 0, 0); // 全零 → 跳过
    drop(conn);

    // 会话侧:manifest.json 归属 + messages.jsonl(user/toolResult 行无 model 不参与)
    std::fs::write(
        sess_dir.join("manifest.json"),
        r#"{"sessionId":"mvs_aaaa"}"#,
    )
    .unwrap();
    std::fs::write(
        sess_dir.join("messages.jsonl"),
        concat!(
            r#"{"message_id":"m1","turn_id":"t-a","message":{"role":"user","content":"hi"}}"#, "\n",
            r#"{"message_id":"m2","turn_id":"t-a","message":{"role":"assistant","model":"MiniMax-M3.1-Flash","provider":"minimax","timestamp":1792412710631}}"#, "\n",
            r#"{"message_id":"m3","turn_id":"t-b","message":{"role":"toolResult","content":"..."}}"#, "\n",
            r#"{"message_id":"m4","turn_id":"t-b","message":{"role":"assistant","model":"MiniMax-M3.1-Flash","provider":"minimax","timestamp":1792412713627}}"#, "\n",
            r#"{"message_id":"m5","turn_id":"t-c","message":{"role":"assistant","model":"custom_provider:deepseek/deepseek-flash","timestamp":1792412710000}}"#, "\n",
        ),
    )
    .unwrap();

    let (records, stats) =
        usagebook_core::sources::minimax::MinimaxSource.scan_roots(&[root.clone()], None);
    assert!(!stats.failed, "扫描不应失败: {:?}", stats.notes);
    assert_eq!(
        records.len(),
        6,
        "8 行中 1 无时间戳 + 1 全零被跳: {records:?}"
    );

    let model_of = |id: i64| {
        records
            .iter()
            .find(|r| r.dedup_key.ends_with(&format!(":{}", id)))
            .unwrap_or_else(|| panic!("缺 id={} 的记录", id))
            .model
            .clone()
    };
    assert_eq!(model_of(1), "MiniMax-M3", "native 值去 provider 前缀");
    assert_eq!(model_of(2), "MiniMax-M3.1-Flash", "turn 锚回填(裸名)");
    assert_eq!(model_of(3), "MiniMax-M3.1-Flash", "turn 未命中走 ts 锚");
    assert_eq!(model_of(4), "MiniMax-M3.1-Flash", "双锚皆空走会话频次兜底");
    assert_eq!(
        model_of(5),
        "unknown",
        "回填索引按会话隔离,同 turn_id 不跨会话串"
    );
    assert_eq!(model_of(6), "deepseek-flash", "回填值本身带前缀,同样去前缀");

    let bf_note = stats
        .notes
        .iter()
        .find(|n| n.contains("model 回填(messages.jsonl)"))
        .expect("应有 model 回填注记");
    assert!(
        bf_note.contains("turn锚 2"),
        "注记应含 turn 锚计数 2: {bf_note}"
    );
    assert!(
        bf_note.contains("ts锚 1") && bf_note.contains("会话频次 1"),
        "注记锚点计数: {bf_note}"
    );
    assert!(
        stats
            .notes
            .iter()
            .any(|n| n.contains("1 行 model 为 NULL 且")),
        "unknown 行应有计数注记: {:?}",
        stats.notes
    );

    let _ = std::fs::remove_dir_all(&root);
}
