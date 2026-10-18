//! 批次 D SQLite 六源 fixture 集成测试 —— Community 源未经本机实证,
//! 以档案 §2 的库格式构造最小 fixture,验证 scan_roots 的 SQL 路径真实可执行
//! (发现判据/五桶映射/减除/去重/锚定时间戳)。路径注入,不依赖本机安装。

use rusqlite::Connection;
use usagebook_core::sources::{goose, hermes, kilo, kiro, opencode, qoder};
use usagebook_core::UsageSource;

/// 进程唯一临时目录,测试结束best-effort 清理。
fn temp_root(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("usagebook-fixture-{}-{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

#[test]
fn goose_sessions_db() {
    let root = temp_root("goose");
    let db = root.join("sessions.db");
    let conn = Connection::open(&db).unwrap();
    conn.execute_batch(
        "CREATE TABLE sessions(id TEXT PRIMARY KEY, model_config_json TEXT, provider_name TEXT, created_at TEXT,
         total_tokens INTEGER DEFAULT 0, input_tokens INTEGER DEFAULT 0, output_tokens INTEGER DEFAULT 0,
         accumulated_total_tokens INTEGER DEFAULT 0, accumulated_input_tokens INTEGER DEFAULT 0,
         accumulated_output_tokens INTEGER DEFAULT 0);
         INSERT INTO sessions VALUES('s1','{\"model_name\":\"claude-sonnet-4-5\"}','anthropic',
           '2026-01-02 03:04:05',0,0,0,110,100,10);
         INSERT INTO sessions VALUES('s2','{\"model_name\":\"gpt-5.2\"}','openai',
           1750000000000,200,150,30,0,0,0);
         INSERT INTO sessions VALUES('s3','','x','2026-01-02 03:04:05',0,0,0,0,0,0);",
    )
    .unwrap();
    drop(conn);

    let (records, stats) = goose::GooseSource.scan_roots(&[db], None);
    assert_eq!(
        records.len(),
        2,
        "空 config 行应跳过;实际 notes: {:?}",
        stats.notes
    );
    let s1 = records.iter().find(|r| r.session_id == "s1").unwrap();
    assert_eq!(s1.model, "claude-sonnet-4-5");
    // 升级:input 已含 cache(本 fixture 无 cache 列 → 不减);reasoning 恒 0(§6.3)
    assert_eq!(
        (s1.input_uncached, s1.output, s1.reasoning_output),
        (100, 10, 0)
    );
    assert_eq!(s1.ts_ms, 1767323045000, "naive created_at 按 UTC 解析(§2)");
    let s2 = records.iter().find(|r| r.session_id == "s2").unwrap();
    // 旧列回落 + reasoning 恒 0(total−in−out 公式已作废,§6.3)
    assert_eq!(
        (s2.input_uncached, s2.output, s2.reasoning_output),
        (150, 30, 0)
    );
    assert_eq!(s2.ts_ms, 1750000000000, "毫秒 created_at 原样(§2)");
    let _ = std::fs::remove_dir_all(root);
}

/// 升级(§6.3):usage_ledger 逐请求通道 —— 五桶拆分(input 含 cache 子集)、
/// compaction 跳过、provider_reported 成本、estimated 不落。
#[test]
fn goose_ledger_channel() {
    let root = temp_root("goose-ledger");
    let db = root.join("sessions.db");
    let conn = Connection::open(&db).unwrap();
    conn.execute_batch(
        "CREATE TABLE sessions(id TEXT PRIMARY KEY, model_config_json TEXT);
         CREATE TABLE usage_ledger(id INTEGER PRIMARY KEY, session_id TEXT,
           created_timestamp INTEGER, model TEXT, input_tokens INTEGER, output_tokens INTEGER,
           total_tokens INTEGER, cache_read_tokens INTEGER, cache_write_tokens INTEGER,
           cost REAL, cost_source TEXT, is_compaction INTEGER DEFAULT 0);
         INSERT INTO usage_ledger VALUES(1,'g1',1791338725,'deepseek-v4-pro',11266,153,11419,11136,0,
           0.000633732,'estimated',0);
         INSERT INTO usage_ledger VALUES(2,'g1',1791338726,'deepseek-v4-pro',100,50,150,0,0,
           0.01,'provider_reported',0);
         INSERT INTO usage_ledger VALUES(3,'g1',1791338727,'deepseek-v4-pro',9000,10,9010,0,0,
           0.0,'estimated',1);",
    )
    .unwrap();
    drop(conn);

    let (records, stats) = goose::GooseSource.scan_roots(&[db], None);
    assert_eq!(
        records.len(),
        2,
        "compaction 行跳过;notes: {:?}",
        stats.notes
    );
    let r1 = records.iter().find(|r| r.ts_ms == 1791338725000).unwrap();
    // input 已含 cache → 拆分:11266 − 11136 = 130(§6.3)
    assert_eq!(
        (r1.input_uncached, r1.cache_read, r1.output),
        (130, 11136, 153)
    );
    assert_eq!(r1.reasoning_output, 0, "ledger 无 reasoning 列(§6.3)");
    assert_eq!(r1.cost_usd, None, "estimated 不落 cost_usd(model.rs 纪律)");
    let r2 = records.iter().find(|r| r.ts_ms == 1791338726000).unwrap();
    assert_eq!(
        r2.cost_usd,
        Some(0.01),
        "provider_reported 视为官方实报(§6.3)"
    );
    assert!(
        stats.notes.iter().any(|n| n.contains("compaction")),
        "compaction 跳过应计数: {:?}",
        stats.notes
    );
    let _ = std::fs::remove_dir_all(root);
}

/// 重写(§7.2):openclaw SQLite 正典通道 —— transcript_events 解析、
/// exclusive 直取、reasoning 明细拆分、event_zstd 解压、全零行跳过。
#[test]
fn openclaw_sqlite_canonical() {
    use usagebook_core::sources::openclaw;

    let root = temp_root("openclaw");
    let db_dir = root.join("agents/main/agent");
    std::fs::create_dir_all(&db_dir).unwrap();
    let conn = Connection::open(db_dir.join("openclaw-agent.sqlite")).unwrap();
    let zstd_input = b"{\"type\":\"message\",\"id\":\"ev-z\",\"timestamp\":\"2026-10-07T02:00:00.000Z\",\"message\":{\"role\":\"assistant\",\"model\":\"glm-5.1\",\"usage\":{\"input\":100,\"output\":30,\"cacheRead\":5,\"cacheWrite\":0,\"reasoningTokens\":10,\"totalTokens\":135}}}";
    let zstd_payload = zstd::stream::encode_all(&zstd_input[..], 0).unwrap();
    conn.execute_batch(
        "CREATE TABLE transcript_events(session_id TEXT, seq INTEGER, event_json TEXT,
           event_zstd BLOB, PRIMARY KEY(session_id, seq));
         INSERT INTO transcript_events VALUES('sessA',1,'{\"type\":\"session\",\"id\":\"sessA\"}',NULL);
         INSERT INTO transcript_events VALUES('sessA',2,'{\"type\":\"message\",\"id\":\"ev-1\",
           \"timestamp\":1750000000000,
           \"message\":{\"role\":\"assistant\",\"model\":\"glm-5.1\",
           \"usage\":{\"input\":15866,\"output\":542,\"cacheRead\":0,\"cacheWrite\":0,
                     \"reasoningTokens\":504,\"totalTokens\":16408}}}',NULL);
         INSERT INTO transcript_events VALUES('sessA',3,'{\"type\":\"message\",\"id\":\"ev-2\",
           \"timestamp\":1750000001000,
           \"message\":{\"role\":\"assistant\",\"model\":\"glm-5.1\",
           \"usage\":{\"input\":0,\"output\":0,\"cacheRead\":0,\"cacheWrite\":0,\"totalTokens\":0}}}',NULL);",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO transcript_events VALUES('sessA',4,NULL,?1)",
        [zstd_payload],
    )
    .unwrap();
    drop(conn);

    let (records, stats) = openclaw::OpenclawSource.scan_roots(std::slice::from_ref(&root), None);
    assert_eq!(
        records.len(),
        2,
        "全零行跳过+zstd 解压;notes: {:?}",
        stats.notes
    );
    let e1 = records
        .iter()
        .find(|r| r.dedup_key == "openclaw:main:sessA:ev-1")
        .unwrap();
    // exclusive 直取(§7.3):input 不减 cache;output 拆 reasoning
    assert_eq!(
        (
            e1.input_uncached,
            e1.cache_read,
            e1.cache_write,
            e1.output,
            e1.reasoning_output
        ),
        (15866, 0, 0, 38, 504)
    );
    assert_eq!(e1.cost_usd, None, "混合口径不落(model.rs 纪律,§7.3)");
    assert_eq!(
        e1.ts_ms, 1750000000000,
        "message.timestamp epoch 毫秒优先(§7.4)"
    );
    let ez = records
        .iter()
        .find(|r| r.dedup_key == "openclaw:main:sessA:ev-z")
        .unwrap();
    assert_eq!(
        (ez.input_uncached, ez.output, ez.reasoning_output),
        (100, 20, 10)
    );
    assert_eq!(ez.ts_ms, 1791338400000, "envelope ISO 回退(§7.4)");
    assert!(
        stats.notes.iter().any(|n| n.contains("zstd")),
        "zstd 应计数: {:?}",
        stats.notes
    );
    let _ = std::fs::remove_dir_all(root);
}

/// 升级(§7.1):copilot session-store.db 逐请求通道 —— 五桶拆分、
/// nano-AIU 成本、列缺省防御。
#[test]
fn copilot_session_store_channel() {
    use usagebook_core::sources::copilot;

    let root = temp_root("copilot-store");
    let conn = Connection::open(root.join("session-store.db")).unwrap();
    conn.execute_batch(
        "CREATE TABLE assistant_usage_events(id INTEGER PRIMARY KEY, session_id TEXT,
           turn_index INTEGER, agent_id TEXT, model TEXT, input_tokens INTEGER,
           output_tokens INTEGER, cache_read_tokens INTEGER, cache_write_tokens INTEGER,
           reasoning_tokens INTEGER, total_nano_aiu INTEGER, created_at TEXT);
         INSERT INTO assistant_usage_events VALUES(1,'csess',0,'','mai-code-1.1-flash',
           24379,32,13184,0,0,254108000,'2026-10-07 02:24:03');
         INSERT INTO assistant_usage_events VALUES(2,'csess',1,'','mai-code-1.1-flash',
           24448,64,24320,0,0,58880000,'2026-10-07 02:24:20');
         INSERT INTO assistant_usage_events VALUES(3,'csess',2,'sub','gpt-5.2',
           0,0,0,0,0,0,'2026-10-07 02:24:30');",
    )
    .unwrap();
    drop(conn);

    let (records, stats) = copilot::CopilotSource.scan_roots(std::slice::from_ref(&root), None);
    assert_eq!(records.len(), 2, "全零行跳过;notes: {:?}", stats.notes);
    let r1 = records.iter().find(|r| r.ts_ms == 1791339843000).unwrap();
    // 双减除(§7.3):24379 − 13184 − 0 = 11195
    assert_eq!(
        (r1.input_uncached, r1.cache_read, r1.output),
        (11195, 13184, 32)
    );
    // nano → 美元:254108000/1e9 × 0.01 = 0.00254108(§7.3)
    assert!(
        (r1.cost_usd.unwrap() - 0.00254108).abs() < 1e-12,
        "nano→美元换算(§7.3)"
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn kilo_message_json() {
    let root = temp_root("kilo");
    let dir = root.join("kilo");
    std::fs::create_dir_all(&dir).unwrap();
    let conn = Connection::open(dir.join("kilo.db")).unwrap();
    conn.execute_batch(
        "CREATE TABLE message(id TEXT PRIMARY KEY, session_id TEXT, data TEXT);
         INSERT INTO message VALUES('m1','ksess1','{\"role\":\"assistant\",
           \"tokens\":{\"input\":10,\"cache\":{\"read\":2,\"write\":1},\"output\":5,\"reasoning\":0,\"total\":18},
           \"time\":{\"created\":1750000000000},\"modelID\":\"claude-sonnet-4-5\",\"sessionID\":\"ksess1\"}');
         INSERT INTO message VALUES('m2','ksess1','{\"role\":\"user\",\"tokens\":{\"input\":9}}');
         INSERT INTO message VALUES('m3','ksess2','{\"role\":\"assistant\",
           \"tokens\":{\"input\":10,\"output\":0,\"total\":25},
           \"time\":{\"created\":1750000001},\"model\":\"gpt-5.2\"}');
         INSERT INTO message VALUES('m4','ksess2','{\"role\":\"assistant\",
           \"tokens\":{\"input\":3,\"output\":4},\"model\":\"x\"}');",
    )
    .unwrap();
    drop(conn);

    let (records, stats) = kilo::KiloSource.scan_roots(&[dir], None);
    assert_eq!(
        records.len(),
        2,
        "user 行与无时间行跳过;notes: {:?}",
        stats.notes
    );
    let m1 = records.iter().find(|r| r.dedup_key == "kilo:m1").unwrap();
    assert_eq!(
        (m1.input_uncached, m1.cache_read, m1.cache_write, m1.output),
        (10, 2, 1, 5)
    );
    assert_eq!(m1.ts_ms, 1750000000000);
    // m3:total 差额兜底补 output(§3)+ 秒级时间戳自适应(§4)
    let m3 = records.iter().find(|r| r.dedup_key == "kilo:m3").unwrap();
    assert_eq!(m3.output, 15);
    assert_eq!(m3.ts_ms, 1750000001000);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn hermes_state_db() {
    let root = temp_root("hermes");
    let conn = Connection::open(root.join("state.db")).unwrap();
    conn.execute_batch(
        "CREATE TABLE sessions(id TEXT PRIMARY KEY, model TEXT, started_at REAL, ended_at REAL,
         input_tokens INTEGER, output_tokens INTEGER, cache_read_tokens INTEGER,
         cache_write_tokens INTEGER, reasoning_tokens INTEGER, message_count INTEGER,
         estimated_cost_usd REAL, actual_cost_usd REAL);
         INSERT INTO sessions VALUES('h1','claude-sonnet-4-5',1750000000.25,NULL,100,50,10,5,5,3,0.01,0.02);
         INSERT INTO sessions VALUES('h2','',1750000001.0,1750000002.0,10,10,0,0,0,1,NULL,NULL);
         INSERT INTO sessions VALUES('h3','gpt-5.2',1750000500.0,NULL,7,8,0,0,0,1,0.05,NULL);",
    )
    .unwrap();
    drop(conn);

    let (records, stats) = hermes::HermesSource.scan_roots(&[root.join("state.db")], None);
    assert_eq!(records.len(), 2, "空 model 行跳过;notes: {:?}", stats.notes);
    let h1 = records.iter().find(|r| r.session_id == "h1").unwrap();
    // 秒(带小数)→ 毫秒(§4);actual > estimated(§5)
    assert_eq!(h1.ts_ms, 1750000000250);
    assert_eq!(h1.cost_usd, Some(0.02));
    assert_eq!(
        (
            h1.input_uncached,
            h1.output,
            h1.cache_read,
            h1.cache_write,
            h1.reasoning_output
        ),
        (100, 50, 10, 5, 5)
    );
    let h3 = records.iter().find(|r| r.session_id == "h3").unwrap();
    assert_eq!(h3.cost_usd, Some(0.05), "actual 缺失回落 estimated(§5)");
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn kiro_conversations_v2() {
    let root = temp_root("kiro");
    let conn = Connection::open(root.join("data.sqlite3")).unwrap();
    let value = r#"{"model_info":{"model_id":"anthropic.claude-sonnet-4-5"},
      "user_turn_metadata":{"requests":[
        {"request_id":"r1","input_token_count":100,"output_token_count":50,
         "request_start_timestamp_ms":1750000000000,
         "model_id":"arn:aws:bedrock:us-east-1:1:model/anthropic.claude-opus-4"},
        {"request_id":"r2","input_token_count":10,"output_token_count":0,
         "request_start_timestamp_ms":1750000001000},
        {"request_id":"r3","user_prompt_length":400,"request_start_timestamp_ms":1750000002000}
      ]}}"#;
    conn.execute_batch(&format!(
        "CREATE TABLE conversations_v2(conversation_id TEXT, value TEXT);
         INSERT INTO conversations_v2 VALUES('conv1','{}');",
        value.replace('\'', "''")
    ))
    .unwrap();
    drop(conn);

    let (records, stats) = kiro::KiroSource.scan_roots(&[root.join("data.sqlite3")], None);
    assert_eq!(
        records.len(),
        2,
        "char/4 估算行不采用(§2);notes: {:?}",
        stats.notes
    );
    let r1 = records.iter().find(|r| r.dedup_key == "kiro:r1").unwrap();
    assert_eq!(r1.model, "claude-opus-4", "arn 规范化(§3)");
    assert_eq!((r1.input_uncached, r1.output), (100, 50));
    let r2 = records.iter().find(|r| r.dedup_key == "kiro:r2").unwrap();
    assert_eq!(r2.model, "claude-sonnet-4-5", "会话级模型兜底(§2)");
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn qoder_ide_local_db() {
    let root = temp_root("qoder");
    let conn = Connection::open(root.join("local.db")).unwrap();
    conn.execute_batch(
        "CREATE TABLE chat_message(id TEXT PRIMARY KEY, session_id TEXT, role TEXT,
         gmt_create INTEGER, token_info TEXT, model_info TEXT);
         INSERT INTO chat_message VALUES('m1','sess1','assistant',1750000000000,
           '{\"prompt_tokens\":110,\"completion_tokens\":7,\"cached_tokens\":10}',
           '{\"model_key\":\"qwen3-coder-plus\",\"model\":\"fallback\"}');
         INSERT INTO chat_message VALUES('m2','sess1','user',1750000000000,
           '{\"prompt_tokens\":1}',NULL);
         INSERT INTO chat_message VALUES('m3','sess2','assistant',1750000000000,'{}',NULL);",
    )
    .unwrap();
    drop(conn);

    let (records, stats) = qoder::QoderSource.scan_roots(&[root.join("local.db")], None);
    assert_eq!(
        records.len(),
        1,
        "user 行与空 token_info 行跳过;notes: {:?}",
        stats.notes
    );
    let m1 = &records[0];
    // inclusive 减除:prompt 110 − cached 10(§2)
    assert_eq!((m1.input_uncached, m1.cache_read, m1.output), (100, 10, 7));
    assert_eq!(m1.model, "qwen3-coder-plus", "model_key 优先(§2)");
    assert_eq!(m1.session_id, "sess1");
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn opencode_v2_and_json_fallback() {
    let root = temp_root("opencode");

    // 通道 A:v2 session_message 表
    let dir_a = root.join("a");
    std::fs::create_dir_all(&dir_a).unwrap();
    let conn = Connection::open(dir_a.join("opencode.db")).unwrap();
    conn.execute_batch(
        "CREATE TABLE session_message(id TEXT PRIMARY KEY, session_id TEXT, data TEXT);
         INSERT INTO session_message VALUES('om1','osess1','{\"type\":\"assistant\",
           \"modelID\":\"claude-sonnet-4-5\",
           \"tokens\":{\"input\":10,\"cache\":{\"read\":2,\"write\":1},\"output\":5,\"reasoning\":1},
           \"time\":{\"created\":1750000000000},\"cost\":0.01}');
         INSERT INTO session_message VALUES('om2','osess1','{\"type\":\"user\",
           \"tokens\":{\"input\":9},\"time\":{\"created\":1750000000000}}');",
    )
    .unwrap();
    drop(conn);
    let (records, stats) = opencode::OpencodeSource.scan_roots(std::slice::from_ref(&dir_a), None);
    assert_eq!(records.len(), 1, "user 行跳过;notes: {:?}", stats.notes);
    let om1 = &records[0];
    assert_eq!(
        (
            om1.input_uncached,
            om1.cache_read,
            om1.cache_write,
            om1.output,
            om1.reasoning_output
        ),
        (10, 2, 1, 5, 1)
    );
    assert_eq!(om1.cost_usd, Some(0.01));

    // 通道 B:无 db → JSON storage/message 兜底
    let dir_b = root.join("b");
    let msg_dir = dir_b.join("storage/message/sess9");
    std::fs::create_dir_all(&msg_dir).unwrap();
    std::fs::write(
        msg_dir.join("mid9.json"),
        r#"{"role":"assistant","modelId":"gpt-5.2",
            "tokens":{"input":20,"cache":0,"output":4},
            "time":{"created":1750000900}}"#,
    )
    .unwrap();
    let (records2, stats2) =
        opencode::OpencodeSource.scan_roots(std::slice::from_ref(&dir_b), None);
    assert_eq!(records2.len(), 1, "JSON 兜底;notes: {:?}", stats2.notes);
    assert_eq!(records2[0].session_id, "sess9");
    assert_eq!(records2[0].input_uncached, 20);
    assert_eq!(records2[0].ts_ms, 1750000900000, "秒级 created 自适应(§3)");
    assert_eq!(records2[0].cache_read, 0, "cache 非对象容错(§3)");

    // 通道 C:明细表全空 → session 聚合兜底(会话级累计锚 time_created,§2)
    let dir_c = root.join("c");
    std::fs::create_dir_all(&dir_c).unwrap();
    let conn = Connection::open(dir_c.join("opencode.db")).unwrap();
    conn.execute_batch(
        "CREATE TABLE session(id TEXT PRIMARY KEY, model TEXT, cost REAL,
           tokens_input INTEGER, tokens_output INTEGER, tokens_reasoning INTEGER,
           tokens_cache_read INTEGER, tokens_cache_write INTEGER, time_created INTEGER);
         CREATE TABLE message(id TEXT, session_id TEXT, data TEXT);
         INSERT INTO session VALUES('ses_x','{\"id\":\"glm-5.3-flash\",\"providerID\":\"zhipuai-coding-plan\"}',0.0,
           63612, 2011, 15242, 137024, 0, 1791282693955);
         INSERT INTO session VALUES('ses_z','{\"id\":\"m\"}',0.0, 0,0,0,0,0, 1791282693955);",
    )
    .unwrap();
    drop(conn);
    let (records3, stats3) =
        opencode::OpencodeSource.scan_roots(std::slice::from_ref(&dir_c), None);
    assert_eq!(
        records3.len(),
        1,
        "全零 session 跳过;notes: {:?}",
        stats3.notes
    );
    let s = &records3[0];
    assert_eq!(s.dedup_key, "opencode:session:ses_x");
    assert_eq!(s.model, "glm-5.3-flash", "model JSON 取 id(§2)");
    assert_eq!(
        (
            s.input_uncached,
            s.output,
            s.reasoning_output,
            s.cache_read,
            s.cache_write
        ),
        (63612, 2011, 15242, 137024, 0)
    );
    assert_eq!(s.ts_ms, 1791282693955);
    let _ = std::fs::remove_dir_all(root);
}
