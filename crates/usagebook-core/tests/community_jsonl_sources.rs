//! Community 源 JSONL 通道 fixture 测试(qwen 月度通道 升级 §7.2)。
//! 路径注入,不依赖本机安装。

use usagebook_core::sources::qwen;
use usagebook_core::UsageSource;

fn temp_root(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!(
        "usagebook-fixture-{}-{}",
        tag,
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// 月度 token-usage 通道优先于 chats(§7.2);authType 分协议;后台辅助 source 归因 agent。
#[test]
fn qwen_monthly_channel_priority() {
    let root = temp_root("qwen-monthly");
    let usage_dir = root.join("usage");
    std::fs::create_dir_all(&usage_dir).unwrap();
    std::fs::write(
        usage_dir.join("token-usage-2026-10.jsonl"),
        r#"{"schemaVersion":1,"id":"u1","timestamp":"2026-10-07T01:59:02.949Z","localDate":"2026-10-07","sessionId":"s1","model":"deepseek-v4-pro","authType":"openai","source":"main","inputTokens":26244,"outputTokens":98,"cachedTokens":25984,"thoughtsTokens":17,"totalTokens":26342}
{"schemaVersion":1,"id":"u2","timestamp":"2026-10-07T01:59:26.284Z","localDate":"2026-10-07","sessionId":"s1","model":"deepseek-v4-pro","authType":"openai","source":"managed-auto-memory-extractor","inputTokens":7493,"outputTokens":2140,"cachedTokens":0,"thoughtsTokens":2009,"totalTokens":9633}
{"schemaVersion":1,"id":"u3","timestamp":"2026-10-07T01:59:30.000Z","localDate":"2026-10-07","sessionId":"s2","model":"gemini-3-pro","authType":"gemini","source":"main","inputTokens":1000,"outputTokens":300,"cachedTokens":100,"thoughtsTokens":50,"totalTokens":1350}
{"schemaVersion":1,"id":"u4","timestamp":"2026-10-07T01:59:31.000Z","localDate":"2026-10-07","sessionId":"s2","model":"m","authType":"openai","source":"main","inputTokens":0,"outputTokens":0,"cachedTokens":0,"thoughtsTokens":0,"totalTokens":0}
"#,
    )
    .unwrap();
    // chats 通道同时存在 → 必须让位(防双计)
    let chats_dir = root.join("projects/-Users-x/chats");
    std::fs::create_dir_all(&chats_dir).unwrap();
    std::fs::write(
        chats_dir.join("s1.jsonl"),
        r#"{"type":"assistant","uuid":"c1","timestamp":"2026-10-07T01:59:02.949Z","sessionId":"s1","model":"deepseek-v4-pro","usageMetadata":{"promptTokenCount":26244,"candidatesTokenCount":98,"thoughtsTokenCount":17,"cachedContentTokenCount":25984,"totalTokenCount":26342}}"#,
    )
    .unwrap();

    let (records, stats) = qwen::QwenSource.scan_roots(std::slice::from_ref(&root), None);
    assert_eq!(records.len(), 3, "全零行跳过+chats 让位;notes: {:?}", stats.notes);
    let u1 = records.iter().find(|r| r.dedup_key == "qwen:monthly:u1").unwrap();
    // inclusive:26244 − 25984;openai 协议:output 98 − thoughts 17(§7.3)
    assert_eq!(
        (u1.input_uncached, u1.cache_read, u1.output, u1.reasoning_output),
        (260, 25984, 81, 17)
    );
    assert_eq!(u1.ts_ms, 1791338342949, "timestamp ISO → 毫秒");
    let u2 = records.iter().find(|r| r.dedup_key == "qwen:monthly:u2").unwrap();
    assert_eq!(u2.agent.as_deref(), Some("managed-auto-memory-extractor"), "后台辅助归因(§7.2)");
    let u3 = records.iter().find(|r| r.dedup_key == "qwen:monthly:u3").unwrap();
    // gemini 协议:output 不含 thoughts → 300 直取(§7.3)
    assert_eq!((u3.output, u3.reasoning_output), (300, 50));
    assert_eq!(u3.cache_read, 100);
    let _ = std::fs::remove_dir_all(root);
}

/// 无月度文件 → chats 通道兜底(§7.2)。
#[test]
fn qwen_chats_fallback() {
    let root = temp_root("qwen-chats");
    let chats_dir = root.join("projects/-Users-x/chats");
    std::fs::create_dir_all(&chats_dir).unwrap();
    std::fs::write(
        chats_dir.join("s9.jsonl"),
        r#"{"type":"assistant","uuid":"c9","timestamp":"2026-10-07T02:00:00.000Z","sessionId":"s9","model":"m","usageMetadata":{"promptTokenCount":100,"candidatesTokenCount":50,"thoughtsTokenCount":20,"cachedContentTokenCount":30,"totalTokenCount":150}}"#,
    )
    .unwrap();

    let (records, stats) = qwen::QwenSource.scan_roots(std::slice::from_ref(&root), None);
    assert_eq!(records.len(), 1, "notes: {:?}", stats.notes);
    let c9 = &records[0];
    assert_eq!(c9.dedup_key, "qwen:c9");
    assert_eq!((c9.input_uncached, c9.cache_read, c9.output, c9.reasoning_output), (70, 30, 30, 20));
    let _ = std::fs::remove_dir_all(root);
}
