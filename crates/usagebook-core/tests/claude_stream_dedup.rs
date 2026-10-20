//! Claude 流式重写去重回归(2026-10-19 修):
//! requestId 缺失时退化键为 session+message.id(不含 ts)——同一消息的多次流式快照
//! (实测同 usage 不同 ts 重复写 33~54 行)必须合并为一条,token 大者胜;
//! 精确键(requestId 存在)与跨会话副本行为不变。

use usagebook_core::UsageSource;

fn temp_root(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!(
        "usagebook-claudededup-{}-{}",
        tag,
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

#[test]
fn stream_rewrite_without_request_id_collapses() {
    let dir = temp_root("main");
    let proj = dir.join("projects/-dedup");
    std::fs::create_dir_all(&proj).unwrap();
    // 同一 msg_id 无 requestId,流式重写 3 遍:usage 相同、ts 不同 → 1 条。
    std::fs::write(
        proj.join("s1.jsonl"),
        concat!(
            r#"{"type":"assistant","sessionId":"s1","timestamp":"2026-10-19T11:13:23.484Z","message":{"id":"msg_a","model":"claude-opus-4-8","usage":{"input_tokens":1531,"cache_read_input_tokens":85696,"output_tokens":1808},"stop_reason":"end_turn"}}"#,
            "\n",
            r#"{"type":"assistant","sessionId":"s1","timestamp":"2026-10-19T11:13:24.146Z","message":{"id":"msg_a","model":"claude-opus-4-8","usage":{"input_tokens":1531,"cache_read_input_tokens":85696,"output_tokens":1808},"stop_reason":"end_turn"}}"#,
            "\n",
            r#"{"type":"assistant","sessionId":"s1","timestamp":"2026-10-19T11:13:27.705Z","message":{"id":"msg_a","model":"claude-opus-4-8","usage":{"input_tokens":1531,"cache_read_input_tokens":85696,"output_tokens":1808},"stop_reason":"end_turn"}}"#,
            "\n",
            // 同会话另一条消息(不同 msg_id)→ 独立成键。
            r#"{"type":"assistant","sessionId":"s1","timestamp":"2026-10-19T11:14:55.714Z","message":{"id":"msg_b","model":"claude-opus-4-8","usage":{"input_tokens":661,"cache_read_input_tokens":90944,"output_tokens":3657},"stop_reason":"end_turn"}}"#,
            "\n",
            // 半截→最终(usage 增长,ts 不同)→ 合并且 token 大者胜。
            r#"{"type":"assistant","sessionId":"s1","timestamp":"2026-10-19T11:20:01.000Z","message":{"id":"msg_c","model":"claude-opus-4-8","usage":{"input_tokens":2271,"cache_read_input_tokens":144768,"output_tokens":900},"stop_reason":null}}"#,
            "\n",
            r#"{"type":"assistant","sessionId":"s1","timestamp":"2026-10-19T11:20:05.500Z","message":{"id":"msg_c","model":"claude-opus-4-8","usage":{"input_tokens":2271,"cache_read_input_tokens":144768,"output_tokens":1877},"stop_reason":"end_turn"}}"#,
            "\n",
        ),
    )
    .unwrap();
    let src = usagebook_core::sources::claude::ClaudeSource;
    let (records, _) = src.scan_roots(&[dir.clone()], None);
    let msgs: Vec<&str> = records.iter().map(|r| r.dedup_key.as_str()).collect();
    assert_eq!(records.len(), 3, "5 行 3 消息,重复快照必须合并: {msgs:?}");
    let c = records
        .iter()
        .find(|r| r.dedup_key.contains("msg_c"))
        .expect("msg_c");
    assert_eq!(c.output, 1877, "分段增长取 token 大者");
    assert_eq!(c.input_uncached, 2271);
    assert_eq!(c.cache_read, 144768);
}

#[test]
fn exact_key_and_cross_session_unchanged() {
    let dir = temp_root("exact");
    let proj = dir.join("projects/-exact");
    std::fs::create_dir_all(&proj).unwrap();
    // 精确键路径:requestId 存在,同 msg 同 rid 不同 ts → 1 条。
    std::fs::write(
        proj.join("s1.jsonl"),
        concat!(
            r#"{"type":"assistant","sessionId":"s1","requestId":"req_1","timestamp":"2026-10-19T10:00:00.000Z","message":{"id":"msg_x","model":"claude-opus-4-8","usage":{"input_tokens":10,"output_tokens":5},"stop_reason":"end_turn"}}"#,
            "\n",
            r#"{"type":"assistant","sessionId":"s1","requestId":"req_1","timestamp":"2026-10-19T10:00:02.000Z","message":{"id":"msg_x","model":"claude-opus-4-8","usage":{"input_tokens":10,"output_tokens":5},"stop_reason":"end_turn"}}"#,
            "\n",
        ),
    )
    .unwrap();
    // 跨会话副本:同 msg_id 无 requestId,不同 session → 两条(键含 session)。
    std::fs::write(
        proj.join("s2.jsonl"),
        concat!(
            r#"{"type":"assistant","sessionId":"s2","timestamp":"2026-10-19T10:05:00.000Z","message":{"id":"msg_y","model":"claude-opus-4-8","usage":{"input_tokens":7,"output_tokens":3},"stop_reason":"end_turn"}}"#,
            "\n",
            r#"{"type":"assistant","sessionId":"s3","timestamp":"2026-10-19T10:06:00.000Z","message":{"id":"msg_y","model":"claude-opus-4-8","usage":{"input_tokens":7,"output_tokens":3},"stop_reason":"end_turn"}}"#,
            "\n",
        ),
    )
    .unwrap();
    let src = usagebook_core::sources::claude::ClaudeSource;
    let (records, _) = src.scan_roots(&[dir.clone()], None);
    assert_eq!(records.len(), 3, "精确键合并 1 + 跨会话 2: {records:?}");
    assert!(
        records
            .iter()
            .filter(|r| r.dedup_key.contains("msg_x"))
            .count()
            == 1
    );
}
