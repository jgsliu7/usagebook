//! Claude 退化键去重 fixture(2026-10-19 修):
//! 无 requestId 时键为 session+msg_id(不掺 ts)——同一请求的流式快照行
//! ts 毫秒级互异,掺 ts 会裂成多键双计;同键五桶大者胜选出终值。
//! 真实撞键(ts 差>60s 且五桶不同)由 DedupTracker 计入 key_clash,不静默。

use usagebook_core::UsageSource;

fn temp_root(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("usagebook-cdd-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn assistant(sid: &str, mid: &str, ts: &str, input: u64, cr: u64, out: u64) -> String {
    format!(
        r#"{{"type":"assistant","sessionId":"{sid}","timestamp":"{ts}","message":{{"id":"{mid}","model":"claude-opus-4-8","usage":{{"input_tokens":{input},"output_tokens":{out},"cache_read_input_tokens":{cr},"cache_creation_input_tokens":0}},"stop_reason":"end_turn"}}}}"#,
    )
}

#[test]
fn claude_degraded_key_merges_snapshot_rows() {
    let dir = temp_root("snap");
    let proj = dir.join("projects/-cdd");
    std::fs::create_dir_all(&proj).unwrap();
    // 同一 (session,msg_id) 三行快照:ts 秒级递进,五桶相同 → 终值 out 更大。
    std::fs::write(
        proj.join("s1.jsonl"),
        [
            assistant("s1", "msg_x", "2026-10-12T10:00:00+08:00", 100, 1000, 50),
            assistant("s1", "msg_x", "2026-10-12T10:00:01+08:00", 100, 1000, 50),
            assistant("s1", "msg_x", "2026-10-12T10:00:02+08:00", 100, 1000, 200),
            // 同 msg_id 不同 session:键含 session 作用域,各自独立。
            assistant("s2", "msg_x", "2026-10-12T10:00:03+08:00", 7, 0, 3),
        ]
        .join("\n"),
    )
    .unwrap();
    let src = usagebook_core::sources::claude::ClaudeSource;
    let (records, stats) = src.scan_roots(std::slice::from_ref(&dir), None);

    let xs: Vec<_> = records
        .iter()
        .filter(|r| r.dedup_key.contains("msg_x"))
        .collect();
    assert_eq!(xs.len(), 2, "三条快照并一条 + 跨 session 一条: {xs:?}");
    let s1 = xs
        .iter()
        .find(|r| r.dedup_key == "s1:msg_x")
        .expect("退化键为 session+msg_id,不掺 ts");
    assert_eq!(s1.output, 200, "同键五桶大者胜选出终值快照");
    assert_eq!(s1.token_total(), 100 + 1000 + 200);
    assert_eq!(stats.key_clash, 0, "快照补全(五桶相同)不计碰撞");
}

#[test]
fn claude_degraded_key_clash_reported_not_silent() {
    let dir = temp_root("clash");
    let proj = dir.join("projects/-cdd");
    std::fs::create_dir_all(&proj).unwrap();
    // 网关复用 message_id 的两个独立请求:ts 差 30 分钟且五桶不同。
    std::fs::write(
        proj.join("s1.jsonl"),
        [
            assistant("s1", "msg_r", "2026-10-12T10:00:00+08:00", 100, 1000, 50),
            assistant("s1", "msg_r", "2026-10-12T10:30:00+08:00", 500, 2000, 80),
        ]
        .join("\n"),
    )
    .unwrap();
    let src = usagebook_core::sources::claude::ClaudeSource;
    let (records, stats) = src.scan_roots(&[dir], None);

    let rs: Vec<_> = records
        .iter()
        .filter(|r| r.dedup_key.contains("msg_r"))
        .collect();
    assert_eq!(rs.len(), 1, "同键合并保留大者(与聚块口径一致)");
    assert_eq!(rs[0].token_total(), 500 + 2000 + 80);
    assert!(
        stats.key_clash >= 1,
        "真撞键必须计入 key_clash 上报,禁止静默"
    );
}

#[test]
fn claude_replay_across_sessions_collapsed() {
    let dir = temp_root("replay");
    let proj = dir.join("projects/-cdd");
    std::fs::create_dir_all(&proj).unwrap();
    // resume/fork:父消息原样重放进新会话文件(msg_id+ts+五桶全同,仅 sessionId 异)。
    std::fs::write(
        proj.join("s1.jsonl"),
        assistant("s1", "msg_p", "2026-10-12T10:00:00+08:00", 100, 1000, 50),
    )
    .unwrap();
    std::fs::write(
        proj.join("s2.jsonl"),
        assistant("s2", "msg_p", "2026-10-12T10:00:00+08:00", 100, 1000, 50),
    )
    .unwrap();
    // 对照:同 msg_id 跨会话但五桶不同 → 真实请求,不合并。
    std::fs::write(
        proj.join("s3.jsonl"),
        assistant("s3", "msg_q", "2026-10-12T10:00:00+08:00", 100, 1000, 50),
    )
    .unwrap();
    std::fs::write(
        proj.join("s4.jsonl"),
        assistant("s4", "msg_q", "2026-10-12T10:00:00+08:00", 200, 3000, 60),
    )
    .unwrap();
    let src = usagebook_core::sources::claude::ClaudeSource;
    let (records, stats) = src.scan_roots(&[dir], None);

    let ps: Vec<_> = records
        .iter()
        .filter(|r| r.dedup_key.contains("msg_p"))
        .collect();
    assert_eq!(ps.len(), 1, "重放副本(msg_id+ts+五桶全同)只计一条");
    let qs: Vec<_> = records
        .iter()
        .filter(|r| r.dedup_key.contains("msg_q"))
        .collect();
    assert_eq!(qs.len(), 2, "五桶不同的同 msg_id 是真实请求,不合并");
    assert!(
        stats
            .notes
            .iter()
            .any(|n| n.contains("跨会话重放合并 1 条")),
        "合并必须上报诊断 note,禁止静默: {:?}",
        stats.notes
    );
}
