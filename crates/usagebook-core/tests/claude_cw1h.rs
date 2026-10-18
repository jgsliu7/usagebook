//! Claude 1h 缓存写子桶 fixture:
//! 提取 cache_creation.ephemeral_1h_input_tokens 为 cache_write_1h(≤cache_write 的子集),
//! 五桶合计口径不变;L2 载荷往返不丢字段;on/off 全量对拍全等。

use usagebook_core::UsageSource;

fn temp_root(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("usagebook-cw1h-{}-{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

#[test]
fn claude_cache_write_1h_subset_and_parity() {
    let dir = temp_root("main");
    let proj = dir.join("projects/-cw1h");
    std::fs::create_dir_all(&proj).unwrap();
    // cache_creation_input_tokens=1000(总量),其中 1h=600、5m=400(Anthropic 语义:总量含两段)。
    std::fs::write(
        proj.join("s1.jsonl"),
        concat!(
            r#"{"type":"assistant","sessionId":"s1","requestId":"r1","timestamp":"2026-10-12T10:00:00+08:00","message":{"id":"msg_a","model":"claude-opus-4-8","usage":{"input_tokens":2,"output_tokens":1,"cache_read_input_tokens":0,"cache_creation_input_tokens":1000,"cache_creation":{"ephemeral_5m_input_tokens":400,"ephemeral_1h_input_tokens":600}},"stop_reason":"end_turn"}}"#,
            "\n",
            // 对照行:无 cache_creation 对象 → 1h=0。
            r#"{"type":"assistant","sessionId":"s1","requestId":"r2","timestamp":"2026-10-12T10:01:00+08:00","message":{"id":"msg_b","model":"claude-opus-4-8","usage":{"input_tokens":3,"output_tokens":2,"cache_read_input_tokens":0,"cache_creation_input_tokens":50},"stop_reason":"end_turn"}}"#,
            "\n",
        ),
    )
    .unwrap();
    let roots = vec![dir.clone()];
    let src = usagebook_core::sources::claude::ClaudeSource;

    // 直读:字段提取与口径
    let (records, _) = src.scan_roots(&roots, None);
    let a = records.iter().find(|r| r.dedup_key.contains("msg_a")).expect("msg_a");
    let b = records.iter().find(|r| r.dedup_key.contains("msg_b")).expect("msg_b");
    assert_eq!(a.cache_write, 1000);
    assert_eq!(a.cache_write_1h, 600, "1h 子桶按 ephemeral_1h_input_tokens 提取");
    assert_eq!(b.cache_write, 50);
    assert_eq!(b.cache_write_1h, 0, "无 cache_creation 对象时为 0");
    // 五桶合计不因子桶双计:2+1000+1 = 1003(1h 不另加)。
    assert_eq!(a.token_total(), 2 + 1000 + 1);

    // 增量开/关对拍(含 L2 载荷往返:StoredRecord 镜像必须携带该字段)
    std::env::set_var("USAGEBOOK_STATE_DB", dir.join("state.db"));
    std::env::remove_var("USAGEBOOK_INCREMENTAL");
    usagebook_core::store::reset_for_tests();
    usagebook_core::common::__reset_caches_for_tests();
    let (on1, _) = usagebook_core::common::with_incremental_cache(|| src.scan_roots(&roots, None));
    assert_eq!(
        on1.iter().find(|r| r.dedup_key.contains("msg_a")).unwrap().cache_write_1h,
        600,
        "L1/L2 路径不得丢 1h 子桶"
    );
    // 跨进程语义(重置缓存)第二轮:Unchanged 回放,字段仍在。
    usagebook_core::store::reset_for_tests();
    usagebook_core::common::__reset_caches_for_tests();
    let (on2, _) = usagebook_core::common::with_incremental_cache(|| src.scan_roots(&roots, None));
    let sorted = |rs: &[usagebook_core::UsageRecord]| {
        let mut v: Vec<String> = rs.iter().map(|r| format!("{r:?}")).collect();
        v.sort();
        v
    };
    assert_eq!(sorted(&records), sorted(&on1));
    assert_eq!(sorted(&on1), sorted(&on2));
}
