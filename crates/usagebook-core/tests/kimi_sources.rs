//! kimi / kimi-work 双源 fixture 测试:同一扫描核(scan_kimi_wire)参数化注入源 id,
//! token 分开统计互不串扰。路径注入(scan_roots 纯函数核),不依赖本机安装。
//! fixture 取自 2026-10-18 本机实证:CLI 布局 session_<uuid>/agents/,桌面版 conv-*/agents/。

use usagebook_core::sources::kimi::KimiSource;
use usagebook_core::sources::kimi_work::KimiWorkSource;
use usagebook_core::UsageSource;

fn temp_root(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("usagebook-fixture-{}-{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// 桌面版布局:conv-*/ctitle-* 会话目录、usageScope 双值、llm.request 真实模型;
/// 同一根再用 KimiSource 扫一遍,证明核已彻底参数化(无 "kimi" 残留硬编码)。
#[test]
fn kimi_work_desktop_layout() {
    let root = temp_root("kimiwork-desktop");
    let main = root.join("sessions/wd_agentledger_h1/conv-abc123/agents/main");
    std::fs::create_dir_all(&main).unwrap();
    std::fs::write(
        main.join("wire.jsonl"),
        concat!(
            r#"{"type":"metadata","protocol_version":"1.4","created_at":1792308000000}"#,
            "\n",
            r#"{"type":"llm.request","kind":"loop","provider":"kimi","model":"k3-agent","modelAlias":"k3-agent"}"#,
            "\n",
            r#"{"type":"usage.record","model":"k3-agent","usage":{"inputOther":470,"output":293,"inputCacheRead":100352,"inputCacheCreation":0},"usageScope":"turn","time":1792308931460}"#,
            "\n",
            r#"{"type":"usage.record","model":"k3-agent","usage":{"inputOther":900,"output":600,"inputCacheRead":100000,"inputCacheCreation":0},"usageScope":"session","time":1792308932460}"#,
            "\n",
            r#"{"type":"turn.ended"}"#,
            "\n",
        ),
    )
    .unwrap();
    // ctitle-* 会话(标题生成后台调用):独立会话目录,session_id 原样保留前缀。
    let title = root.join("sessions/wd_agentledger_h1/ctitle-01a14def/agents/main");
    std::fs::create_dir_all(&title).unwrap();
    std::fs::write(
        title.join("wire.jsonl"),
        concat!(
            r#"{"type":"llm.request","kind":"loop","provider":"kimi","model":"k3-agent"}"#,
            "\n",
            r#"{"type":"usage.record","model":"k3-agent","usage":{"inputOther":10,"output":20,"inputCacheRead":0,"inputCacheCreation":0},"usageScope":"turn","time":1792308940000}"#,
            "\n",
        ),
    )
    .unwrap();

    let (records, stats) = KimiWorkSource.scan_roots(std::slice::from_ref(&root), None);
    assert_eq!(
        records.len(),
        2,
        "session 累计口径跳过;notes: {:?}",
        stats.notes
    );
    assert!(records.iter().all(|r| r.source == "kimi-work"));

    let main_rec = records
        .iter()
        .find(|r| r.session_id == "conv-abc123")
        .expect("conv- 会话在列");
    assert_eq!(main_rec.agent, None, "agents/main 归主代理");
    assert_eq!(
        (
            main_rec.input_uncached,
            main_rec.cache_read,
            main_rec.cache_write,
            main_rec.output
        ),
        (470, 100352, 0, 293)
    );
    assert_eq!(main_rec.ts_ms, 1792308931460, "桌面版 time 已是毫秒");
    assert_eq!(main_rec.model, "k3-agent", "llm.request.model 真实模型优先");
    assert!(
        main_rec
            .dedup_key
            .starts_with("kimi-work:conv-abc123:main:"),
        "去重前缀随源 id,实测 {}",
        main_rec.dedup_key
    );

    let title_rec = records
        .iter()
        .find(|r| r.session_id == "ctitle-01a14def")
        .expect("ctitle- 会话在列");
    assert_eq!(
        (title_rec.input_uncached, title_rec.output),
        (10, 20),
        "标题生成后台调用照常计入"
    );

    // 同根换 KimiSource 扫:源 id 与去重前缀必须整体切换(核参数化回归)。
    let (cli_records, _) = KimiSource.scan_roots(std::slice::from_ref(&root), None);
    assert!(cli_records.iter().all(|r| r.source == "kimi"));
    assert!(cli_records.iter().all(|r| r.dedup_key.starts_with("kimi:")));

    let _ = std::fs::remove_dir_all(root);
}

/// CLI 布局:session_<uuid> 会话 + agent-N 子代理文件归 agent 字段(§5.2)。
#[test]
fn kimi_cli_session_and_subagent() {
    let root = temp_root("kimi-cli");
    let main =
        root.join("sessions/wd_proj_h2/session_11111111-2222-3333-4444-555555555555/agents/main");
    std::fs::create_dir_all(&main).unwrap();
    std::fs::write(
        main.join("wire.jsonl"),
        concat!(
            r#"{"type":"llm.request","kind":"loop","provider":"kimi","model":"kimi-for-coding"}"#,
            "\n",
            r#"{"type":"usage.record","model":"alias","usage":{"inputOther":100,"output":200,"inputCacheRead":300,"inputCacheCreation":50},"usageScope":"turn","time":1791300000.5}"#,
            "\n",
        ),
    )
    .unwrap();
    let sub = root
        .join("sessions/wd_proj_h2/session_11111111-2222-3333-4444-555555555555/agents/agent-7");
    std::fs::create_dir_all(&sub).unwrap();
    std::fs::write(
        sub.join("wire.jsonl"),
        concat!(
            r#"{"type":"llm.request","kind":"loop","provider":"kimi","model":"kimi-for-coding"}"#,
            "\n",
            r#"{"type":"usage.record","model":"alias","usage":{"inputOther":1,"output":2,"inputCacheRead":3,"inputCacheCreation":0},"usageScope":"turn","time":1791300001.5}"#,
            "\n",
        ),
    )
    .unwrap();

    let (records, stats) = KimiSource.scan_roots(std::slice::from_ref(&root), None);
    assert_eq!(records.len(), 2, "notes: {:?}", stats.notes);
    let main_rec = records
        .iter()
        .find(|r| r.agent.is_none())
        .expect("main 在列");
    // session_ 前缀剥除 + 秒级 time 归一毫秒(§4)。
    assert_eq!(main_rec.session_id, "11111111-2222-3333-4444-555555555555");
    assert_eq!(main_rec.ts_ms, 1791300000500);
    assert_eq!(main_rec.model, "kimi-for-coding");
    let sub_rec = records
        .iter()
        .find(|r| r.agent.is_some())
        .expect("子代理在列");
    assert_eq!(sub_rec.agent.as_deref(), Some("agent-7"));
    assert!(
        sub_rec
            .dedup_key
            .starts_with("kimi:11111111-2222-3333-4444-555555555555:agent-7:"),
        "实测 {}",
        sub_rec.dedup_key
    );

    let _ = std::fs::remove_dir_all(root);
}

/// 空根:两源各自给出空结果与源专属提示,不 panic。
#[test]
fn empty_roots_notes() {
    let (records, stats) = KimiWorkSource.scan_roots(&[], None);
    assert!(records.is_empty());
    assert!(stats.notes.iter().any(|n| n.contains("Kimi Work")));
    let (records, stats) = KimiSource.scan_roots(&[], None);
    assert!(records.is_empty());
    assert!(stats.notes.iter().any(|n| n.contains("~/.kimi-code")));
}
