//! workbuddy / codebuddy 双源 fixture 测试:同一扫描核(scan_buddy_wire)参数化注入源 id,
//! token 分开统计互不串扰。路径注入(scan_roots 纯函数核),不依赖本机安装。
//! fixture 字段形状取自上游双腿:juejin-usage `parsers/codebuddy.ts`(CLI 通道 ~/.codebuddy/projects)
//! 与 tokscale `sessions/codebuddy.rs`(message.usage / function_call.rawUsage 实测行);数值断言按
//! 本仓 tencent_buddy 族统一口径(exclusive 五桶,workbuddy 档案 05 §4 逐位对账同款语义)。

use usagebook_core::sources::codebuddy::CodebuddySource;
use usagebook_core::sources::codebuddy_ide::CodebuddyIdeSource;
use usagebook_core::sources::workbuddy::WorkbuddySource;
use usagebook_core::UsageSource;

fn temp_root(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("usagebook-fixture-{}-{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// CodeBuddy CLI 布局:projects/<项目键>/<sessionId>.jsonl,行形状=tencent_buddy 族。
/// 同一根再用 WorkbuddySource 扫一遍,证明核已彻底参数化(无 "workbuddy" 残留硬编码)。
#[test]
fn codebuddy_cli_layout() {
    let root = temp_root("codebuddy-cli");
    let dir = root.join("projects/c-Users-alice-repo");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("session-1.jsonl"),
        concat!(
            // message.usage 通道(Anthropic 名 OpenAI 义,inclusive 减除):24486 含 14720 缓存读
            r#"{"id":"assistant-1","timestamp":1780000000100,"type":"message","role":"assistant","status":"completed","sessionId":"session-1","cwd":"/Users/alice/repo","providerData":{"model":"glm-5.2","messageId":"msg-1"},"message":{"usage":{"input_tokens":24486,"output_tokens":3,"total_tokens":24489,"cache_read_input_tokens":14720}}}"#,
            "\n",
            // function_call 挂 providerData.rawUsage(OpenAI 原生,别名族 prompt_cache_hit_tokens)
            r#"{"id":"call-1","timestamp":1780000000200,"type":"function_call","sessionId":"session-1","providerData":{"requestModelId":"minimax-m3-pay","messageId":"msg-2","rawUsage":{"prompt_tokens":10,"completion_tokens":2,"prompt_cache_hit_tokens":3}}}"#,
            "\n",
            // user 消息与已知非用量行:不产生记录
            r#"{"id":"user-1","timestamp":1780000000300,"type":"message","role":"user","sessionId":"session-1"}"#,
            "\n",
            r#"{"id":"snap-1","timestamp":1780000000400,"type":"file-history-snapshot","sessionId":"session-1"}"#,
            "\n",
            // 2026-10-18 本机实证新行型(档案 30 §2 增补):summary 纯文本、turn-metrics 轮级聚合
            // (tokenDelta=逐请求和,计入会双计)——均无逐请求用量,按未知行跳过+计数
            r#"{"id":"sum-1","timestamp":1780000000500,"type":"summary","summary":"问了一句","providerData":{"source":"initial-user-message"},"sessionId":"session-1"}"#,
            "\n",
            r#"{"id":"tm-1","timestamp":1780000000600,"type":"turn-metrics","durationMs":31913,"tokenDelta":52376,"sessionId":"session-1"}"#,
            "\n",
        ),
    )
    .unwrap();

    let (records, stats) = CodebuddySource.scan_roots(std::slice::from_ref(&root), None);
    assert_eq!(
        records.len(),
        2,
        "user 消息/快照/summary/turn-metrics 行不计;notes: {:?}",
        stats.notes
    );
    assert_eq!(
        stats.lines_unknown_type, 2,
        "summary/turn-metrics 计未知跳过"
    );
    assert!(records.iter().all(|r| r.source == "codebuddy"));
    assert!(records
        .iter()
        .all(|r| r.caliber == usagebook_core::Caliber::RequestDelta));

    let msg1 = records
        .iter()
        .find(|r| r.dedup_key.ends_with("msg-1"))
        .expect("msg-1 在列");
    assert_eq!(msg1.model, "glm-5.2");
    assert_eq!(msg1.input_uncached, 9766, "24486 inclusive − 14720 缓存读");
    assert_eq!(msg1.cache_read, 14720);
    assert_eq!(msg1.output, 3);
    assert_eq!(msg1.token_total(), 24489);
    assert_eq!(msg1.ts_ms, 1780000000100);

    let msg2 = records
        .iter()
        .find(|r| r.dedup_key.ends_with("msg-2"))
        .expect("msg-2 在列");
    assert_eq!(msg2.model, "minimax-m3-pay");
    assert_eq!(msg2.input_uncached, 7, "prompt 10 − 缓存读 3");
    assert_eq!(msg2.cache_read, 3);
    assert_eq!(msg2.output, 2);
    assert_eq!(msg2.token_total(), 12);
    assert!(msg2.dedup_key.starts_with("codebuddy:session-1:"));

    // 同一根用 WorkbuddySource 扫:同核换源 id,记录/去重前缀整体改挂 workbuddy,数值不变
    let (wb, _) = WorkbuddySource.scan_roots(std::slice::from_ref(&root), None);
    assert_eq!(wb.len(), 2);
    assert!(wb.iter().all(|r| r.source == "workbuddy"));
    assert!(wb.iter().all(|r| r.dedup_key.starts_with("workbuddy:")));
    let wb_total: u64 = wb.iter().map(|r| r.token_total()).sum();
    let cb_total: u64 = records.iter().map(|r| r.token_total()).sum();
    assert_eq!(wb_total, cb_total, "核参数化后两源对同一语料合计一致");
    assert!(stats
        .notes
        .iter()
        .any(|n| n.contains("rawUsage 1 / providerData.usage 0 / message.usage 1")));
    let _ = std::fs::remove_dir_all(&root);
}

/// workbuddy 布局回归:扫描核自 WorkbuddySource::scan_roots 抽出后,既有行为逐位保持——
/// status 非 completed 过滤、缺 status 视为 completed、同键 token 大者胜、坏 JSON 行计数。
#[test]
fn workbuddy_layout_regression() {
    let root = temp_root("workbuddy-reg");
    let dir = root.join("projects/eUsers-bob-api");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("sess-a.jsonl"),
        concat!(
            // 同 messageId 两行(多 sink 镜像):token 大者胜 → 保留 100+50
            r#"{"timestamp":1780000000000,"type":"message","role":"assistant","status":"completed","sessionId":"sess-a","providerData":{"model":"deepseek-v4","messageId":"m-1","rawUsage":{"prompt_tokens":100,"completion_tokens":50,"prompt_tokens_details":{"cached_tokens":0}}}}"#,
            "\n",
            r#"{"timestamp":1780000000500,"type":"message","role":"assistant","status":"completed","sessionId":"sess-a","providerData":{"model":"deepseek-v4","messageId":"m-1","rawUsage":{"prompt_tokens":10,"completion_tokens":5}}}"#,
            "\n",
            // incomplete 过滤;无 status 视为 completed
            r#"{"timestamp":1780000001000,"type":"message","role":"assistant","status":"incomplete","sessionId":"sess-a","providerData":{"messageId":"m-2","rawUsage":{"prompt_tokens":999,"completion_tokens":999}}}"#,
            "\n",
            r#"{"timestamp":1780000002000,"type":"function_call","sessionId":"sess-a","providerData":{"isSubAgent":true,"agent":"general-purpose","messageId":"m-3","rawUsage":{"prompt_tokens":30,"completion_tokens":7,"completion_tokens_details":{"reasoning_tokens":7}}}}"#,
            "\n",
            // 坏 JSON 行:计数不中断
            "{not json}\n",
        ),
    )
    .unwrap();

    let (records, stats) = WorkbuddySource.scan_roots(std::slice::from_ref(&root), None);
    assert_eq!(records.len(), 2, "同键去重后 m-1 + m-3;incomplete 过滤");
    assert_eq!(stats.lines_bad_json, 1);

    let m1 = records
        .iter()
        .find(|r| r.dedup_key.ends_with("m-1"))
        .expect("m-1 大者胜在列");
    assert_eq!(m1.input_uncached, 100);
    assert_eq!(m1.output, 50);

    let m3 = records
        .iter()
        .find(|r| r.dedup_key.ends_with("m-3"))
        .expect("无 status 的 function_call 视为 completed");
    assert_eq!(m3.agent.as_deref(), Some("general-purpose"));
    assert_eq!(m3.reasoning_output, 7, "reasoning 单列钳制");
    assert_eq!(m3.output, 0, "completion 7 全为 reasoning");
    assert_eq!(m3.token_total(), 37);

    assert!(stats
        .notes
        .iter()
        .any(|n| n.contains("非 completed 跳过 1 行")));
    // workbuddy.db 不存在 → 不出现 db 注记(codebuddy 复用核时不携带 workbuddy 专属注记)
    assert!(!stats.notes.iter().any(|n| n.contains("workbuddy.db")));
    let _ = std::fs::remove_dir_all(&root);
}

/// 空根:源专属空提示,互不串用。
#[test]
fn empty_roots_source_specific_note() {
    let (recs, stats_cb) = CodebuddySource.scan_roots(&[], None);
    assert!(recs.is_empty());
    assert!(stats_cb.notes.iter().any(|n| n.contains("~/.codebuddy")));
    let (_, stats_wb) = WorkbuddySource.scan_roots(&[], None);
    assert!(stats_wb.notes.iter().any(|n| n.contains("~/.workbuddy")));
    let (_, stats_ci) = CodebuddyIdeSource.scan_roots(&[], None);
    assert!(stats_ci
        .notes
        .iter()
        .any(|n| n.contains("CodeBuddyExtension")));
}

/// CodeBuddy IDE 扩展通道布局:Data/<uid>/CodeBuddyIDE/<uid>/history/<ws>/<session>/messages/*.json
/// (extra 盘上为序列化 JSON 字符串,本机实证)+ 匿名 base64 树。
/// 覆盖:statsSnapshot 显式 cacheMiss / 缺省回退 input−cached / lastStep* 旧记录回退 / 非 assistant 跳过。
#[test]
fn codebuddy_ide_extension_layout() {
    let root = temp_root("codebuddy-ide");
    let msgs = root.join("Data/00aa11bb/CodeBuddyIDE/00aa11bb/history/aa112233/ws-s1/messages");
    std::fs::create_dir_all(&msgs).unwrap();
    let mk = |file: &str, body: &str| std::fs::write(msgs.join(file), body).unwrap();
    // ① statsSnapshot(2026-10-18 本机实证形状:extra 为字符串,cacheMissTokens 显式新输入)
    mk(
        "m1.json",
        r#"{"role":"assistant","message":"…","id":"m-1","createdAt":"2026-10-18T10:25:41.989Z","extra":"{\"modelId\":\"step-5-preview\",\"statsSnapshot\":{\"inputTokens\":71351,\"outputTokens\":814,\"cachedInputTokens\":68864,\"cacheWriteTokens\":0,\"cacheMissTokens\":2487,\"thinkingTokens\":0},\"lastStepInputTokens\":387,\"lastStepOutputTokens\":387,\"lastStepCachedInputTokens\":0}"}"#,
    );
    // ② snapshot 无 cacheMissTokens:回退 input−cached
    mk(
        "m2.json",
        r#"{"role":"assistant","id":"m-2","createdAt":"2026-10-18T10:30:00.000Z","extra":"{\"modelId\":\"step-5-preview\",\"statsSnapshot\":{\"inputTokens\":500,\"outputTokens\":50,\"cachedInputTokens\":300,\"cacheWriteTokens\":20,\"thinkingTokens\":10}}"}"#,
    );
    // ③ 旧记录仅 lastStep*(末步口径):input−cached 减除,无 cache_write/reasoning
    mk(
        "m3.json",
        r#"{"role":"assistant","id":"m-3","createdAt":"2026-10-18T10:35:00.000Z","extra":"{\"lastStepInputTokens\":1000,\"lastStepOutputTokens\":40,\"lastStepCachedInputTokens\":600}"}"#,
    );
    // ④ 非 assistant(user/tool)跳过;⑤ assistant 无用量(纯中间态)跳过
    mk(
        "m4.json",
        r#"{"role":"user","id":"m-4","createdAt":"2026-10-18T10:20:00.000Z","extra":"{}"}"#,
    );
    mk(
        "m5.json",
        r#"{"role":"tool","id":"m-5","createdAt":"2026-10-18T10:26:00.000Z","extra":"{}"}"#,
    );
    mk(
        "m6.json",
        r#"{"role":"assistant","id":"m-6","createdAt":"2026-10-18T10:27:00.000Z","extra":"{}"}"#,
    );
    // 匿名 base64(cwd) 树:同被 host 无关遍历覆盖;无 id 回退文件名
    let anon =
        root.join("Data/default/CodeBuddyIDE/L1VzZXJzL2FsaWNl/history/ws-anon/sess-anon/messages");
    std::fs::create_dir_all(&anon).unwrap();
    std::fs::write(
        anon.join("anon-1.json"),
        r#"{"role":"assistant","createdAt":"2026-10-18T10:40:00.000Z","extra":"{\"modelId\":\"step-5-preview\",\"statsSnapshot\":{\"inputTokens\":100,\"outputTokens\":10,\"cachedInputTokens\":0,\"cacheWriteTokens\":0,\"cacheMissTokens\":100,\"thinkingTokens\":0}}"}"#,
    )
    .unwrap();

    let (records, stats) = CodebuddyIdeSource.scan_roots(std::slice::from_ref(&root), None);
    assert_eq!(
        records.len(),
        4,
        "user/tool/无用量 assistant 不计;notes: {:?}",
        stats.notes
    );
    assert!(records.iter().all(|r| r.source == "codebuddy-ide"));

    let m1 = records
        .iter()
        .find(|r| r.dedup_key.ends_with("m-1"))
        .unwrap();
    assert_eq!(m1.model, "step-5-preview");
    assert_eq!(m1.input_uncached, 2487, "显式 cacheMissTokens 优先");
    assert_eq!(m1.cache_read, 68864);
    assert_eq!(m1.output, 814);
    assert_eq!(m1.token_total(), 72165, "lastStep* 同时存在不作数,快照优先");
    assert_eq!(m1.ts_ms, 1792319141989);

    let m2 = records
        .iter()
        .find(|r| r.dedup_key.ends_with("m-2"))
        .unwrap();
    assert_eq!(m2.input_uncached, 200, "无 cacheMissTokens 回退 500−300");
    assert_eq!(m2.cache_read, 300);
    assert_eq!(m2.cache_write, 20);
    assert_eq!(m2.reasoning_output, 10);
    assert_eq!(m2.output, 40, "thinking 是 output 子集,减除:50−10");
    assert_eq!(
        m2.token_total(),
        570,
        "=200+300+20+40+10,五桶和==snapshot totalTokens"
    );

    let m3 = records
        .iter()
        .find(|r| r.dedup_key.ends_with("m-3"))
        .unwrap();
    assert_eq!(m3.input_uncached, 400);
    assert_eq!(m3.cache_read, 600);
    assert_eq!(m3.cache_write, 0);
    assert_eq!(m3.output, 40);

    let anon1 = records
        .iter()
        .find(|r| r.session_id == "sess-anon")
        .unwrap();
    assert_eq!(anon1.input_uncached, 100);
    assert!(
        anon1
            .dedup_key
            .starts_with("codebuddy-ide:sess-anon:anon-1"),
        "无 id 回退文件名"
    );

    assert!(records
        .iter()
        .filter(|r| r.session_id == "ws-s1")
        .all(|r| r.dedup_key.starts_with("codebuddy-ide:ws-s1:m-")));
    assert!(stats
        .notes
        .iter()
        .any(|n| n.contains("statsSnapshot 3 / lastStep 1")));
    assert!(stats
        .notes
        .iter()
        .any(|n| n.contains("非 assistant 消息 2 个")));
    let _ = std::fs::remove_dir_all(&root);
}
