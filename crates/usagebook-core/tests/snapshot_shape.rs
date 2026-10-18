//! /api/data 契约锁定(golden):前后端字段形状的唯一权威。
//! 快照序列化体 + serve 拼接注入(scanning/last_scan_failed/eff_refresh_secs)共同构成 /api/data,
//! 此测试锁死快照侧键集;serve 注入键见 serve.rs 注释。
//! 字段关系:refresh_secs = CLI 配置回退值(烤入快照),eff_refresh_secs = 当前生效重扫间隔(serve 响应时现算)。

use usagebook_core::snapshot::{Buckets, HourRow, ModelAgg, Snapshot, SourceAgg, Totals};
use usagebook_core::SOURCE_CATALOG;

fn snap() -> Snapshot {
    let b = Buckets { i: 1, cr: 2, cw: 3, o: 4, r: 1, n: 2, c: 0.5 };
    Snapshot {
        ready: true,
        generated_at_ms: 1,
        local_date: "1970-01-01".into(),
        prior_day_to_now: Some(vec![]),
        source_scans: vec![],
        scan_ms: 2,
        refresh_secs: 30,
        totals: Totals { b: b.clone(), sessions: 1, cost_official_usd: 0.0, cost_sessions: 0, cost_req_usd: 0.5 },
        hours: vec![
            HourRow { h: "2026-10-09T10".into(), s: "codex".into(), m: "gpt-5".into(), b: b.clone() },
            HourRow { h: "2026-10-09T11".into(), s: "claude".into(), m: "glm".into(), b },
        ],
        per_source: vec![SourceAgg { s: "codex".into(), b: Default::default(), sessions: 1, files: 1, cost_usd: 0.0 }],
        per_model: vec![ModelAgg { m: "gpt-5".into(), sources: vec!["codex".into()], b: Default::default() }],
        notes: vec![],
        sources: SOURCE_CATALOG,
    }
}

fn keys(v: &serde_json::Value) -> Vec<String> {
    // serde_json::Value::Object 是 BTreeMap(键已排序);契约关心的是键集,与顺序无关
    let mut k: Vec<String> = v.as_object().unwrap().keys().cloned().collect();
    k.sort();
    k
}

#[test]
fn snapshot_shape_golden() {
    let bytes = serde_json::to_vec(&snap()).unwrap();
    // serve 拼接的前提约定:尾字节必为 '}' 且非空(serve.rs /api_data 分支依赖此)
    assert_eq!(*bytes.last().unwrap(), b'}');
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let mut want = ["ready", "generated_at_ms", "scan_ms", "refresh_secs", "totals", "hours", "per_source", "per_model", "notes", "sources", "local_date", "prior_day_to_now", "source_scans"];
    want.sort_unstable();
    assert_eq!(keys(&v), want);
assert_eq!(keys(&v["hours"][0]), vec!["b", "h", "m", "s"]);
assert_eq!(keys(&v["hours"][0]["b"]), vec!["c", "cr", "cw", "i", "n", "o", "r"]);
assert_eq!(keys(&v["totals"]), vec!["b", "cost_official_usd", "cost_req_usd", "cost_sessions", "sessions"]);
    // 前端引用面(dashboard.html):per_source[].s/.b、per_model[].m、sources[].id/.label/.tier
assert_eq!(keys(&v["per_source"][0]), vec!["b", "cost_usd", "files", "s", "sessions"]);
assert_eq!(keys(&v["per_model"][0]), vec!["b", "m", "sources"]);
    for s in v["sources"].as_array().unwrap() {
assert_eq!(keys(s), vec!["id", "label", "tier"]);
    }
}
