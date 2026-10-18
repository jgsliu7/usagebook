//! 聚合与报表(口径纪律:只对 request_delta 求和;session_total 单列校验)。

use crate::model::{Caliber, UsageRecord};
use serde::Serialize;
use std::collections::BTreeMap;

#[derive(Default, Serialize)]
pub struct Bucket {
    pub input_uncached: u64,
    pub cache_read: u64,
    pub cache_write: u64,
    pub cache_write_1h: u64,
    pub output: u64,
    pub reasoning_output: u64,
    pub requests: u64,
}

impl Bucket {
    fn add(&mut self, r: &UsageRecord) {
        self.input_uncached += r.input_uncached;
        self.cache_read += r.cache_read;
        self.cache_write += r.cache_write;
        self.cache_write_1h += r.cache_write_1h;
        self.output += r.output;
        self.reasoning_output += r.reasoning_output;
        self.requests += 1;
    }
    pub fn total(&self) -> u64 {
        self.input_uncached
            .saturating_add(self.cache_read)
            .saturating_add(self.cache_write)
            .saturating_add(self.output)
            .saturating_add(self.reasoning_output)
    }
}

fn fmt(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::new();
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    out
}

fn print_table(title: &str, first_col: &str, rows: Vec<(String, Bucket)>) {
    println!("\n{title}");
    println!(
        "{:<12} {:>14} {:>14} {:>12} {:>14} {:>9} {:>14}",
        first_col, "输入(新)", "缓存读", "缓存写", "输出(含推理)", "次数", "合计"
    );
    println!("{}", "-".repeat(96));
    let mut grand = Bucket::default();
    for (k, b) in &rows {
        println!(
            "{:<12} {:>14} {:>14} {:>12} {:>14} {:>9} {:>14}",
            k,
            fmt(b.input_uncached),
            fmt(b.cache_read),
            fmt(b.cache_write),
            fmt(b.output + b.reasoning_output),
            fmt(b.requests),
            fmt(b.total())
        );
        grand.input_uncached += b.input_uncached;
        grand.cache_read += b.cache_read;
        grand.cache_write += b.cache_write;
        grand.cache_write_1h += b.cache_write_1h;
        grand.output += b.output + b.reasoning_output;
        grand.requests += b.requests;
    }
    println!("{}", "-".repeat(96));
    println!(
        "{:<12} {:>14} {:>14} {:>12} {:>14} {:>9} {:>14}",
        "合计",
        fmt(grand.input_uncached),
        fmt(grand.cache_read),
        fmt(grand.cache_write),
        fmt(grand.output),
        fmt(grand.requests),
        fmt(grand.total())
    );
}

/// 聚合结果(print_report 与 json_report 共用同一口径)。
pub struct Agg {
    pub by_day: Vec<(String, Bucket)>,
    pub by_source: Vec<(String, Bucket)>,
    pub by_model: Vec<(String, Bucket)>,
    pub totals: Bucket,
    pub baseline_count: usize,
}

fn aggregate(records: &[UsageRecord], days: Option<usize>) -> Agg {
    let cutoff =
        days.map(|n| (chrono::Local::now() - chrono::Duration::days(n as i64)).timestamp_millis());
    let delta: Vec<&UsageRecord> = records
        .iter()
        .filter(|r| r.caliber == Caliber::RequestDelta && cutoff.is_none_or(|c| r.ts_ms >= c))
        .collect();
    let baseline_count = records
        .iter()
        .filter(|r| r.caliber == Caliber::SessionTotal)
        .count();

    let mut by_day: BTreeMap<String, Bucket> = BTreeMap::new();
    let mut by_source: BTreeMap<String, Bucket> = BTreeMap::new();
    // 模型名大小写归一(GLM-5.3 与 glm-5.3 同源不同写):小写为键;显示名取
    // "出现次数最多的拼写,并列取字典序最小"(与 snapshot.rs 同语义,跨扫描稳定)
    let mut by_model: BTreeMap<String, Bucket> = BTreeMap::new();
    let mut model_spell: BTreeMap<String, BTreeMap<String, u64>> = BTreeMap::new();
    let mut totals = Bucket::default();
    for r in &delta {
        by_day
            .entry(crate::common::ms_to_local_date(r.ts_ms))
            .or_default()
            .add(r);
        by_source.entry(r.source.to_string()).or_default().add(r);
        let mkey = r.model.to_lowercase();
        *model_spell
            .entry(mkey.clone())
            .or_default()
            .entry(r.model.clone())
            .or_default() += 1;
        by_model.entry(mkey).or_default().add(r);
        totals.add(r);
    }
    let by_model: Vec<(String, Bucket)> = by_model
        .into_iter()
        .map(|(k, v)| {
            let disp = model_spell
                .get(&k)
                .and_then(|spells| {
                    spells
                        .iter()
                        .max_by_key(|(s, c)| (**c, std::cmp::Reverse(s.as_str())))
                        .map(|(s, _)| s.clone())
                })
                .unwrap_or(k.clone());
            (disp, v)
        })
        .collect();
    Agg {
        by_day: by_day.into_iter().collect(),
        by_source: by_source.into_iter().collect(),
        by_model,
        totals,
        baseline_count,
    }
}

/// 输出三张表:按日 / 按工具 / 按模型;另附会话级基线(不并入)。
pub fn print_report(records: &[UsageRecord], days: Option<usize>) {
    let agg = aggregate(records, days);
    print_table("▎按日(本地时区)", "日期", agg.by_day);
    print_table("▎按工具", "工具", agg.by_source);
    print_table("▎按模型", "模型", agg.by_model);
    let cw1h = agg.totals.cache_write_1h;
    if cw1h > 0 {
        println!(
            "\n注:缓存写合计中含 1 小时 TTL 写入 {}(子集,已计入缓存写列,不重复相加)。",
            fmt(cw1h)
        );
    }

    if agg.baseline_count > 0 {
        println!("\n▎会话级基线(session_total,官方口径,校验用,不并入上表)");
        for b in records
            .iter()
            .filter(|r| r.caliber == Caliber::SessionTotal)
            .take(20)
        {
            let sid: String = b.session_id.chars().take(8).collect();
            println!(
                "  {} {} {:<24} 输入{} 缓存读{} 输出{} 成本${:.2}",
                b.source,
                sid,
                b.model,
                fmt(b.input_uncached),
                fmt(b.cache_read),
                fmt(b.output),
                b.cost_usd.unwrap_or(0.0)
            );
        }
        if agg.baseline_count > 20 {
            println!("  …共 {} 条", agg.baseline_count);
        }
    }
}

/// `--json` 机器可读输出:与终端报表同口径(按日/按源/按模型 + 合计)。
pub fn json_report(records: &[UsageRecord], days: Option<usize>) -> String {
    #[derive(Serialize)]
    struct Row {
        key: String,
        #[serde(flatten)]
        b: Bucket,
    }
    #[derive(Serialize)]
    struct Out {
        generated_at: String,
        window_days: Option<usize>,
        totals: Bucket,
        by_day: Vec<Row>,
        by_source: Vec<Row>,
        by_model: Vec<Row>,
        session_baseline_count: usize,
    }
    let rows = |v: Vec<(String, Bucket)>| {
        v.into_iter()
            .map(|(key, b)| Row { key, b })
            .collect::<Vec<_>>()
    };
    let agg = aggregate(records, days);
    let out = Out {
        generated_at: chrono::Local::now()
            .format("%Y-%m-%dT%H:%M:%S%:z")
            .to_string(),
        window_days: days,
        totals: agg.totals,
        by_day: rows(agg.by_day),
        by_source: rows(agg.by_source),
        by_model: rows(agg.by_model),
        session_baseline_count: agg.baseline_count,
    };
    serde_json::to_string_pretty(&out).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Caliber;

    fn rec(ts_ms: i64, caliber: Caliber, model: &str, input: u64) -> UsageRecord {
        UsageRecord {
            source: "test",
            session_id: "s1".into(),
            agent: None,
            model: model.into(),
            ts_ms,
            caliber,
            input_uncached: input,
            cache_read: 0,
            cache_write: 0,
            cache_write_1h: 0,
            output: 0,
            reasoning_output: 0,
            cost_usd: None,
            dedup_key: format!("k{ts_ms}"),
        }
    }

    #[test]
    fn aggregate_only_counts_request_delta() {
        let now = chrono::Local::now().timestamp_millis();
        let records = vec![
            rec(now, Caliber::RequestDelta, "m1", 100),
            rec(now, Caliber::SessionTotal, "m1", 999), // 会话基线,不得混入
        ];
        let agg = aggregate(&records, None);
        assert_eq!(agg.totals.input_uncached, 100);
        assert_eq!(agg.totals.requests, 1);
        assert_eq!(agg.baseline_count, 1);
    }

    #[test]
    fn aggregate_days_cutoff_drops_old() {
        let now = chrono::Local::now().timestamp_millis();
        let day = 86_400_000i64;
        let records = vec![
            rec(now, Caliber::RequestDelta, "m1", 10),
            rec(now - 2 * day, Caliber::RequestDelta, "m1", 20), // 超 1 天窗
        ];
        let agg = aggregate(&records, Some(1));
        assert_eq!(agg.totals.input_uncached, 10);
    }

    #[test]
    fn aggregate_merges_model_spelling_case() {
        let now = chrono::Local::now().timestamp_millis();
        let records = vec![
            rec(now, Caliber::RequestDelta, "GLM-5.3", 1),
            rec(now, Caliber::RequestDelta, "glm-5.3", 2),
        ];
        let agg = aggregate(&records, None);
        assert_eq!(agg.by_model.len(), 1, "同模型不同大小写应归一为一行");
        assert_eq!(agg.by_model[0].1.input_uncached, 3);
    }

    #[test]
    fn bucket_total_saturates() {
        let b = Bucket {
            input_uncached: u64::MAX,
            output: 10,
            ..Default::default()
        };
        assert_eq!(b.total(), u64::MAX, "五桶求和必须饱和而非回绕");
    }

    #[test]
    fn json_report_matches_aggregate_totals() {
        let now = chrono::Local::now().timestamp_millis();
        let records = vec![rec(now, Caliber::RequestDelta, "m1", 7)];
        let s = json_report(&records, None);
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["totals"]["input_uncached"], 7);
        assert_eq!(v["by_source"][0]["key"], "test");
    }
}
