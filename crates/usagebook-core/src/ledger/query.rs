//! Snapshot queries over published facts. Query state and aggregation buffers are bounded;
//! source files and parsers are deliberately absent from this module.
use super::{
    legacy::{legacy_metrics, model_key, tuple_hash, LegacyEntry, LegacySnapshot},
    metrics::evaluate,
    query_types::*,
    types::{MetricValue, ValueState},
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use chrono::{Datelike, Duration, LocalResult, NaiveDate, TimeZone, Timelike, Utc};
use chrono_tz::Tz;
use rusqlite::{params, Connection, OptionalExtension, Row};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Instant,
};

const RESPONSE_LIMIT: usize = 8 * 1024 * 1024;
const GROUP_LIMIT: usize = 20_000;
const FACT_COLUMNS:&str="o.observation_id,o.identity_family,o.namespace_key,e.identity_key,o.fact_kind,o.report_source,o.native_tool,o.model_key,o.model_display,o.session_key,o.occurred_ms,o.native_date,o.native_timezone,o.day_start_ms,o.day_end_ms,o.input_uncached,o.cache_read,o.cache_write,o.output_normal,o.output_reasoning,o.input_total,o.output_total,o.total_tokens,o.cache_write_1h,o.request_count,o.cost_state,o.cost_units,o.cost_scale,o.cost_currency";

pub struct QueryService<'a> {
    conn: &'a Connection,
    legacy: Option<&'a LegacySnapshot>,
    model_labels: std::cell::RefCell<BTreeMap<String, String>>,
    all_labels: std::cell::Cell<Option<bool>>,
}
#[derive(Clone)]
struct Fact {
    id: i64,
    family: String,
    namespace: String,
    key: String,
    kind: String,
    source: String,
    native: String,
    model: String,
    session: Option<String>,
    time: Option<i64>,
    date: Option<String>,
    timezone: Option<String>,
    start: Option<i64>,
    end: Option<i64>,
    metrics: [Option<i64>; 10],
    cost_state: String,
    cost_units: Option<i64>,
    cost_scale: Option<i64>,
    currency: Option<String>,
    live: bool,
    known_fields: u16,
    summary: Option<[Cell; 10]>,
}
impl Fact {
    fn from_row(r: &Row<'_>) -> rusqlite::Result<Self> {
        let mut m = [None; 10];
        for (i, v) in m.iter_mut().enumerate() {
            *v = r.get(15 + i)?;
        }
        let model: String = r.get(7)?;
        Ok(Self {
            id: r.get(0)?,
            family: r.get(1)?,
            namespace: r.get(2)?,
            key: r.get(3)?,
            kind: r.get(4)?,
            source: r.get(5)?,
            native: r.get(6)?,
            model,
            session: r.get(9)?,
            time: r.get(10)?,
            date: r.get(11)?,
            timezone: r.get(12)?,
            start: r.get(13)?,
            end: r.get(14)?,
            metrics: m,
            cost_state: r.get(25)?,
            cost_units: r.get(26)?,
            cost_scale: r.get(27)?,
            currency: r.get(28)?,
            live: false,
            known_fields: 1023,
            summary: None,
        })
    }
}
#[derive(Clone, Default)]
struct Cell {
    sum: i64,
    known: i64,
    exact: i64,
    count: i64,
}
impl Cell {
    fn add(&mut self, v: &MetricValue) -> QueryResult<()> {
        self.count = add(self.count, 1)?;
        if let Some(n) = v.known_value {
            self.sum = add(self.sum, n)?;
            self.known = add(self.known, 1)?;
        }
        if v.state == ValueState::Exact {
            self.exact = add(self.exact, 1)?;
        }
        Ok(())
    }
    fn merge(&mut self, v: &Cell) -> QueryResult<()> {
        self.sum = add(self.sum, v.sum)?;
        self.known = add(self.known, v.known)?;
        self.exact = add(self.exact, v.exact)?;
        self.count = add(self.count, v.count)?;
        Ok(())
    }
    fn value(&self) -> Value {
        if self.count == 0 || self.exact == self.count {
            json!({"state":"exact","known_value":self.sum.to_string()})
        } else if self.known == 0 {
            json!({"state":"unknown","known_value":null})
        } else {
            json!({"state":"partial","known_value":self.sum.to_string()})
        }
    }
    fn complete(&self) -> bool {
        self.count == 0 || self.count == self.exact
    }
}
#[derive(Clone, Default)]
struct Aggregate {
    cells: [Cell; 10],
    sessions: BTreeSet<String>,
    ledger_sessions: i64,
    session_unknown: i64,
    ledger: bool,
    live: bool,
    requests: bool,
    days: bool,
    excluded: i64,
    costs: BTreeMap<String, (i128, u32, i64)>,
    cost_unknown: i64,
    session_costs: Vec<Value>,
}
impl Aggregate {
    fn add_usage(&mut self, f: &Fact) -> QueryResult<()> {
        if let Some(cells) = &f.summary {
            for (i, cell) in cells.iter().enumerate() {
                self.cells[i].merge(cell)?;
            }
            self.ledger = true;
            self.requests = true;
            return Ok(());
        }
        let mut vals =
            evaluate(&f.metrics).map_err(|e| QueryError::new(e.code(), e.to_string()))?;
        if f.live {
            for (i, v) in vals.iter_mut().enumerate() {
                if f.known_fields & (1 << i) == 0 && v.known_value.is_some() {
                    v.state = ValueState::Partial;
                }
            }
        }
        for (c, v) in self.cells.iter_mut().zip(vals.iter()) {
            c.add(v)?;
        }
        self.live |= f.live;
        self.ledger |= !f.live;
        self.requests |= f.kind == "request";
        self.days |= f.kind == "day";
        Ok(())
    }
    fn add(&mut self, f: &Fact) -> QueryResult<()> {
        self.add_usage(f)?;
        if f.summary.is_some() {
            return Ok(());
        }
        if let Some(s) = &f.session {
            if f.live {
                self.sessions.insert(format!(
                    "{}\0{}\0{}\0{}",
                    f.source, f.family, f.namespace, s
                ));
            }
        } else if f.live || f.kind != "request" {
            self.session_unknown = add(self.session_unknown, 1)?;
        }
        if !f.live && f.kind == "request" { /* SQL metadata pass accounts for ledger costs. */
        } else if f.cost_state == "known" {
            if let (Some(units), Some(scale), Some(currency)) =
                (f.cost_units, f.cost_scale, &f.currency)
            {
                let e = self
                    .costs
                    .entry(currency.clone())
                    .or_insert((0, scale as u32, 0));
                let target = e.1.max(scale as u32);
                let a = e.0.checked_mul(pow10(target - e.1)?).ok_or_else(overflow)?;
                let b = i128::from(units)
                    .checked_mul(pow10(target - scale as u32)?)
                    .ok_or_else(overflow)?;
                e.0 = a.checked_add(b).ok_or_else(overflow)?;
                e.1 = target;
                e.2 = add(e.2, 1)?;
            }
        } else {
            self.cost_unknown = add(self.cost_unknown, 1)?;
        }
        Ok(())
    }
    fn bundle(&self) -> Value {
        let metrics = json!({"total":self.cells[7].value(),"input_uncached":self.cells[0].value(),"cache_read":self.cells[1].value(),"cache_write":self.cells[2].value(),"output":self.cells[3].value(),"reasoning_output":self.cells[4].value(),"output_combined":self.cells[6].value(),"requests":self.cells[9].value()});
        let sessions = if self.session_unknown == 0 {
            json!({"state":"exact","known_value":(self.sessions.len() as i64+self.ledger_sessions).to_string()})
        } else if self.sessions.is_empty() && self.ledger_sessions == 0 {
            json!({"state":"unknown","known_value":null})
        } else {
            json!({"state":"partial","known_value":(self.sessions.len() as i64+self.ledger_sessions).to_string()})
        };
        let mut costs:Vec<Value>=self.costs.iter().map(|(c,(units,scale,_))|json!({"kind":"reported_request","currency":c,"amount":decimal(*units,*scale),"state":if self.cost_unknown==0&&!self.live{"exact"}else{"partial"},"scope":"filtered","reason":null,"pricing_version":null})).collect();
        costs.extend(self.session_costs.iter().cloned());
        json!({"metrics":metrics,"sessions":sessions,"costs":costs,"coverage":{"protection":match(self.ledger,self.live){(true,true)=>"mixed",(true,false)=>"ledger",_=>"unprotected"},"grain":match(self.requests,self.days){(true,true)=>"mixed",(true,false)=>"request",(false,true)=>"day",_=>"none"},"undated_count":"0","excluded_day_count":self.excluded.to_string(),"incomplete_sources":[]}})
    }
}
fn add(a: i64, b: i64) -> QueryResult<i64> {
    a.checked_add(b).ok_or_else(overflow)
}
fn overflow() -> QueryError {
    QueryError::new(
        "NUMERIC_OVERFLOW",
        "integer aggregate exceeds its exact range",
    )
}
fn pow10(n: u32) -> QueryResult<i128> {
    10_i128.checked_pow(n).ok_or_else(overflow)
}
fn decimal(n: i128, scale: u32) -> String {
    if scale == 0 {
        return n.to_string();
    }
    let raw = n.abs().to_string();
    let raw = format!("{:0>width$}", raw, width = scale as usize + 1);
    let split = raw.len() - scale as usize;
    format!(
        "{}{}.{}",
        if n < 0 { "-" } else { "" },
        &raw[..split],
        &raw[split..]
    )
}
fn model_id(key: &str) -> String {
    if key.is_empty() {
        "m1:u".into()
    } else {
        format!(
            "m1:n:{}",
            URL_SAFE_NO_PAD.encode(key.strip_prefix("n:").unwrap_or(key))
        )
    }
}
fn decode_model(id: &str) -> QueryResult<String> {
    if id == "m1:u" {
        return Ok(String::new());
    }
    let raw = id
        .strip_prefix("m1:n:")
        .ok_or_else(|| QueryError::invalid("invalid model ID"))?;
    let name = String::from_utf8(
        URL_SAFE_NO_PAD
            .decode(raw)
            .map_err(|_| QueryError::invalid("invalid model ID"))?,
    )
    .map_err(|_| QueryError::invalid("model ID is not UTF-8"))?;
    if name.len() > 1024 {
        return Err(QueryError::limit("model name exceeds 1024 bytes"));
    }
    if name.to_lowercase() != name || URL_SAFE_NO_PAD.encode(name.as_bytes()) != raw {
        return Err(QueryError::invalid("noncanonical model ID"));
    }
    Ok(format!("n:{name}"))
}
fn parse_date(s: &str) -> QueryResult<NaiveDate> {
    if s.len() != 10 {
        return Err(QueryError::invalid("date must be YYYY-MM-DD"));
    }
    let d = NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .map_err(|_| QueryError::invalid("invalid date"))?;
    if !(1..=9999).contains(&d.year()) || d.to_string() != s {
        return Err(QueryError::invalid("date outside 0001..9999"));
    }
    Ok(d)
}
fn parse_ms(s: &str) -> QueryResult<i64> {
    let n = s
        .parse::<i64>()
        .map_err(|_| QueryError::invalid("milliseconds must be a signed decimal string"))?;
    if n.to_string() != s {
        return Err(QueryError::invalid("noncanonical milliseconds"));
    }
    Ok(n)
}
fn midnight(tz: Tz, d: NaiveDate) -> QueryResult<i64> {
    let wall = d
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| QueryError::invalid("invalid midnight"))?;
    match tz.from_local_datetime(&wall) {
        LocalResult::Single(v) => Ok(v.timestamp_millis()),
        LocalResult::Ambiguous(a, b) => Ok(a.timestamp_millis().min(b.timestamp_millis())),
        LocalResult::None => {
            for m in 1..=1440 {
                if let Some(v) = tz
                    .from_local_datetime(&(wall + Duration::minutes(m)))
                    .earliest()
                {
                    return Ok(v.timestamp_millis());
                }
            }
            Err(QueryError::invalid(
                "date does not exist in report timezone",
            ))
        }
    }
}
fn bounds(w: &Window, tz: Tz) -> QueryResult<(i64, i64)> {
    let b = match w {
        Window::All => (midnight(tz, parse_date("0001-01-01")?)?, i64::MAX),
        Window::Calendar {
            start_date,
            end_date_exclusive,
        } => (
            midnight(tz, parse_date(start_date)?)?,
            midnight(tz, parse_date(end_date_exclusive)?)?,
        ),
        Window::Instant {
            start_ms,
            end_ms_exclusive,
        } => (parse_ms(start_ms)?, parse_ms(end_ms_exclusive)?),
    };
    if b.0 >= b.1 {
        return Err(QueryError::invalid("window end must follow start"));
    }
    Ok(b)
}
fn local_date(tz: Tz, ms: i64) -> QueryResult<NaiveDate> {
    tz.timestamp_millis_opt(ms)
        .single()
        .map(|d| d.date_naive())
        .ok_or_else(|| QueryError::invalid("timestamp outside supported range"))
}
fn check_page(p: &Page) -> QueryResult<()> {
    if !(1..=200).contains(&p.limit) {
        return Err(QueryError::limit("page limit must be 1..200"));
    }
    if p.cursor.as_ref().is_some_and(|c| c.len() > 4096) {
        return Err(QueryError::limit("cursor exceeds 4 KiB"));
    }
    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    v: u8,
    ledger_uuid: String,
    epoch: String,
    commit_seq: String,
    derived_version: String,
    legacy_generation: Option<String>,
    timezone: String,
    caliber: u32,
    asof_ms: String,
    query_hash: String,
    kind: String,
    dimension: String,
    limit: usize,
    last_value: Option<String>,
    last_id: String,
    routing_hash: String,
}
impl Cursor {
    fn decode(s: &str) -> QueryResult<Self> {
        if s.len() > 4096 {
            return Err(QueryError::limit("cursor exceeds 4 KiB"));
        }
        let bytes = URL_SAFE_NO_PAD
            .decode(s)
            .map_err(|_| QueryError::invalid("invalid cursor"))?;
        serde_json::from_slice(&bytes).map_err(|_| QueryError::invalid("invalid cursor"))
    }
    fn encode(&self) -> QueryResult<String> {
        let s = URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(self).map_err(|_| QueryError::invalid("cursor serialization"))?,
        );
        if s.len() > 4096 {
            return Err(QueryError::limit("cursor exceeds 4 KiB"));
        }
        Ok(s)
    }
}
struct Context {
    stamp: Stamp,
    tz: Tz,
    asof: i64,
    routing_hash: String,
    started: Instant,
    warnings: BTreeSet<String>,
    incomplete: BTreeSet<String>,
    excluded_days: std::cell::Cell<bool>,
}
impl Context {
    fn budget(&self) -> QueryResult<()> {
        if self.started.elapsed().as_secs_f64() > 5.0 {
            Err(QueryError::new(
                "QUERY_TIMEOUT",
                "query exceeded five seconds",
            ))
        } else {
            Ok(())
        }
    }
}
struct SnapshotGuard<'a> {
    conn: &'a Connection,
}
impl Drop for SnapshotGuard<'_> {
    fn drop(&mut self) {
        let _ = self.conn.progress_handler(0, None::<fn() -> bool>);
        if !self.conn.is_autocommit() {
            let _ = self.conn.execute_batch("ROLLBACK");
        }
        let _ = self.conn.execute_batch(
            "DELETE FROM temp.query_sources; DELETE FROM temp.query_models; DELETE FROM temp.query_labels;",
        );
    }
}
impl<'a> QueryService<'a> {
    pub fn new(conn: &'a Connection, legacy: Option<&'a LegacySnapshot>) -> Self {
        Self {
            conn,
            legacy,
            model_labels: std::cell::RefCell::new(BTreeMap::new()),
            all_labels: std::cell::Cell::new(None),
        }
    }
    pub fn routing_hash(conn: &Connection) -> QueryResult<String> {
        let mut st=conn.prepare("SELECT report_source, MIN(route),MAX(route) FROM source_roots GROUP BY report_source ORDER BY report_source COLLATE BINARY")?;
        let mut parts = Vec::new();
        let mut rows = st.query([])?;
        while let Some(r) = rows.next()? {
            let source: String = r.get(0)?;
            let lo: String = r.get(1)?;
            let hi: String = r.get(2)?;
            if lo != hi {
                return Err(QueryError::new(
                    "LEDGER_UNREADABLE",
                    "inconsistent source route",
                ));
            }
            parts.push(format!("{source}:{lo}"));
        }
        Ok(tuple_hash(
            &parts.iter().map(String::as_str).collect::<Vec<_>>(),
        ))
    }
    fn snapshot(
        &self,
        selectors: Option<(&mut Selector, &mut Selector)>,
    ) -> QueryResult<(SnapshotGuard<'_>, Context)> {
        self.model_labels.borrow_mut().clear();
        self.all_labels.set(None);
        self.conn.create_scalar_function(
            "usagebook_local_day",
            2,
            rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC
                | rusqlite::functions::FunctionFlags::SQLITE_UTF8,
            |args| {
                let ms: i64 = args.get(0)?;
                let tz: String = args.get(1)?;
                let tz: Tz = tz
                    .parse()
                    .map_err(|e| rusqlite::Error::UserFunctionError(Box::new(e)))?;
                let date = tz
                    .timestamp_millis_opt(ms)
                    .single()
                    .ok_or_else(|| rusqlite::Error::UserFunctionError("invalid timestamp".into()))?
                    .date_naive();
                Ok(date.to_string())
            },
        )?;
        self.conn.execute_batch("CREATE TEMP TABLE IF NOT EXISTS query_labels(model_key TEXT,label TEXT,n INTEGER NOT NULL,PRIMARY KEY(model_key,label)) WITHOUT ROWID; DELETE FROM query_labels; CREATE TEMP TABLE IF NOT EXISTS query_sources(id TEXT PRIMARY KEY) WITHOUT ROWID; CREATE TEMP TABLE IF NOT EXISTS query_models(id TEXT PRIMARY KEY) WITHOUT ROWID; DELETE FROM query_sources; DELETE FROM query_models;")?;
        let started = Instant::now();
        self.conn
            .progress_handler(1000, Some(move || started.elapsed().as_secs_f64() > 5.0))?;
        let guard = SnapshotGuard { conn: self.conn };
        if let Some((sources, models)) = selectors {
            self.load_selectors(sources, models)?;
        }
        // Selector decoding and TEMP writes do not hold the published ledger snapshot open.
        self.conn.execute_batch("BEGIN DEFERRED")?;
        let mut stamp=self.conn.query_row("SELECT ledger_uuid,epoch,commit_seq,report_timezone,derived_version FROM ledger_meta WHERE singleton=1",[],|r|Ok(Stamp{ledger_uuid:r.get(0)?,epoch:r.get(1)?,commit_seq:r.get::<_,i64>(2)?.to_string(),report_timezone:r.get(3)?,derived_version:r.get::<_,i64>(4)?.to_string(),caliber_version:1,legacy_generation:None}))?;
        let routing_hash = Self::routing_hash(self.conn)?;
        if let Some(l) = self.legacy {
            if !l.routing_hash.is_empty() && l.routing_hash != routing_hash {
                return Err(QueryError::new(
                    "BUSY",
                    "source routing changed during refresh",
                ));
            }
            stamp.legacy_generation = Some(l.generation.clone());
        }
        let tz = stamp
            .report_timezone
            .parse()
            .map_err(|_| QueryError::new("LEDGER_UNREADABLE", "invalid stored report timezone"))?;
        let ctx = Context {
            stamp,
            tz,
            asof: Utc::now().timestamp_millis(),
            routing_hash,
            started,
            warnings: BTreeSet::new(),
            incomplete: BTreeSet::new(),
            excluded_days: std::cell::Cell::new(false),
        };
        Ok((guard, ctx))
    }
    fn load_selectors(&self, sources: &mut Selector, models: &mut Selector) -> QueryResult<()> {
        sources.normalize(128, 256)?;
        models.normalize(10000, 1400)?;
        let mut insert = self
            .conn
            .prepare("INSERT INTO query_sources(id) VALUES(?)")?;
        for id in sources.ids() {
            insert.execute([id])?;
        }
        let mut insert = self
            .conn
            .prepare("INSERT INTO query_models(id) VALUES(?)")?;
        for id in models.ids() {
            let key = decode_model(id)?;
            insert.execute([key])?;
        }
        Ok(())
    }
    fn selector_warnings(
        &self,
        sources: &Selector,
        models: &Selector,
        ctx: &mut Context,
    ) -> QueryResult<()> {
        for id in sources.ids() {
            if !crate::SOURCE_CATALOG.iter().any(|s| s.id == id) {
                ctx.warnings.insert(format!("UNKNOWN_SOURCE:{id}"));
            }
        }
        if !models.ids().is_empty() {
            let mut st=self.conn.prepare("SELECT s.id FROM temp.query_models s WHERE NOT EXISTS(SELECT 1 FROM usage_events e WHERE e.state='active' AND e.fact_kind='request' AND e.model_key=s.id) AND NOT EXISTS(SELECT 1 FROM usage_events e WHERE e.state='active' AND e.fact_kind='day' AND e.model_key=s.id) AND NOT EXISTS(SELECT 1 FROM usage_events e WHERE e.state='active' AND e.fact_kind='session_total' AND e.model_key=s.id)")?;
            let mut unknown: BTreeSet<String> = st
                .query_map([], |r| r.get(0))?
                .collect::<rusqlite::Result<_>>()?;
            if let Some(l) = self.legacy {
                for e in &l.entries {
                    let key = match e {
                        LegacyEntry::Request { record, .. }
                        | LegacyEntry::SessionTotal { record, .. } => model_key(&record.model),
                        LegacyEntry::Day(d) => d.model_key.clone(),
                    };
                    unknown.remove(&key);
                }
            }
            if !unknown.is_empty() {
                ctx.warnings
                    .insert(format!("UNKNOWN_MODEL_IDS:{}", unknown.len()));
                if matches!(models, Selector::Include { .. }) && unknown.len() == models.ids().len()
                {
                    ctx.warnings.insert("ALL_SELECTED_MODELS_UNKNOWN".into());
                }
            }
        }
        Ok(())
    }
    fn normalize_query(
        &self,
        q: &mut Query,
        ctx: &mut Context,
        cursor: Option<&Cursor>,
    ) -> QueryResult<()> {
        if q.report_timezone != ctx.stamp.report_timezone {
            return Err(QueryError::new(
                "TIMEZONE_CHANGED",
                "query timezone differs from ledger timezone",
            ));
        }
        self.selector_warnings(&q.sources, &q.models, ctx)?;
        ctx.asof = if let Some(ms) = &q.as_of_ms {
            parse_ms(ms)?
        } else if let Some(c) = cursor {
            parse_ms(&c.asof_ms)?
        } else {
            ctx.asof
        };
        if let Some(c) = cursor {
            if c.asof_ms != ctx.asof.to_string() {
                return Err(QueryError::new("CURSOR_STALE", "cursor as-of changed"));
            }
        }
        q.as_of_ms = Some(ctx.asof.to_string());
        bounds(&q.window, ctx.tz)?;
        Ok(())
    }
    fn condition(selector: &Selector, column: &str, table: &str) -> String {
        match selector {
            Selector::All => "1".into(),
            Selector::Include { .. } => {
                format!("EXISTS(SELECT 1 FROM temp.{table} s WHERE s.id={column})")
            }
            Selector::Exclude { .. } => {
                format!("NOT EXISTS(SELECT 1 FROM temp.{table} s WHERE s.id={column})")
            }
        }
    }
    fn source_routes(&self) -> QueryResult<BTreeMap<String, String>> {
        let mut st = self
            .conn
            .prepare("SELECT report_source,MIN(route) FROM source_roots GROUP BY report_source")?;
        let rows = st
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<rusqlite::Result<_>>()?;
        Ok(rows)
    }
    fn visit<F>(
        &self,
        q: &Query,
        ctx: &Context,
        summaries: bool,
        mut callback: F,
    ) -> QueryResult<()>
    where
        F: FnMut(Fact, bool) -> QueryResult<()>,
    {
        let (start, end) = bounds(&q.window, ctx.tz)?;
        let end = end.min(ctx.asof.checked_add(1).unwrap_or(i64::MAX));
        let sc = Self::condition(&q.sources, "e.report_source", "query_sources");
        let mc = Self::condition(&q.models, "e.model_key", "query_models");
        let filter = format!("({sc}) AND ({mc})");
        let sql=format!("SELECT {FACT_COLUMNS} FROM usage_events e JOIN usage_observations o ON o.observation_id=e.selected_observation_id WHERE e.state='active' AND e.fact_kind='request' AND e.occurred_ms>=?1 AND e.occurred_ms<?2 AND {filter} AND EXISTS(SELECT 1 FROM source_roots rt WHERE rt.report_source=e.report_source AND rt.route='ledger') ORDER BY e.occurred_ms");
        let mut seen = 0usize;
        let full = if summaries {
            self.complete_span(start, end, ctx)?
        } else {
            None
        };
        let ranges = match full {
            Some((a, b)) => vec![(start, end.min(a)), (start.max(b), end)],
            None => vec![(start, end)],
        };
        for (lo, hi) in ranges {
            if lo >= hi {
                continue;
            }
            let mut st = self.conn.prepare(&sql)?;
            let mut rows = st.query(params![lo, hi])?;
            while let Some(r) = rows.next()? {
                let f = Fact::from_row(r)?;
                callback(f, true)?;
                seen += 1;
                if seen % 512 == 0 {
                    ctx.budget()?;
                }
            }
        }
        if let Some((a, b)) = full {
            let sc = Self::condition(&q.sources, "r.report_source", "query_sources");
            let mc = Self::condition(&q.models, "r.model_key", "query_models");
            let filter = format!("({sc}) AND ({mc})");
            let columns = super::types::METRIC_NAMES
                .iter()
                .flat_map(|m| {
                    [
                        format!("r.{m}_known_sum"),
                        format!("r.{m}_known_count"),
                        format!("r.{m}_exact_count"),
                    ]
                })
                .collect::<Vec<_>>()
                .join(",");
            let sql=format!("SELECT r.bucket_start_ms,r.bucket_end_ms,r.local_day,r.report_source,r.model_key,r.event_count,{columns} FROM usage_rollups r WHERE r.derived_version=?1 AND r.grain='day' AND r.bucket_start_ms>=?2 AND r.bucket_end_ms<=?3 AND {filter} AND EXISTS(SELECT 1 FROM source_roots rt WHERE rt.report_source=r.report_source AND rt.route='ledger') ORDER BY r.bucket_start_ms");
            let mut st = self.conn.prepare(&sql)?;
            let mut rows = st.query(params![
                ctx.stamp
                    .derived_version
                    .parse::<i64>()
                    .map_err(|_| QueryError::invalid("invalid derived version"))?,
                a,
                b
            ])?;
            while let Some(r) = rows.next()? {
                let count: i64 = r.get(5)?;
                let mut cells: [Cell; 10] = std::array::from_fn(|_| Cell::default());
                for (i, c) in cells.iter_mut().enumerate() {
                    *c = Cell {
                        sum: r.get(6 + i * 3)?,
                        known: r.get(7 + i * 3)?,
                        exact: r.get(8 + i * 3)?,
                        count,
                    };
                }
                let model: String = r.get(4)?;
                callback(
                    Fact {
                        id: 0,
                        family: String::new(),
                        namespace: String::new(),
                        key: String::new(),
                        kind: "request".into(),
                        source: r.get(3)?,
                        native: String::new(),
                        model,
                        session: None,
                        time: r.get(0)?,
                        date: r.get(2)?,
                        timezone: Some(ctx.stamp.report_timezone.clone()),
                        start: r.get(0)?,
                        end: r.get(1)?,
                        metrics: [None; 10],
                        cost_state: "unknown".into(),
                        cost_units: None,
                        cost_scale: None,
                        currency: None,
                        live: false,
                        known_fields: 1023,
                        summary: Some(cells),
                    },
                    true,
                )?;
                ctx.budget()?;
            }
        }
        // Daily evidence uses its real interval. Native coverage ignores user selectors,
        // but uses the same live/ledger source route as the report.
        let routes = self.source_routes()?;
        let sql=format!("SELECT {FACT_COLUMNS} FROM usage_events e JOIN usage_observations o ON o.observation_id=e.selected_observation_id WHERE e.state='active' AND e.fact_kind='day' AND e.day_end_ms>?1 AND e.day_start_ms<?2 AND {filter} AND EXISTS(SELECT 1 FROM source_roots rt WHERE rt.report_source=e.report_source AND rt.route='ledger')");
        let mut st = self.conn.prepare(&sql)?;
        let mut rows = st.query(params![start, end])?;
        while let Some(r) = rows.next()? {
            let f = Fact::from_row(r)?;
            if self.day_covered(&f, &routes)? {
                continue;
            }
            let usable = self.day_usable(&f, start, end, ctx)?;
            if !usable {
                ctx.excluded_days.set(true);
            }
            callback(f, usable)?;
            ctx.budget()?;
        }
        if let Some(legacy) = self.legacy {
            for e in &legacy.entries {
                let f = legacy_fact(e)?;
                if routes.get(&f.source).is_some_and(|r| r == "ledger") {
                    continue;
                }
                let selected_s = q.sources.allows(&f.source);
                let selected_m = q.models.allows(&model_id(&f.model));
                if !(selected_s && selected_m) {
                    continue;
                }
                if f.kind == "request" {
                    if f.time.is_some_and(|t| t >= start && t < end) {
                        callback(f, true)?;
                    }
                } else if f.kind == "day" {
                    if self.day_covered(&f, &routes)? {
                        continue;
                    }
                    let relevant = match (f.start, f.end) {
                        (Some(s), Some(e)) => e > start && s < end,
                        _ => true,
                    };
                    if relevant {
                        let usable = self.day_usable(&f, start, end, ctx)?;
                        if !usable {
                            ctx.excluded_days.set(true);
                        }
                        callback(f, usable)?;
                    }
                }
                seen += 1;
                if seen % 512 == 0 {
                    ctx.budget()?;
                }
            }
        }
        Ok(())
    }
    fn day_usable(&self, f: &Fact, start: i64, end: i64, ctx: &Context) -> QueryResult<bool> {
        let (Some(s), Some(e)) = (f.start, f.end) else {
            return Ok(false);
        };
        if (f.live && f.namespace.is_empty())
            || f.timezone.is_none()
            || s < start
            || e > end
            || ctx.asof.checked_add(1).unwrap_or(i64::MAX) < e
        {
            return Ok(false);
        }
        let d = local_date(ctx.tz, s)?;
        let next = d
            .succ_opt()
            .ok_or_else(|| QueryError::invalid("day overflow"))?;
        Ok(midnight(ctx.tz, d)? == s && midnight(ctx.tz, next)? == e)
    }
    fn day_covered(&self, f: &Fact, routes: &BTreeMap<String, String>) -> QueryResult<bool> {
        let (Some(start), Some(end)) = (f.start, f.end) else {
            return Ok(false);
        };
        if f.namespace.is_empty() {
            return Ok(false);
        }
        let native:bool=self.conn.query_row("SELECT EXISTS(SELECT 1 FROM usage_events e WHERE e.state='active' AND e.fact_kind='request' AND e.native_tool=? AND e.namespace_key=? AND e.occurred_ms>=? AND e.occurred_ms<? AND EXISTS(SELECT 1 FROM source_roots r WHERE r.report_source=e.report_source AND r.route='ledger'))",params![f.native,f.namespace,start,end],|r|r.get(0))?;
        if native {
            return Ok(true);
        }
        // A source rule protects that provider. Cross-provider backfill is protected only
        // by a proved native/account coverage rule, never by a coincidental native_tool.
        let suppressed:bool=self.conn.query_row("SELECT EXISTS(SELECT 1 FROM suppression_rules WHERE native_tool=?1 AND ((kind='source' AND report_source=?5) OR (namespace_key=?2 AND(kind='coverage_family' OR (kind='coverage_interval' AND start_ms<?4 AND end_ms>?3)))))",params![f.native,f.namespace,start,end,f.source],|r|r.get(0))?;
        if suppressed {
            return Ok(true);
        }
        if let Some(l) = self.legacy {
            for e in &l.entries {
                if let LegacyEntry::Request {
                    record,
                    native_tool,
                    namespace,
                    ..
                } = e
                {
                    if !routes.get(record.source).is_some_and(|r| r == "ledger")
                        && native_tool == &f.native
                        && namespace.as_deref() == Some(f.namespace.as_str())
                        && record.ts_ms >= start
                        && record.ts_ms < end
                    {
                        return Ok(true);
                    }
                }
            }
        }
        Ok(false)
    }
    pub fn status(&self) -> QueryResult<Status> {
        let (_guard, ctx) = self.snapshot(None)?;
        self.status_inner(&ctx)
    }
    fn status_inner(&self, ctx: &Context) -> QueryResult<Status> {
        let mut sources = Vec::new();
        let mut last_saved = None::<i64>;
        let routes = self.source_routes()?;
        let mut saved_counts = BTreeMap::<String, (i64, Option<i64>)>::new();
        let mut st=self.conn.prepare("SELECT report_source,COUNT(*),MAX(observed_ms) FROM usage_observations GROUP BY report_source")?;
        let mut rows = st.query([])?;
        while let Some(r) = rows.next()? {
            saved_counts.insert(r.get(0)?, (r.get(1)?, r.get(2)?));
        }
        drop(rows);
        drop(st);
        let mut unresolved = BTreeMap::<String, (i64, i64)>::new();
        let mut st=self.conn.prepare("SELECT report_source,SUM(has_conflict),COUNT(*) FROM(SELECT DISTINCT o.report_source,e.identity_family,e.namespace_key,e.identity_key,e.has_conflict FROM usage_events e JOIN usage_observations o ON o.identity_family=e.identity_family AND o.namespace_key=e.namespace_key AND o.identity_key=e.identity_key WHERE e.state='unresolved') GROUP BY report_source")?;
        let mut rows = st.query([])?;
        while let Some(r) = rows.next()? {
            unresolved.insert(r.get(0)?, (r.get(1)?, r.get(2)?));
        }
        drop(rows);
        drop(st);
        for info in crate::SOURCE_CATALOG {
            if routes.get(info.id).is_some_and(|r| r == "ledger") {
                let(root_count,enabled,root_errors,unenumerated,retry,error):(i64,i64,i64,i64,Option<i64>,Option<String>)=self.conn.query_row("SELECT COUNT(*),COALESCE(SUM(enabled),0),COALESCE(SUM(error_code IS NOT NULL),0),COALESCE(SUM(enumeration_done=0),0),MIN(retry_after_ms),MAX(error_code) FROM source_roots WHERE report_source=?",[info.id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?)))?;
                let(isolated,incomplete,blocked,objretry,audit,objerror):(i64,i64,i64,Option<i64>,Option<i64>,Option<String>)=self.conn.query_row("SELECT COALESCE(SUM(o.state IN('missing','error','replay_required')),0),COALESCE(SUM(o.target_done=0),0),COALESCE(SUM(o.error_code IN('IdentityLinkNeedsReview','IDENTITY_LINK_NEEDS_REVIEW')),0),MIN(o.retry_after_ms),MIN(o.audit_completed_ms),MAX(o.error_code) FROM source_objects o JOIN source_roots r USING(root_id) WHERE r.report_source=?",[info.id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?)))?;
                let (saved, saved_at) = saved_counts.get(info.id).copied().unwrap_or((0, None));
                let (conflict, pending) = unresolved.get(info.id).copied().unwrap_or((0, 0));
                let(publishing,last_pub):(bool,Option<i64>)=self.conn.query_row("SELECT EXISTS(SELECT 1 FROM ingest_runs u JOIN source_roots r ON r.identity_family=u.identity_family AND r.namespace_key=u.namespace_key WHERE r.report_source=?1 AND u.state='publishing'),(SELECT MAX(u.finished_ms) FROM ingest_runs u JOIN source_roots r ON r.identity_family=u.identity_family AND r.namespace_key=u.namespace_key WHERE r.report_source=?1)",[info.id],|r|Ok((r.get(0)?,r.get(1)?)))?;
                let retry = retry.into_iter().chain(objretry).min();
                last_saved = last_saved.into_iter().chain(saved_at).max();
                let overdue:bool=self.conn.query_row("SELECT EXISTS(SELECT 1 FROM source_objects o JOIN source_roots r USING(root_id) WHERE r.report_source=?1 AND o.audit_due_ms<?2)",params![info.id,ctx.asof],|r|r.get(0))?;
                let state = if root_count > 0 && enabled == 0 {
                    "paused"
                } else if root_errors + isolated > 0 {
                    "error"
                } else if unenumerated + incomplete > 0 {
                    "collecting"
                } else {
                    "ready"
                };
                sources.push(json!({"id":info.id,"route":"ledger","state":state,"pending_identities":pending.to_string(),"conflicted_identities":conflict.to_string(),"saved_candidates":saved.to_string(),"identity_link_blocked_objects":blocked.to_string(),"source_readable":root_errors+isolated==0,"isolated_objects":isolated.to_string(),"failed_roots":root_errors.to_string(),"incomplete_objects":incomplete.to_string(),"next_retry_at_ms":retry.map(|n|n.to_string()),"last_published_at_ms":last_pub.map(|n|n.to_string()),"publishing":publishing,"audit_last_completed_ms":audit.map(|n|n.to_string()),"audit_overdue":overdue,"last_saved_at_ms":saved_at.map(|n|n.to_string()),"error":error.or(objerror)}));
            } else {
                let state = self.legacy.and_then(|l| l.sources.get(info.id));
                sources.push(json!({"id":info.id,"route":"live","state":if state.is_some_and(|s|s.error.as_deref()==Some("LOADING")){"collecting"}else if state.is_some_and(|s|s.stale){"stale"}else if state.is_some_and(|s|s.error.is_some()){"error"}else if state.is_some(){"ready"}else{"collecting"},"pending_identities":"0","conflicted_identities":"0","saved_candidates":"0","identity_link_blocked_objects":"0","source_readable":state.is_some_and(|s|s.error.is_none()),"isolated_objects":"0","failed_roots":"0","incomplete_objects":"0","next_retry_at_ms":null,"last_published_at_ms":null,"publishing":false,"audit_last_completed_ms":null,"audit_overdue":false,"last_saved_at_ms":state.and_then(|s|s.last_scanned_ms).map(|n|n.to_string()),"error":state.and_then(|s|s.error.clone())}));
            }
        }
        Ok(
            json!({"stamp":ctx.stamp,"ready":routes.values().any(|r|r=="ledger")||self.legacy.is_some_and(|l|l.sources.values().any(|s|s.last_scanned_ms.is_some())),"refreshing":sources.iter().any(|s|s["publishing"]==true||s["state"]=="collecting"),"last_saved_at_ms":last_saved.map(|n|n.to_string()),"sources":sources}),
        )
    }
    fn coverage(&self, selection: &Selector, ctx: &mut Context) -> QueryResult<()> {
        let routes = self.source_routes()?;
        for info in crate::SOURCE_CATALOG {
            if !selection.allows(info.id) {
                continue;
            }
            if !routes.get(info.id).is_some_and(|r| r == "ledger") {
                ctx.warnings.insert("LIVE_HISTORY_UNPROTECTED".into());
                let live = self.legacy.and_then(|l| l.sources.get(info.id));
                if live.is_none_or(|s| s.stale || s.error.is_some()) {
                    ctx.incomplete.insert(info.id.into());
                    ctx.warnings.insert("SOURCE_READ_GAP".into());
                }
            }
        }
        let mut st=self.conn.prepare("SELECT DISTINCT r.report_source FROM source_roots r WHERE r.route='ledger' AND(r.enabled=0 OR r.error_code IS NOT NULL OR r.enumeration_done=0 OR EXISTS(SELECT 1 FROM source_objects o WHERE o.root_id=r.root_id AND(o.state<>'ready' OR o.target_done=0)))")?;
        let mut rows = st.query([])?;
        while let Some(r) = rows.next()? {
            let id: String = r.get(0)?;
            if selection.allows(&id) {
                ctx.incomplete.insert(id);
                ctx.warnings.insert("SOURCE_READ_GAP".into());
            }
        }
        drop(rows);
        drop(st);
        // These paths use sparse partial indexes; a healthy ledger with millions of requests
        // does not visit any request facts to determine whether its evidence is complete.
        let mut st=self.conn.prepare("SELECT DISTINCT o.report_source,e.has_conflict FROM usage_events e JOIN usage_observations o ON o.identity_family=e.identity_family AND o.namespace_key=e.namespace_key AND o.identity_key=e.identity_key WHERE e.state='unresolved'")?;
        let mut rows = st.query([])?;
        while let Some(r) = rows.next()? {
            let id: String = r.get(0)?;
            if selection.allows(&id) {
                ctx.incomplete.insert(id);
                ctx.warnings.insert(
                    if r.get::<_, bool>(1)? {
                        "EVIDENCE_CONFLICT"
                    } else {
                        "IDENTITY_PENDING"
                    }
                    .into(),
                );
            }
        }
        drop(rows);
        drop(st);
        let mut st=self.conn.prepare("SELECT DISTINCT o.report_source FROM usage_observations o WHERE o.eligibility IN('identity_unknown','time_unknown','inconsistent') AND NOT EXISTS(SELECT 1 FROM usage_events e WHERE e.identity_family=o.identity_family AND e.namespace_key=o.namespace_key AND e.identity_key=COALESCE((SELECT a.canonical_key FROM identity_alias a WHERE a.identity_family=o.identity_family AND a.namespace_key=o.namespace_key AND a.alias_key=o.identity_key),o.identity_key) AND e.state<>'unresolved')")?;
        let mut rows = st.query([])?;
        while let Some(r) = rows.next()? {
            let id: String = r.get(0)?;
            if selection.allows(&id) {
                ctx.incomplete.insert(id);
                ctx.warnings.insert("IDENTITY_PENDING".into());
            }
        }
        drop(rows);
        drop(st);
        let mut st=self.conn.prepare("SELECT DISTINCT r.report_source FROM source_roots r WHERE r.route='ledger' AND EXISTS(SELECT 1 FROM usage_observations o WHERE o.identity_family=r.identity_family AND o.namespace_key=r.namespace_key AND o.observation_id>COALESCE((SELECT MAX(upper_observation_id) FROM ingest_runs u WHERE u.identity_family=r.identity_family AND u.namespace_key=r.namespace_key AND u.state='done'),0))")?;
        let rows = st.query_map([], |r| r.get::<_, String>(0))?;
        for id in rows {
            let id = id?;
            if selection.allows(&id) {
                ctx.incomplete.insert(id);
                ctx.warnings.insert("IDENTITY_PENDING".into());
            }
        }
        Ok(())
    }
    fn validate_cursor(
        &self,
        c: &Cursor,
        ctx: &Context,
        hash: &str,
        kind: &str,
        dimension: &str,
        limit: usize,
    ) -> QueryResult<()> {
        if c.v != 1
            || c.ledger_uuid != ctx.stamp.ledger_uuid
            || c.epoch != ctx.stamp.epoch
            || c.commit_seq != ctx.stamp.commit_seq
            || c.derived_version != ctx.stamp.derived_version
            || c.legacy_generation != ctx.stamp.legacy_generation
            || c.timezone != ctx.stamp.report_timezone
            || c.caliber != 1
            || c.query_hash != hash
            || c.kind != kind
            || c.dimension != dimension
            || c.limit != limit
            || c.routing_hash != ctx.routing_hash
        {
            return Err(QueryError::new(
                "CURSOR_STALE",
                "cursor no longer matches this query snapshot",
            ));
        }
        Ok(())
    }
    fn cursor(
        &self,
        ctx: &Context,
        hash: &str,
        kind: &str,
        dimension: &str,
        limit: usize,
        value: Option<String>,
        id: String,
    ) -> QueryResult<String> {
        Cursor {
            v: 1,
            ledger_uuid: ctx.stamp.ledger_uuid.clone(),
            epoch: ctx.stamp.epoch.clone(),
            commit_seq: ctx.stamp.commit_seq.clone(),
            derived_version: ctx.stamp.derived_version.clone(),
            legacy_generation: ctx.stamp.legacy_generation.clone(),
            timezone: ctx.stamp.report_timezone.clone(),
            caliber: 1,
            asof_ms: ctx.asof.to_string(),
            query_hash: hash.into(),
            kind: kind.into(),
            dimension: dimension.into(),
            limit,
            last_value: value,
            last_id: id,
            routing_hash: ctx.routing_hash.clone(),
        }
        .encode()
    }
}
fn legacy_fact(entry: &LegacyEntry) -> QueryResult<Fact> {
    match entry {
        LegacyEntry::Request {
            record,
            known_fields,
            ..
        }
        | LegacyEntry::SessionTotal {
            record,
            known_fields,
        } => {
            // Bind optional native/account metadata only on request records.
            let (native, ns) = match entry {
                LegacyEntry::Request {
                    native_tool,
                    namespace,
                    ..
                } => (native_tool.clone(), namespace.clone().unwrap_or_default()),
                _ => (record.source.into(), String::new()),
            };
            Ok(Fact {
                id: 0,
                family: record.source.into(),
                namespace: ns,
                key: record.dedup_key.clone(),
                kind: if matches!(entry, LegacyEntry::SessionTotal { .. }) {
                    "session_total"
                } else {
                    "request"
                }
                .into(),
                source: record.source.into(),
                native,
                model: model_key(&record.model),
                session: (!record.session_id.is_empty()).then(|| record.session_id.clone()),
                time: Some(record.ts_ms),
                date: None,
                timezone: None,
                start: None,
                end: None,
                metrics: legacy_metrics(record)?,
                cost_state: if legacy_money(record.cost_usd).is_some() {
                    "known"
                } else {
                    "unknown"
                }
                .into(),
                cost_units: legacy_money(record.cost_usd).map(|v| v.0),
                cost_scale: legacy_money(record.cost_usd).map(|v| v.1),
                currency: record.cost_usd.map(|_| "USD".into()),
                live: true,
                known_fields: *known_fields,
                summary: None,
            })
        }
        LegacyEntry::Day(d) => Ok(Fact {
            id: 0,
            family: d.report_source.clone(),
            namespace: d.namespace.clone().unwrap_or_default(),
            key: d.row_key.clone(),
            kind: "day".into(),
            source: d.report_source.clone(),
            native: d.native_tool.clone(),
            model: d.model_key.clone(),
            session: None,
            time: None,
            date: Some(d.native_date.clone()),
            timezone: d.timezone.clone(),
            start: d.start_ms,
            end: d.end_ms,
            metrics: d.projection,
            cost_state: "unknown".into(),
            cost_units: None,
            cost_scale: None,
            currency: None,
            live: true,
            known_fields: 1023,
            summary: None,
        }),
    }
}
impl QueryService<'_> {
    pub fn query(&self, mut request: QueryRequest) -> QueryResult<QueryResponse> {
        let mut day_source;
        let selectors = match &mut request {
            QueryRequest::Totals { query }
            | QueryRequest::Dashboard { query, .. }
            | QueryRequest::Table { query, .. }
            | QueryRequest::Day { query, .. } => (&mut query.sources, &mut query.models),
            QueryRequest::SourceDays { source, models, .. } => {
                day_source = Selector::Include {
                    ids: vec![source.clone()],
                };
                (&mut day_source, models)
            }
            QueryRequest::SessionTotals {
                sources, models, ..
            } => (sources, models),
        };
        let (_guard, mut ctx) = self.snapshot(Some(selectors))?;
        let payload = match &mut request {
            QueryRequest::Totals { query } => {
                self.normalize_query(query, &mut ctx, None)?;
                self.coverage(&query.sources, &mut ctx)?;
                let mut total = Aggregate::default();
                self.visit(query, &ctx, true, |fact, usable| {
                    if usable {
                        total.add_usage(&fact)
                    } else {
                        total.excluded = add(total.excluded, 1)?;
                        Ok(())
                    }
                })?;
                let mut data = total.bundle();
                self.decorate_bundle(&mut data, &ctx);
                // This bounded token summary deliberately does not compute
                // session cardinality or cost metadata. Omit those fields.
                data.as_object_mut().unwrap().remove("sessions");
                data.as_object_mut().unwrap().remove("costs");
                json!({"kind":"totals","data":data})
            }
            QueryRequest::Dashboard {
                query,
                series_unit,
                max_points,
                table_limit,
                comparison,
                trend,
            } => {
                let point_limit = if *series_unit == SeriesUnit::Day {
                    10_000
                } else {
                    2000
                };
                if !(1..=point_limit).contains(max_points) || !(1..=200).contains(table_limit) {
                    return Err(QueryError::limit("dashboard limits exceeded"));
                }
                self.normalize_query(query, &mut ctx, None)?;
                self.coverage(&query.sources, &mut ctx)?;
                let (total, groups) = self.aggregate(query, &ctx, query.group_by)?;

                let mut totals = total.bundle();
                self.decorate_bundle(&mut totals, &ctx);
                if total.excluded > 0 {
                    ctx.warnings.insert("DAY_EVIDENCE_EXCLUDED".into());
                }

                let source_table = self.table(
                    query,
                    Dimension::Source,
                    Scope::Facet,
                    &Page {
                        limit: *table_limit,
                        cursor: None,
                    },
                    &ctx,
                )?;

                let model_table = self.table(
                    query,
                    Dimension::Model,
                    Scope::Facet,
                    &Page {
                        limit: *table_limit,
                        cursor: None,
                    },
                    &ctx,
                )?;

                let distribution = self.distribution(&groups, query.metric, query.group_by)?;

                let extent = self.extent(query, &ctx)?;

                let series = self.series(query, &ctx, *series_unit, *max_points, &groups)?;

                let comparison = self.comparison(query, comparison.as_ref(), &ctx)?;
                let trend = match trend {
                    Some(t) => Some(self.trend(query, t, &ctx)?),
                    None => None,
                };
                let latest_day = match extent.1 {
                    Some(d) => {
                        let mut dq = query.clone();
                        dq.window = calendar_day(d)?;
                        Some(self.day(&dq, &d.to_string(), &ctx)?)
                    }
                    None => None,
                };

                let (identified, unidentified) = self.undated(query)?;

                json!({"kind":"dashboard","data":{"totals":totals,"series":series,"distribution":distribution,"source_table":source_table,"model_table":model_table,"comparison":comparison,"trend":trend,"latest_day":latest_day,"date_extent":{"first":extent.0.map(|d|d.to_string()),"last":extent.1.map(|d|d.to_string())},"undated":{"identified_count":identified.to_string(),"unidentified_candidate_count":unidentified.to_string()}}})
            }
            QueryRequest::Table {
                query,
                dimension,
                scope,
                page,
            } => {
                check_page(page)?;
                let c = page.cursor.as_deref().map(Cursor::decode).transpose()?;
                self.normalize_query(query, &mut ctx, c.as_ref())?;
                self.coverage(&query.sources, &mut ctx)?;
                json!({"kind":"table","data":self.table(query,*dimension,*scope,page,&ctx)?})
            }
            QueryRequest::Day { query, date } => {
                self.normalize_query(query, &mut ctx, None)?;
                let d = parse_date(date)?;
                if bounds(&query.window, ctx.tz)? != bounds(&calendar_day(d)?, ctx.tz)? {
                    return Err(QueryError::invalid(
                        "day query window must equal the requested local date",
                    ));
                }
                self.coverage(&query.sources, &mut ctx)?;
                json!({"kind":"day","data":self.day(query,date,&ctx)?})
            }
            QueryRequest::SourceDays {
                source,
                models,
                start_date,
                end_date_exclusive,
                page,
            } => {
                check_page(page)?;
                if source.len() > 256 {
                    return Err(QueryError::limit("source ID exceeds 256 bytes"));
                }
                let a = parse_date(start_date)?;
                let b = parse_date(end_date_exclusive)?;
                if a >= b {
                    return Err(QueryError::invalid("invalid native day range"));
                }
                let mut selected = Selector::Include {
                    ids: vec![source.clone()],
                };
                self.selector_warnings(&mut selected, models, &mut ctx)?;
                self.coverage(&selected, &mut ctx)?;
                if let Some(c) = page.cursor.as_deref().map(Cursor::decode).transpose()? {
                    ctx.asof = parse_ms(&c.asof_ms)?;
                }
                self.source_days(source, models, start_date, end_date_exclusive, page, &ctx)?
            }
            QueryRequest::SessionTotals {
                sources,
                models,
                page,
            } => {
                check_page(page)?;
                self.selector_warnings(sources, models, &mut ctx)?;
                self.coverage(sources, &mut ctx)?;
                if let Some(c) = page.cursor.as_deref().map(Cursor::decode).transpose()? {
                    ctx.asof = parse_ms(&c.asof_ms)?;
                }
                self.session_totals(sources, models, page, &ctx)?
            }
        };
        ctx.budget()?;
        if ctx.excluded_days.get() {
            ctx.warnings.insert("DAY_EVIDENCE_EXCLUDED".into());
        }
        let query_key = tuple_hash(&[
            "usagebook/query/v2",
            &serde_json::to_string(&request)
                .map_err(|_| QueryError::invalid("query serialization"))?,
            &serde_json::to_string(&ctx.stamp).unwrap_or_default(),
            &ctx.asof.to_string(),
        ]);
        let excluded = payload.to_string().contains("DAY_EVIDENCE_EXCLUDED")
            || ctx.warnings.contains("DAY_EVIDENCE_EXCLUDED");
        let response = QueryResponse {
            api_version: 2,
            stamp: ctx.stamp,
            as_of_ms: ctx.asof.to_string(),
            query_key,
            payload,
            source_coverage: if ctx.incomplete.is_empty() && !excluded {
                "complete"
            } else {
                "partial"
            }
            .into(),
            warnings: ctx.warnings.into_iter().collect(),
        };
        if serde_json::to_vec(&response)
            .map_err(|_| QueryError::invalid("response serialization"))?
            .len()
            > RESPONSE_LIMIT
        {
            return Err(QueryError::limit("response exceeds 8 MiB"));
        }
        Ok(response)
    }
    fn decorate_bundle(&self, b: &mut Value, ctx: &Context) {
        b["coverage"]["incomplete_sources"] = json!(ctx.incomplete);
    }
    fn aggregate_usage(
        &self,
        q: &Query,
        ctx: &Context,
        group: GroupBy,
    ) -> QueryResult<(Aggregate, BTreeMap<String, Aggregate>)> {
        let mut total = Aggregate::default();
        let mut groups = BTreeMap::new();
        self.visit(q, ctx, true, |f, usable| {
            if !usable {
                total.excluded = add(total.excluded, 1)?;
                return Ok(());
            }
            total.add(&f)?;
            let id = match group {
                GroupBy::Source => f.source.clone(),
                GroupBy::Model => model_id(&f.model),
                GroupBy::None => "__total__".into(),
            };
            if groups.len() >= GROUP_LIMIT && !groups.contains_key(&id) {
                return Err(QueryError::limit("too many groups"));
            }
            groups.entry(id).or_insert_with(Aggregate::default).add(&f)
        })?;

        Ok((total, groups))
    }
    fn aggregate(
        &self,
        q: &Query,
        ctx: &Context,
        group: GroupBy,
    ) -> QueryResult<(Aggregate, BTreeMap<String, Aggregate>)> {
        let (mut total, groups) = self.aggregate_usage(q, ctx, group)?;
        if let Some((count, missing)) = self.session_counts(q, ctx, None)?.get("__total__") {
            total.ledger_sessions = *count;
            total.session_unknown = add(total.session_unknown, *missing)?;
        }

        let mut single = BTreeMap::from([("__total__".to_string(), total)]);
        self.request_costs(q, ctx, None, &mut single)?;
        let mut total = single.remove("__total__").unwrap_or_default();

        total.session_costs = self.session_costs(q, ctx)?;

        Ok((total, groups))
    }
    fn table(
        &self,
        q: &Query,
        dimension: Dimension,
        scope: Scope,
        page: &Page,
        ctx: &Context,
    ) -> QueryResult<Value> {
        let mut actual = q.clone();
        if scope == Scope::Facet {
            match dimension {
                Dimension::Source => actual.sources = Selector::All,
                Dimension::Model => actual.models = Selector::All,
                Dimension::Day => {}
            }
        }
        let hash = table_hash(q, dimension, scope, page.limit)?;
        let cursor = page.cursor.as_deref().map(Cursor::decode).transpose()?;
        let dim = dimension_name(dimension);
        if let Some(c) = &cursor {
            self.validate_cursor(c, ctx, &hash, "table", dim, page.limit)?;
        }
        let mut groups = BTreeMap::<String, Aggregate>::new();
        // Preserve the existing model-table source column without collecting
        // request rows. Distinct (model, source) labels share the group budget.
        let mut model_sources = BTreeMap::<String, BTreeSet<String>>::new();
        let mut source_pairs = 0;
        let mut denominator = Cell::default();
        self.visit(&actual, ctx, true, |f, usable| {
            if !usable {
                return Ok(());
            }
            let id = match dimension {
                Dimension::Source => f.source.clone(),
                Dimension::Model => model_id(&f.model),
                Dimension::Day => local_date(
                    ctx.tz,
                    f.time
                        .or(f.start)
                        .ok_or_else(|| QueryError::invalid("undated formal fact"))?,
                )?
                .to_string(),
            };
            if groups.len() >= GROUP_LIMIT && !groups.contains_key(&id) {
                return Err(QueryError::limit("table group budget exceeded"));
            }
            if dimension == Dimension::Model {
                let sources = model_sources.entry(id.clone()).or_default();
                if sources.insert(f.source.clone()) {
                    source_pairs += 1;
                    if source_pairs > GROUP_LIMIT {
                        return Err(QueryError::limit("model source-label budget exceeded"));
                    }
                }
            }
            let entry = groups.entry(id).or_default();
            entry.add(&f)?;
            Ok(())
        })?;

        for a in groups.values() {
            denominator.merge(&a.cells[q.metric.index()])?;
        }
        let count = groups.len();
        let mut sorted: Vec<_> = groups.into_iter().collect();
        if dimension != Dimension::Day {
            sorted.sort_by(|a, b| compare_row(a, b, q.metric));
        }
        let mut chosen = Vec::new();
        for (id, a) in sorted {
            if let Some(c) = &cursor {
                let after = if dimension == Dimension::Day {
                    id > c.last_id
                } else {
                    after_key(&a.cells[q.metric.index()], &id, c)?
                };
                if !after {
                    continue;
                }
            }
            chosen.push((id, a));
            if chosen.len() > page.limit {
                break;
            }
        }
        let more = chosen.len() > page.limit;
        if more {
            chosen.pop();
        }
        let mut metadata = actual.clone();
        if dimension == Dimension::Day && !chosen.is_empty() {
            let original = bounds(&actual.window, ctx.tz)?;
            let first = parse_date(&chosen[0].0)?;
            let last = parse_date(&chosen[chosen.len() - 1].0)?
                .succ_opt()
                .ok_or_else(|| QueryError::invalid("date overflow"))?;
            metadata.window = Window::Instant {
                start_ms: original.0.max(midnight(ctx.tz, first)?).to_string(),
                end_ms_exclusive: original.1.min(midnight(ctx.tz, last)?).to_string(),
            };
        }
        let ids: Vec<_> = chosen.iter().map(|(id, _)| id.clone()).collect();
        let mut groups: BTreeMap<_, _> = chosen.into_iter().collect();
        if !groups.is_empty() {
            for (id, n) in self.session_counts(&metadata, ctx, Some(dimension))? {
                if let Some(a) = groups.get_mut(&id) {
                    a.ledger_sessions = n.0;
                    a.session_unknown = add(a.session_unknown, n.1)?;
                }
            }

            self.request_costs(&metadata, ctx, Some(dimension), &mut groups)?;
        }

        let selected = match dimension {
            Dimension::Source => &q.sources,
            Dimension::Model => &q.models,
            Dimension::Day => &Selector::All,
        };
        let mut rows = Vec::new();
        let mut last = None;
        for id in ids {
            let a = groups
                .remove(&id)
                .ok_or_else(|| QueryError::invalid("table page missing group"))?;
            let cell = &a.cells[q.metric.index()];
            let mut bundle = a.bundle();
            self.decorate_bundle(&mut bundle, ctx);
            let mut row = json!({"id":id,"label":if dimension==Dimension::Model{self.preferred_model_label(&id)?}else{if dimension==Dimension::Source{source_label(&id)}else{id.clone()}},"bundle":bundle,"selected":selected.allows(&id),"share":share(cell,&denominator)});
            if dimension == Dimension::Model {
                row["sources"] = json!(model_sources.remove(&id).unwrap_or_default());
            }
            rows.push(row);
            last = Some((cell.known.gt(&0).then(|| cell.sum.to_string()), id));
        }
        let next = if more {
            let (v, id) = last.ok_or_else(|| QueryError::invalid("empty cursor page"))?;
            Some(self.cursor(ctx, &hash, "table", dim, page.limit, v, id)?)
        } else {
            None
        };
        Ok(json!({"rows":rows,"next_cursor":next,"total_groups":count.to_string()}))
    }
    fn extent(
        &self,
        q: &Query,
        ctx: &Context,
    ) -> QueryResult<(Option<NaiveDate>, Option<NaiveDate>)> {
        let sc = Self::condition(&q.sources, "e.report_source", "query_sources");
        let mc = Self::condition(&q.models, "e.model_key", "query_models");
        let sql=format!("SELECT MIN(e.occurred_ms),MAX(e.occurred_ms) FROM usage_events e WHERE state='active' AND fact_kind='request' AND e.occurred_ms<=? AND {sc} AND {mc} AND EXISTS(SELECT 1 FROM source_roots r WHERE r.report_source=e.report_source AND r.route='ledger')");
        let (mut lo, mut hi): (Option<i64>, Option<i64>) =
            self.conn
                .query_row(&sql, [ctx.asof], |r| Ok((r.get(0)?, r.get(1)?)))?;
        let routes = self.source_routes()?;
        let (start, end) = bounds(&Window::All, ctx.tz)?;
        let day_sql = format!("SELECT {FACT_COLUMNS} FROM usage_events e JOIN usage_observations o ON o.observation_id=e.selected_observation_id WHERE e.state='active' AND e.fact_kind='day' AND {sc} AND {mc} AND EXISTS(SELECT 1 FROM source_roots r WHERE r.report_source=e.report_source AND r.route='ledger')");
        let mut st = self.conn.prepare(&day_sql)?;
        let mut rows = st.query([])?;
        while let Some(row) = rows.next()? {
            let fact = Fact::from_row(row)?;
            if self.day_usable(&fact, start, end, ctx)? && !self.day_covered(&fact, &routes)? {
                let timestamp = fact.start.unwrap();
                lo = Some(lo.map_or(timestamp, |value| value.min(timestamp)));
                hi = Some(hi.map_or(timestamp, |value| value.max(timestamp)));
            }
            ctx.budget()?;
        }
        if let Some(l) = self.legacy {
            for e in &l.entries {
                if let LegacyEntry::Request { record, .. } = e {
                    if record.ts_ms <= ctx.asof
                        && q.sources.allows(record.source)
                        && q.models.allows(&model_id(&model_key(&record.model)))
                        && !routes.get(record.source).is_some_and(|r| r == "ledger")
                    {
                        lo = Some(lo.map_or(record.ts_ms, |v| v.min(record.ts_ms)));
                        hi = Some(hi.map_or(record.ts_ms, |v| v.max(record.ts_ms)));
                    }
                } else if let LegacyEntry::Day(day) = e {
                    if q.sources.allows(&day.report_source)
                        && q.models.allows(&model_id(&day.model_key))
                        && !routes
                            .get(&day.report_source)
                            .is_some_and(|route| route == "ledger")
                    {
                        let fact = legacy_fact(e)?;
                        if self.day_usable(&fact, start, end, ctx)?
                            && !self.day_covered(&fact, &routes)?
                        {
                            let timestamp = fact.start.unwrap();
                            lo = Some(lo.map_or(timestamp, |value| value.min(timestamp)));
                            hi = Some(hi.map_or(timestamp, |value| value.max(timestamp)));
                        }
                    }
                }
                ctx.budget()?;
            }
        }
        Ok((
            lo.map(|n| local_date(ctx.tz, n)).transpose()?,
            hi.map(|n| local_date(ctx.tz, n)).transpose()?,
        ))
    }
    fn undated(&self, q: &Query) -> QueryResult<(i64, i64)> {
        let sc = Self::condition(&q.sources, "o.report_source", "query_sources");
        let mc = Self::condition(&q.models, "o.model_key", "query_models");
        let sql=format!("SELECT COUNT(DISTINCT CASE WHEN identity_key IS NOT NULL THEN identity_family||':'||namespace_key||':'||identity_key END),COALESCE(SUM(identity_key IS NULL),0) FROM usage_observations o WHERE eligibility IN('identity_unknown','time_unknown','inconsistent') AND eligibility<>'inconsistent' AND {sc} AND {mc}");
        Ok(self
            .conn
            .query_row(&sql, [], |r| Ok((r.get(0)?, r.get(1)?)))?)
    }
    fn comparison(
        &self,
        q: &Query,
        comp: Option<&Comparison>,
        ctx: &Context,
    ) -> QueryResult<Value> {
        let Some(comp) = comp else {
            return Ok(
                json!({"comparable":false,"reason":"NOT_REQUESTED","window":null,"totals":null}),
            );
        };
        let w = match comp {
            Comparison::Explicit { window } => window.clone(),
            Comparison::YesterdaySameLocalTime => {
                let now = ctx
                    .tz
                    .timestamp_millis_opt(ctx.asof)
                    .single()
                    .ok_or_else(|| QueryError::invalid("invalid asof"))?;
                let Some(previous) = now.date_naive().pred_opt() else {
                    return Err(QueryError::invalid("yesterday outside range"));
                };
                let target = previous.and_time(now.time());
                let Some(end) = ctx.tz.from_local_datetime(&target).earliest() else {
                    return Ok(
                        json!({"comparable":false,"reason":"LOCAL_TIME_NONEXISTENT","window":null,"totals":null}),
                    );
                };
                Window::Instant {
                    start_ms: midnight(ctx.tz, previous)?.to_string(),
                    end_ms_exclusive: end
                        .timestamp_millis()
                        .checked_add(1)
                        .ok_or_else(overflow)?
                        .to_string(),
                }
            }
        };
        let mut cq = q.clone();
        cq.window = w.clone();
        let (a, _) = self.aggregate(&cq, ctx, GroupBy::None)?;
        Ok(json!({"comparable":true,"reason":null,"window":w,"totals":a.bundle()}))
    }
    fn trend(&self, q: &Query, t: &Trend, ctx: &Context) -> QueryResult<Value> {
        if !(1..=120).contains(&t.periods) {
            return Err(QueryError::limit("trend periods must be 1..120"));
        }
        let anchor = parse_date(&t.anchor_date)?;
        let (first, end, unit) = match t.unit {
            TrendUnit::Week => {
                let last = anchor - Duration::days(anchor.weekday().num_days_from_monday() as i64);
                (
                    last - Duration::weeks(i64::from(t.periods) - 1),
                    last + Duration::weeks(1),
                    SeriesUnit::Week,
                )
            }
            TrendUnit::Month => {
                let last = NaiveDate::from_ymd_opt(anchor.year(), anchor.month(), 1)
                    .ok_or_else(|| QueryError::invalid("invalid month"))?;
                (
                    month_shift(last, 1 - i32::from(t.periods))?,
                    month_shift(last, 1)?,
                    SeriesUnit::Month,
                )
            }
        };
        let mut tq = q.clone();
        tq.window = Window::Calendar {
            start_date: first.to_string(),
            end_date_exclusive: end.to_string(),
        };
        let (_, groups) = self.aggregate_usage(&tq, ctx, tq.group_by)?;
        self.series(&tq, ctx, unit, 2000, &groups)
    }
}
fn table_hash(q: &Query, d: Dimension, s: Scope, limit: usize) -> QueryResult<String> {
    Ok(tuple_hash(&[
        "usagebook/table/v2",
        &serde_json::to_string(&(q, d, s, limit))
            .map_err(|_| QueryError::invalid("table parameters"))?,
    ]))
}
fn dimension_name(d: Dimension) -> &'static str {
    match d {
        Dimension::Source => "source",
        Dimension::Model => "model",
        Dimension::Day => "day",
    }
}
fn source_label(id: &str) -> String {
    crate::SOURCE_CATALOG
        .iter()
        .find(|s| s.id == id)
        .map(|s| s.label.to_string())
        .unwrap_or_else(|| id.into())
}
fn compare_row(a: &(String, Aggregate), b: &(String, Aggregate), m: Metric) -> std::cmp::Ordering {
    let ac = &a.1.cells[m.index()];
    let bc = &b.1.cells[m.index()];
    bc.known
        .gt(&0)
        .cmp(&ac.known.gt(&0))
        .then(bc.sum.cmp(&ac.sum))
        .then(a.0.cmp(&b.0))
}
fn after_key(cell: &Cell, id: &str, c: &Cursor) -> QueryResult<bool> {
    let prev = c.last_value.as_deref().map(parse_ms).transpose()?;
    Ok(match (prev, cell.known > 0) {
        (Some(v), true) => cell.sum < v || (cell.sum == v && id > c.last_id.as_str()),
        (Some(_), false) => true,
        (None, true) => false,
        (None, false) => id > c.last_id.as_str(),
    })
}
fn share(c: &Cell, total: &Cell) -> Value {
    let ratio = if total.sum > 0 && c.known > 0 {
        let scaled = i128::from(c.sum) * 1_000_000_000 / i128::from(total.sum);
        Some(format!(
            "{}.{:09}",
            scaled / 1_000_000_000,
            scaled % 1_000_000_000
        ))
    } else {
        None
    };
    json!({"basis":"known_subtotal","ratio_decimal":ratio,"complete":c.complete()&&total.complete()})
}
fn calendar_day(d: NaiveDate) -> QueryResult<Window> {
    let e = d
        .succ_opt()
        .ok_or_else(|| QueryError::invalid("day overflow"))?;
    Ok(Window::Calendar {
        start_date: d.to_string(),
        end_date_exclusive: e.to_string(),
    })
}
fn month_shift(d: NaiveDate, n: i32) -> QueryResult<NaiveDate> {
    let v = d.year() * 12 + d.month0() as i32 + n;
    NaiveDate::from_ymd_opt(v.div_euclid(12), v.rem_euclid(12) as u32 + 1, 1)
        .ok_or_else(|| QueryError::invalid("month outside supported range"))
}
impl QueryService<'_> {
    fn session_counts(
        &self,
        q: &Query,
        ctx: &Context,
        dim: Option<Dimension>,
    ) -> QueryResult<BTreeMap<String, (i64, i64)>> {
        let (start, end) = bounds(&q.window, ctx.tz)?;
        let end = end.min(ctx.asof.checked_add(1).unwrap_or(i64::MAX));
        let sc = Self::condition(&q.sources, "e.report_source", "query_sources");
        let mc = Self::condition(&q.models, "e.model_key", "query_models");
        let group = match dim {
            Some(Dimension::Source) => "e.report_source",
            Some(Dimension::Model) => "e.model_key",
            Some(Dimension::Day) => "usagebook_local_day(e.occurred_ms,?3)",
            None => "'__total__'",
        };
        let sql=format!("SELECT {group},COUNT(DISTINCT e.report_source||char(0)||e.identity_family||char(0)||e.namespace_key||char(0)||e.session_key),SUM(e.session_key IS NULL) FROM usage_events e WHERE e.state='active' AND e.fact_kind='request' AND e.occurred_ms>=?1 AND e.occurred_ms<?2 AND {sc} AND {mc} AND EXISTS(SELECT 1 FROM source_roots r WHERE r.report_source=e.report_source AND r.route='ledger') GROUP BY {group}");
        let mut st = self.conn.prepare(&sql)?;
        let mut rows = if dim == Some(Dimension::Day) {
            st.query(params![start, end, ctx.stamp.report_timezone])?
        } else {
            st.query(params![start, end])?
        };
        let mut out = BTreeMap::new();
        while let Some(r) = rows.next()? {
            let key: String = r.get(0)?;
            out.insert(
                if dim == Some(Dimension::Model) {
                    model_id(&key)
                } else {
                    key
                },
                (r.get(1)?, r.get(2)?),
            );
            ctx.budget()?;
        }
        Ok(out)
    }
    fn series_keys(
        &self,
        group: GroupBy,
        groups: &BTreeMap<String, Aggregate>,
        metric: Metric,
    ) -> QueryResult<Vec<(String, String)>> {
        let output = match group {
            GroupBy::None => {
                if groups.values().any(|group| group.cells[7].count > 0) {
                    vec![("__total__".into(), "Total".into())]
                } else {
                    Vec::new()
                }
            }
            GroupBy::Source => {
                let mut keys: Vec<_> = crate::SOURCE_CATALOG
                    .iter()
                    .filter(|source| {
                        groups
                            .get(source.id)
                            .is_some_and(|group| group.cells[7].count > 0)
                    })
                    .map(|s| (s.id.to_string(), s.label.to_string()))
                    .collect();
                for (id, group) in groups {
                    if group.cells[7].count > 0 && !keys.iter().any(|k| &k.0 == id) {
                        keys.push((id.clone(), id.clone()));
                    }
                }
                keys
            }
            GroupBy::Model => {
                let mut g: Vec<_> = groups
                    .iter()
                    .filter(|(_, group)| group.cells[7].count > 0)
                    .map(|(id, a)| (id.clone(), a.clone()))
                    .collect();
                g.sort_by(|a, b| compare_row(a, b, metric));
                let mut keys: Vec<_> = g
                    .iter()
                    .take(9)
                    .map(|(id, _)| Ok((id.clone(), self.preferred_model_label(id)?)))
                    .collect::<QueryResult<_>>()?;
                if g.len() > 9 {
                    keys.push(("__other__".into(), "Other".into()));
                }
                keys
            }
        };
        Ok(output)
    }
    fn series(
        &self,
        q: &Query,
        ctx: &Context,
        unit: SeriesUnit,
        max_points: usize,
        groups: &BTreeMap<String, Aggregate>,
    ) -> QueryResult<Value> {
        let keys = self.series_keys(q.group_by, groups, q.metric)?;
        if unit == SeriesUnit::Day {
            return self.recorded_day_series(q, ctx, max_points, keys);
        }
        let limit = max_points.min(20_000 / keys.len().max(1));
        if limit == 0 {
            return Err(QueryError::limit("series exceeds cell budget"));
        }
        let (first, last) = if q.window == Window::All {
            let e = self.extent(q, ctx)?;
            (
                e.0.unwrap_or(local_date(ctx.tz, ctx.asof)?),
                e.1.unwrap_or(local_date(ctx.tz, ctx.asof)?),
            )
        } else {
            let (a, b) = bounds(&q.window, ctx.tz)?;
            (
                local_date(ctx.tz, a)?,
                local_date(ctx.tz, b.checked_sub(1).ok_or_else(overflow)?)?,
            )
        };
        let mut actual = match unit {
            SeriesUnit::Auto | SeriesUnit::Day => "day",
            SeriesUnit::Week => "week",
            SeriesUnit::Month => "month",
        };
        let mut year_step = 1;
        let intervals = loop {
            let v = make_buckets(first, last, actual, year_step, ctx.tz, limit + 1)?;
            if v.len() <= limit {
                break v;
            }
            if unit != SeriesUnit::Auto {
                return Err(QueryError::limit(
                    "requested series granularity exceeds point/cell limit",
                ));
            }
            actual = match actual {
                "day" => "week",
                "week" => "month",
                _ => "year_group",
            };
            if actual == "year_group" {
                year_step = ((last.year() - first.year() + 1) as usize)
                    .div_ceil(limit)
                    .max(1) as i32;
            }
        };
        let mut cells = vec![vec![Cell::default(); keys.len()]; intervals.len()];
        let keymap: BTreeMap<_, _> = keys
            .iter()
            .enumerate()
            .map(|(i, k)| (k.0.as_str(), i))
            .collect();
        self.visit(q, ctx, true, |f, usable| {
            if !usable {
                return Ok(());
            }
            let ts = f
                .time
                .or(f.start)
                .ok_or_else(|| QueryError::invalid("undated series fact"))?;
            let idx = intervals
                .partition_point(|(s, _, _)| *s <= ts)
                .checked_sub(1);
            let Some(idx) = idx else {
                return Ok(());
            };
            if ts >= intervals[idx].1 {
                return Ok(());
            }
            let id = match q.group_by {
                GroupBy::Source => f.source.clone(),
                GroupBy::Model => model_id(&f.model),
                GroupBy::None => "__total__".into(),
            };
            let key = keymap
                .get(id.as_str())
                .copied()
                .or_else(|| keymap.get("__other__").copied());
            if let Some(key) = key {
                if let Some(summary) = &f.summary {
                    cells[idx][key].merge(&summary[q.metric.index()])?;
                    return Ok(());
                }
                let mut values =
                    evaluate(&f.metrics).map_err(|e| QueryError::new(e.code(), e.to_string()))?;
                if f.live
                    && f.known_fields & (1 << q.metric.index()) == 0
                    && values[q.metric.index()].known_value.is_some()
                {
                    values[q.metric.index()].state = ValueState::Partial;
                }
                cells[idx][key].add(&values[q.metric.index()])?;
            }
            Ok(())
        })?;
        let points:Vec<_>=intervals.iter().zip(cells).map(|((s,e,label),row)|json!({"start_ms":s.to_string(),"end_ms_exclusive":e.to_string(),"label":label,"values":row.iter().map(Cell::value).collect::<Vec<_>>()})).collect();
        Ok(
            json!({"unit":actual,"keys":keys.iter().map(|(id,label)|json!({"id":id,"label":label})).collect::<Vec<_>>(),"points":points,"compressed":unit==SeriesUnit::Auto&&actual!="day"}),
        )
    }
    fn recorded_day_series(
        &self,
        q: &Query,
        ctx: &Context,
        max_points: usize,
        keys: Vec<(String, String)>,
    ) -> QueryResult<Value> {
        // The original daily view lists recorded dates, including dates whose
        // selected metric is zero or unknown. Bound actual points/cells rather
        // than the calendar distance between the first and last record.
        const MAX_DAY_CELLS: usize = 100_000;
        let limit = max_points.min(MAX_DAY_CELLS / keys.len().max(1));
        if limit == 0 {
            return Err(QueryError::limit("daily series exceeds cell budget"));
        }
        let keymap: BTreeMap<_, _> = keys
            .iter()
            .enumerate()
            .map(|(index, key)| (key.0.as_str(), index))
            .collect();
        let mut days = BTreeMap::<NaiveDate, Vec<Cell>>::new();
        self.visit(q, ctx, true, |fact, usable| {
            if !usable
                || fact
                    .summary
                    .as_ref()
                    .is_some_and(|summary| summary[7].count == 0)
            {
                return Ok(());
            }
            let id = match q.group_by {
                GroupBy::Source => fact.source.clone(),
                GroupBy::Model => model_id(&fact.model),
                GroupBy::None => "__total__".into(),
            };
            let Some(key) = keymap
                .get(id.as_str())
                .copied()
                .or_else(|| keymap.get("__other__").copied())
            else {
                return Ok(());
            };
            let day = local_date(
                ctx.tz,
                fact.time
                    .or(fact.start)
                    .ok_or_else(|| QueryError::invalid("undated series fact"))?,
            )?;
            if days.len() >= limit && !days.contains_key(&day) {
                return Err(QueryError::limit(
                    "requested daily series exceeds point/cell limit",
                ));
            }
            let row = days
                .entry(day)
                .or_insert_with(|| vec![Cell::default(); keys.len()]);
            if let Some(summary) = &fact.summary {
                row[key].merge(&summary[q.metric.index()])?;
            } else {
                let mut values = evaluate(&fact.metrics)
                    .map_err(|e| QueryError::new(e.code(), e.to_string()))?;
                let value = &mut values[q.metric.index()];
                if fact.live
                    && fact.known_fields & (1 << q.metric.index()) == 0
                    && value.known_value.is_some()
                {
                    value.state = ValueState::Partial;
                }
                row[key].add(value)?;
            }
            Ok(())
        })?;
        let mut points = Vec::with_capacity(days.len());
        for (day, row) in days {
            let next = day
                .succ_opt()
                .ok_or_else(|| QueryError::invalid("day overflow"))?;
            points.push(json!({
                "start_ms":midnight(ctx.tz,day)?.to_string(),
                "end_ms_exclusive":midnight(ctx.tz,next)?.to_string(),
                "label":day.to_string(),
                "values":row.iter().map(Cell::value).collect::<Vec<_>>()
            }));
            ctx.budget()?;
        }
        Ok(json!({
            "unit":"day",
            "keys":keys.iter().map(|(id,label)|json!({"id":id,"label":label})).collect::<Vec<_>>(),
            "points":points,
            "compressed":false
        }))
    }
    fn day(&self, q: &Query, date: &str, ctx: &Context) -> QueryResult<Value> {
        let d = parse_date(date)?;
        let (start, end) = bounds(&calendar_day(d)?, ctx.tz)?;
        let (total, _) = self.aggregate(q, ctx, GroupBy::None)?;
        // Daily evidence contributes to the day total, but cannot rank or
        // displace request models in an hourly chart. Complete request days
        // can use their existing rollups for this small ranking pass.
        let mut request_groups = BTreeMap::<String, Aggregate>::new();
        self.visit(q, ctx, true, |fact, usable| {
            if !usable || fact.kind != "request" {
                return Ok(());
            }
            let id = match q.group_by {
                GroupBy::Source => fact.source.clone(),
                GroupBy::Model => model_id(&fact.model),
                GroupBy::None => "__total__".into(),
            };
            if request_groups.len() >= GROUP_LIMIT && !request_groups.contains_key(&id) {
                return Err(QueryError::limit("hourly model budget exceeded"));
            }
            request_groups.entry(id).or_default().add_usage(&fact)
        })?;
        let keys = self.series_keys(q.group_by, &request_groups, q.metric)?;
        if keys.len() * 24 > 20_000 {
            return Err(QueryError::limit("hour cell budget exceeded"));
        }
        // Sweep real UTC minutes. Repeated wall hours retain separate intervals; unusual half-hour
        // transitions split at their true boundary instead of assuming 24 physical hours.
        let mut intervals: Vec<Vec<(i64, i64)>> = vec![Vec::new(); 24];
        let mut t = start;
        while t < end {
            let dt = ctx
                .tz
                .timestamp_millis_opt(t)
                .single()
                .ok_or_else(|| QueryError::invalid("invalid day timestamp"))?;
            let hour = dt.hour() as usize;
            let next = (t + 60_000).min(end);
            let boundary = (dt.minute() == 0
                && dt.second() == 0
                && dt.timestamp_subsec_millis() == 0)
                || (t > start
                    && ctx
                        .tz
                        .timestamp_millis_opt(t - 1)
                        .single()
                        .is_some_and(|prev| prev.offset().to_string() != dt.offset().to_string()));
            if let Some(last) = intervals[hour].last_mut() {
                if last.1 == t && !boundary {
                    last.1 = next;
                } else {
                    intervals[hour].push((t, next));
                }
            } else {
                intervals[hour].push((t, next));
            }
            t = next;
        }
        let mut cells = vec![vec![Cell::default(); keys.len()]; 24];
        let mut requests = Aggregate::default();
        let mut daily = Aggregate::default();
        let keymap: BTreeMap<_, _> = keys
            .iter()
            .enumerate()
            .map(|(i, k)| (k.0.as_str(), i))
            .collect();
        self.visit(q, ctx, false, |f, usable| {
            if !usable {
                return Ok(());
            }
            if f.kind == "day" {
                daily.add(&f)?;
                return Ok(());
            }
            requests.add(&f)?;
            let ts = f
                .time
                .ok_or_else(|| QueryError::invalid("request missing timestamp"))?;
            let h = ctx
                .tz
                .timestamp_millis_opt(ts)
                .single()
                .ok_or_else(|| QueryError::invalid("bad timestamp"))?
                .hour() as usize;
            let id = match q.group_by {
                GroupBy::Source => f.source.clone(),
                GroupBy::Model => model_id(&f.model),
                GroupBy::None => "__total__".into(),
            };
            if let Some(k) = keymap
                .get(id.as_str())
                .copied()
                .or_else(|| keymap.get("__other__").copied())
            {
                let mut v =
                    evaluate(&f.metrics).map_err(|e| QueryError::new(e.code(), e.to_string()))?;
                if f.live
                    && f.known_fields & (1 << q.metric.index()) == 0
                    && v[q.metric.index()].known_value.is_some()
                {
                    v[q.metric.index()].state = ValueState::Partial;
                }
                cells[h][k].add(&v[q.metric.index()])?;
            }
            Ok(())
        })?;
        requests.ledger_sessions = total.ledger_sessions;
        let recorded: Vec<_> = (0..keys.len())
            .filter(|key| cells.iter().any(|hour| hour[*key].count > 0))
            .collect();
        let hours:Vec<_>=(0..24).map(|h|json!({"hour":h,"state":if intervals[h].is_empty(){"nonexistent"}else{"present"},"intervals":intervals[h].iter().map(|(s,e)|json!({"start_ms":s.to_string(),"end_ms_exclusive":e.to_string()})).collect::<Vec<_>>(),"values":recorded.iter().map(|key|cells[h][*key].value()).collect::<Vec<_>>()})).collect();
        Ok(
            json!({"date":date,"keys":recorded.iter().map(|index|{let(id,label)=&keys[*index];json!({"id":id,"label":label})}).collect::<Vec<_>>(),"hours":hours,"total":total.bundle(),"request_total":requests.bundle(),"day_only_total":daily.bundle()}),
        )
    }
}
fn make_buckets(
    first: NaiveDate,
    last: NaiveDate,
    unit: &str,
    years: i32,
    tz: Tz,
    cap: usize,
) -> QueryResult<Vec<(i64, i64, String)>> {
    let mut date = match unit {
        "week" => first - Duration::days(first.weekday().num_days_from_monday() as i64),
        "month" => NaiveDate::from_ymd_opt(first.year(), first.month(), 1).unwrap(),
        "year_group" => NaiveDate::from_ymd_opt(first.year(), 1, 1).unwrap(),
        _ => first,
    };
    let mut out = Vec::new();
    while date <= last && out.len() < cap {
        let next = match unit {
            "week" => date.checked_add_signed(Duration::days(7)),
            "month" => Some(month_shift(date, 1)?),
            "year_group" => NaiveDate::from_ymd_opt(date.year() + years, 1, 1),
            _ => date.succ_opt(),
        }
        .ok_or_else(|| QueryError::invalid("series date overflow"))?;
        out.push((midnight(tz, date)?, midnight(tz, next)?, date.to_string()));
        date = next;
    }
    Ok(out)
}
impl QueryService<'_> {
    pub fn catalog(&self, mut request: CatalogRequest) -> QueryResult<CatalogPage> {
        check_page(&request.page)?;
        if request.dimension == Dimension::Day {
            return Err(QueryError::invalid(
                "catalog dimension must be source or model",
            ));
        }
        if request.search.len() > 1024 {
            return Err(QueryError::limit("catalog search exceeds 1024 bytes"));
        }
        let (_guard, mut ctx) = self.snapshot(None)?;
        let c = request
            .page
            .cursor
            .as_deref()
            .map(Cursor::decode)
            .transpose()?;
        if let Some(c) = &c {
            ctx.asof = parse_ms(&c.asof_ms)?;
        }
        request.search = request.search.to_lowercase();
        let hash = tuple_hash(&[
            "usagebook/catalog/v2",
            dimension_name(request.dimension),
            &request.search,
            &request.page.limit.to_string(),
        ]);
        if let Some(c) = &c {
            self.validate_cursor(
                c,
                &ctx,
                &hash,
                "catalog",
                dimension_name(request.dimension),
                request.page.limit,
            )?;
        }
        let mut items = BTreeMap::<String, String>::new();
        let mut color_indices = BTreeMap::<String, usize>::new();
        if request.dimension == Dimension::Source {
            for id in self.catalog_usage_sources(&ctx)? {
                let label = source_label(&id);
                if id.to_lowercase().contains(&request.search)
                    || label.to_lowercase().contains(&request.search)
                {
                    items.insert(id, label);
                }
            }
        } else {
            self.populate_model_labels()?;
            let mut keys = BTreeMap::<String, String>::new();
            let mut st=self.conn.prepare("SELECT model_key,label,n FROM temp.query_labels ORDER BY model_key COLLATE BINARY,n DESC,label COLLATE BINARY")?;
            let mut rows = st.query([])?;
            while let Some(r) = rows.next()? {
                let k: String = r.get(0)?;
                let id = model_id(&k);
                let label: String = r.get(1)?;
                if keys.len() >= GROUP_LIMIT && !keys.contains_key(&id) {
                    return Err(QueryError::limit("catalog model budget exceeded"));
                }
                keys.entry(id.clone()).or_insert_with(|| {
                    if k.is_empty() {
                        "Unknown".into()
                    } else if label.is_empty() {
                        k.strip_prefix("n:").unwrap_or(&k).into()
                    } else {
                        label.clone()
                    }
                });
                if label.to_lowercase().contains(&request.search) || k.contains(&request.search) {
                    if items.len() >= GROUP_LIMIT && !items.contains_key(&id) {
                        return Err(QueryError::limit("catalog model budget exceeded"));
                    }
                    items.entry(id).or_insert_with(|| {
                        if label.is_empty() {
                            "Unknown".into()
                        } else {
                            label
                        }
                    });
                }
                ctx.budget()?;
            }
            // JS Array.sort() orders strings by UTF-16 code units. Rank the
            // complete preferred-display-name catalog before search and pagination;
            // base64 IDs and the loaded page must not change a model's color.
            let mut ordered: Vec<_> = keys.into_iter().collect();
            ordered.sort_by(|a, b| a.1.encode_utf16().cmp(b.1.encode_utf16()));
            for (index, (id, _)) in ordered.into_iter().enumerate() {
                color_indices.insert(id, index);
            }
        }
        let total = items.len();
        let mut out = Vec::new();
        let mut more = false;
        for (id, label) in items {
            if c.as_ref().is_some_and(|c| id <= c.last_id) {
                continue;
            }
            if out.len() >= request.page.limit {
                more = true;
                break;
            }
            let label = if request.dimension == Dimension::Model {
                self.preferred_model_label(&id)?
            } else {
                label
            };
            let mut item = json!({"id":id,"label":label,"color_key":id});
            if let Some(index) = color_indices.get(&id) {
                item["color_index"] = json!(index.to_string());
            }
            out.push(item);
        }
        let next = if more {
            Some(
                self.cursor(
                    &ctx,
                    &hash,
                    "catalog",
                    dimension_name(request.dimension),
                    request.page.limit,
                    None,
                    out.last()
                        .and_then(|v| v["id"].as_str())
                        .unwrap_or("")
                        .into(),
                )?,
            )
        } else {
            None
        };
        Ok(
            json!({"stamp":ctx.stamp,"items":out,"next_cursor":next,"total_count":total.to_string()}),
        )
    }
    fn catalog_usage_sources(&self, ctx: &Context) -> QueryResult<BTreeSet<String>> {
        let mut sources = BTreeSet::new();
        // One indexed existence probe per routed source, rather than visiting
        // every historical request just to discover its provider.
        let mut st = self.conn.prepare("SELECT DISTINCT r.report_source FROM source_roots r WHERE r.route='ledger' AND EXISTS(SELECT 1 FROM usage_events e WHERE e.report_source=r.report_source AND e.state='active' AND e.fact_kind='request' AND e.occurred_ms<=?1)")?;
        let mut rows = st.query([ctx.asof])?;
        while let Some(row) = rows.next()? {
            sources.insert(row.get::<_, String>(0)?);
            if sources.len() > GROUP_LIMIT {
                return Err(QueryError::limit("catalog source budget exceeded"));
            }
            ctx.budget()?;
        }
        let routes = self.source_routes()?;
        let (start, end) = bounds(&Window::All, ctx.tz)?;
        let sql = format!("SELECT {FACT_COLUMNS} FROM usage_events e JOIN usage_observations o ON o.observation_id=e.selected_observation_id WHERE e.state='active' AND e.fact_kind='day' AND EXISTS(SELECT 1 FROM source_roots r WHERE r.report_source=e.report_source AND r.route='ledger')");
        let mut st = self.conn.prepare(&sql)?;
        let mut rows = st.query([])?;
        while let Some(row) = rows.next()? {
            let fact = Fact::from_row(row)?;
            if !sources.contains(&fact.source)
                && self.day_usable(&fact, start, end, ctx)?
                && !self.day_covered(&fact, &routes)?
            {
                sources.insert(fact.source);
            }
            if sources.len() > GROUP_LIMIT {
                return Err(QueryError::limit("catalog source budget exceeded"));
            }
            ctx.budget()?;
        }
        if let Some(legacy) = self.legacy {
            for entry in &legacy.entries {
                let fact = legacy_fact(entry)?;
                if !routes
                    .get(&fact.source)
                    .is_some_and(|route| route == "ledger")
                    && !sources.contains(&fact.source)
                    && (fact.kind == "request" && fact.time.is_some_and(|time| time <= ctx.asof)
                        || fact.kind == "day"
                            && self.day_usable(&fact, start, end, ctx)?
                            && !self.day_covered(&fact, &routes)?)
                {
                    sources.insert(fact.source);
                }
                if sources.len() > GROUP_LIMIT {
                    return Err(QueryError::limit("catalog source budget exceeded"));
                }
                ctx.budget()?;
            }
        }
        Ok(sources)
    }
    fn source_days(
        &self,
        source: &str,
        models: &Selector,
        start: &str,
        end: &str,
        page: &Page,
        ctx: &Context,
    ) -> QueryResult<Value> {
        let hash = tuple_hash(&[
            "usagebook/source-days/v2",
            source,
            start,
            end,
            &serde_json::to_string(models).unwrap_or_default(),
            &page.limit.to_string(),
        ]);
        let c = page.cursor.as_deref().map(Cursor::decode).transpose()?;
        if let Some(c) = &c {
            self.validate_cursor(c, ctx, &hash, "source_days", "day", page.limit)?;
        }
        let mut out = BTreeMap::<String, Value>::new();
        let routes = self.source_routes()?;
        if routes.get(source).is_some_and(|r| r == "ledger") {
            let parts: Vec<_> = c
                .as_ref()
                .map(|c| c.last_id.split('\0').collect::<Vec<_>>())
                .unwrap_or_default();
            let last_date = parts.first().copied().unwrap_or("");
            let last_id = if parts.get(1) == Some(&"ledger") {
                parts
                    .get(2)
                    .and_then(|v| v.parse::<i64>().ok())
                    .unwrap_or(0)
            } else if !parts.is_empty() {
                i64::MAX
            } else {
                0
            };
            let mc = Self::condition(models, "o.model_key", "query_models");
            let cols = FACT_COLUMNS.replace(
                "e.identity_key",
                "COALESCE(e.identity_key,o.identity_key,'')",
            );
            let sql=format!("SELECT {cols},e.state,e.has_conflict,e.selected_observation_id FROM usage_observations o LEFT JOIN usage_events e ON e.identity_family=o.identity_family AND e.namespace_key=o.namespace_key AND e.identity_key=o.identity_key WHERE o.fact_kind='day' AND o.report_source=?1 AND o.native_date>=?2 AND o.native_date<?3 AND (o.native_date>?4 OR(o.native_date=?4 AND o.observation_id>?5)) AND {mc} AND NOT EXISTS(SELECT 1 FROM suppression_rules s WHERE (s.kind='source' AND s.report_source=o.report_source AND s.native_tool=o.native_tool) OR (s.kind='identity' AND s.identity_family=o.identity_family AND s.namespace_key=o.namespace_key AND s.identity_key=o.identity_key)) ORDER BY o.native_date,o.observation_id LIMIT ?6");
            let mut st = self.conn.prepare(&sql)?;
            let mut rows = st.query(params![
                source,
                start,
                end,
                last_date,
                last_id,
                (page.limit + 1) as i64
            ])?;
            while let Some(r) = rows.next()? {
                let f = Fact::from_row(r)?;
                let state: Option<String> = r.get(29)?;
                let conflict: Option<bool> = r.get(30)?;
                let selected: Option<i64> = r.get(31)?;
                let formal = state.as_deref() == Some("active") && selected == Some(f.id);
                let status = if formal {
                    "formal"
                } else if conflict == Some(true) {
                    "conflict"
                } else {
                    "pending"
                };
                let row_key = format!("{:020}", f.id);
                let key = format!("{}\0ledger\0{row_key}", f.date.as_deref().unwrap_or(""));
                let bundle = diagnostic_bundle(&f)?;
                out.insert(key,json!({"source":source,"native_tool":f.native,"model":model_id(&f.model),"row_key":row_key,"date":f.date,"timezone":f.timezone,"start_ms":f.start.map(|n|n.to_string()),"end_ms_exclusive":f.end.map(|n|n.to_string()),"status":status,"candidate_id":f.id.to_string(),"bundle":bundle,"selected":formal,"reason":if formal{None}else{Some(if status=="conflict"{"EVIDENCE_CONFLICT"}else{"DAY_NOT_PUBLISHED"})}}));
            }
        } else if let Some(l) = self.legacy {
            for e in &l.entries {
                let LegacyEntry::Day(d) = e else {
                    continue;
                };
                if d.report_source != source
                    || d.native_date.as_str() < start
                    || d.native_date.as_str() >= end
                    || !models.allows(&model_id(&d.model_key))
                {
                    continue;
                }
                let key = format!("{}\0live\0{}", d.native_date, d.row_key);
                if c.as_ref().is_some_and(|c| key <= c.last_id) {
                    continue;
                }
                let f = legacy_fact(e)?;
                let bundle = diagnostic_bundle(&f)?;
                out.insert(key,json!({"source":source,"native_tool":d.native_tool,"model":model_id(&d.model_key),"row_key":d.row_key,"date":d.native_date,"timezone":d.timezone,"start_ms":d.start_ms.map(|n|n.to_string()),"end_ms_exclusive":d.end_ms.map(|n|n.to_string()),"status":"pending","candidate_id":null,"bundle":bundle,"selected":false,"reason":"LEGACY_DAY_COVERAGE_UNKNOWN"}));
                if out.len() > page.limit + 1 {
                    out.pop_last();
                }
                ctx.budget()?;
            }
        }
        let more = out.len() > page.limit;
        if more {
            out.pop_last();
        }
        let next = if more {
            Some(
                self.cursor(
                    ctx,
                    &hash,
                    "source_days",
                    "day",
                    page.limit,
                    None,
                    out.last_key_value()
                        .map(|(k, _)| k.clone())
                        .unwrap_or_default(),
                )?,
            )
        } else {
            None
        };
        Ok(
            json!({"kind":"source_days","rows":out.into_values().collect::<Vec<_>>(),"next_cursor":next}),
        )
    }
    fn session_totals(
        &self,
        sources: &Selector,
        models: &Selector,
        page: &Page,
        ctx: &Context,
    ) -> QueryResult<Value> {
        let hash = tuple_hash(&[
            "usagebook/session-totals/v2",
            &serde_json::to_string(&(sources, models, page.limit)).unwrap_or_default(),
        ]);
        let c = page.cursor.as_deref().map(Cursor::decode).transpose()?;
        if let Some(c) = &c {
            self.validate_cursor(c, ctx, &hash, "session_totals", "session", page.limit)?;
        }
        let mut out = BTreeMap::<String, Value>::new();
        let parts: Vec<_> = c
            .as_ref()
            .map(|c| c.last_id.split('\0').collect::<Vec<_>>())
            .unwrap_or_default();
        if parts.first() != Some(&"live") {
            let family = parts.get(1).copied().unwrap_or("");
            let namespace = parts.get(2).copied().unwrap_or("");
            let key = parts.get(3).copied().unwrap_or("");
            let sc = Self::condition(sources, "e.report_source", "query_sources");
            let mc = Self::condition(models, "e.model_key", "query_models");
            let sql=format!("SELECT {FACT_COLUMNS} FROM usage_events e INDEXED BY events_session_totals JOIN usage_observations o ON o.observation_id=e.selected_observation_id WHERE e.state='active' AND e.fact_kind='session_total' AND(e.identity_family,e.namespace_key,e.identity_key)>(?1,?2,?3) AND {sc} AND {mc} AND EXISTS(SELECT 1 FROM source_roots r WHERE r.report_source=e.report_source AND r.route='ledger') ORDER BY e.identity_family,e.namespace_key,e.identity_key LIMIT ?4");
            let mut st = self.conn.prepare(&sql)?;
            let mut rows = st.query(params![family, namespace, key, (page.limit + 1) as i64])?;
            while let Some(r) = rows.next()? {
                let f = Fact::from_row(r)?;
                let key = format!("ledger\0{}\0{}\0{}", f.family, f.namespace, f.key);
                out.insert(key, session_row(&f)?);
                ctx.budget()?;
            }
        }
        let routes = self.source_routes()?;
        if let Some(l) = self.legacy {
            for e in &l.entries {
                if !matches!(e, LegacyEntry::SessionTotal { .. }) {
                    continue;
                }
                let f = legacy_fact(e)?;
                if routes.get(&f.source).is_some_and(|r| r == "ledger")
                    || !sources.allows(&f.source)
                    || !models.allows(&model_id(&f.model))
                {
                    continue;
                }
                let key = format!("live\0{}\0{}\0{}", f.family, f.namespace, f.key);
                if c.as_ref().is_some_and(|c| key <= c.last_id) {
                    continue;
                }
                out.insert(key, session_row(&f)?);
                if out.len() > page.limit + 1 {
                    out.pop_last();
                }
                ctx.budget()?;
            }
        }
        let more = out.len() > page.limit;
        if more {
            out.pop_last();
        }
        let next = if more {
            Some(
                self.cursor(
                    ctx,
                    &hash,
                    "session_totals",
                    "session",
                    page.limit,
                    None,
                    out.last_key_value()
                        .map(|(k, _)| k.clone())
                        .unwrap_or_default(),
                )?,
            )
        } else {
            None
        };
        Ok(
            json!({"kind":"session_totals","rows":out.into_values().collect::<Vec<_>>(),"next_cursor":next}),
        )
    }
}
fn session_row(f: &Fact) -> QueryResult<Value> {
    let mut a = Aggregate::default();
    a.add(f)?;
    if !f.live {
        a.ledger_sessions = i64::from(f.session.is_some());
    }
    let mut bundle = a.bundle();
    if let Some(costs) = bundle["costs"].as_array_mut() {
        for cost in costs {
            cost["kind"] = json!("reported_session");
            cost["scope"] = json!("whole_session");
            cost["reason"] = json!("UNALLOCATABLE_SESSION_COST");
        }
    }
    Ok(
        json!({"id":format!("{}:{}:{}",f.family,f.namespace,f.key),"source":f.source,"model":model_id(&f.model),"session_key":f.session,"bundle":bundle,"window_applied":false}),
    )
}
impl QueryService<'_> {
    fn complete_span(
        &self,
        start: i64,
        end: i64,
        ctx: &Context,
    ) -> QueryResult<Option<(i64, i64)>> {
        if start >= end {
            return Ok(None);
        }
        let d = local_date(ctx.tz, start)?;
        let midnight_start = midnight(ctx.tz, d)?;
        let a = if midnight_start == start {
            start
        } else {
            midnight(
                ctx.tz,
                d.succ_opt()
                    .ok_or_else(|| QueryError::invalid("date overflow"))?,
            )?
        };
        let b = midnight(ctx.tz, local_date(ctx.tz, end)?)?;
        if a >= b {
            return Ok(None);
        }
        // Production publication updates summaries atomically. The no-summary fallback also permits
        // importing pre-summary fixture databases without interpreting missing caches as zero.
        let available: bool = self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM usage_rollups WHERE derived_version=? AND grain='day')",
            [ctx.stamp
                .derived_version
                .parse::<i64>()
                .map_err(|_| QueryError::invalid("invalid derived version"))?],
            |r| r.get(0),
        )?;
        Ok(available.then_some((a, b)))
    }
    fn request_costs(
        &self,
        q: &Query,
        ctx: &Context,
        dim: Option<Dimension>,
        groups: &mut BTreeMap<String, Aggregate>,
    ) -> QueryResult<()> {
        let (start, end) = bounds(&q.window, ctx.tz)?;
        let end = end.min(ctx.asof.checked_add(1).unwrap_or(i64::MAX));
        let sc = Self::condition(&q.sources, "e.report_source", "query_sources");
        let mc = Self::condition(&q.models, "e.model_key", "query_models");
        let group = match dim {
            Some(Dimension::Source) => "e.report_source",
            Some(Dimension::Model) => "e.model_key",
            Some(Dimension::Day) => "usagebook_local_day(e.occurred_ms,?3)",
            None => "'__total__'",
        };
        let sql=format!("SELECT {group},o.cost_state,o.cost_currency,o.cost_scale,SUM(o.cost_units),COUNT(*) FROM usage_events e JOIN usage_observations o ON o.observation_id=e.selected_observation_id WHERE e.state='active' AND e.fact_kind='request' AND e.occurred_ms>=?1 AND e.occurred_ms<?2 AND {sc} AND {mc} AND EXISTS(SELECT 1 FROM source_roots r WHERE r.report_source=e.report_source AND r.route='ledger') GROUP BY {group},o.cost_state,o.cost_currency,o.cost_scale");
        let mut st = self.conn.prepare(&sql)?;
        let mut rows = if dim == Some(Dimension::Day) {
            st.query(params![start, end, ctx.stamp.report_timezone])?
        } else {
            st.query(params![start, end])?
        };
        while let Some(r) = rows.next()? {
            let key: String = r.get(0)?;
            let key = if dim == Some(Dimension::Model) {
                model_id(&key)
            } else {
                key
            };
            let Some(a) = groups.get_mut(&key) else {
                continue;
            };
            let state: String = r.get(1)?;
            let count: i64 = r.get(5)?;
            if state == "known" {
                let currency: String = r.get(2)?;
                let scale: u32 = r.get(3)?;
                let units: i64 = r.get(4)?;
                merge_money(&mut a.costs, currency, i128::from(units), scale, count)?;
            } else {
                a.cost_unknown = add(a.cost_unknown, count)?;
            }
            ctx.budget()?;
        }
        Ok(())
    }
    fn session_costs(&self, q: &Query, ctx: &Context) -> QueryResult<Vec<Value>> {
        let (start, end) = bounds(&q.window, ctx.tz)?;
        let end = end.min(ctx.asof.checked_add(1).unwrap_or(i64::MAX));
        let sc = Self::condition(&q.sources, "m.report_source", "query_sources");
        let mc = Self::condition(&q.models, "m.model_key", "query_models");
        let direct = Self::condition(&q.sources, "e.report_source", "query_sources");
        let sql=format!("SELECT o.cost_state,o.cost_currency,o.cost_scale,o.cost_units,NOT EXISTS(SELECT 1 FROM usage_events m WHERE m.state='active' AND m.fact_kind='request' AND m.identity_family=e.identity_family AND m.namespace_key=e.namespace_key AND m.session_key=e.session_key AND (m.occurred_ms<?1 OR m.occurred_ms>=?2 OR NOT({sc}) OR NOT({mc}))) FROM usage_events e JOIN usage_observations o ON o.observation_id=e.selected_observation_id WHERE e.state='active' AND e.fact_kind='session_cost' AND {direct} AND EXISTS(SELECT 1 FROM source_roots r WHERE r.report_source=e.report_source AND r.route='ledger') AND EXISTS(SELECT 1 FROM usage_events m WHERE m.state='active' AND m.fact_kind='request' AND m.identity_family=e.identity_family AND m.namespace_key=e.namespace_key AND m.session_key=e.session_key AND m.occurred_ms>=?1 AND m.occurred_ms<?2 AND {sc} AND {mc})");
        let mut st = self.conn.prepare(&sql)?;
        let mut rows = st.query(params![start, end])?;
        let mut sums = BTreeMap::<String, (i128, u32, i64)>::new();
        let mut unknown = BTreeSet::new();
        while let Some(r) = rows.next()? {
            let state: String = r.get(0)?;
            let currency: Option<String> = r.get(1)?;
            let scale: Option<u32> = r.get(2)?;
            let units: Option<i64> = r.get(3)?;
            let whole: bool = r.get(4)?;
            let scope = if whole && ctx.incomplete.is_empty() {
                "filtered"
            } else {
                "whole_session"
            };
            let currency = currency.unwrap_or_else(|| "USD".into());
            let key = format!("{scope}:{currency}");
            if state == "known" {
                merge_money(
                    &mut sums,
                    key,
                    i128::from(units.ok_or_else(|| {
                        QueryError::new("LEDGER_UNREADABLE", "invalid session cost")
                    })?),
                    scale.ok_or_else(|| {
                        QueryError::new("LEDGER_UNREADABLE", "invalid session cost")
                    })?,
                    1,
                )?;
            } else {
                unknown.insert(key);
            }
            if sums.len() + unknown.len() > 128 {
                return Err(QueryError::limit("cost currencies exceed output limit"));
            }
            ctx.budget()?;
        }
        let keys: BTreeSet<_> = sums.keys().chain(unknown.iter()).cloned().collect();
        let mut out = Vec::new();
        for key in keys {
            let (scope, currency) = key
                .split_once(':')
                .ok_or_else(|| QueryError::invalid("cost scope"))?;
            let value = sums.get(&key);
            out.push(json!({"kind":"reported_session","currency":currency,"amount":value.map(|v|decimal(v.0,v.1)),"state":if value.is_none(){"unknown"}else if unknown.contains(&key){"partial"}else{"exact"},"scope":scope,"reason":if scope=="whole_session"{Some("UNALLOCATABLE_SESSION_COST")}else{None},"pricing_version":null}));
        }
        Ok(out)
    }
}
fn merge_money(
    map: &mut BTreeMap<String, (i128, u32, i64)>,
    currency: String,
    units: i128,
    scale: u32,
    count: i64,
) -> QueryResult<()> {
    if scale > 18 {
        return Err(QueryError::new("LEDGER_UNREADABLE", "invalid money scale"));
    }
    let e = map.entry(currency).or_insert((0, scale, 0));
    let target = e.1.max(scale);
    e.0 =
        e.0.checked_mul(pow10(target - e.1)?)
            .and_then(|a| {
                units
                    .checked_mul(10_i128.pow(target - scale))
                    .and_then(|b| a.checked_add(b))
            })
            .ok_or_else(overflow)?;
    e.1 = target;
    e.2 = add(e.2, count)?;
    Ok(())
}
impl QueryService<'_> {
    fn preferred_model_label(&self, id: &str) -> QueryResult<String> {
        if id == "m1:u" {
            return Ok("Unknown".into());
        }
        if let Some(v) = self.model_labels.borrow().get(id) {
            return Ok(v.clone());
        }
        let key = decode_model(id)?;
        self.populate_model_labels()?;
        let label=self.conn.query_row("SELECT label FROM temp.query_labels WHERE model_key=? ORDER BY n DESC,label COLLATE BINARY LIMIT 1",[&key],|r|r.get::<_,String>(0)).optional()?.filter(|label|!label.is_empty()).unwrap_or_else(||key.strip_prefix("n:").unwrap_or("Unknown").into());
        self.model_labels
            .borrow_mut()
            .insert(id.into(), label.clone());
        Ok(label)
    }
    fn populate_model_labels(&self) -> QueryResult<()> {
        if self.all_labels.get() == Some(true) {
            return Ok(());
        }
        // This covering projection avoids reading large immutable candidate payloads. Its
        // spelling counts are global within the current routes, independent of UI filters.
        self.conn.execute("INSERT INTO temp.query_labels(model_key,label,n) SELECT e.model_key,COALESCE(e.model_display,''),COUNT(*) FROM usage_events e INDEXED BY events_model_display WHERE e.state='active' AND e.fact_kind IN('request','day','session_total') AND e.report_source IN(SELECT report_source FROM source_roots WHERE route='ledger') GROUP BY e.model_key,e.model_display ON CONFLICT(model_key,label) DO UPDATE SET n=n+excluded.n",[])?;
        let routes = self.source_routes()?;
        if let Some(l) = self.legacy {
            let mut upsert=self.conn.prepare("INSERT INTO temp.query_labels VALUES(?1,?2,1) ON CONFLICT(model_key,label) DO UPDATE SET n=n+1")?;
            for e in &l.entries {
                let (source, model, label) = match e {
                    LegacyEntry::Request { record, .. }
                    | LegacyEntry::SessionTotal { record, .. } => (
                        record.source,
                        model_key(&record.model),
                        record.model.as_str(),
                    ),
                    LegacyEntry::Day(d) => (
                        d.report_source.as_str(),
                        d.model_key.clone(),
                        d.model_display.as_str(),
                    ),
                };
                if !routes.get(source).is_some_and(|r| r == "ledger") {
                    upsert.execute(params![model, label])?;
                }
            }
        }
        self.all_labels.set(Some(true));
        Ok(())
    }
    fn distribution(
        &self,
        groups: &BTreeMap<String, Aggregate>,
        metric: Metric,
        group: GroupBy,
    ) -> QueryResult<Value> {
        let mut total = Cell::default();
        for a in groups.values() {
            total.merge(&a.cells[metric.index()])?;
        }
        let mut sorted: Vec<_> = groups
            .iter()
            .map(|(id, a)| (id.clone(), a.clone()))
            .collect();
        sorted.sort_by(|a, b| compare_row(a, b, metric));
        let split = if group == GroupBy::Model {
            9
        } else {
            sorted.len()
        };
        let mut other = Cell::default();
        let mut rows = Vec::new();
        for (i, (id, a)) in sorted.iter().enumerate() {
            let cell = &a.cells[metric.index()];
            if i >= split {
                other.merge(cell)?;
                continue;
            }
            let label = if group == GroupBy::Model {
                self.preferred_model_label(id)?
            } else if group == GroupBy::Source {
                source_label(id)
            } else {
                "Total".into()
            };
            rows.push(
                json!({"id":id,"label":label,"value":cell.value(),"share":share(cell,&total)}),
            );
        }
        if sorted.len() > split {
            rows.push(json!({"id":"__other__","label":"Other","value":other.value(),"share":share(&other,&total)}));
        }
        Ok(json!({"rows":rows,"known_total":total.sum.to_string(),"complete":total.complete()}))
    }
}
fn legacy_money(value: Option<f64>) -> Option<(i64, i64)> {
    // The legacy adapter already supplied f64; preserve its decimal spelling as a partial value.
    // No float arithmetic or conversion is used to add monetary values.
    let value = value.filter(|n| n.is_finite())?;
    let text = value.to_string();
    let (mantissa, exponent) = match text.split_once(['e', 'E']) {
        Some((m, e)) => (m, e.parse::<i32>().ok()?),
        None => (text.as_str(), 0),
    };
    let negative = mantissa.starts_with('-');
    let m = mantissa.trim_start_matches('-');
    let integer = m.replace('.', "");
    let fraction = m.split_once('.').map(|(_, f)| f.len()).unwrap_or(0) as i32;
    let mut scale = fraction - exponent;
    let mut units = integer.parse::<i64>().ok()?;
    if scale < 0 {
        units = units.checked_mul(10_i64.checked_pow((-scale) as u32)?)?;
        scale = 0;
    }
    if scale > 18 {
        return None;
    }
    if negative {
        units = units.checked_neg()?;
    }
    Some((units, i64::from(scale)))
}
fn diagnostic_bundle(f: &Fact) -> QueryResult<Value> {
    let mut aggregate = Aggregate::default();
    match aggregate.add(f) {
        Ok(()) => Ok(aggregate.bundle()),
        Err(e) if e.code == "METRIC_CONFLICT" => {
            // Conflicting daily candidates are inspectable evidence, never an arithmetic contribution.
            // Preserve each observed number and mark it partial rather than fabricating a coherent total.
            let value = |i: usize| match f.metrics[i] {
                Some(n) => json!({"state":"partial","known_value":n.to_string()}),
                None => json!({"state":"unknown","known_value":null}),
            };
            let mut bundle = Aggregate::default().bundle();
            bundle["metrics"] = json!({"total":value(7),"input_uncached":value(0),"cache_read":value(1),"cache_write":value(2),"output":value(3),"reasoning_output":value(4),"output_combined":value(6),"requests":value(9)});
            bundle["sessions"] = json!({"state":"unknown","known_value":null});
            bundle["coverage"]["grain"] = json!("day");
            bundle["coverage"]["protection"] = json!(if f.live { "unprotected" } else { "ledger" });
            Ok(bundle)
        }
        Err(e) => Err(e),
    }
}
