//! One checked constraint evaluator for storage, summaries and query output.
use super::types::*;
use chrono::{Datelike, Duration, NaiveDate, TimeZone, Timelike};
#[derive(Clone, Copy, PartialEq, Eq)]
struct Interval {
    lo: i128,
    hi: Option<i128>,
}
fn intersect(x: &mut Interval, lo: i128, hi: Option<i128>) -> Result<bool> {
    let before = *x;
    x.lo = x.lo.max(lo);
    x.hi = match (x.hi, hi) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, None) | (None, a) => a,
    };
    if x.hi.is_some_and(|h| h < x.lo) {
        return Err(LedgerError::MetricConflict);
    }
    Ok(*x != before)
}
pub fn evaluate(fields: &Metrics) -> Result<[MetricValue; 10]> {
    if fields.iter().flatten().any(|v| *v < 0) {
        return Err(LedgerError::InvalidUsageNumber);
    }
    let mut nodes: [Interval; 8] = std::array::from_fn(|i| match fields[i] {
        Some(v) => Interval {
            lo: v as i128,
            hi: Some(v as i128),
        },
        None => Interval { lo: 0, hi: None },
    });
    let tree: [(usize, &[usize]); 3] = [(5, &[0, 1, 2]), (6, &[3, 4]), (7, &[5, 6])];
    loop {
        let mut changed = false;
        for (parent, children) in tree {
            let lo = children.iter().try_fold(0i128, |a, &i| {
                a.checked_add(nodes[i].lo)
                    .ok_or(LedgerError::NumericOverflow)
            })?;
            let hi = children
                .iter()
                .try_fold(Some(0i128), |a, &i| -> Result<Option<i128>> {
                    Ok(match (a, nodes[i].hi) {
                        (Some(a), Some(b)) => {
                            Some(a.checked_add(b).ok_or(LedgerError::NumericOverflow)?)
                        }
                        _ => None,
                    })
                })?;
            changed |= intersect(&mut nodes[parent], lo, hi)?;
            for &child in children {
                let mut rest_lo = 0i128;
                let mut rest_hi = Some(0i128);
                for &sibling in children {
                    if sibling == child {
                        continue;
                    }
                    rest_lo = rest_lo
                        .checked_add(nodes[sibling].lo)
                        .ok_or(LedgerError::NumericOverflow)?;
                    rest_hi = match (rest_hi, nodes[sibling].hi) {
                        (Some(a), Some(b)) => {
                            Some(a.checked_add(b).ok_or(LedgerError::NumericOverflow)?)
                        }
                        _ => None,
                    };
                }
                let parent = nodes[parent];
                let lo = rest_hi.map(|h| (parent.lo - h).max(0)).unwrap_or(0);
                let hi = parent.hi.map(|h| h - rest_lo);
                changed |= intersect(&mut nodes[child], lo, hi)?;
            }
        }
        if !changed {
            break;
        }
    }
    let mut evidence: [bool; 8] = std::array::from_fn(|i| fields[i].is_some());
    for (parent, children) in tree {
        evidence[parent] |= children.iter().any(|i| evidence[*i]);
    }
    let mut out: [MetricValue; 10] = std::array::from_fn(|_| MetricValue::unknown());
    for i in 0..8 {
        let n = nodes[i];
        if n.lo > i64::MAX as i128 {
            return Err(LedgerError::NumericOverflow);
        }
        out[i] = if n.hi == Some(n.lo) {
            MetricValue::exact(n.lo as i64)
        } else if n.lo > 0 || evidence[i] {
            MetricValue {
                state: ValueState::Partial,
                known_value: Some(n.lo as i64),
            }
        } else {
            MetricValue::unknown()
        };
    }
    if let Some(v) = fields[8] {
        if nodes[2].hi.is_some_and(|h| v as i128 > h) {
            return Err(LedgerError::MetricConflict);
        }
        out[8] = MetricValue::exact(v)
    }
    if let Some(v) = fields[9] {
        out[9] = MetricValue::exact(v)
    }
    Ok(out)
}
#[derive(Clone, Debug, Default)]
pub struct Aggregate {
    pub event_count: i64,
    pub known_sum: [i64; 10],
    pub known_count: [i64; 10],
    pub exact_count: [i64; 10],
}
impl Aggregate {
    pub fn add(&mut self, values: &[MetricValue; 10], direction: i64) -> Result<()> {
        let mut next = self.clone();
        next.event_count = checked(next.event_count, direction)?;
        for (i, v) in values.iter().enumerate() {
            if let Some(n) = v.known_value {
                next.known_sum[i] = checked(
                    next.known_sum[i],
                    n.checked_mul(direction)
                        .ok_or(LedgerError::NumericOverflow)?,
                )?;
                next.known_count[i] = checked(next.known_count[i], direction)?;
                if v.state == ValueState::Exact {
                    next.exact_count[i] = checked(next.exact_count[i], direction)?;
                }
            }
        }
        *self = next;
        Ok(())
    }
    pub fn values(&self) -> [MetricValue; 10] {
        std::array::from_fn(|i| {
            if self.event_count == 0 || self.exact_count[i] == self.event_count {
                MetricValue::exact(self.known_sum[i])
            } else if self.known_count[i] == 0 {
                MetricValue::unknown()
            } else {
                MetricValue {
                    state: ValueState::Partial,
                    known_value: Some(self.known_sum[i]),
                }
            }
        })
    }
}
fn checked(a: i64, b: i64) -> Result<i64> {
    a.checked_add(b)
        .filter(|n| *n >= 0)
        .ok_or(LedgerError::NumericOverflow)
}
pub fn timezone(name: &str) -> Result<chrono_tz::Tz> {
    name.parse()
        .map_err(|_| LedgerError::InvalidInput("unknown IANA timezone".into()))
}
pub fn date_boundary(tz: chrono_tz::Tz, date: NaiveDate) -> Result<i64> {
    // Midnight transitions can skip part/all of a local day. First actual instant wins.
    for minute in 0..=1440 {
        let local = date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| LedgerError::InvalidInput("invalid date".into()))?
            + Duration::minutes(minute);
        if let Some(v) = tz.from_local_datetime(&local).earliest() {
            return Ok(v.timestamp_millis());
        }
    }
    Err(LedgerError::InvalidInput(
        "date has no representable boundary".into(),
    ))
}
pub fn day_bounds(tz: &str, ts_ms: i64) -> Result<(i64, i64, String)> {
    let tz = timezone(tz)?;
    let dt = tz
        .timestamp_millis_opt(ts_ms)
        .single()
        .ok_or_else(|| LedgerError::InvalidInput("invalid timestamp".into()))?;
    let day = dt.date_naive();
    let next = day.succ_opt().ok_or(LedgerError::NumericOverflow)?;
    Ok((
        date_boundary(tz, day)?,
        date_boundary(tz, next)?,
        day.to_string(),
    ))
}
pub fn hour_bounds(tz: &str, ts_ms: i64) -> Result<(i64, i64, String)> {
    let zone = timezone(tz)?;
    let dt = zone
        .timestamp_millis_opt(ts_ms)
        .single()
        .ok_or_else(|| LedgerError::InvalidInput("invalid timestamp".into()))?;
    let (day_start, day_end, label) = day_bounds(tz, ts_ms)?;
    let signature = |t: i64| -> Result<_> {
        let d = zone
            .timestamp_millis_opt(t)
            .single()
            .ok_or(LedgerError::NumericOverflow)?;
        Ok((d.year(), d.ordinal(), d.hour(), d.offset().to_string()))
    };
    let current = signature(ts_ms)?;
    let second = ts_ms - dt.timestamp_subsec_millis() as i64;
    let mut start =
        (second - dt.minute() as i64 * 60_000 - dt.second() as i64 * 1000).max(day_start);
    if signature(start)? != current {
        // A partial repeated hour (Lord Howe) starts at its offset transition.
        let mut lo = start;
        let mut hi = second;
        while hi - lo > 1000 {
            let mid = lo + ((hi - lo) / 2000) * 1000;
            if signature(mid)? == current {
                hi = mid
            } else {
                lo = mid + 1000
            }
        }
        start = if signature(lo)? == current { lo } else { hi };
    }
    let nominal =
        (second + (60 - dt.minute() as i64) * 60_000 - dt.second() as i64 * 1000).min(day_end);
    let mut end = nominal;
    // Physical offset changes can split a wall-clock hour before the next :00.
    let mut probe = second + 1000;
    while probe < nominal {
        if signature(probe)? != current {
            end = probe;
            break;
        }
        probe += 1000;
    }
    Ok((start, end, label))
}
