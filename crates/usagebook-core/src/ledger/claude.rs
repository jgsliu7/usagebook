//! Lossless, white-list Claude projection. No prompt or response text is retained.
use super::types::Observation;
use super::{LedgerError, Result};
use serde_json::Value;
use sha2::{Digest, Sha256};

pub const CLAUDE_FAMILY: &str = "claude";
pub const CLAUDE_NAMESPACE: &str = "unknown:claude";
pub const PARSER_VERSION: &str = "claude-ledger-v1";
pub const MAX_LINE_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_CANDIDATE_BYTES: usize = 1024 * 1024;

/// Canonical encoding excludes acquisition metadata and includes every allowed business column.
pub fn normalized_bytes(observation: &Observation) -> Result<Vec<u8>> {
    let mut canonical = observation.clone();
    canonical.observation_id = 0;
    canonical.object_id = 0;
    canonical.content_generation.clear();
    canonical.record_key.clear();
    canonical.observed_ms = 0;
    canonical.normalized_hash.clear();
    let bytes = serde_json::to_vec(&canonical)
        .map_err(|_| LedgerError::InvalidInput("cannot encode projection".into()))?;
    if bytes.len() > MAX_CANDIDATE_BYTES {
        return Err(LedgerError::RecordTooLarge);
    }
    Ok(bytes)
}
pub fn normalized_hash(observation: &Observation) -> Result<Vec<u8>> {
    Ok(Sha256::digest(normalized_bytes(observation)?).to_vec())
}
#[derive(Debug, Default)]
pub struct ParsedRecord {
    pub observations: Vec<Observation>,
    pub ignored: u64,
}
fn put_str(out: &mut Vec<u8>, value: &str) {
    out.extend_from_slice(&(value.len() as u32).to_be_bytes());
    out.extend_from_slice(value.as_bytes());
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
pub fn identity_key(role: &str, parts: &[&str]) -> String {
    super::types::identity_key(CLAUDE_FAMILY, CLAUDE_NAMESPACE, role, parts)
}
pub fn suppression_link(message: &str) -> String {
    super::types::identity_link(CLAUDE_FAMILY, CLAUDE_NAMESPACE, message)
}
fn identity_only_key(o: &Observation) -> String {
    let mut bytes = Vec::new();
    put_str(&mut bytes, "usagebook/identity-only");
    bytes.extend_from_slice(&1u32.to_be_bytes());
    for s in [
        CLAUDE_FAMILY,
        CLAUDE_NAMESPACE,
        o.native_message_id.as_deref().unwrap_or(""),
    ] {
        put_str(&mut bytes, s);
    }
    for value in [&o.native_session_id, &o.native_request_id] {
        bytes.push(u8::from(value.is_some()));
        if let Some(s) = value {
            put_str(&mut bytes, s);
        }
    }
    hex(&Sha256::digest(bytes))
}
fn native_string(v: &Value, pointer: &str) -> Result<Option<String>> {
    match v.pointer(pointer) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) if s.is_empty() => Ok(None),
        Some(Value::String(s)) if s.len() <= 4096 => Ok(Some(s.clone())),
        Some(Value::String(_)) => Err(LedgerError::RecordTooLarge),
        _ => Err(LedgerError::InvalidInput(
            "InvalidRecordStructure: identity must be a string".into(),
        )),
    }
}
/// Reject decimals/exponents, negative values and overflow before any candidate is saved.
fn number(v: &Value, pointer: &str) -> Result<Option<i64>> {
    let Some(value) = v.pointer(pointer) else {
        return Ok(None);
    };
    let Value::Number(n) = value else {
        return Err(LedgerError::InvalidUsageNumber);
    };
    let text = n.to_string();
    if text.is_empty() || !text.bytes().all(|b| b.is_ascii_digit()) {
        return Err(LedgerError::InvalidUsageNumber);
    }
    text.parse::<i64>()
        .map(Some)
        .map_err(|_| LedgerError::InvalidUsageNumber)
}
/// Exact JSON decimal; exponent notation is accepted without a floating-point conversion.
fn decimal(v: &Value) -> Option<(i64, i64)> {
    let Value::Number(n) = v else { return None };
    let text = n.to_string();
    if text.starts_with('-') {
        return None;
    }
    let (mantissa, exp) = match text.find(['e', 'E']) {
        Some(i) => (text[..i].to_string(), text[i + 1..].parse::<i32>().ok()?),
        None => (text, 0),
    };
    let dot = mantissa.find('.');
    let frac = dot.map(|i| mantissa.len() - i - 1).unwrap_or(0) as i32;
    let mut digits = mantissa.replace('.', "");
    if !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let mut scale = frac.checked_sub(exp)?;
    while scale > 0 && digits.ends_with('0') {
        digits.pop();
        scale -= 1;
    }
    if scale < 0 {
        let extra = usize::try_from(-scale).ok()?;
        if extra > 19 || digits.len() + extra > 19 {
            return None;
        }
        digits.extend(std::iter::repeat_n('0', extra));
        scale = 0;
    }
    if scale > 18 {
        return None;
    }
    Some((digits.parse::<i64>().ok()?, scale as i64))
}
fn base(v: &Value) -> Result<Observation> {
    let mut o = Observation::default();
    o.native_session_id = native_string(v, "/sessionId")?;
    o.session_key = o
        .native_session_id
        .as_deref()
        .map(|s| identity_key("session", &[s]));
    o.occurred_ms = match v.get("timestamp") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => chrono::DateTime::parse_from_rfc3339(s)
            .ok()
            .map(|d| d.timestamp_millis()),
        _ => None,
    };
    Ok(o)
}
fn finish(o: &mut Observation) {
    if o.eligibility != "identity_only" {
        if o.identity_key.is_none() {
            o.eligibility = "identity_unknown".into();
        } else if o.fact_kind == "request" && o.occurred_ms.is_none() {
            o.eligibility = "time_unknown".into();
        }
        if o.metrics[8]
            .zip(o.metrics[2])
            .is_some_and(|(one, all)| one > all)
        {
            o.eligibility = "inconsistent".into();
        }
    }
    if o.eligibility == "identity_only" {
        o.candidate_key = identity_only_key(o);
    } else if let Some(id) = &o.identity_key {
        o.candidate_key = id.clone();
    }
}
/// A complete source record is validated before returning any candidates.
pub fn parse_line(line: &[u8]) -> Result<ParsedRecord> {
    if line.len() > MAX_LINE_BYTES {
        return Err(LedgerError::RecordTooLarge);
    }
    let v: Value = serde_json::from_slice(line)
        .map_err(|_| LedgerError::InvalidInput("InvalidJson: invalid JSON record".into()))?;
    let Some(t) = v.get("type").and_then(Value::as_str) else {
        return Err(LedgerError::InvalidInput(
            "UnknownRecordType: record has no recognized type".into(),
        ));
    };
    match t {
        "assistant" => {
            if v.get("isApiErrorMessage").and_then(Value::as_bool) == Some(true)
                || v.pointer("/message/model").and_then(Value::as_str) == Some("<synthetic>")
            {
                return Ok(ParsedRecord {
                    ignored: 1,
                    ..Default::default()
                });
            }
            if !v.get("message").is_some_and(Value::is_object) {
                return Err(LedgerError::InvalidInput(
                    "InvalidRecordStructure: assistant message must be an object".into(),
                ));
            }
            let mut o = base(&v)?;
            o.native_message_id = native_string(&v, "/message/id")?;
            o.native_request_id = native_string(&v, "/requestId")?;
            o.identity_key = match (
                o.native_message_id.as_deref(),
                o.native_request_id.as_deref(),
                o.native_session_id.as_deref(),
            ) {
                (Some(m), Some(r), _) => Some(identity_key("request/full", &[m, r])),
                (Some(m), None, Some(s)) => Some(identity_key("request/fallback", &[s, m])),
                _ => None,
            };
            let usage = v.pointer("/message/usage");
            if usage.is_none() || usage == Some(&Value::Null) {
                if o.identity_key.is_none() {
                    return Ok(ParsedRecord {
                        ignored: 1,
                        ..Default::default()
                    });
                }
                o.eligibility = "identity_only".into();
                o.occurred_ms = None;
                finish(&mut o);
                return Ok(ParsedRecord {
                    observations: vec![o],
                    ignored: 0,
                });
            }
            if !usage.is_some_and(Value::is_object) {
                return Err(LedgerError::InvalidInput(
                    "InvalidRecordStructure: usage must be an object".into(),
                ));
            }
            let u = usage.unwrap();
            o.metrics[0] = number(u, "/input_tokens")?;
            o.metrics[1] = number(u, "/cache_read_input_tokens")?;
            o.metrics[2] = number(u, "/cache_creation_input_tokens")?;
            o.metrics[6] = number(u, "/output_tokens")?;
            o.metrics[8] = number(u, "/cache_creation/ephemeral_1h_input_tokens")?;
            o.metrics[9] = Some(1);
            if [0, 1, 2, 6].iter().all(|&i| o.metrics[i] == Some(0)) {
                return Ok(ParsedRecord {
                    ignored: 1,
                    ..Default::default()
                });
            }
            o.model_display = native_string(&v, "/message/model")?;
            o.model_key = o
                .model_display
                .as_ref()
                .map(|m| format!("n:{}", m.to_lowercase()))
                .unwrap_or_default();
            finish(&mut o);
            normalized_bytes(&o)?;
            Ok(ParsedRecord {
                observations: vec![o],
                ignored: 0,
            })
        }
        "cost-state" => {
            let base = base(&v)?;
            if base.native_session_id.is_none() {
                return Err(LedgerError::InvalidInput(
                    "InvalidRecordStructure: cost-state requires session identity".into(),
                ));
            }
            let session = base.native_session_id.as_deref().unwrap();
            let mut out = Vec::new();
            if let Some(models) = v.get("modelUsage") {
                let Some(models) = models.as_object() else {
                    return Err(LedgerError::InvalidInput(
                        "InvalidRecordStructure: modelUsage must be an object".into(),
                    ));
                };
                for (name, u) in models {
                    if name.len() > 4096 || !u.is_object() {
                        return Err(LedgerError::InvalidInput(
                            "InvalidRecordStructure: invalid modelUsage entry".into(),
                        ));
                    }
                    let mut o = base.clone();
                    o.fact_kind = "session_total".into();
                    o.model_display = (!name.is_empty()).then(|| name.clone());
                    o.model_key = if name.is_empty() {
                        String::new()
                    } else {
                        format!("n:{}", name.to_lowercase())
                    };
                    o.identity_key = Some(identity_key("session_total", &[session, &o.model_key]));
                    o.metrics[0] = number(u, "/inputTokens")?;
                    o.metrics[1] = number(u, "/cacheReadInputTokens")?;
                    o.metrics[2] = number(u, "/cacheCreationInputTokens")?;
                    o.metrics[6] = number(u, "/outputTokens")?;
                    finish(&mut o);
                    normalized_bytes(&o)?;
                    out.push(o);
                }
            }
            if let Some(cost) = v.get("totalCostUSD") {
                let mut o = base.clone();
                o.fact_kind = "session_cost".into();
                o.identity_key = Some(identity_key("session_cost", &[session, "USD"]));
                match decimal(cost) {
                    Some((u, s)) => {
                        o.cost_state = "known".into();
                        o.cost_units = Some(u);
                        o.cost_scale = Some(s);
                        o.cost_currency = Some("USD".into());
                    }
                    None => o.cost_state = "invalid".into(),
                }
                finish(&mut o);
                normalized_bytes(&o)?;
                out.push(o);
            }
            Ok(ParsedRecord {
                observations: out,
                ignored: 0,
            })
        }
        "user"
        | "attachment"
        | "summary"
        | "file-history-snapshot"
        | "file-history-delta"
        | "permission-mode"
        | "mode"
        | "custom-title"
        | "agent-name"
        | "system"
        | "queue-operation"
        | "last-prompt"
        | "atis-latch"
        | "ai-title"
        | "started"
        | "result"
        | "progress" => Ok(ParsedRecord {
            ignored: 1,
            ..Default::default()
        }),
        _ => Err(LedgerError::InvalidInput(
            "UnknownRecordType: unrecognized source record type".into(),
        )),
    }
}

/// Strict identity projection for deletion proofs. Malformed identifiers are gaps,
/// never silently absent edges; token payload is intentionally not interpreted.
// Keep the existing (session, message, request) projection contract together.
#[allow(clippy::type_complexity)]
pub(crate) fn identity_projection(
    v: &Value,
) -> Result<Option<(Option<String>, Option<String>, Option<String>)>> {
    match v.get("type").and_then(Value::as_str) {
        Some("assistant") => {
            if !v.get("message").is_some_and(Value::is_object) {
                return Err(LedgerError::IncompleteEvidence);
            }
            let s = native_string(v, "/sessionId").map_err(|_| LedgerError::IncompleteEvidence)?;
            let m = native_string(v, "/message/id").map_err(|_| LedgerError::IncompleteEvidence)?;
            let r = native_string(v, "/requestId").map_err(|_| LedgerError::IncompleteEvidence)?;
            Ok(Some((s, m, r)))
        }
        Some(
            "cost-state"
            | "user"
            | "attachment"
            | "summary"
            | "file-history-snapshot"
            | "file-history-delta"
            | "permission-mode"
            | "mode"
            | "custom-title"
            | "agent-name"
            | "system"
            | "queue-operation"
            | "last-prompt"
            | "atis-latch"
            | "ai-title"
            | "started"
            | "result"
            | "progress",
        ) => Ok(None),
        _ => Err(LedgerError::IncompleteEvidence),
    }
}

/// Bounded multi-candidate projection. Every numeric field is validated before
/// the first callback; cost-state model counts do not grow a candidate Vec.
pub(crate) fn visit_line(
    line: &[u8],
    mut emit: impl FnMut(Observation) -> Result<()>,
) -> Result<u64> {
    if line.len() > MAX_LINE_BYTES {
        return Err(LedgerError::RecordTooLarge);
    }
    let v: Value = serde_json::from_slice(line)
        .map_err(|_| LedgerError::InvalidInput("InvalidJson".into()))?;
    if v.get("type").and_then(Value::as_str) != Some("cost-state") {
        let parsed = parse_line(line)?;
        // Only cost-state may stream multiple candidates. Request identities
        // require admission before any sub-batch can be committed; future
        // multi-request records must add a whole-record preflight first.
        if parsed.observations.len() > 1 {
            return Err(LedgerError::InvalidInput(
                "multi-request record requires identity preflight".into(),
            ));
        }
        for o in parsed.observations {
            emit(o)?;
        }
        return Ok(parsed.ignored);
    }
    let base = base(&v)?;
    let Some(session) = base.native_session_id.as_deref() else {
        return Err(LedgerError::InvalidInput(
            "cost-state requires session identity".into(),
        ));
    };
    let models = match v.get("modelUsage") {
        None => None,
        Some(Value::Object(m)) => Some(m),
        _ => {
            return Err(LedgerError::InvalidInput(
                "modelUsage must be an object".into(),
            ))
        }
    };
    if let Some(models) = models {
        for (name, u) in models {
            if name.len() > 4096 || !u.is_object() {
                return Err(LedgerError::InvalidInput("invalid modelUsage entry".into()));
            }
            for field in [
                "/inputTokens",
                "/cacheReadInputTokens",
                "/cacheCreationInputTokens",
                "/outputTokens",
            ] {
                number(u, field)?;
            }
        }
    }
    if let Some(models) = models {
        for (name, u) in models {
            let mut o = base.clone();
            o.fact_kind = "session_total".into();
            o.model_display = (!name.is_empty()).then(|| name.clone());
            o.model_key = super::types::model_key(name);
            o.identity_key = Some(identity_key("session_total", &[session, &o.model_key]));
            o.metrics[0] = number(u, "/inputTokens")?;
            o.metrics[1] = number(u, "/cacheReadInputTokens")?;
            o.metrics[2] = number(u, "/cacheCreationInputTokens")?;
            o.metrics[6] = number(u, "/outputTokens")?;
            finish(&mut o);
            normalized_bytes(&o)?;
            emit(o)?;
        }
    }
    if let Some(cost) = v.get("totalCostUSD") {
        let mut o = base.clone();
        o.fact_kind = "session_cost".into();
        o.identity_key = Some(identity_key("session_cost", &[session, "USD"]));
        match decimal(cost) {
            Some((units, scale)) => {
                o.cost_state = "known".into();
                o.cost_units = Some(units);
                o.cost_scale = Some(scale);
                o.cost_currency = Some("USD".into());
            }
            None => o.cost_state = "invalid".into(),
        }
        finish(&mut o);
        emit(o)?;
    }
    Ok(0)
}
