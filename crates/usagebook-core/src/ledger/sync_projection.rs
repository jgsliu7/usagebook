//! Cloud projection of the published local query model, without costs or native IDs.
use super::{sync_identity, types::*, LedgerError, Result};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

#[derive(Clone)]
pub struct Identity {
    pub binding_id: String,
    pub account_id: String,
    pub device_id: String,
    pub secret: Vec<u8>,
    pub ledger_uuid: String,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct ProjectedRow {
    pub id: String,
    pub local_key: Option<String>,
    pub basis: Option<String>,
    pub entry: Value,
    pub payload_hash: Option<String>,
}
#[derive(Clone)]
pub(crate) struct SelectedFact {
    pub observation_id: i64,
    pub family: String,
    pub namespace: String,
    pub key: String,
    pub source: String,
    pub kind: String,
    pub model: String,
    pub live: bool,
    pub reported: bool,
    pub time: Value,
    pub metrics: Value,
}
pub fn export_model_key(model: &str) -> String {
    let Some(name) = model.strip_prefix("n:") else {
        return "@unknown".into();
    };
    let blocked = [
        "sk-",
        "sk_",
        "ghp_",
        "gho_",
        "ghu_",
        "ghs_",
        "ghr_",
        "github_pat_",
        "xoxb-",
        "xoxp-",
    ];
    if model.len() > 256
        || name.is_empty()
        || !name.as_bytes()[0].is_ascii_alphanumeric()
        || name
            .bytes()
            .any(|b| !b.is_ascii_lowercase() && !b.is_ascii_digit() && !b"._:+-".contains(&b))
        || blocked.iter().any(|v| name.starts_with(v))
    {
        "@unknown".into()
    } else {
        model.into()
    }
}
pub(crate) fn metrics_value(fields: &Metrics, reported: bool, known: u16) -> Result<Value> {
    let mut values = super::metrics::evaluate(fields)?;
    for (i, v) in values.iter_mut().enumerate() {
        if (reported || known & (1 << i) == 0) && v.known_value.is_some() {
            v.state = ValueState::Partial;
        }
    }
    Ok(Value::Object(
        METRIC_NAMES
            .iter()
            .zip(values.iter())
            .map(|(n, v)| {
                (
                    n.to_string(),
                    json!({"state":v.state,"known_value":v.known_value.map(|n|n.to_string())}),
                )
            })
            .collect(),
    ))
}
fn encode(value: &Value) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|_| LedgerError::CorruptLedger)
}
/// Snapshot stays open for the entire source. The writer consumes bounded pages
/// on a separate connection and never owns this read transaction.
pub fn snapshot(
    c: &Connection,
    identity: &Identity,
    source: &str,
    mut emit: impl FnMut(ProjectedRow) -> Result<()>,
) -> Result<()> {
    let owned = c.is_autocommit();
    if owned {
        c.execute_batch("BEGIN;")?;
    }
    let outcome = (|| {
        let current = super::db::stamp(c)?;
        if current.ledger_uuid != identity.ledger_uuid {
            return Err(LedgerError::InvalidInput("LEDGER_IDENTITY_CHANGED".into()));
        }
        let mut previous_key = String::new();
        let mut next_occurrence = 0usize;
        super::query::QueryService::new(c,None).visit_selected_facts(source,|fact| {
   let mut header=json!({"fact_kind":fact.kind,"source":fact.source,"model":export_model_key(&fact.model),"time":fact.time,"metrics":fact.metrics,"allocation_metrics":[],"part_count":0});
   let key=if fact.key.is_empty(){sync_identity::hash(&encode(&header)?)} else {fact.key.clone()};
   let occurrence=if fact.live {if previous_key!=key{previous_key=key.clone();next_occurrence=0;}let result=next_occurrence;next_occurrence+=1;result}else{0};
   let domain=if fact.live {"live"}else{"ledger"};
   let local=sync_identity::local_key(&identity.secret,&identity.ledger_uuid,domain,&fact.source,&fact.family,&fact.namespace,&fact.kind,&key,occurrence);
   let id=if fact.live {sync_identity::inherited_id(c,&identity.binding_id,&local)?} else {sync_identity::resolve_lineage(c,&identity.binding_id,&identity.secret,&identity.ledger_uuid,&fact.source,&fact.family,&fact.namespace,&fact.kind,&key)?}.unwrap_or_else(||sync_identity::contribution_id(&identity.secret,&identity.account_id,&identity.device_id,&local));
   // Validate sealed evidence in constant memory, then make ordered passes in
   // the same WAL snapshot. Neither pass consults mutable source files.
   let mut table=if fact.kind=="day" && fact.reported && !fact.live {super::time_allocation::validate_stream(c,fact.observation_id)?}else{None};
   let mut allocation=Vec::new();let mut count=0usize;
   if let Some(table_name)=table {
    let mut sums=[Some(0i128);10];
    let mut q=c.prepare(&format!("SELECT metrics_json FROM {table_name} WHERE parent_observation_id=?1"))?;
    let mut rows=q.query([fact.observation_id])?;
    while let Some(row)=rows.next()? {let m:Metrics=serde_json::from_str(&row.get::<_,String>(0)?).map_err(|_|LedgerError::CorruptLedger)?;let values=super::metrics::evaluate(&m)?;for(i,v)in values.iter().enumerate(){sums[i]=sums[i].zip(v.known_value).map(|(a,b)|a+i128::from(b));}count+=1;}
    for(i,name)in METRIC_NAMES.iter().enumerate(){if let Some(expected)=header["metrics"][name]["known_value"].as_str().and_then(|s|s.parse::<i128>().ok()){if sums[i]==Some(expected){allocation.push(*name);}}}
    if allocation.is_empty(){table=None;count=0;}
   }
   header["allocation_metrics"]=json!(allocation);header["part_count"]=json!(count);
   let mut digest=Sha256::new();digest.update(b"{\"header\":");digest.update(encode(&header)?);digest.update(b",\"parts\":[");
   let mut entry=header.clone();entry["type"]=json!("put");entry["id"]=json!(id);entry["expected_revision"]=json!("0");
   emit(ProjectedRow{id:id.clone(),local_key:Some(local),basis:Some(json!({"version":1,"domain":domain,"family":fact.family,"namespace":fact.namespace,"kind":fact.kind,"key":key,"occurrence":occurrence}).to_string()),entry,payload_hash:None})?;
   if let Some(table_name)=table {
    let mut q=c.prepare(&format!("SELECT occurred_ms,metrics_json FROM {table_name} WHERE parent_observation_id=?1 ORDER BY occurred_ms,member_key"))?;
    let mut rows=q.query([fact.observation_id])?;let mut index=0usize;
    while let Some(row)=rows.next()? {
     let occurred:i64=row.get(0)?;let fields:Metrics=serde_json::from_str(&row.get::<_,String>(1)?).map_err(|_|LedgerError::CorruptLedger)?;
     let mut m=metrics_value(&fields,true,1023)?;
     for name in METRIC_NAMES {if !allocation.contains(&name){m[name]=json!({"state":"unknown","known_value":null});}}
     let payload=json!({"part_index":index,"at_ms":occurred.to_string(),"metrics":m});
     if index>0{digest.update(b",");}digest.update(encode(&payload)?);
     let mut entry=payload;entry["type"]=json!("part");entry["id"]=json!(id);
     emit(ProjectedRow{id:id.clone(),local_key:None,basis:None,entry,payload_hash:None})?;index+=1;
    }
   }
   digest.update(b"]}");
   emit(ProjectedRow{id,local_key:None,basis:None,entry:Value::Null,payload_hash:Some(format!("{:x}",digest.finalize()))})?;
   Ok(())
  }).map_err(|e|LedgerError::InvalidInput(e.code))?;
        Ok(())
    })();
    if owned {
        let _ = c.execute_batch("ROLLBACK;");
    }
    outcome
}
