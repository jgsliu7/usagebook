//! Original schema v2 migration gate, followed through the current v7 schema:
//! a v1 database upgrades in place
//! on the first write open with every fact, derived row, and suppression rule
//! preserved. The frozen v1 DDL lives beside this file; the rebuilt tables
//! must end up byte-identical (modulo whitespace) to a fresh init.
use rusqlite::Connection;
use usagebook_core::ledger::{Ledger, LedgerError};

const SCHEMA_V1: &str = include_str!("ledger_schema_v1.sql");
const DIGEST: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"; // 32 bytes hex

fn temp_db(tag: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "usagebook-schema-v2-{}-{nanos}-{tag}.db",
        std::process::id()
    ))
}

fn build_v1(path: &std::path::Path) {
    let c = Connection::open(path).unwrap();
    c.execute_batch(SCHEMA_V1).unwrap();
    c.execute_batch(&format!(
        "INSERT INTO ledger_meta VALUES(1,1,'uuid','epoch',0,1,1,1,'UTC','v',0);
         INSERT INTO source_roots(root_id,identity_family,namespace_key,collector_key,report_source,native_tool,locator,enabled,identity_version,parser_version,route) VALUES('r','claude','unknown:claude','claude','claude','claude','/nonexistent',1,1,'claude-ledger-v1','live');
         INSERT INTO source_objects(root_id,object_key,locator,content_generation,byte_length,mtime_ns,quick_fingerprint,object_revision,state,parser_version,saved_offset,checkpoint_format,checkpoint,target_generation,target_end,target_done,last_observed_ms,audit_completed_ms,audit_due_ms,audit_generation,audit_offset,audit_digest)
          VALUES('r','k1','k1','g1',1048576,1,x'{DIGEST}',0,'ready','claude-ledger-v1',1048576,1,x'00','g1',1048576,1,1,1,99999999999999,'g1',1048576,x'{DIGEST}');
         INSERT INTO source_objects(root_id,object_key,locator,content_generation,byte_length,object_revision,state,parser_version,audit_due_ms)
          VALUES('r','k2','k2','g2',0,0,'missing','claude-ledger-v1',99999999999999);
         INSERT INTO usage_observations(identity_family,namespace_key,candidate_key,normalized_hash,object_id,content_generation,record_key,parser_version,identity_version,semantics_version,observed_ms,eligibility,action,fact_kind,report_source,native_tool,model_key,cost_state)
          VALUES('claude','unknown:claude','k1',x'{DIGEST}',1,'g1','0','claude-ledger-v1',1,1,1,'identity_only','upsert','request','claude','claude','m','unknown');
         INSERT INTO usage_rollups(derived_version,grain,bucket_start_ms,bucket_end_ms,local_day,report_source,model_key,event_count,bucket_revision,hour_cache_revision,
           input_uncached_known_sum,input_uncached_known_count,input_uncached_exact_count,
           cache_read_known_sum,cache_read_known_count,cache_read_exact_count,
           cache_write_known_sum,cache_write_known_count,cache_write_exact_count,
           output_normal_known_sum,output_normal_known_count,output_normal_exact_count,
           output_reasoning_known_sum,output_reasoning_known_count,output_reasoning_exact_count,
           input_total_known_sum,input_total_known_count,input_total_exact_count,
           output_total_known_sum,output_total_known_count,output_total_exact_count,
           total_tokens_known_sum,total_tokens_known_count,total_tokens_exact_count,
           cache_write_1h_known_sum,cache_write_1h_known_count,cache_write_1h_exact_count,
           request_count_known_sum,request_count_known_count,request_count_exact_count)
          VALUES(1,'day',0,86400000,'1970-01-01','claude','m',1,1,NULL,
           7,1,1,
           0,0,0, 0,0,0, 0,0,0, 0,0,0, 0,0,0, 0,0,0, 0,0,0, 0,0,0, 0,0,0);
         INSERT INTO suppression_rules(kind,report_source,native_tool,operation_id,created_ms) VALUES('source','claude','claude','op1',0);"
    ))
    .unwrap();
}

fn columns(c: &Connection, table: &str) -> Vec<String> {
    let mut stmt = c.prepare(&format!("PRAGMA table_info({table})")).unwrap();
    stmt.query_map([], |r| r.get::<_, String>(1))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

fn normalized_ddl(c: &Connection) -> Vec<(String, String)> {
    let mut stmt = c
        .prepare(
            "SELECT name,COALESCE(sql,'') FROM sqlite_master WHERE name IN('ledger_meta','source_objects','usage_rollups','objects_retry','objects_audit','rollups_window') ORDER BY name",
        )
        .unwrap();
    stmt.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            // ALTER TABLE ... RENAME stores the new table name quoted; the
            // quoting is cosmetic, so normalize it away before comparing.
            r.get::<_, String>(1)?
                .replace('"', "")
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" "),
        ))
    })
    .unwrap()
    .map(Result::unwrap)
    .collect()
}

#[test]
fn v1_migrates_in_place_and_preserves_data() {
    let path = temp_db("migrate");
    build_v1(&path);
    let db = Ledger::open(&path, true).unwrap();
    let c = db.connection();

    let schema: i64 = c
        .query_row("SELECT schema_version FROM ledger_meta", [], |r| r.get(0))
        .unwrap();
    assert_eq!(schema, 7);

    let object_cols = columns(c, "source_objects");
    for dead in ["audit_generation", "audit_offset", "audit_digest"] {
        assert!(
            !object_cols.contains(&dead.to_string()),
            "{dead} still present"
        );
    }
    let rollup_cols = columns(c, "usage_rollups");
    assert!(!rollup_cols.contains(&"hour_cache_revision".to_string()));

    for (table, expected) in [
        ("source_roots", 1),
        ("source_objects", 2),
        ("usage_observations", 1),
        ("usage_rollups", 1),
        ("suppression_rules", 1),
    ] {
        let n: i64 = c
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, expected, "{table} rows changed across migration");
    }

    let sum: i64 = c
        .query_row(
            "SELECT input_uncached_known_sum FROM usage_rollups WHERE model_key='m'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(sum, 7);

    let sealed: Option<Vec<u8>> = c
        .query_row(
            "SELECT quick_fingerprint FROM source_objects WHERE object_key='k1'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(sealed.map(|v| v.len()), Some(32));

    let seq: i64 = c
        .query_row("SELECT commit_seq FROM ledger_meta", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        seq, 6,
        "each migration from v1 through v7 must bump commit_seq so stale cursors die"
    );

    {
        let mut stmt = c.prepare("PRAGMA foreign_key_check").unwrap();
        let mut rows = stmt.query([]).unwrap();
        assert!(rows.next().unwrap().is_none(), "foreign key violated");
    }

    // AUTOINCREMENT must continue past pre-migration ids.
    c.execute(
        "INSERT INTO source_objects(root_id,object_key,locator,content_generation,byte_length,object_revision,state,parser_version,audit_due_ms) VALUES('r','k3','k3','g3',1,0,'ready','claude-ledger-v1',99999999999999)",
        [],
    )
    .unwrap();
    assert_eq!(c.last_insert_rowid(), 3);

    // The rebuilt tables must match a fresh current init definition for definition.
    let fresh_path = temp_db("fresh");
    let fresh = Ledger::init(&fresh_path, "UTC").unwrap();
    assert_eq!(normalized_ddl(c), normalized_ddl(fresh.connection()));
    drop(db);
    drop(fresh);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&fresh_path);
}

#[test]
fn fresh_init_has_current_schema_and_reopens() {
    let path = temp_db("init");
    {
        let db = Ledger::init(&path, "UTC").unwrap();
        let object_cols = columns(db.connection(), "source_objects");
        for dead in ["audit_generation", "audit_offset", "audit_digest"] {
            assert!(!object_cols.contains(&dead.to_string()));
        }
        assert!(
            !columns(db.connection(), "usage_rollups").contains(&"hour_cache_revision".to_string())
        );
    }
    // Reopen both ways to prove validate accepts the current schema.
    Ledger::open(&path, true).unwrap();
    Ledger::open(&path, false).unwrap();
    let _ = std::fs::remove_file(&path);
}

#[test]
fn v1_read_only_open_is_unsupported_until_writer_migrates() {
    let path = temp_db("readonly");
    build_v1(&path);
    match Ledger::open(&path, false) {
        Err(LedgerError::UnsupportedVersion) => {}
        Err(e) => panic!("expected UnsupportedVersion, got {e}"),
        Ok(_) => panic!("expected UnsupportedVersion, got Ok"),
    }
    // The first write open upgrades; a later read open then succeeds.
    drop(Ledger::open(&path, true).unwrap());
    Ledger::open(&path, false).unwrap();
    let _ = std::fs::remove_file(&path);
}
