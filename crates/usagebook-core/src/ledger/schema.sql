-- Canonical schema v1. Migrations must also preserve suppression rules.
PRAGMA foreign_keys=ON;
CREATE TABLE ledger_meta (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 schema_version INTEGER NOT NULL CHECK(schema_version=1),
 ledger_uuid TEXT NOT NULL, epoch TEXT NOT NULL,
 commit_seq INTEGER NOT NULL CHECK(commit_seq>=0),
 derived_version INTEGER NOT NULL CHECK(derived_version>0),
 identity_version INTEGER NOT NULL CHECK(identity_version>0),
 normalization_version INTEGER NOT NULL CHECK(normalization_version>0),
 report_timezone TEXT NOT NULL, tzdb_version TEXT NOT NULL,
 created_ms INTEGER NOT NULL
) STRICT;

CREATE TABLE ingest_runs (
 run_id INTEGER PRIMARY KEY AUTOINCREMENT,
 identity_family TEXT NOT NULL, namespace_key TEXT NOT NULL,
 epoch TEXT NOT NULL, resolver_version TEXT NOT NULL,
 identity_version INTEGER NOT NULL CHECK(identity_version>0),
 state TEXT NOT NULL CHECK(state IN('publishing','done')),
 lower_observation_id INTEGER NOT NULL CHECK(lower_observation_id>=0),
 upper_observation_id INTEGER NOT NULL CHECK(upper_observation_id>lower_observation_id),
 last_publish_key TEXT COLLATE BINARY,
 created_ms INTEGER NOT NULL, finished_ms INTEGER, error_code TEXT,
 CHECK((state='publishing' AND finished_ms IS NULL)
    OR (state='done' AND finished_ms IS NOT NULL))
) STRICT;
CREATE UNIQUE INDEX ingest_one_active ON ingest_runs(identity_family,namespace_key) WHERE state<>'done';
CREATE INDEX ingest_history ON ingest_runs(identity_family,namespace_key,run_id DESC);

CREATE TABLE source_roots (
 root_id TEXT PRIMARY KEY,
 identity_family TEXT NOT NULL, namespace_key TEXT NOT NULL,
 collector_key TEXT NOT NULL, report_source TEXT NOT NULL, native_tool TEXT NOT NULL,
 locator TEXT NOT NULL,
 enabled INTEGER NOT NULL CHECK(enabled IN(0,1)),
 identity_version INTEGER NOT NULL CHECK(identity_version>0),
 parser_version TEXT NOT NULL,
 route TEXT NOT NULL CHECK(route IN('live','ledger')),
 enumeration_done INTEGER NOT NULL DEFAULT 0 CHECK(enumeration_done IN(0,1)),
 last_enumerated_ms INTEGER, error_code TEXT,
 failure_count INTEGER NOT NULL DEFAULT 0 CHECK(failure_count>=0),
 retry_after_ms INTEGER,
 UNIQUE(identity_family,namespace_key,collector_key,locator)
) STRICT;
CREATE INDEX roots_scope ON source_roots(identity_family,namespace_key,enabled);
CREATE INDEX roots_route ON source_roots(report_source,route,root_id);

CREATE TABLE source_objects (
 object_id INTEGER PRIMARY KEY AUTOINCREMENT,
 root_id TEXT NOT NULL REFERENCES source_roots(root_id),
 object_key TEXT NOT NULL, locator TEXT NOT NULL,
 content_generation TEXT NOT NULL,
 byte_length INTEGER NOT NULL CHECK(byte_length>=0), mtime_ns INTEGER,
 quick_fingerprint BLOB CHECK(quick_fingerprint IS NULL OR length(quick_fingerprint)=32),
 object_revision INTEGER NOT NULL CHECK(object_revision>=0),
 state TEXT NOT NULL CHECK(state IN('ready','missing','error','replay_required')),
 parser_version TEXT NOT NULL,
 saved_offset INTEGER CHECK(saved_offset>=0),
 checkpoint_format INTEGER, checkpoint BLOB,
 target_generation TEXT, target_end INTEGER CHECK(target_end>=0),
 target_done INTEGER NOT NULL DEFAULT 0 CHECK(target_done IN(0,1)),
 last_observed_ms INTEGER,
 audit_completed_ms INTEGER, audit_due_ms INTEGER NOT NULL,
 audit_generation TEXT, audit_offset INTEGER NOT NULL DEFAULT 0 CHECK(audit_offset>=0),
 audit_digest BLOB CHECK(audit_digest IS NULL OR length(audit_digest)=32),
 error_code TEXT,
 failure_count INTEGER NOT NULL DEFAULT 0 CHECK(failure_count>=0),
 retry_after_ms INTEGER,
 UNIQUE(root_id,object_key),
 CHECK((checkpoint IS NULL AND checkpoint_format IS NULL AND saved_offset IS NULL)
    OR (checkpoint IS NOT NULL AND length(checkpoint)<=65536 AND checkpoint_format IS NOT NULL AND checkpoint_format>0 AND saved_offset IS NOT NULL)),
 CHECK(state<>'replay_required' OR saved_offset IS NULL),
 CHECK((target_generation IS NULL AND target_end IS NULL AND target_done=0)
    OR (target_generation IS NOT NULL AND target_end IS NOT NULL))
) STRICT;
CREATE INDEX objects_retry ON source_objects(retry_after_ms,object_id) WHERE retry_after_ms IS NOT NULL;
CREATE INDEX objects_audit ON source_objects(audit_due_ms,object_id);

CREATE TABLE usage_observations (
 observation_id INTEGER PRIMARY KEY AUTOINCREMENT,
 identity_family TEXT NOT NULL, namespace_key TEXT NOT NULL,
 identity_key TEXT COLLATE BINARY,
 candidate_key TEXT NOT NULL,
 normalized_hash BLOB NOT NULL CHECK(length(normalized_hash)=32),
 object_id INTEGER NOT NULL REFERENCES source_objects(object_id),
 content_generation TEXT NOT NULL, record_key TEXT NOT NULL,
 parser_version TEXT NOT NULL,
 identity_version INTEGER NOT NULL CHECK(identity_version>0),
 semantics_version INTEGER NOT NULL CHECK(semantics_version>0),
 observed_ms INTEGER NOT NULL,
 eligibility TEXT NOT NULL CHECK(eligibility IN('eligible','identity_unknown','time_unknown','inconsistent','identity_only')),
 action TEXT NOT NULL CHECK(action IN('upsert','retract')),
 revision_authority TEXT, revision_no INTEGER CHECK(revision_no>=0),
 fact_kind TEXT NOT NULL CHECK(fact_kind IN('request','day','session_cost','session_total')),
 report_source TEXT NOT NULL, native_tool TEXT NOT NULL,
 model_key TEXT NOT NULL, model_display TEXT,
 session_key TEXT,
 native_session_id TEXT, native_message_id TEXT, native_request_id TEXT,
 occurred_ms INTEGER,
 native_date TEXT, native_timezone TEXT, day_start_ms INTEGER, day_end_ms INTEGER,
 input_uncached INTEGER CHECK(input_uncached>=0), cache_read INTEGER CHECK(cache_read>=0),
 cache_write INTEGER CHECK(cache_write>=0), output_normal INTEGER CHECK(output_normal>=0),
 output_reasoning INTEGER CHECK(output_reasoning>=0),
 input_total INTEGER CHECK(input_total>=0), output_total INTEGER CHECK(output_total>=0),
 total_tokens INTEGER CHECK(total_tokens>=0), cache_write_1h INTEGER CHECK(cache_write_1h>=0),
 request_count INTEGER CHECK(request_count>=0),
 cost_state TEXT NOT NULL CHECK(cost_state IN('known','unknown','invalid')),
 cost_units INTEGER, cost_scale INTEGER, cost_currency TEXT,
 UNIQUE(identity_family,namespace_key,candidate_key,normalized_hash),
 CHECK((revision_authority IS NULL AND revision_no IS NULL) OR (revision_authority IS NOT NULL AND revision_no IS NOT NULL)),
 CHECK(action<>'retract' OR (identity_key IS NOT NULL AND revision_no IS NOT NULL)),
 CHECK((cost_state='known' AND cost_units IS NOT NULL AND cost_scale IS NOT NULL AND cost_scale BETWEEN 0 AND 18 AND cost_currency IS NOT NULL)
    OR (cost_state IN('unknown','invalid') AND cost_units IS NULL AND cost_scale IS NULL AND cost_currency IS NULL)),
 CHECK((day_start_ms IS NULL AND day_end_ms IS NULL) OR (day_start_ms IS NOT NULL AND day_end_ms IS NOT NULL AND day_end_ms>day_start_ms)),
 CHECK(eligibility<>'eligible' OR identity_key IS NOT NULL),
 CHECK(eligibility<>'eligible' OR action='retract' OR fact_kind<>'request' OR occurred_ms IS NOT NULL),
 CHECK(eligibility<>'eligible' OR action='retract' OR fact_kind<>'day' OR
       (native_date IS NOT NULL AND native_timezone IS NOT NULL AND day_start_ms IS NOT NULL AND day_end_ms IS NOT NULL)),
 CHECK(fact_kind NOT IN('session_cost','session_total') OR session_key IS NOT NULL)
) STRICT;
CREATE INDEX observations_identity ON usage_observations(identity_family,namespace_key,identity_key,observation_id);
CREATE INDEX observations_unpublished ON usage_observations(identity_family,namespace_key,observation_id,identity_key);
CREATE INDEX observations_object ON usage_observations(object_id,observation_id);
CREATE INDEX observations_time ON usage_observations(occurred_ms,report_source,identity_key);
CREATE INDEX observations_pending_day ON usage_observations(report_source,native_date,observation_id) WHERE fact_kind='day';
CREATE INDEX observations_session ON usage_observations(identity_family,namespace_key,session_key,observation_id);
CREATE INDEX observations_unresolved ON usage_observations(report_source,identity_family,namespace_key,identity_key) WHERE eligibility IN('identity_unknown','time_unknown','inconsistent');
CREATE INDEX observations_saved ON usage_observations(report_source,observed_ms);
CREATE TRIGGER observations_immutable BEFORE UPDATE ON usage_observations
 BEGIN SELECT RAISE(ABORT,'immutable observation'); END;

CREATE TABLE usage_events (
 identity_family TEXT NOT NULL, namespace_key TEXT NOT NULL,
 identity_key TEXT NOT NULL COLLATE BINARY,
 selected_observation_id INTEGER UNIQUE REFERENCES usage_observations(observation_id),
 state TEXT NOT NULL CHECK(state IN('active','retracted','unresolved')),
 has_conflict INTEGER NOT NULL CHECK(has_conflict IN(0,1)),
 fact_kind TEXT NOT NULL CHECK(fact_kind IN('request','day','session_cost','session_total')),
 report_source TEXT, native_tool TEXT, model_key TEXT, session_key TEXT,
 occurred_ms INTEGER, day_start_ms INTEGER, day_end_ms INTEGER,
 event_revision INTEGER NOT NULL CHECK(event_revision>0),
 updated_commit_seq INTEGER NOT NULL CHECK(updated_commit_seq>=0),
 model_display TEXT,
 PRIMARY KEY(identity_family,namespace_key,identity_key),
 CHECK((state='unresolved' AND selected_observation_id IS NULL) OR (state<>'unresolved' AND selected_observation_id IS NOT NULL)),
 CHECK(state<>'active' OR (report_source IS NOT NULL AND native_tool IS NOT NULL AND model_key IS NOT NULL)),
 CHECK(state<>'active' OR fact_kind<>'request' OR occurred_ms IS NOT NULL),
 CHECK(state<>'active' OR fact_kind<>'day' OR (day_start_ms IS NOT NULL AND day_end_ms IS NOT NULL AND day_end_ms>day_start_ms)),
 CHECK(state<>'active' OR fact_kind NOT IN('session_cost','session_total') OR session_key IS NOT NULL)
) STRICT;
CREATE INDEX events_time ON usage_events(occurred_ms) WHERE state='active' AND fact_kind='request';
CREATE INDEX events_source_time ON usage_events(report_source,occurred_ms) WHERE state='active' AND fact_kind='request';
CREATE INDEX events_model_time ON usage_events(model_key,occurred_ms) WHERE state='active' AND fact_kind='request';
CREATE INDEX events_coverage ON usage_events(native_tool,namespace_key,occurred_ms) WHERE state='active' AND fact_kind='request';
CREATE INDEX events_day ON usage_events(native_tool,namespace_key,day_start_ms,day_end_ms) WHERE state='active' AND fact_kind='day';
CREATE INDEX events_session ON usage_events(identity_family,namespace_key,session_key,fact_kind,occurred_ms) WHERE state='active';
CREATE INDEX events_session_totals ON usage_events(identity_family,namespace_key,identity_key) WHERE state='active' AND fact_kind='session_total';
CREATE INDEX events_session_costs ON usage_events(identity_family,namespace_key,session_key) WHERE state='active' AND fact_kind='session_cost';
CREATE INDEX events_unresolved ON usage_events(identity_family,namespace_key,identity_key,report_source,has_conflict) WHERE state='unresolved';
CREATE INDEX events_model_display ON usage_events(model_key,model_display,report_source,fact_kind) WHERE state='active';

CREATE TABLE usage_rollups (
 derived_version INTEGER NOT NULL CHECK(derived_version>0),
 grain TEXT NOT NULL CHECK(grain IN('day','hour')),
 bucket_start_ms INTEGER NOT NULL, bucket_end_ms INTEGER NOT NULL CHECK(bucket_end_ms>bucket_start_ms),
 local_day TEXT NOT NULL, report_source TEXT NOT NULL, model_key TEXT NOT NULL,
 event_count INTEGER NOT NULL CHECK(event_count>=0),
 bucket_revision INTEGER NOT NULL CHECK(bucket_revision>=0),
 hour_cache_revision INTEGER CHECK(hour_cache_revision>=0),
 input_uncached_known_sum INTEGER NOT NULL CHECK(input_uncached_known_sum>=0),
 input_uncached_known_count INTEGER NOT NULL CHECK(input_uncached_known_count>=0), input_uncached_exact_count INTEGER NOT NULL CHECK(input_uncached_exact_count>=0),
 cache_read_known_sum INTEGER NOT NULL CHECK(cache_read_known_sum>=0),
 cache_read_known_count INTEGER NOT NULL CHECK(cache_read_known_count>=0), cache_read_exact_count INTEGER NOT NULL CHECK(cache_read_exact_count>=0),
 cache_write_known_sum INTEGER NOT NULL CHECK(cache_write_known_sum>=0),
 cache_write_known_count INTEGER NOT NULL CHECK(cache_write_known_count>=0), cache_write_exact_count INTEGER NOT NULL CHECK(cache_write_exact_count>=0),
 output_normal_known_sum INTEGER NOT NULL CHECK(output_normal_known_sum>=0),
 output_normal_known_count INTEGER NOT NULL CHECK(output_normal_known_count>=0), output_normal_exact_count INTEGER NOT NULL CHECK(output_normal_exact_count>=0),
 output_reasoning_known_sum INTEGER NOT NULL CHECK(output_reasoning_known_sum>=0),
 output_reasoning_known_count INTEGER NOT NULL CHECK(output_reasoning_known_count>=0), output_reasoning_exact_count INTEGER NOT NULL CHECK(output_reasoning_exact_count>=0),
 input_total_known_sum INTEGER NOT NULL CHECK(input_total_known_sum>=0),
 input_total_known_count INTEGER NOT NULL CHECK(input_total_known_count>=0), input_total_exact_count INTEGER NOT NULL CHECK(input_total_exact_count>=0),
 output_total_known_sum INTEGER NOT NULL CHECK(output_total_known_sum>=0),
 output_total_known_count INTEGER NOT NULL CHECK(output_total_known_count>=0), output_total_exact_count INTEGER NOT NULL CHECK(output_total_exact_count>=0),
 total_tokens_known_sum INTEGER NOT NULL CHECK(total_tokens_known_sum>=0),
 total_tokens_known_count INTEGER NOT NULL CHECK(total_tokens_known_count>=0), total_tokens_exact_count INTEGER NOT NULL CHECK(total_tokens_exact_count>=0),
 cache_write_1h_known_sum INTEGER NOT NULL CHECK(cache_write_1h_known_sum>=0),
 cache_write_1h_known_count INTEGER NOT NULL CHECK(cache_write_1h_known_count>=0), cache_write_1h_exact_count INTEGER NOT NULL CHECK(cache_write_1h_exact_count>=0),
 request_count_known_sum INTEGER NOT NULL CHECK(request_count_known_sum>=0),
 request_count_known_count INTEGER NOT NULL CHECK(request_count_known_count>=0), request_count_exact_count INTEGER NOT NULL CHECK(request_count_exact_count>=0),
 PRIMARY KEY(derived_version,grain,bucket_start_ms,report_source,model_key),
 CHECK(grain='day' OR hour_cache_revision IS NULL),
 CHECK(input_uncached_exact_count<=input_uncached_known_count AND input_uncached_known_count<=event_count AND (input_uncached_known_count>0 OR input_uncached_known_sum=0)),
 CHECK(cache_read_exact_count<=cache_read_known_count AND cache_read_known_count<=event_count AND (cache_read_known_count>0 OR cache_read_known_sum=0)),
 CHECK(cache_write_exact_count<=cache_write_known_count AND cache_write_known_count<=event_count AND (cache_write_known_count>0 OR cache_write_known_sum=0)),
 CHECK(output_normal_exact_count<=output_normal_known_count AND output_normal_known_count<=event_count AND (output_normal_known_count>0 OR output_normal_known_sum=0)),
 CHECK(output_reasoning_exact_count<=output_reasoning_known_count AND output_reasoning_known_count<=event_count AND (output_reasoning_known_count>0 OR output_reasoning_known_sum=0)),
 CHECK(input_total_exact_count<=input_total_known_count AND input_total_known_count<=event_count AND (input_total_known_count>0 OR input_total_known_sum=0)),
 CHECK(output_total_exact_count<=output_total_known_count AND output_total_known_count<=event_count AND (output_total_known_count>0 OR output_total_known_sum=0)),
 CHECK(total_tokens_exact_count<=total_tokens_known_count AND total_tokens_known_count<=event_count AND (total_tokens_known_count>0 OR total_tokens_known_sum=0)),
 CHECK(cache_write_1h_exact_count<=cache_write_1h_known_count AND cache_write_1h_known_count<=event_count AND (cache_write_1h_known_count>0 OR cache_write_1h_known_sum=0)),
 CHECK(request_count_exact_count<=request_count_known_count AND request_count_known_count<=event_count AND (request_count_known_count>0 OR request_count_known_sum=0))
 ) STRICT;
CREATE INDEX rollups_window ON usage_rollups(derived_version,grain,local_day,report_source,model_key);

CREATE TABLE suppression_rules (
 rule_id INTEGER PRIMARY KEY AUTOINCREMENT,
 kind TEXT NOT NULL CHECK(kind IN('source','window','identity','identity_link','session','coverage_interval','coverage_family')),
 report_source TEXT, identity_family TEXT, namespace_key TEXT, identity_key TEXT, session_key TEXT,
 link_version INTEGER, link_key TEXT,
 native_tool TEXT, start_ms INTEGER, end_ms INTEGER,
 operation_id TEXT NOT NULL, created_ms INTEGER NOT NULL,
 CHECK((start_ms IS NULL AND end_ms IS NULL) OR (start_ms IS NOT NULL AND end_ms IS NOT NULL AND end_ms>start_ms)),
 CHECK(
  (kind='source' AND report_source IS NOT NULL AND native_tool IS NOT NULL AND identity_family IS NULL AND namespace_key IS NULL AND identity_key IS NULL AND start_ms IS NULL)
  OR (kind='window' AND identity_family IS NULL AND namespace_key IS NULL AND identity_key IS NULL AND start_ms IS NOT NULL AND end_ms IS NOT NULL)
  OR (kind='session' AND identity_family IS NOT NULL AND namespace_key IS NOT NULL AND session_key IS NOT NULL AND identity_key IS NULL AND report_source IS NULL AND native_tool IS NULL AND start_ms IS NULL)
  OR (kind='identity' AND identity_family IS NOT NULL AND namespace_key IS NOT NULL AND identity_key IS NOT NULL AND report_source IS NULL AND native_tool IS NULL AND start_ms IS NULL)
  OR (kind='identity_link' AND identity_family IS NOT NULL AND namespace_key IS NOT NULL AND identity_key IS NULL AND report_source IS NULL AND native_tool IS NULL AND start_ms IS NULL)
  OR (kind='coverage_interval' AND native_tool IS NOT NULL AND namespace_key IS NOT NULL AND start_ms IS NOT NULL AND end_ms IS NOT NULL AND report_source IS NULL AND identity_family IS NULL AND identity_key IS NULL)
  OR (kind='coverage_family' AND native_tool IS NOT NULL AND namespace_key IS NOT NULL AND start_ms IS NULL AND report_source IS NULL AND identity_family IS NULL AND identity_key IS NULL)
 ),
 CHECK(kind='session' OR session_key IS NULL),
 CHECK((kind='identity_link' AND link_version IS NOT NULL AND link_version=1 AND link_key IS NOT NULL AND length(link_key)=64 AND link_key NOT GLOB '*[^0-9a-f]*')
    OR (kind<>'identity_link' AND link_version IS NULL AND link_key IS NULL)),
 CHECK(kind<>'window' OR (report_source IS NULL AND native_tool IS NULL) OR (report_source IS NOT NULL AND native_tool IS NOT NULL))
) STRICT;
CREATE UNIQUE INDEX suppression_session ON suppression_rules(identity_family,namespace_key,session_key) WHERE kind='session';
CREATE UNIQUE INDEX suppression_identity ON suppression_rules(identity_family,namespace_key,identity_key) WHERE kind='identity';
CREATE UNIQUE INDEX suppression_link ON suppression_rules(identity_family,namespace_key,link_version,link_key) WHERE kind='identity_link';
CREATE UNIQUE INDEX suppression_source ON suppression_rules(report_source,native_tool) WHERE kind='source';
CREATE UNIQUE INDEX suppression_family ON suppression_rules(native_tool,namespace_key) WHERE kind='coverage_family';
CREATE UNIQUE INDEX suppression_interval ON suppression_rules(native_tool,namespace_key,start_ms,end_ms) WHERE kind='coverage_interval';
CREATE INDEX suppression_window ON suppression_rules(kind,report_source,native_tool,start_ms,end_ms);
CREATE INDEX suppression_coverage ON suppression_rules(native_tool,namespace_key,kind,start_ms,end_ms);

CREATE TABLE identity_alias (
 identity_family TEXT NOT NULL, namespace_key TEXT NOT NULL,
 alias_key TEXT NOT NULL CHECK(length(alias_key)=64 AND alias_key NOT GLOB '*[^0-9a-f]*'),
 canonical_key TEXT NOT NULL CHECK(length(canonical_key)=64 AND canonical_key NOT GLOB '*[^0-9a-f]*'),
 identity_version INTEGER NOT NULL CHECK(identity_version>0),
 bound_commit_seq INTEGER NOT NULL CHECK(bound_commit_seq>=0),
 PRIMARY KEY(identity_family,namespace_key,alias_key),
 FOREIGN KEY(identity_family,namespace_key,canonical_key)
  REFERENCES identity_alias(identity_family,namespace_key,alias_key)
) STRICT;
CREATE INDEX alias_canonical ON identity_alias(identity_family,namespace_key,canonical_key,alias_key);
CREATE INDEX observations_claude_session_message ON usage_observations(identity_family,namespace_key,native_session_id,native_message_id,native_request_id,observation_id) WHERE identity_family='claude' AND native_session_id IS NOT NULL AND native_message_id IS NOT NULL;
CREATE INDEX observations_claude_message_request ON usage_observations(identity_family,namespace_key,native_message_id,native_request_id,native_session_id,observation_id) WHERE identity_family='claude' AND native_message_id IS NOT NULL;
CREATE TRIGGER alias_insert_one_hop BEFORE INSERT ON identity_alias
WHEN NEW.alias_key<>NEW.canonical_key AND NOT EXISTS (
 SELECT 1 FROM identity_alias a WHERE a.identity_family=NEW.identity_family
 AND a.namespace_key=NEW.namespace_key AND a.alias_key=NEW.canonical_key AND a.canonical_key=a.alias_key)
BEGIN SELECT RAISE(ABORT,'alias target must be canonical'); END;
CREATE TRIGGER alias_key_immutable BEFORE UPDATE OF identity_family,namespace_key,alias_key ON identity_alias
BEGIN SELECT RAISE(ABORT,'alias key is immutable'); END;
CREATE TRIGGER alias_update_one_hop BEFORE UPDATE OF canonical_key ON identity_alias
WHEN NEW.alias_key<>NEW.canonical_key AND (
 NOT EXISTS (SELECT 1 FROM identity_alias a WHERE a.identity_family=NEW.identity_family
 AND a.namespace_key=NEW.namespace_key AND a.alias_key=NEW.canonical_key AND a.canonical_key=a.alias_key)
 OR EXISTS (SELECT 1 FROM identity_alias child WHERE child.identity_family=OLD.identity_family
 AND child.namespace_key=OLD.namespace_key AND child.canonical_key=OLD.alias_key AND child.alias_key<>OLD.alias_key))
BEGIN SELECT RAISE(ABORT,'alias target or children must remain one hop'); END;
