-- Disposable normalized compatibility projection. It never creates ledger evidence.
CREATE TABLE IF NOT EXISTS query_live_state (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    ledger_uuid TEXT NOT NULL,
    epoch TEXT NOT NULL,
    generation TEXT NOT NULL,
    routing_hash TEXT NOT NULL,
    sources_json TEXT NOT NULL CHECK(json_valid(sources_json))
) STRICT;
CREATE TABLE IF NOT EXISTS query_live_facts (
    row_id INTEGER PRIMARY KEY,
    report_source TEXT NOT NULL,
    fact_kind TEXT NOT NULL CHECK(fact_kind IN ('request','day','session_total')),
    model_key TEXT NOT NULL,
    occurred_ms INTEGER,
    payload_json TEXT NOT NULL CHECK(json_valid(payload_json))
) STRICT;
CREATE INDEX IF NOT EXISTS query_live_source_time ON query_live_facts(report_source,fact_kind,occurred_ms);
