-- Small, versioned operational records; never source payloads or credentials.
CREATE TABLE ledger_runtime_state (
 state_key TEXT PRIMARY KEY CHECK(length(state_key)<=512),
 value_json TEXT NOT NULL CHECK(length(CAST(value_json AS BLOB))<=65536 AND json_valid(value_json))
) STRICT;
