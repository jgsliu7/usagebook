-- Restartable compatibility capture; disposable and never evidence adoption.
CREATE TABLE IF NOT EXISTS legacy_capture_state (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 ledger_uuid TEXT NOT NULL, epoch TEXT NOT NULL,
 generation TEXT NOT NULL, metadata_json TEXT NOT NULL CHECK(json_valid(metadata_json))
) STRICT;
CREATE TABLE IF NOT EXISTS legacy_capture_entries (
 ordinal INTEGER PRIMARY KEY,
 report_source TEXT NOT NULL,
 entry_json TEXT NOT NULL CHECK(json_valid(entry_json))
) STRICT;
CREATE INDEX IF NOT EXISTS legacy_capture_source ON legacy_capture_entries(report_source);
