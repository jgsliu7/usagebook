-- Rebuildable source-reader projections. Epoch binding prevents an old derived
-- cache from bypassing forget/restore controls. No prompt/response bodies.
CREATE TABLE IF NOT EXISTS source_projection_scopes (
    root_id TEXT PRIMARY KEY REFERENCES source_roots(root_id) ON DELETE CASCADE,
    epoch TEXT NOT NULL,
    capability_version TEXT NOT NULL,
    report_source TEXT NOT NULL,
    layout_json TEXT NOT NULL,
    complete INTEGER NOT NULL DEFAULT 0 CHECK(complete IN(0,1)),
    observed_ms INTEGER NOT NULL,
    reader_domain TEXT,
    error_code TEXT
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS source_projection_rows (
    root_id TEXT NOT NULL REFERENCES source_projection_scopes(root_id) ON DELETE CASCADE,
    table_name TEXT NOT NULL,
    row_key TEXT NOT NULL,
    projection_json TEXT NOT NULL,
    row_hash TEXT NOT NULL,
    observed_ms INTEGER NOT NULL,
    PRIMARY KEY(root_id,table_name,row_key)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS source_projection_source ON source_projection_scopes(report_source,complete);
-- Irreversible physical-row suppression survives cache invalidation and restore.
-- It contains neither the native key nor a usage value, timestamp or model.
CREATE TABLE IF NOT EXISTS source_projection_tombstones (
    root_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    row_key_hash TEXT NOT NULL,
    PRIMARY KEY(root_id,table_name,row_key_hash)
) WITHOUT ROWID;
