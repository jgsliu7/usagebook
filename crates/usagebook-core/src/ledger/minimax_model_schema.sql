-- Model attribution only: never message content, prompts, workspaces or secrets.
CREATE TABLE IF NOT EXISTS minimax_model_files (
 path TEXT PRIMARY KEY, root_path TEXT NOT NULL, epoch TEXT NOT NULL,
 session_id TEXT NOT NULL, identity TEXT NOT NULL,
 byte_length INTEGER NOT NULL, mtime_ns TEXT NOT NULL,
 saved_offset INTEGER NOT NULL, anchor BLOB NOT NULL,
 audit_ms INTEGER NOT NULL
) STRICT;
CREATE TABLE IF NOT EXISTS minimax_model_rows (
 path TEXT NOT NULL REFERENCES minimax_model_files(path) ON DELETE CASCADE,
 line_offset INTEGER NOT NULL, turn_id TEXT, timestamp_ms INTEGER, model TEXT NOT NULL,
 PRIMARY KEY(path,line_offset)
) STRICT;
CREATE INDEX IF NOT EXISTS minimax_model_root ON minimax_model_files(root_path,session_id);
