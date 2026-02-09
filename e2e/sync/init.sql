-- Postgres schema for bidirectional DS sync.
-- Runs automatically on container start via docker-entrypoint-initdb.d.

-- PG -> Stream direction: source table for structured data.
-- Changes to this table are replicated via Electric Shape API into a DS stream.
CREATE TABLE IF NOT EXISTS items (
  id SERIAL PRIMARY KEY,
  title TEXT NOT NULL,
  body TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Stream -> PG direction: sink table for session events.
-- The sync service consumes DS stream events via SSE and inserts them here.
CREATE TABLE IF NOT EXISTS session_events (
  id SERIAL PRIMARY KEY,
  stream_name TEXT NOT NULL,
  event_key TEXT,
  event_type TEXT,
  operation TEXT,
  payload JSONB NOT NULL,
  ds_offset TEXT,
  received_at TIMESTAMPTZ DEFAULT NOW()
);
