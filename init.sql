-- ═══════════════════════════════════════════════════════
--  init.sql  –  PostgreSQL initialisation script
--  Runs automatically when the postgres container first boots.
-- ═══════════════════════════════════════════════════════

-- Table that stores all critical operations written by the elected leader
CREATE TABLE IF NOT EXISTS distributed_ops (
    id         SERIAL PRIMARY KEY,
    written_by TEXT         NOT NULL,       -- which client wrote this row
    operation  TEXT         NOT NULL,       -- SQL or operation description
    ts         TIMESTAMPTZ  DEFAULT NOW()
);

-- Index for the visualiser's ORDER BY ts DESC queries
CREATE INDEX IF NOT EXISTS idx_distributed_ops_ts ON distributed_ops (ts DESC);

-- Optional: separate audit log table for leader-change events
CREATE TABLE IF NOT EXISTS leader_events (
    id         SERIAL PRIMARY KEY,
    event_type TEXT         NOT NULL,       -- 'acquired' | 'released' | 'crashed'
    client_id  TEXT         NOT NULL,
    ts         TIMESTAMPTZ  DEFAULT NOW()
);

-- Seed a first row so the visualiser shows something immediately
INSERT INTO distributed_ops (written_by, operation)
VALUES ('system', 'init_schema');
