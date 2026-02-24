-- ============================================================================
-- LSATS Data Hub — Production Initialization Script
-- ============================================================================
--
-- Purpose:
--   Server-safe equivalent of docker/postgres/init.sql. Contains extensions,
--   schemas, grants, helper functions, and meta tables only.
--
--   Does NOT include:
--     - \i directives (Docker-specific)
--     - silver schema tables/indexes (run production_schema.sql after this)
--     - silver views (run docker/postgres/views/silver_views.sql after that)
--
-- Execution order on a fresh PostgreSQL instance:
--   1. psql -U lsats_user -d lsats_db -f production_init.sql
--   2. psql -U lsats_user -d lsats_db -f production_schema.sql
--   3. psql -U lsats_user -d lsats_db -f docker/postgres/views/silver_views.sql
--
-- Idempotent: safe to re-run (all objects use IF NOT EXISTS / OR REPLACE).
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Extensions
-- ----------------------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";  -- UUID generation
CREATE EXTENSION IF NOT EXISTS "pg_trgm";    -- Fuzzy text matching

-- ----------------------------------------------------------------------------
-- Schemas
-- ----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS bronze;   -- Raw data from all sources
CREATE SCHEMA IF NOT EXISTS silver;   -- Cleaned, standardized data
CREATE SCHEMA IF NOT EXISTS gold;     -- Master records and golden truth
CREATE SCHEMA IF NOT EXISTS meta;     -- Metadata and system tracking

-- ----------------------------------------------------------------------------
-- Schema grants for application user
-- ----------------------------------------------------------------------------
GRANT USAGE  ON SCHEMA bronze TO lsats_user;
GRANT USAGE  ON SCHEMA silver TO lsats_user;
GRANT USAGE  ON SCHEMA gold   TO lsats_user;
GRANT USAGE  ON SCHEMA meta   TO lsats_user;

GRANT CREATE ON SCHEMA bronze TO lsats_user;
GRANT CREATE ON SCHEMA silver TO lsats_user;
GRANT CREATE ON SCHEMA gold   TO lsats_user;
GRANT CREATE ON SCHEMA meta   TO lsats_user;

-- ----------------------------------------------------------------------------
-- Helper functions
-- ----------------------------------------------------------------------------

-- Automatically update updated_at on row modification
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Generate a consistent SHA-256 hash for entity identity across sources
CREATE OR REPLACE FUNCTION generate_entity_hash(
    entity_type VARCHAR,
    source_system VARCHAR,
    external_id VARCHAR
) RETURNS VARCHAR AS $$
BEGIN
    RETURN encode(sha256((entity_type || '|' || source_system || '|' || external_id)::bytea), 'hex');
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- ----------------------------------------------------------------------------
-- Meta: ingestion_runs
-- Tracks all data ingestion operations for monitoring and debugging
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS meta.ingestion_runs (
    run_id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_system     VARCHAR(50)  NOT NULL,
    entity_type       VARCHAR(50)  NOT NULL,
    started_at        TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    completed_at      TIMESTAMP WITH TIME ZONE,
    status            VARCHAR(20)  DEFAULT 'running'
                          CHECK (status IN ('running', 'completed', 'failed')),
    records_processed INTEGER      DEFAULT 0,
    records_created   INTEGER      DEFAULT 0,
    records_updated   INTEGER      DEFAULT 0,
    error_message     TEXT,
    metadata          JSONB        DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_ingestion_runs_recent
    ON meta.ingestion_runs (source_system, entity_type, started_at DESC);

-- ----------------------------------------------------------------------------
-- Meta: daemon_action_log
-- Tracks all actions performed by the ticket queue daemon for idempotent execution
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS meta.daemon_action_log (
    log_id      UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    ticket_id   INTEGER     NOT NULL,
    action_type VARCHAR(100) NOT NULL,
    action_id   VARCHAR(255) NOT NULL,
    action_hash VARCHAR(64),
    executed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    status      VARCHAR(20)  NOT NULL DEFAULT 'completed',
    error_message TEXT,
    metadata    JSONB        DEFAULT '{}'::jsonb,
    CONSTRAINT unique_ticket_action UNIQUE (ticket_id, action_id)
);

CREATE INDEX IF NOT EXISTS idx_daemon_log_ticket
    ON meta.daemon_action_log (ticket_id);
CREATE INDEX IF NOT EXISTS idx_daemon_log_action_type
    ON meta.daemon_action_log (action_type);
CREATE INDEX IF NOT EXISTS idx_daemon_log_executed_at
    ON meta.daemon_action_log (executed_at DESC);
CREATE INDEX IF NOT EXISTS idx_daemon_log_status
    ON meta.daemon_action_log (status);
CREATE INDEX IF NOT EXISTS idx_daemon_log_ticket_status
    ON meta.daemon_action_log (ticket_id, status);
CREATE INDEX IF NOT EXISTS idx_daemon_log_metadata_gin
    ON meta.daemon_action_log USING gin (metadata);

-- ----------------------------------------------------------------------------
-- Meta: views
-- ----------------------------------------------------------------------------

CREATE OR REPLACE VIEW meta.current_ingestion_status AS
SELECT
    source_system,
    entity_type,
    MAX(started_at) AS last_run,
    (SELECT status FROM meta.ingestion_runs ir2
     WHERE ir2.source_system = ir1.source_system
       AND ir2.entity_type   = ir1.entity_type
       AND ir2.started_at    = MAX(ir1.started_at)) AS last_status,
    (SELECT records_processed FROM meta.ingestion_runs ir3
     WHERE ir3.source_system = ir1.source_system
       AND ir3.entity_type   = ir1.entity_type
       AND ir3.started_at    = MAX(ir1.started_at)) AS last_records_processed
FROM meta.ingestion_runs ir1
GROUP BY source_system, entity_type
ORDER BY last_run DESC;

CREATE OR REPLACE VIEW meta.daemon_activity_summary AS
SELECT
    action_type,
    status,
    COUNT(*)       AS action_count,
    MAX(executed_at) AS last_executed,
    MIN(executed_at) AS first_executed
FROM meta.daemon_action_log
GROUP BY action_type, status
ORDER BY action_type, status;

CREATE OR REPLACE VIEW meta.daemon_recent_activity AS
SELECT
    log_id,
    ticket_id,
    action_type,
    action_id,
    status,
    executed_at,
    error_message
FROM meta.daemon_action_log
WHERE executed_at >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
ORDER BY executed_at DESC;

-- ----------------------------------------------------------------------------
-- Meta: table grants
-- ----------------------------------------------------------------------------
GRANT ALL ON ALL TABLES    IN SCHEMA meta TO lsats_user;
GRANT ALL ON ALL SEQUENCES IN SCHEMA meta TO lsats_user;

-- ----------------------------------------------------------------------------
-- Schema comments
-- ----------------------------------------------------------------------------
COMMENT ON SCHEMA bronze IS 'Raw data exactly as received from source systems';
COMMENT ON SCHEMA silver IS 'Cleaned and standardized data ready for analysis';
COMMENT ON SCHEMA gold   IS 'Master records representing authoritative truth';
COMMENT ON SCHEMA meta   IS 'System metadata and ingestion tracking';

COMMENT ON FUNCTION generate_entity_hash IS
    'Creates consistent hashes for tracking entities across sources';
COMMENT ON TABLE meta.ingestion_runs IS
    'Tracks all data ingestion operations for monitoring and debugging';
COMMENT ON TABLE meta.daemon_action_log IS
    'Tracks all actions performed by the ticket queue daemon for idempotent execution';
COMMENT ON COLUMN meta.daemon_action_log.action_id IS
    'Unique identifier format: {action_type}:{content_hash}:{version}';
COMMENT ON COLUMN meta.daemon_action_log.action_hash IS
    'SHA256 hash of action configuration for content-aware idempotency';
COMMENT ON COLUMN meta.daemon_action_log.status IS
    'Action execution status: completed, failed, or skipped';
COMMENT ON VIEW meta.daemon_activity_summary IS
    'Summary view of daemon activity by action type and status';
COMMENT ON VIEW meta.daemon_recent_activity IS
    'Shows daemon activity from the last 24 hours';
