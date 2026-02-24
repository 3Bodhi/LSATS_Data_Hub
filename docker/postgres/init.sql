-- LSATS Database Initialization Script
-- This script sets up the foundational structure for the Bronze-Silver-Gold data architecture

-- Create extensions we'll need
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";  -- For generating UUIDs
CREATE EXTENSION IF NOT EXISTS "pg_trgm";    -- For fuzzy text matching (master record reconciliation)

-- Create schemas to organize our data layers
CREATE SCHEMA IF NOT EXISTS bronze;   -- Raw data from all sources
CREATE SCHEMA IF NOT EXISTS silver;   -- Cleaned, standardized data
CREATE SCHEMA IF NOT EXISTS gold;     -- Master records and golden truth
CREATE SCHEMA IF NOT EXISTS meta;     -- Metadata and system tracking

-- Grant permissions to our application user
GRANT USAGE ON SCHEMA bronze TO lsats_user;
GRANT USAGE ON SCHEMA silver TO lsats_user;
GRANT USAGE ON SCHEMA gold TO lsats_user;
GRANT USAGE ON SCHEMA meta TO lsats_user;

GRANT CREATE ON SCHEMA bronze TO lsats_user;
GRANT CREATE ON SCHEMA silver TO lsats_user;
GRANT CREATE ON SCHEMA gold TO lsats_user;
GRANT CREATE ON SCHEMA meta TO lsats_user;

-- Create a function to update the 'updated_at' timestamp automatically
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create a function to generate consistent entity IDs
-- This helps us track the same entity across different sources
CREATE OR REPLACE FUNCTION generate_entity_hash(
    entity_type VARCHAR,
    source_system VARCHAR,
    external_id VARCHAR
) RETURNS VARCHAR AS $$
BEGIN
    RETURN encode(sha256((entity_type || '|' || source_system || '|' || external_id)::bytea), 'hex');
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- System metadata table for tracking ingestion runs
CREATE TABLE meta.ingestion_runs (
    run_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_system VARCHAR(50) NOT NULL,
    entity_type VARCHAR(50) NOT NULL,
    started_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP WITH TIME ZONE,
    status VARCHAR(20) DEFAULT 'running' CHECK (status IN ('running', 'completed', 'failed')),
    records_processed INTEGER DEFAULT 0,
    records_created INTEGER DEFAULT 0,
    records_updated INTEGER DEFAULT 0,
    error_message TEXT,
    metadata JSONB DEFAULT '{}'::jsonb
);

-- Index for quick lookups of recent runs
CREATE INDEX idx_ingestion_runs_recent ON meta.ingestion_runs (source_system, entity_type, started_at DESC);

-- Create a view to easily see current ingestion status
CREATE VIEW meta.current_ingestion_status AS
SELECT
    source_system,
    entity_type,
    MAX(started_at) as last_run,
    (SELECT status FROM meta.ingestion_runs ir2
     WHERE ir2.source_system = ir1.source_system
     AND ir2.entity_type = ir1.entity_type
     AND ir2.started_at = MAX(ir1.started_at)) as last_status,
    (SELECT records_processed FROM meta.ingestion_runs ir3
     WHERE ir3.source_system = ir1.source_system
     AND ir3.entity_type = ir1.entity_type
     AND ir3.started_at = MAX(ir1.started_at)) as last_records_processed
FROM meta.ingestion_runs ir1
GROUP BY source_system, entity_type
ORDER BY last_run DESC;

-- Daemon action log table for tracking idempotent ticket processing
CREATE TABLE meta.daemon_action_log (
    log_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ticket_id INTEGER NOT NULL,
    action_type VARCHAR(100) NOT NULL,
    action_id VARCHAR(255) NOT NULL,
    action_hash VARCHAR(64),
    executed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) NOT NULL DEFAULT 'completed',
    error_message TEXT,
    metadata JSONB DEFAULT '{}'::jsonb,
    CONSTRAINT unique_ticket_action UNIQUE (ticket_id, action_id)
);

-- Indexes for daemon action log
CREATE INDEX idx_daemon_log_ticket ON meta.daemon_action_log (ticket_id);
CREATE INDEX idx_daemon_log_action_type ON meta.daemon_action_log (action_type);
CREATE INDEX idx_daemon_log_executed_at ON meta.daemon_action_log (executed_at DESC);
CREATE INDEX idx_daemon_log_status ON meta.daemon_action_log (status);
CREATE INDEX idx_daemon_log_ticket_status ON meta.daemon_action_log (ticket_id, status);
CREATE INDEX idx_daemon_log_metadata_gin ON meta.daemon_action_log USING gin (metadata);

-- Daemon activity summary view
CREATE VIEW meta.daemon_activity_summary AS
SELECT
    action_type,
    status,
    COUNT(*) as action_count,
    MAX(executed_at) as last_executed,
    MIN(executed_at) as first_executed
FROM meta.daemon_action_log
GROUP BY action_type, status
ORDER BY action_type, status;

-- Daemon recent activity view (last 24 hours)
CREATE VIEW meta.daemon_recent_activity AS
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

-- Grant access to the metadata tables
GRANT ALL ON ALL TABLES IN SCHEMA meta TO lsats_user;
GRANT ALL ON ALL SEQUENCES IN SCHEMA meta TO lsats_user;

-- Add some helpful comments
COMMENT ON SCHEMA bronze IS 'Raw data exactly as received from source systems';
COMMENT ON SCHEMA silver IS 'Cleaned and standardized data ready for analysis';
COMMENT ON SCHEMA gold IS 'Master records representing authoritative truth';
COMMENT ON SCHEMA meta IS 'System metadata and ingestion tracking';

COMMENT ON FUNCTION generate_entity_hash IS 'Creates consistent hashes for tracking entities across sources';
COMMENT ON TABLE meta.ingestion_runs IS 'Tracks all data ingestion operations for monitoring and debugging';
COMMENT ON TABLE meta.daemon_action_log IS 'Tracks all actions performed by the ticket queue daemon for idempotent execution';
COMMENT ON COLUMN meta.daemon_action_log.action_id IS 'Unique identifier format: {action_type}:{content_hash}:{version}';
COMMENT ON COLUMN meta.daemon_action_log.action_hash IS 'SHA256 hash of action configuration for content-aware idempotency';
COMMENT ON COLUMN meta.daemon_action_log.status IS 'Action execution status: completed, failed, or skipped';
COMMENT ON VIEW meta.daemon_activity_summary IS 'Summary view of daemon activity by action type and status';
COMMENT ON VIEW meta.daemon_recent_activity IS 'Shows daemon activity from the last 24 hours';

-- ============================================================================
-- NOTE: Silver layer views are loaded by 03-views.sql
-- (must run after 02-schemas.sql creates all tables)
-- ============================================================================
