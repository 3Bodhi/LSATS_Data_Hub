-- Migration 008: Add daemon action log table to meta schema
-- Purpose: Track daemon actions for idempotent ticket processing
-- Created: 2025-11-19

-- Create daemon_action_log table in meta schema
CREATE TABLE IF NOT EXISTS meta.daemon_action_log (
    -- Primary identifier
    log_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Ticket identification
    ticket_id INTEGER NOT NULL,

    -- Action identification (composite uniqueness constraint)
    action_type VARCHAR(100) NOT NULL,      -- 'comment', 'status_change', 'assign', etc.
    action_id VARCHAR(255) NOT NULL,        -- Unique identifier: {type}:{content_hash}:{version}
    action_hash VARCHAR(64),                -- SHA256 hash of action configuration/content

    -- Execution tracking
    executed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) NOT NULL DEFAULT 'completed',  -- 'completed', 'failed', 'skipped'
    error_message TEXT,

    -- Additional metadata (for debugging and audit)
    metadata JSONB DEFAULT '{}'::jsonb,

    -- Prevent duplicate action execution on same ticket
    CONSTRAINT unique_ticket_action UNIQUE (ticket_id, action_id)
);

-- Indexes for efficient querying
CREATE INDEX idx_daemon_log_ticket ON meta.daemon_action_log (ticket_id);
CREATE INDEX idx_daemon_log_action_type ON meta.daemon_action_log (action_type);
CREATE INDEX idx_daemon_log_executed_at ON meta.daemon_action_log (executed_at DESC);
CREATE INDEX idx_daemon_log_status ON meta.daemon_action_log (status);
CREATE INDEX idx_daemon_log_ticket_status ON meta.daemon_action_log (ticket_id, status);
CREATE INDEX idx_daemon_log_metadata_gin ON meta.daemon_action_log USING gin (metadata);

-- Add comment explaining the table
COMMENT ON TABLE meta.daemon_action_log IS
'Tracks all actions performed by the ticket queue daemon for idempotent execution. Each action on a ticket is logged once to prevent duplicate processing.';

COMMENT ON COLUMN meta.daemon_action_log.action_id IS
'Unique identifier format: {action_type}:{content_hash}:{version}. If action configuration changes, hash changes, creating a new action_id.';

COMMENT ON COLUMN meta.daemon_action_log.action_hash IS
'SHA256 hash of action configuration (template, parameters, etc.). Used for content-aware idempotency.';

COMMENT ON COLUMN meta.daemon_action_log.status IS
'Action execution status: completed (success), failed (error occurred), skipped (already in desired state)';

-- Create view for monitoring daemon activity
CREATE OR REPLACE VIEW meta.daemon_activity_summary AS
SELECT
    action_type,
    status,
    COUNT(*) as action_count,
    MAX(executed_at) as last_executed,
    MIN(executed_at) as first_executed
FROM meta.daemon_action_log
GROUP BY action_type, status
ORDER BY action_type, status;

COMMENT ON VIEW meta.daemon_activity_summary IS
'Summary view of daemon activity by action type and status for monitoring and reporting.';

-- Create view for recent daemon activity (last 24 hours)
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

COMMENT ON VIEW meta.daemon_recent_activity IS
'Shows daemon activity from the last 24 hours for operational monitoring.';
