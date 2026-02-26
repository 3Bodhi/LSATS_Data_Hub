-- Migration: 027_create_lab_aggregation_tables.sql
-- Purpose: Create intermediate aggregation tables for lab modernization (Phase 1)
-- Date: 2025-01-24

-- ============================================================================
-- 1. silver.tdx_labs (Pipeline Helper)
-- ============================================================================
-- Aggregates TDX computer ownership data per PI
-- This is an intermediate table, not a source-specific table

CREATE TABLE IF NOT EXISTS silver.tdx_labs (
    tdx_lab_id VARCHAR(100) PRIMARY KEY,              -- Same as pi_uniqname
    pi_uniqname VARCHAR(50) NOT NULL,
    lab_name VARCHAR(255),                            -- Generated from PI name
    computer_count INTEGER DEFAULT 0,
    has_tdx_presence BOOLEAN DEFAULT true,
    
    -- Standard silver columns
    data_quality_score DECIMAL(3,2),
    quality_flags JSONB DEFAULT '[]'::jsonb,
    source_system VARCHAR(50) DEFAULT 'tdx',
    entity_hash VARCHAR(64) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ingestion_run_id UUID REFERENCES meta.ingestion_runs(run_id),
    
    FOREIGN KEY (pi_uniqname) REFERENCES silver.users(uniqname)
);

COMMENT ON TABLE silver.tdx_labs IS 'Pipeline Helper: Aggregates TDX computer ownership data per PI. Intermediate step for composite labs.';

-- ============================================================================
-- 2. silver.award_labs (Pipeline Helper)
-- ============================================================================
-- Aggregates lab award data per PI
-- This is an intermediate table, not a source-specific table

CREATE TABLE IF NOT EXISTS silver.award_labs (
    award_lab_id VARCHAR(100) PRIMARY KEY,             -- Same as pi_uniqname
    pi_uniqname VARCHAR(50) NOT NULL,
    lab_name VARCHAR(255),
    
    -- Financial aggregates
    total_award_dollars NUMERIC(15,2) DEFAULT 0.00,
    total_direct_dollars NUMERIC(15,2) DEFAULT 0.00,
    total_indirect_dollars NUMERIC(15,2) DEFAULT 0.00,
    award_count INTEGER DEFAULT 0,
    active_award_count INTEGER DEFAULT 0,
    earliest_award_start DATE,
    latest_award_end DATE,
    
    -- Department from awards
    primary_department_id VARCHAR(50),
    department_ids JSONB DEFAULT '[]'::jsonb,
    
    -- Standard silver columns
    data_quality_score DECIMAL(3,2),
    quality_flags JSONB DEFAULT '[]'::jsonb,
    source_system VARCHAR(50) DEFAULT 'lab_award',
    entity_hash VARCHAR(64) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ingestion_run_id UUID REFERENCES meta.ingestion_runs(run_id),
    
    FOREIGN KEY (pi_uniqname) REFERENCES silver.users(uniqname),
    FOREIGN KEY (primary_department_id) REFERENCES silver.departments(dept_id)
);

COMMENT ON TABLE silver.award_labs IS 'Pipeline Helper: Aggregates lab award data per PI. Intermediate step for composite labs.';

-- ============================================================================
-- 3. silver.ad_labs (Pipeline Helper)
-- ============================================================================
-- Aggregates AD organizational unit data per PI
-- This is an intermediate table, not a source-specific table

CREATE TABLE IF NOT EXISTS silver.ad_labs (
    ad_lab_id VARCHAR(100) PRIMARY KEY,                -- Same as pi_uniqname
    pi_uniqname VARCHAR(50) NOT NULL,
    lab_name VARCHAR(255),                             -- From OU name
    
    -- AD OU details
    has_ad_ou BOOLEAN DEFAULT true,
    ad_ou_dn TEXT,
    ad_ou_hierarchy JSONB DEFAULT '[]'::jsonb,
    ad_parent_ou TEXT,
    ad_ou_depth INTEGER,
    ad_ou_created TIMESTAMP WITH TIME ZONE,
    ad_ou_modified TIMESTAMP WITH TIME ZONE,
    
    -- Standard silver columns
    data_quality_score DECIMAL(3,2),
    quality_flags JSONB DEFAULT '[]'::jsonb,
    source_system VARCHAR(50) DEFAULT 'active_directory',
    entity_hash VARCHAR(64) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ingestion_run_id UUID REFERENCES meta.ingestion_runs(run_id),
    
    FOREIGN KEY (pi_uniqname) REFERENCES silver.users(uniqname)
);

COMMENT ON TABLE silver.ad_labs IS 'Pipeline Helper: Aggregates AD OU data per PI. Intermediate step for composite labs.';
