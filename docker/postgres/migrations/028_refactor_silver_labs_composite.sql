-- Migration: 028_refactor_silver_labs_composite.sql
-- Purpose: Refactor silver.labs to be a true composite entity (Tier 3)
-- Date: 2025-01-24

-- ============================================================================
-- Refactor silver.labs
-- ============================================================================

-- Add missing columns for composite tracking
ALTER TABLE silver.labs
ADD COLUMN IF NOT EXISTS has_tdx_data BOOLEAN DEFAULT false;

-- Ensure other columns exist (idempotent checks)
ALTER TABLE silver.labs
ADD COLUMN IF NOT EXISTS has_tdx_presence BOOLEAN DEFAULT false,
ADD COLUMN IF NOT EXISTS has_award_data BOOLEAN DEFAULT false,
ADD COLUMN IF NOT EXISTS has_ou_data BOOLEAN DEFAULT false,
ADD COLUMN IF NOT EXISTS data_source VARCHAR(100) DEFAULT '',
ADD COLUMN IF NOT EXISTS source_system VARCHAR(100) DEFAULT 'composite';

-- Update comments to reflect Tier 3 status
COMMENT ON TABLE silver.labs IS 'Tier 3 Composite Entity: Consolidates lab data from TDX, Awards, AD, and Groups. Source of truth for Labs.';

COMMENT ON COLUMN silver.labs.has_tdx_presence IS 'Indicates if the lab has a presence in TDX (e.g. owns computers)';
COMMENT ON COLUMN silver.labs.has_tdx_data IS 'Indicates if TDX data was successfully merged for this lab';
COMMENT ON COLUMN silver.labs.has_award_data IS 'Indicates if Award data was successfully merged for this lab';
COMMENT ON COLUMN silver.labs.has_ou_data IS 'Indicates if AD OU data was successfully merged for this lab';
COMMENT ON COLUMN silver.labs.data_source IS 'Concatenated string of data sources (e.g. tdx+award+ad)';
