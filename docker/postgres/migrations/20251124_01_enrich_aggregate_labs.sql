-- Migration: Enrich Aggregate Labs Tables
-- Description: Adds missing columns to silver.tdx_labs, silver.award_labs, and silver.ad_labs
-- Date: 2025-11-24

-- 1. Enrich silver.tdx_labs
ALTER TABLE silver.tdx_labs
    ADD COLUMN IF NOT EXISTS tdx_user_uid UUID,
    ADD COLUMN IF NOT EXISTS primary_email VARCHAR(255),
    ADD COLUMN IF NOT EXISTS work_phone VARCHAR(50),
    ADD COLUMN IF NOT EXISTS title VARCHAR(255),
    ADD COLUMN IF NOT EXISTS department_name VARCHAR(255),
    ADD COLUMN IF NOT EXISTS company VARCHAR(255),
    ADD COLUMN IF NOT EXISTS is_active BOOLEAN;

COMMENT ON COLUMN silver.tdx_labs.department_name IS 'Derived from default_account_name in TDX';

-- 2. Enrich silver.award_labs
ALTER TABLE silver.award_labs
    ADD COLUMN IF NOT EXISTS sponsors JSONB DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS award_titles JSONB DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS primary_department_name VARCHAR(255);

-- 3. Enrich silver.ad_labs
ALTER TABLE silver.ad_labs
    ADD COLUMN IF NOT EXISTS description VARCHAR(1024),
    ADD COLUMN IF NOT EXISTS managed_by VARCHAR(255);
