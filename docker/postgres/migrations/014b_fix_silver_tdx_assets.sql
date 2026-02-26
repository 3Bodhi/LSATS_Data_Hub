-- Migration Fix: Adjust silver.tdx_assets schema for data issues
-- Date: 2025-11-22
-- Purpose: Fix two issues discovered during initial transformation:
--          1. attr_processor_count VARCHAR(50) too small (max observed: 102 chars)
--          2. tag should allow NULL (software licenses don't have tags)

-- Fix 1: Increase attr_processor_count from VARCHAR(50) to VARCHAR(150)
ALTER TABLE silver.tdx_assets
    ALTER COLUMN attr_processor_count TYPE VARCHAR(150);

COMMENT ON COLUMN silver.tdx_assets.attr_processor_count IS
    'Extracted from Attributes array - processor description can be verbose (max 102 chars observed)';

-- Fix 2: Allow NULL for tag (4 out of 41,208 assets don't have tags - mostly software licenses)
ALTER TABLE silver.tdx_assets
    ALTER COLUMN tag DROP NOT NULL;

COMMENT ON COLUMN silver.tdx_assets.tag IS
    'TDX asset tag/barcode identifier - NULL for some asset types like software licenses (0.01% of assets)';

-- Success message
DO $$
BEGIN
    RAISE NOTICE '✅ Fixed silver.tdx_assets schema:';
    RAISE NOTICE '   - attr_processor_count: VARCHAR(50) → VARCHAR(150)';
    RAISE NOTICE '   - tag: NOT NULL → nullable (allows 4 software license assets)';
END $$;
