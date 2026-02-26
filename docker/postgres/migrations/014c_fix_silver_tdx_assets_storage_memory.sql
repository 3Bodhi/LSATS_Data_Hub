-- Migration Fix: Adjust silver.tdx_assets attribute field sizes
-- Date: 2025-11-22
-- Purpose: Fix storage and memory attribute field sizes based on actual data analysis
--          - attr_storage VARCHAR(50) too small (max observed: 110 chars)
--          - attr_memory VARCHAR(50) too small (max observed: 64 chars)

-- Fix 1: Increase attr_storage from VARCHAR(50) to VARCHAR(150)
ALTER TABLE silver.tdx_assets
    ALTER COLUMN attr_storage TYPE VARCHAR(150);

COMMENT ON COLUMN silver.tdx_assets.attr_storage IS
    'Extracted from Attributes array - storage description (e.g., "1TB", or complex RAID configs up to 110 chars)';

-- Fix 2: Increase attr_memory from VARCHAR(50) to VARCHAR(100)
ALTER TABLE silver.tdx_assets
    ALTER COLUMN attr_memory TYPE VARCHAR(100);

COMMENT ON COLUMN silver.tdx_assets.attr_memory IS
    'Extracted from Attributes array - memory description (e.g., "8GB", or detailed specs up to 64 chars)';

-- Success message
DO $$
BEGIN
    RAISE NOTICE '✅ Fixed silver.tdx_assets attribute field sizes:';
    RAISE NOTICE '   - attr_storage: VARCHAR(50) → VARCHAR(150) (max observed: 110 chars)';
    RAISE NOTICE '   - attr_memory: VARCHAR(50) → VARCHAR(100) (max observed: 64 chars)';
END $$;
