-- Migration Fix: Adjust silver.tdx_assets MAC and IP address field sizes
-- Date: 2025-11-22
-- Purpose: Fix MAC and IP address attribute field sizes based on actual data analysis
--          - attr_mac_address VARCHAR(100) too small (max observed: 138 chars - multiple MACs comma-separated)
--          - attr_ip_address VARCHAR(100) too small (max observed: 75 chars - multiple IPs comma-separated)

-- Fix 1: Increase attr_mac_address from VARCHAR(100) to VARCHAR(200)
ALTER TABLE silver.tdx_assets
    ALTER COLUMN attr_mac_address TYPE VARCHAR(200);

COMMENT ON COLUMN silver.tdx_assets.attr_mac_address IS
    'Extracted from Attributes array - can contain multiple comma-separated MAC addresses (max 138 chars observed)';

-- Fix 2: Increase attr_ip_address from VARCHAR(100) to VARCHAR(150)
ALTER TABLE silver.tdx_assets
    ALTER COLUMN attr_ip_address TYPE VARCHAR(150);

COMMENT ON COLUMN silver.tdx_assets.attr_ip_address IS
    'Extracted from Attributes array - can contain multiple comma-separated IP addresses (max 75 chars observed)';

-- Success message
DO $$
BEGIN
    RAISE NOTICE '✅ Fixed silver.tdx_assets network attribute field sizes:';
    RAISE NOTICE '   - attr_mac_address: VARCHAR(100) → VARCHAR(200) (max observed: 138 chars)';
    RAISE NOTICE '   - attr_ip_address: VARCHAR(100) → VARCHAR(150) (max observed: 75 chars)';
    RAISE NOTICE '';
    RAISE NOTICE '📝 Summary of all field size fixes applied:';
    RAISE NOTICE '   - attr_processor_count: VARCHAR(50) → VARCHAR(150)';
    RAISE NOTICE '   - attr_storage: VARCHAR(50) → VARCHAR(150)';
    RAISE NOTICE '   - attr_memory: VARCHAR(50) → VARCHAR(100)';
    RAISE NOTICE '   - attr_mac_address: VARCHAR(100) → VARCHAR(200)';
    RAISE NOTICE '   - attr_ip_address: VARCHAR(100) → VARCHAR(150)';
    RAISE NOTICE '   - tag: NOT NULL → nullable';
END $$;
