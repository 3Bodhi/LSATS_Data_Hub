-- Migration: Add department columns to silver.lab_managers
-- Created: 2025-11-25
-- Purpose: Add lab and manager department information for better context

-- Add new columns for department information
ALTER TABLE silver.lab_managers
    ADD COLUMN lab_department_id VARCHAR(50),
    ADD COLUMN lab_department_name VARCHAR(255),
    ADD COLUMN manager_department_id VARCHAR(50),
    ADD COLUMN manager_department_name VARCHAR(255);

-- Add column comments for documentation
COMMENT ON COLUMN silver.lab_managers.lab_department_id IS 
    'Department ID for the lab (from silver.labs.primary_department_id)';

COMMENT ON COLUMN silver.lab_managers.lab_department_name IS 
    'Department name for the lab (from silver.departments.dept_name)';

COMMENT ON COLUMN silver.lab_managers.manager_department_id IS 
    'Department ID for the manager (from silver.lab_members.member_department_id)';

COMMENT ON COLUMN silver.lab_managers.manager_department_name IS 
    'Department name for the manager (from silver.departments.dept_name)';

-- Create indexes for common query patterns
CREATE INDEX idx_lab_managers_lab_dept 
    ON silver.lab_managers (lab_department_id) 
    WHERE lab_department_id IS NOT NULL;

CREATE INDEX idx_lab_managers_manager_dept 
    ON silver.lab_managers (manager_department_id) 
    WHERE manager_department_id IS NOT NULL;

-- Verification query
DO $$
BEGIN
    RAISE NOTICE 'Migration completed successfully';
    RAISE NOTICE 'Added 4 new columns to silver.lab_managers:';
    RAISE NOTICE '  - lab_department_id';
    RAISE NOTICE '  - lab_department_name';
    RAISE NOTICE '  - manager_department_id';
    RAISE NOTICE '  - manager_department_name';
    RAISE NOTICE 'Created 2 new indexes for query optimization';
END $$;
