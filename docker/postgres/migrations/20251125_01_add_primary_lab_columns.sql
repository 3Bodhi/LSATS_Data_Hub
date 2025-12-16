-- Add primary lab association columns to silver.computers
ALTER TABLE silver.computers
ADD COLUMN IF NOT EXISTS primary_lab_id VARCHAR(100),
ADD COLUMN IF NOT EXISTS primary_lab_method VARCHAR(50),
ADD COLUMN IF NOT EXISTS lab_association_count INTEGER DEFAULT 0;

-- Add index for primary lab
CREATE INDEX IF NOT EXISTS idx_silver_computers_primary_lab ON silver.computers(primary_lab_id);
