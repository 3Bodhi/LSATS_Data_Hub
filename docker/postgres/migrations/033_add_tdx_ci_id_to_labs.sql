-- Migration 033: Add tdx_ci_id column to silver.labs
-- Date: 2025-01-26
-- Purpose: Store TeamDynamix Configuration Item ID for bidirectional sync
--
-- Context:
-- After creating lab CIs in TeamDynamix using create_lab_cis.py, we need to
-- store the CI IDs in the database to enable bidirectional synchronization.
-- This allows us to update existing CIs and track lab-to-CI relationships.

BEGIN;

-- Add tdx_ci_id column to store the TDX Configuration Item ID
ALTER TABLE silver.labs
ADD COLUMN tdx_ci_id INTEGER;

-- Create index for fast lookups and joins
CREATE INDEX idx_labs_tdx_ci_id
ON silver.labs(tdx_ci_id)
WHERE tdx_ci_id IS NOT NULL;

-- Add column comment explaining purpose
COMMENT ON COLUMN silver.labs.tdx_ci_id IS
'TeamDynamix Configuration Item ID (Type 10132 - Labs). Populated by sync_tdx_lab_ci_ids.py script. Enables bidirectional sync with TDX for lab CI updates.';

COMMIT;
