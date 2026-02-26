-- Migration: Add ou_depth_category to silver.labs
-- Purpose: Add OU depth categorization metadata for refined filtering of labs vs. non-lab OUs
-- Context: Bronze layer extracts _depth_category based on OU hierarchy depth
--          This helps distinguish actual labs (deep OUs) from departments/divisions (shallow OUs)

-- Add the column
ALTER TABLE silver.labs
ADD COLUMN ou_depth_category VARCHAR(50);

-- Add comment explaining the field
COMMENT ON COLUMN silver.labs.ou_depth_category IS
'Categorizes labs by OU hierarchy depth from bronze layer:
  - potential_lab: Deep OU likely representing actual lab (ou=labname,ou=pi,...)
  - shallow_ou: Shallow OU likely representing department/division (ou=dept,...)
  - no_ou: Lab without AD OU data (award-only)
Used to filter v_labs_refined view for higher-quality lab identification.';

-- Create index for efficient filtering
CREATE INDEX idx_labs_depth_category ON silver.labs(ou_depth_category) WHERE ou_depth_category IS NOT NULL;

COMMENT ON INDEX idx_labs_depth_category IS
'Supports efficient filtering by ou_depth_category in v_labs_refined view';
