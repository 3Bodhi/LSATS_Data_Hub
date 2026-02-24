-- Migration: Fix computer_attributes index for large values
-- Issue: B-tree index cannot handle values > 2704 bytes
-- Solution: Drop full index, create partial index for smaller values + hash index for lookups

-- Drop the problematic index
DROP INDEX IF EXISTS silver.idx_computer_attributes_value;

-- Create partial B-tree index for values under 2000 characters (safe for indexing)
CREATE INDEX idx_computer_attributes_value_small
ON silver.computer_attributes (attribute_value)
WHERE LENGTH(attribute_value) < 2000;

-- Create hash index for exact lookups on all values (hash doesn't have size limit)
CREATE INDEX idx_computer_attributes_value_hash
ON silver.computer_attributes USING hash (attribute_value);

-- For large text values, consider using GIN index with pg_trgm for text search
-- Requires: CREATE EXTENSION IF NOT EXISTS pg_trgm;
-- CREATE INDEX idx_computer_attributes_value_trgm
-- ON silver.computer_attributes USING gin (attribute_value gin_trgm_ops);
