-- Migration script to rename 'enabled' column to 'is_enabled' in feature_toggles table
BEGIN TRANSACTION;

-- Create a new table with the desired schema
CREATE TABLE feature_toggles_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    feature_name TEXT UNIQUE NOT NULL,
    is_enabled INTEGER NOT NULL DEFAULT 0,
    description TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    feature TEXT DEFAULT 'user_management'
);

-- Copy data from the old table to the new table
INSERT INTO feature_toggles_new (id, feature_name, is_enabled, description, created_at, updated_at, feature)
SELECT id, feature_name, enabled, description, created_at, updated_at, feature 
FROM feature_toggles;

-- Drop the old table
DROP TABLE feature_toggles;

-- Rename the new table to the original name
ALTER TABLE feature_toggles_new RENAME TO feature_toggles;

COMMIT;
