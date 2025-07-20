-- Migration: Add 'feature' column to feature_toggles table
ALTER TABLE feature_toggles ADD COLUMN feature TEXT DEFAULT 'user_management';
