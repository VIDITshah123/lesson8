-- Migration: Add feature toggle permissions
INSERT INTO permissions_master (name, description) VALUES
  ('feature_toggle_view', 'View feature toggles'),
  ('feature_toggle_edit', 'Create, edit, or delete feature toggles');
