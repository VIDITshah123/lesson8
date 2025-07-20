-- SQLite script to create a full_access role and FA user
-- First create the full_access role
INSERT INTO roles_master (name, description, created_at, updated_at) 
VALUES ('full_access', 'Role with access to all functionality', datetime('now'), datetime('now'));

-- Assign all permissions to the full_access role
INSERT INTO role_permissions_tx (role_id, permission_id, created_at)
SELECT 
    (SELECT role_id FROM roles_master WHERE name = 'full_access'), 
    permission_id,
    datetime('now')
FROM permissions_master;

-- Create FA user with password User@123
INSERT INTO users_master (mobile_number, email, password_hash, first_name, last_name, is_active, created_at, updated_at) 
VALUES ('8888888888', 'fa@employdex.com', '$2a$10$LjZl9CjeQFg1nrz8KvTYlOC.Nvsr5loM2qHbppZrbksSBPbFGVT5S', 'FA', 'User', 1, datetime('now'), datetime('now'));

-- Assign full_access role to FA user
INSERT INTO user_roles_tx (user_id, role_id, created_at)
VALUES (
    (SELECT user_id FROM users_master WHERE mobile_number = '8888888888'),
    (SELECT role_id FROM roles_master WHERE name = 'full_access'),
    datetime('now')
);
