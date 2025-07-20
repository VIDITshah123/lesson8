-- EmployDEX Base Platform - Database Initialization Script
-- Created: 2025-06-26

-- Enable foreign keys
PRAGMA foreign_keys = ON;

-- Create users table (master data)
CREATE TABLE IF NOT EXISTS users_master (
    user_id INTEGER PRIMARY KEY AUTOINCREMENT,
    mobile_number TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    is_active BOOLEAN DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create roles table (master data)
CREATE TABLE IF NOT EXISTS roles_master (
    role_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create permissions table (master data)
CREATE TABLE IF NOT EXISTS permissions_master (
    permission_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create user_roles junction table (transaction data)
CREATE TABLE IF NOT EXISTS user_roles_tx (
    user_role_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    role_id INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, role_id),
    FOREIGN KEY (user_id) REFERENCES users_master(user_id) ON DELETE CASCADE,
    FOREIGN KEY (role_id) REFERENCES roles_master(role_id) ON DELETE CASCADE
);

-- Create role_permissions junction table (transaction data)
CREATE TABLE IF NOT EXISTS role_permissions_tx (
    role_permission_id INTEGER PRIMARY KEY AUTOINCREMENT,
    role_id INTEGER NOT NULL,
    permission_id INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(role_id, permission_id),
    FOREIGN KEY (role_id) REFERENCES roles_master(role_id) ON DELETE CASCADE,
    FOREIGN KEY (permission_id) REFERENCES permissions_master(permission_id) ON DELETE CASCADE
);

-- Create activity_logs table (transaction data)
CREATE TABLE IF NOT EXISTS activity_logs_tx (
    activity_log_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    action TEXT NOT NULL,
    details TEXT,
    ip_address TEXT,
    user_agent TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users_master(user_id) ON DELETE SET NULL
);

-- Insert default roles
INSERT INTO roles_master (name, description) VALUES 
    ('Admin', 'Administrator with full system access'),
    ('User', 'Standard user with limited access');

-- Insert default permissions
INSERT INTO permissions_master (name, description) VALUES
    ('user_view', 'Can view user details'),
    ('user_create', 'Can create users'),
    ('user_edit', 'Can edit user details'),
    ('user_delete', 'Can delete users'),
    ('role_view', 'Can view roles'),
    ('role_create', 'Can create roles'),
    ('role_edit', 'Can edit roles'),
    ('role_delete', 'Can delete roles'),
    ('permission_view', 'Can view permissions'),
    ('permission_assign', 'Can assign permissions to roles');

-- Assign all permissions to Admin role
INSERT INTO role_permissions_tx (role_id, permission_id)
SELECT 
    (SELECT role_id FROM roles_master WHERE name = 'Admin'), 
    permission_id 
FROM permissions_master;

-- Assign basic permissions to User role
INSERT INTO role_permissions_tx (role_id, permission_id)
SELECT 
    (SELECT role_id FROM roles_master WHERE name = 'User'), 
    permission_id 
FROM permissions_master 
WHERE name IN ('user_view');

-- Insert default admin user with password Admin@123 (admin/admin as per requirements)
INSERT INTO users_master (mobile_number, email, password_hash, first_name, last_name) 
VALUES ('9999999999', 'admin@employdex.com', '$2a$10$HCJ5Yd0YR1P4TGPJOyyAWe6jVXnjYQLTP8EuoNRPnT4l4XzUKCNbS', 'Admin', 'User');
-- Note: password_hash is for 'admin' using bcrypt

-- Assign Admin role to the admin user
INSERT INTO user_roles_tx (user_id, role_id)
VALUES (
    (SELECT user_id FROM users_master WHERE email = 'admin@employdex.com'),
    (SELECT role_id FROM roles_master WHERE name = 'Admin')
);
