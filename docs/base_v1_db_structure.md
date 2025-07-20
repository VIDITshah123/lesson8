# EmployDEX Base Platform - Database Structure

This document describes the database structure for the EmployDEX Base Platform, including table definitions, relationships, and SQL scripts for creating the database.

## Database Overview

The database is built using SQLite for development/non-production environments (with the ability to migrate to PostgreSQL for production). The structure follows these conventions:
- Primary keys use the naming pattern `tablename_id`
- Master data tables use the suffix `_master`
- Transaction data tables use the suffix `_tx`
- Foreign key relationships are maintained for data integrity

## Entity Relationship Diagram

```
+----------------+       +----------------+       +----------------+
|  users_master  |       |  roles_master  |       |permissions_master|
+----------------+       +----------------+       +----------------+
| user_id (PK)   |       | role_id (PK)   |       |permission_id (PK)|
| mobile_number  |       | name           |       | name          |
| password_hash  |       | description    |       | description   |
| email          |       | created_at     |       | created_at    |
| first_name     |       | updated_at     |       | updated_at    |
| last_name      |       +-------+--------+       +-------+-------+
| is_active      |               |                        |
| created_at     |               |                        |
| updated_at     |               |                        |
+-------+--------+               |                        |
        |                        |                        |
        |                +-------+--------+               |
        |                |                |               |
        |                |                |               |
+-------+--------+       |    +--------+--+-----+         |
| user_roles_tx  |-------+    | role_permissions_tx |-----+
+----------------+            +--------------------+
| user_role_id (PK)           | role_permission_id (PK) |
| user_id (FK)    |           | role_id (FK)      |
| role_id (FK)    |           | permission_id (FK)|
| created_at      |           | created_at        |
+----------------+            +--------------------+
        |
        |
+-------+--------+
| activity_logs_tx|
+----------------+
| activity_log_id (PK) |
| user_id (FK)   |
| action         |
| details        |
| ip_address     |
| user_agent     |
| created_at     |
+----------------+
```

## SQL Create Scripts

```sql
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
```

## Insert Default Data

```sql
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

-- Insert default admin user with password Admin@123
INSERT INTO users_master (mobile_number, email, password_hash, first_name, last_name) 
VALUES ('9999999999', 'admin@employdex.com', '$2a$10$HCJ5Yd0YR1P4TGPJOyyAWe6jVXnjYQLTP8EuoNRPnT4l4XzUKCNbS', 'Admin', 'User');
-- Note: password_hash is for 'Admin@123' using bcrypt

-- Assign Admin role to the admin user
INSERT INTO user_roles_tx (user_id, role_id)
VALUES (
    (SELECT user_id FROM users_master WHERE email = 'admin@employdex.com'),
    (SELECT role_id FROM roles_master WHERE name = 'Admin')
);
```

## Database Info

### Table Descriptions

1. **users_master**
   - Purpose: Stores user account information
   - Key fields:
     - `user_id`: Unique identifier for each user
     - `mobile_number`: User's mobile phone number (unique)
     - `email`: User's email address (unique)
     - `password_hash`: Bcrypt-hashed password
     - `is_active`: Flag indicating if the account is active

2. **roles_master**
   - Purpose: Stores role definitions for RBAC
   - Key fields:
     - `role_id`: Unique identifier for each role
     - `name`: Role name (e.g., "Admin", "User")
     - `description`: Detailed description of the role

3. **permissions_master**
   - Purpose: Stores individual permission definitions
   - Key fields:
     - `permission_id`: Unique identifier for each permission
     - `name`: Permission name (e.g., "user_create")
     - `description`: Detailed description of what the permission allows

4. **user_roles_tx**
   - Purpose: Junction table establishing many-to-many relationship between users and roles
   - Key fields:
     - `user_role_id`: Unique identifier for each association
     - `user_id`: References a user in users_master
     - `role_id`: References a role in roles_master

5. **role_permissions_tx**
   - Purpose: Junction table establishing many-to-many relationship between roles and permissions
   - Key fields:
     - `role_permission_id`: Unique identifier for each association
     - `role_id`: References a role in roles_master
     - `permission_id`: References a permission in permissions_master

6. **activity_logs_tx**
   - Purpose: Stores user activity for auditing and security monitoring
   - Key fields:
     - `activity_log_id`: Unique identifier for each log entry
     - `user_id`: References the user who performed the action
     - `action`: Description of the action performed
     - `details`: Additional information about the action
     - `ip_address`: IP address from which the action was performed
     - `user_agent`: Browser/client information

## Database Migration

For production environments, migration to PostgreSQL can be handled through standard SQLite to PostgreSQL migration tools. The table structures provided are compatible with PostgreSQL with minimal modifications.

## Changelog

- **2025-06-26**: Initial database structure created
