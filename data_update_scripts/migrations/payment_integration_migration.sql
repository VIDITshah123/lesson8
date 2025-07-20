-- Payment Integration Module Database Migration
-- Created: 2025-07-11

-- Payment QR Codes Table
-- Stores information about uploaded QR codes for payment processing
CREATE TABLE IF NOT EXISTS payment_qr_codes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    payment_name VARCHAR(100) NOT NULL,
    payment_description TEXT,
    qr_code_image BLOB NOT NULL, -- Binary storage of the QR code image
    qr_code_path VARCHAR(255),   -- File system path to the QR code image
    payment_type VARCHAR(50) NOT NULL, -- e.g., 'UPI', 'BANK', 'WALLET'
    is_active BOOLEAN DEFAULT 0, -- Only one QR code can be active at a time
    created_by INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (created_by) REFERENCES users_master(user_id)
);

-- Payment Transactions Table
-- Records all payment transactions processed through the system
CREATE TABLE IF NOT EXISTS payment_transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_reference VARCHAR(100) NOT NULL UNIQUE, -- Unique reference number for the transaction
    amount DECIMAL(10, 2) NOT NULL,
    currency VARCHAR(3) DEFAULT 'INR',
    payment_status VARCHAR(20) NOT NULL, -- 'PENDING', 'COMPLETED', 'FAILED', 'REFUNDED'
    qr_code_id INTEGER,
    user_id INTEGER, -- User who initiated the transaction (can be null for anonymous payments)
    transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    transaction_notes TEXT,
    transaction_metadata TEXT, -- JSON field for additional data
    FOREIGN KEY (qr_code_id) REFERENCES payment_qr_codes(id),
    FOREIGN KEY (user_id) REFERENCES users_master(user_id)
);

-- Create index on transaction reference for faster lookups
CREATE INDEX IF NOT EXISTS idx_transaction_reference ON payment_transactions(transaction_reference);

-- Create index on transaction date for reporting
CREATE INDEX IF NOT EXISTS idx_transaction_date ON payment_transactions(transaction_date);

-- Create index on payment_status for filtering
CREATE INDEX IF NOT EXISTS idx_transaction_status ON payment_transactions(payment_status);

-- Add payment feature to feature_toggles table
-- This allows the payment integration to be toggled on/off
INSERT OR IGNORE INTO feature_toggles (feature_name, description, is_enabled, feature)
VALUES ('payment_integration', 'Enable payment integration with QR code support', 0, 'payment');

-- Create permissions table if it doesn't exist
CREATE TABLE IF NOT EXISTS permissions (
    permission_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    description TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Add payment permissions
INSERT OR IGNORE INTO permissions (name, description)
VALUES
('payment_view', 'View payment QR codes and transactions'),
('payment_create', 'Create new payment QR codes and transactions'),
('payment_edit', 'Edit payment QR codes and settings'),
('payment_delete', 'Delete payment QR codes');

-- Create role_permissions table if it doesn't exist
CREATE TABLE IF NOT EXISTS role_permissions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    role_id INTEGER NOT NULL,
    permission_id INTEGER NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (role_id) REFERENCES roles_master(role_id),
    FOREIGN KEY (permission_id) REFERENCES permissions(permission_id),
    UNIQUE(role_id, permission_id)
);

-- Assign payment permissions to admin role
INSERT OR IGNORE INTO role_permissions (role_id, permission_id)
SELECT r.role_id, p.permission_id
FROM roles_master r, permissions p
WHERE r.name = 'Admin' AND p.name IN ('payment_view', 'payment_create', 'payment_edit', 'payment_delete');
