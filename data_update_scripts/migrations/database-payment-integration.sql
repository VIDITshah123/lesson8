-- Payment Integration Module Database Design
-- This file contains the SQL schema for the payment integration module

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

-- Sample data for payment_qr_codes
INSERT OR IGNORE INTO payment_qr_codes (payment_name, payment_description, qr_code_image, qr_code_path, payment_type, is_active, created_by)
VALUES 
('Default UPI QR', 'Default UPI payment QR code', X'00112233', '/uploads/qr/default_upi.png', 'UPI', 1, 1),
('Corporate Account QR', 'Corporate bank account QR code', X'44556677', '/uploads/qr/corporate.png', 'BANK', 0, 1);

-- Sample data for payment_transactions
INSERT OR IGNORE INTO payment_transactions (transaction_reference, amount, currency, payment_status, qr_code_id, user_id, transaction_notes)
VALUES 
('TXN123456789', 1000.00, 'INR', 'COMPLETED', 1, 2, 'Test transaction'),
('TXN987654321', 1500.50, 'INR', 'PENDING', 1, 3, 'Awaiting confirmation'),
('TXN567890123', 750.25, 'INR', 'FAILED', 2, 4, 'Payment gateway error');
