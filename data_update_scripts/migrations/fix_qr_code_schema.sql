-- Fix QR Code Table Schema
-- Created: 2025-07-15

-- Drop and recreate the payment_qr_codes table with proper structure
DROP TABLE IF EXISTS payment_qr_codes;

-- Create updated QR codes table
CREATE TABLE IF NOT EXISTS payment_qr_codes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    payment_type VARCHAR(50) NOT NULL, -- e.g., 'UPI', 'BANK', 'WALLET'
    image_url VARCHAR(255),   -- File system path to the QR code image
    active BOOLEAN DEFAULT 0, -- Only one QR code can be active at a time
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index on payment type for faster lookups
CREATE INDEX IF NOT EXISTS idx_payment_type ON payment_qr_codes(payment_type);

-- Update payment transactions table to match
DROP TABLE IF EXISTS payment_transactions;

CREATE TABLE IF NOT EXISTS payment_transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    qr_code_id INTEGER,
    transaction_ref VARCHAR(100) NOT NULL UNIQUE, -- Unique reference number for the transaction
    user_id INTEGER, -- User who initiated the transaction
    verified BOOLEAN DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT NULL,
    FOREIGN KEY (qr_code_id) REFERENCES payment_qr_codes(id),
    FOREIGN KEY (user_id) REFERENCES users(id)
);
