-- Payment Integration Sample Data
-- Created: 2025-07-11

-- Sample data for payment_qr_codes
INSERT OR IGNORE INTO payment_qr_codes (payment_name, payment_description, qr_code_image, qr_code_path, payment_type, is_active, created_by)
VALUES 
('Default UPI QR', 'Default UPI payment QR code', X'00112233', '/uploads/qr/default_upi.png', 'UPI', 1, 1),
('Corporate Account QR', 'Corporate bank account QR code', X'44556677', '/uploads/qr/corporate.png', 'BANK', 0, 1);

-- Sample data for payment_transactions
INSERT OR IGNORE INTO payment_transactions (transaction_reference, amount, currency, payment_status, qr_code_id, user_id, transaction_notes)
VALUES 
('TXN123456789', 1000.00, 'INR', 'COMPLETED', 1, 1, 'Test transaction'),
('TXN987654321', 1500.50, 'INR', 'PENDING', 1, 1, 'Awaiting confirmation'),
('TXN567890123', 750.25, 'INR', 'FAILED', 2, 1, 'Payment gateway error');
