# Payment Integration Module Implementation Tasks

This document outlines the tasks required to implement a payment integration module for the EmployDEX Base Platform. The module will allow administrators to upload QR codes for receiving payments and will be available as a feature toggle for integration with other applications.

## Relevant Files

### Backend Files
- `backend/routes/payment.js` - API routes for payment integration features
- `backend/controllers/paymentController.js` - Controller logic for payment features
- `backend/models/payment.js` - Data models for payment-related entities
- `backend/services/qrCodeService.js` - Service for handling QR code upload, validation, and storage
- `backend/middleware/paymentFeatureCheck.js` - Middleware to check if payment feature is enabled

### Frontend Files
- `frontend/src/components/payment/QRCodeUpload.js` - Component for uploading QR codes
- `frontend/src/components/payment/PaymentSettings.js` - Admin settings for payment integration
- `frontend/src/components/payment/PaymentDashboard.js` - Dashboard for payment status and reports
- `frontend/src/pages/admin/PaymentIntegration.js` - Admin page for payment integration configuration
- `frontend/src/services/paymentService.js` - Service for API communication with payment endpoints

### Database Files
- `db/payment_integration_migration.sql` - SQL migration for payment tables
- `db/feature_toggles_payment_integration.sql` - SQL for adding payment toggle to feature toggles

### Config and Documentation
- `docs/payment_integration.md` - Documentation for using the payment integration module
- `frontend/src/config/paymentConfig.js` - Configuration for payment integration

### Notes

- The payment integration will be implemented as a feature toggle that can be enabled/disabled through the admin interface
- QR codes will be stored securely with proper validation
- The feature can be integrated into any application by enabling the toggle

## Tasks

- [x] 1.0 Database Setup for Payment Integration
  - [x] 1.1 Create payment_qr_codes table to store uploaded QR code data
  - [x] 1.2 Create payment_transactions table to track payment activities
  - [x] 1.3 Add payment integration feature toggle to feature_toggles table
  - [x] 1.4 Create SQL scripts for initial data population
  - [ ] 1.5 Update database documentation with new tables

- [x] 2.0 Backend API Implementation
  - [x] 2.1 Create payment routes in Express.js
  - [x] 2.2 Implement QR code upload controller with validation
  - [x] 2.3 Implement feature toggle check middleware
  - [x] 2.4 Create service for QR code processing and storage
  - [x] 2.5 Implement API endpoints for retrieving payment information
  - [x] 2.6 Add authentication and authorization for payment-related routes
  - [ ] 2.7 Write unit tests for payment controllers and services

- [x] 3.0 Frontend Admin UI for QR Code Management
  - [x] 3.1 Design and implement QR code upload component
  - [x] 3.2 Create form validation for QR code uploads
  - [x] 3.3 Implement QR code preview functionality
  - [x] 3.4 Add QR code management interface (list, delete, activate)
  - [x] 3.5 Implement notification system for successful/failed uploads
  - [x] 3.6 Create responsive design for mobile admin access

- [x] 4.0 Payment Feature Toggle Implementation
  - [x] 4.1 Extend existing feature toggle system for payment integration
  - [x] 4.2 Create admin UI for enabling/disabling payment feature
  - [x] 4.3 Implement feature toggle check on frontend components
  - [x] 4.4 Add payment toggle status to system status page
  - [x] 4.5 Implement feature toggle API for external applications

- [x] 5.0 Payment Integration Documentation and Testing
  - [ ] 5.1 Write comprehensive documentation for payment integration setup
  - [ ] 5.2 Create integration examples for other applications
  - [ ] 5.3 Perform end-to-end testing of payment flow
  - [ ] 5.4 Conduct security review of payment implementation
  - [ ] 5.5 Update README with payment integration information
  - [x] 5.6 Update CHANGELOG with payment integration feature

## Database Design

```sql
-- Payment QR Codes Table
CREATE TABLE IF NOT EXISTS payment_qr_codes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    payment_name VARCHAR(100) NOT NULL,
    payment_description TEXT,
    qr_code_image BLOB NOT NULL,
    qr_code_path VARCHAR(255),
    payment_type VARCHAR(50) NOT NULL,
    is_active BOOLEAN DEFAULT 0,
    created_by INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (created_by) REFERENCES users(id)
);

-- Payment Transactions Table
CREATE TABLE IF NOT EXISTS payment_transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_reference VARCHAR(100) NOT NULL UNIQUE,
    amount DECIMAL(10, 2) NOT NULL,
    currency VARCHAR(3) DEFAULT 'INR',
    payment_status VARCHAR(20) NOT NULL,
    qr_code_id INTEGER,
    user_id INTEGER,
    transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    transaction_notes TEXT,
    transaction_metadata TEXT, -- JSON field for additional data
    FOREIGN KEY (qr_code_id) REFERENCES payment_qr_codes(id),
    FOREIGN KEY (user_id) REFERENCES users(id)
);

-- Add payment feature to feature_toggles table
INSERT INTO feature_toggles (name, description, is_enabled, feature)
VALUES ('payment_integration', 'Enable payment integration with QR code support', 0, 'payment');
```

## Sample Data

```sql
-- Sample data for payment_qr_codes
INSERT INTO payment_qr_codes (payment_name, payment_description, qr_code_image, qr_code_path, payment_type, is_active, created_by)
VALUES 
('Default UPI QR', 'Default UPI payment QR code', X'00112233', '/uploads/qr/default_upi.png', 'UPI', 1, 1),
('Corporate Account QR', 'Corporate bank account QR code', X'44556677', '/uploads/qr/corporate.png', 'BANK', 0, 1);

-- Sample data for payment_transactions
INSERT INTO payment_transactions (transaction_reference, amount, currency, payment_status, qr_code_id, user_id, transaction_notes)
VALUES 
('TXN123456789', 1000.00, 'INR', 'COMPLETED', 1, 2, 'Test transaction'),
('TXN987654321', 1500.50, 'INR', 'PENDING', 1, 3, 'Awaiting confirmation'),
('TXN567890123', 750.25, 'INR', 'FAILED', 2, 4, 'Payment gateway error');
```

## API Endpoints

The following API endpoints will be implemented:

- `GET /api/payment/qr-codes` - Get all QR codes
- `GET /api/payment/qr-codes/:id` - Get a specific QR code
- `POST /api/payment/qr-codes` - Upload a new QR code
- `PUT /api/payment/qr-codes/:id` - Update QR code details
- `DELETE /api/payment/qr-codes/:id` - Delete a QR code
- `PATCH /api/payment/qr-codes/:id/activate` - Activate a QR code
- `PATCH /api/payment/qr-codes/:id/deactivate` - Deactivate a QR code
- `GET /api/payment/transactions` - Get all transactions
- `GET /api/payment/transactions/:id` - Get a specific transaction
- `POST /api/payment/transactions` - Create a new transaction
- `GET /api/payment/status` - Check if payment integration is enabled
