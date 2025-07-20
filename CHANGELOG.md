## [Unreleased]

### 2025-07-15
- Fixed QR code fetching error (500 Internal Server Error) by adding database table initialization for payment_qr_codes and payment_transactions tables
- Fixed QR code upload error (500 Internal Server Error) by correcting the path mismatch between multer storage destination and Express static file serving
- Added improved error handling for file uploads in the payment QR code module with better error messages
- Enhanced file validation to ensure only supported image types are accepted
- Added automatic creation of upload directories to prevent errors
- Fixed database column name mismatch by renaming 'enabled' to 'is_enabled' in feature_toggles table
- Updated SQL queries in payment module and feature toggles routes to use correct column names ('feature_name' and 'is_enabled')
- Updated migration scripts and data update scripts to use consistent column names

### 2025-07-12
- Fixed 403 Forbidden errors by updating feature toggle routes to allow Admin users access without requiring specific permissions
- Added missing `/api/logging/entities` endpoint to support the ActivityLogs component
- Fixed source map warnings from react-datepicker by adding GENERATE_SOURCEMAP=false to frontend/.env
- Fixed dependency issues by installing missing packages (express, express-validator, jsonwebtoken, @mui/material, @mui/icons-material)
- Fixed middleware import path in payment-transactions.js
- Added checkPermission function to auth middleware
- Resolved module resolution issues for both frontend and backend
- Fixed 500 Server Error in payment module by correcting the feature toggle check middleware to properly handle SQLite integer representation of boolean values

### 2025-07-11
- Added Payment Integration Module with QR code upload/management functionality and feature toggle support
- Added dedicated API endpoints for payment QR code CRUD operations
- Added frontend components for payment settings and QR code management
- Added database schema for payment transactions and QR code storage
- Extended feature toggle system to include payment integration toggle

### 2025-07-10
- Added `proxy` configuration to `frontend/package.json` to forward API requests to Express backend on port 5000. This resolves 404 errors for `/api` requests from React development server.

### Added
- Feature Toggle system: backend API (CRUD, admin/full_access only), DB migration, and frontend admin UI for managing feature flags.
### Fixed
- ActivityLogList: Timestamp now always displays in a readable format using formatTimestamp utility.
- ActivityLogList: Added 'IP Address / Port' column to activity log table. Now displays the source IP/port for each activity log if available.

### Fixed
- Fixed JSX syntax errors in `frontend/src/components/roles/RoleList.js`, specifically the missing or mismatched `<tr>` closing tag and incorrect button/action JSX structure, which caused rendering issues on the Roles List page.

### Improved
- Enhanced the Role Management table UI in `RoleList.js` for better clarity and aesthetics: improved alignment, better action buttons, permission badge wrapping, and custom styles for a modern look.

### Changed
- Role List: Replaced the "View role" action in the Actions column with an "Edit Role" action. The button now navigates to the edit role page and uses the edit icon with text for clarity.

