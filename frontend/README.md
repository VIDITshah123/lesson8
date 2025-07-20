# EmployDEX Base Platform Frontend

## Overview

This React application provides the frontend for the EmployDEX Base Platform, supporting authentication, user management, role management, permission management, logging, and database operations. It runs on port 3000 and communicates with the Express.js backend on port 5000.

## Key Features
- Role-based access control (RBAC)
- User and role CRUD operations
- Permission management
- Modern glassmorphism UI

## Recent Changes

### [Unreleased]
- Role List: Replaced the "View role" action in the Actions column with an "Edit Role" action. The button now navigates to the edit role page and uses the edit icon with text for clarity.

## Setup

1. Ensure Node.js and npm are installed.
2. Install dependencies (from the root project directory):
   ```sh
   npm run install:all
   ```
3. Start the application:
   ```sh
   npm run start
   ```
   This will start both the backend (port 5000) and frontend (port 3000) concurrently.

## Default Admin Credentials
- **Username:** admin
- **Password:** Admin@123

## Directory Structure
- `src/components/roles/RoleList.js` — Role list and management UI
- `src/components/users/UserList.js` — User list and management UI

## Notes
- The application uses SQLite for the database.
- To edit a role, use the "Edit Role" button in the Role List table.
- To manage permissions, use the permission modal accessible from the Role List.

For more details, see the backend README and CHANGELOG.
