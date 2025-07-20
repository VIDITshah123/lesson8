# Product Requirements Document (PRD): EmployDEX Base Platform

## Introduction/Overview
The EmployDEX Base Platform serves as a foundational system providing essential user management capabilities, including user registration, authentication, role-based access control, and an administrative dashboard. This platform will act as the building block for future EmployDEX applications, allowing for a consistent and secure user management approach across the ecosystem.

## Goals
1. Create a secure and scalable user authentication system
2. Implement role-based access control for flexible permission management
3. Provide an intuitive dashboard for users based on their roles
4. Develop an administrative interface to manage users and roles
5. Establish a solid architectural foundation for future application development

## User Stories

### Authentication
- As a new user, I want to register for an account so that I can access the system.
- As a registered user, I want to log in securely so that I can access my account.
- As a user, I want to log out of the system so that my session is terminated securely.
- As a user, I want to reset my password if I forget it so that I can regain access to my account.

### User Dashboard
- As a logged-in user, I want to see a personalized dashboard so that I can access relevant functions.
- As a user, I want to view and edit my profile information so that my details remain up to date.

### Role Management
- As an administrator, I want to create new roles so that I can organize users by function.
- As an administrator, I want to assign roles to users so that they have appropriate access permissions.
- As an administrator, I want to modify existing roles so that I can adjust permissions as needed.
- As an administrator, I want to remove roles from users so that I can revoke access when necessary.

### Administration
- As an administrator, I want to view all registered users so that I can manage the user base.
- As an administrator, I want to enable/disable user accounts so that I can control system access.
- As an administrator, I want to view system activity logs so that I can monitor usage and security.

## Functional Requirements

### User Registration & Authentication
1. The system must allow users to register with mobilenumber, email address, password, first name, and last name .
2. The system must validate mobileNumber during registration to ensure they are properly formatted.
3. The system must enforce password complexity requirements (minimum 8 characters, at least one uppercase letter, one lowercase letter, one number).
4. The system must hash and securely store passwords in the database.
5. The system must provide a login form accepting email and password.
6. The system must implement session-based authentication with JWT tokens.
7. The system must allow users to reset their password via email link.
8. The system must implement rate limiting to prevent brute force attacks.

### User Dashboard
9. The system must display a personalized dashboard after successful login.
10. The system must show different dashboard content based on user roles.
11. The system must provide a user profile section allowing users to update their information.
12. The system must display notifications and alerts relevant to the user.

### Role Management
13. The system must provide CRUD operations for role management.
14. The system must allow roles to be assigned specific permissions.
15. The system must maintain a many-to-many relationship between users and roles.
16. The system must enforce role-based access control on all protected routes.
17. The system must include predefined roles: Admin, User.
18. The system must allow custom roles to be created with specific permissions.

### Administration
19. The system must provide an administrator dashboard with user management capabilities.
20. The system must display a list of all registered users with filtering and sorting options.
21. The system must allow administrators to enable/disable user accounts.
22. The system must provide tools for role assignment to users.
23. The system must include an activity log of important system events.
24. create a default admin with password Admin@123

## Non-Goals (Out of Scope)
1. Advanced analytics or reporting features
2. Integration with external authentication providers (OAuth, SAML, etc.)
3. Multi-factor authentication
4. Automated user provisioning
5. Customizable dashboard widgets
6. Public-facing content management
7. Email marketing or communication features

## Design Considerations
- The user interface should follow a clean, modern design with responsive layouts.
- The dashboard should utilize a sidebar navigation pattern for easy access to different sections.
- Form validation should provide immediate feedback to users.
- The system should use consistent color schemes and typography throughout.
- All interfaces should be responsive and mobile-friendly.

## Technical Considerations
- Frontend: React.js with modular component architecture
- Backend: Express.js API with RESTful endpoints
- Database: SQLite with potential for migration to more robust solutions in the future
- Authentication: JWT-based with secure token management
- Form validation: Client-side and server-side validation
- API documentation: Swagger/OpenAPI specification

## Success Metrics
1. Successfully register and authenticate users
2. Properly enforce role-based permissions
3. Administrators can effectively manage users and roles
4. System maintains security during authentication and authorization processes
5. UI/UX receives positive feedback from test users
6. System passes security review with no critical vulnerabilities

## Database Structure

### Users Table
```
users
- user_id (INTEGER PRIMARY KEY AUTOINCREMENT)
- mobile_number (TEXT UNIQUE NOT NULL)
- password_hash (TEXT NOT NULL)
- email (TEXT UNIQUE NOT NULL)
- first_name (TEXT NOT NULL)
- last_name (TEXT NOT NULL)
- is_active (BOOLEAN DEFAULT 1)
- created_at (TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
- updated_at (TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
```

### Roles Table
```
roles
- role_id (INTEGER PRIMARY KEY AUTOINCREMENT)
- name (TEXT UNIQUE NOT NULL)
- description (TEXT)
- created_at (TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
- updated_at (TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
```

### Permissions Table
```
permissions
- permission_id (INTEGER PRIMARY KEY AUTOINCREMENT)
- name (TEXT UNIQUE NOT NULL)
- description (TEXT)
- created_at (TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
- updated_at (TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
```

### User_Roles Junction Table
```
user_roles
- user_role_id (INTEGER NOT NULL)
- user_id (INTEGER NOT NULL)
- role_id (INTEGER NOT NULL)
- created_at (TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
- PRIMARY KEY (user_id, role_id)
- FOREIGN KEY (user_id) REFERENCES users(id)
- FOREIGN KEY (role_id) REFERENCES roles(id)
```

### Role_Permissions Junction Table
```
role_permissions
- role_permission_id (INTEGER NOT NULL)
- role_id (INTEGER NOT NULL)
- permission_id (INTEGER NOT NULL)
- created_at (TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
- PRIMARY KEY (role_id, permission_id)
- FOREIGN KEY (role_id) REFERENCES roles(id)
- FOREIGN KEY (permission_id) REFERENCES permissions(id)
```

### Activity_Logs Table
```
activity_logs
- activity_log_id (INTEGER PRIMARY KEY AUTOINCREMENT)
- user_id (INTEGER)
- action (TEXT NOT NULL)
- details (TEXT)
- ip_address (TEXT)
- user_agent (TEXT)
- created_at (TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
- FOREIGN KEY (user_id) REFERENCES users(id)
```

## Application Modules

### Backend (Express.js)
1. **Authentication Module**
   - User registration
   - User login/logout
   - Password reset
   - JWT token management

2. **User Management Module**
   - User CRUD operations
   - Profile management
   - Account status management

3. **Role Management Module**
   - Role CRUD operations
   - Permission management
   - Role-user assignments

4. **Permission Module**
   - Permission definitions
   - Permission checks
   - Role-permission assignments

5. **Logging Module**
   - Activity logging
   - Error logging
   - Security event logging

6. **Database Module**
   - Database connection management
   - Query execution
   - Data model implementations

### Frontend (React)
1. **Authentication Components**
   - Registration form
   - Login form
   - Password reset form
   - Protected route components

2. **Dashboard Components**
   - Main dashboard layout
   - Role-specific dashboard views
   - Navigation sidebar/header

3. **User Profile Components**
   - Profile viewing
   - Profile editing
   - Password change

4. **Admin Components**
   - User management interface
   - Role management interface
   - Permission configuration
   - Activity log viewer

5. **Common Components**
   - Form controls
   - Data tables
   - Modal dialogs
   - Notification components
   - Loading indicators

6. **Service Layer**
   - API connection services
   - Authentication services
   - Data transformation utilities
   - Error handling services

## Open Questions
1. Should we implement token refresh mechanism or rely on session expiration?
2. What level of logging detail is required for the activity logs?
3. Should we prepare for future internationalization requirements?
4. Are there specific security standards or compliance requirements to follow?
5. What is the expected user volume for initial deployment?

## Changelog
- **2025-06-26**: Initial PRD created
