---
description: 
---

---
description: 
globs: 
alwaysApply: false
---
# Run Application

This document provides step-by-step instructions on how to run the EmployDEX Base Platform application.

## Prerequisites
- Node.js and npm must be installed on your system
- Git repository cloned to your local machine

## Installation
1. Install all dependencies for backend and frontend:
   ```bash
   npm run install:all
   ```
   This command installs dependencies for the root project, backend, and frontend.

## Running the Application

### Option 1: Run Both Backend and Frontend Concurrently
1. From the project root directory, run:
   ```bash
   npm run start
   ```
   This will start both the backend server (port 5000) and frontend development server (port 3000) concurrently.

### Option 2: Run Backend and Frontend Separately
1. To run only the backend:
   ```bash
   npm run start:backend
   ```
   The backend API will be available at http://localhost:5000

2. To run only the frontend:
   ```bash
   npm run start:frontend
   ```
   The frontend will be available at http://localhost:3000

## Accessing the Application
Once the application is running, you can access it at http://localhost:3000

### Default Admin Credentials
- Username: admin
- Password: Admin@123

