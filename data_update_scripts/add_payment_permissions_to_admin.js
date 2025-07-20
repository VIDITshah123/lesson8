/**
 * Script to add payment permissions to Admin role
 * Created: 2025-07-12
 * 
 * This script ensures Admin role has payment_view and payment_edit permissions
 */

const sqlite3 = require('sqlite3').verbose();
const path = require('path');

// Connect to the database
const dbPath = path.join(__dirname, '..', 'db', 'employdex-base.db');
console.log(`Connecting to database at: ${dbPath}`);
const db = new sqlite3.Database(dbPath, (err) => {
  if (err) {
    console.error('Error connecting to database:', err.message);
    process.exit(1);
  }
  console.log('Connected to the database.');
});

// Configure SQLite to handle concurrent connections better
db.configure('busyTimeout', 5000);

// Main function to ensure payment permissions for Admin role
function ensurePaymentPermissionsForAdmin() {
  // Get Admin role ID
  db.get('SELECT role_id FROM roles_master WHERE name = ?', ['Admin'], (err, adminRole) => {
    if (err) {
      console.error('Error finding Admin role:', err.message);
      closeAndExit(1);
      return;
    }
    
    if (!adminRole) {
      console.error('Admin role not found in database');
      closeAndExit(1);
      return;
    }
    
    const adminRoleId = adminRole.role_id;
    console.log(`Found Admin role with ID: ${adminRoleId}`);
    
    // Check and add payment_view permission
    ensurePermission('payment_view', 'View payment transactions and QR codes', adminRoleId);
    
    // Check and add payment_edit permission
    ensurePermission('payment_edit', 'Manage payment transactions and QR codes', adminRoleId);
  });
}

// Helper function to ensure a permission exists and is assigned to the Admin role
function ensurePermission(permissionName, permissionDescription, adminRoleId) {
  // Check if permission exists
  db.get('SELECT permission_id FROM permissions_master WHERE name = ?', [permissionName], (err, permission) => {
    if (err) {
      console.error(`Error checking for ${permissionName} permission:`, err.message);
      return;
    }
    
    if (permission) {
      // Permission exists, check if it's assigned to Admin role
      const permissionId = permission.permission_id;
      console.log(`Found ${permissionName} permission with ID: ${permissionId}`);
      
      db.get('SELECT * FROM role_permissions_tx WHERE role_id = ? AND permission_id = ?', [adminRoleId, permissionId], (err, rolePermission) => {
        if (err) {
          console.error(`Error checking if ${permissionName} is assigned to Admin:`, err.message);
          return;
        }
        
        if (rolePermission) {
          console.log(`Admin role already has ${permissionName} permission`);
        } else {
          // Assign permission to Admin role
          db.run('INSERT INTO role_permissions_tx (role_id, permission_id) VALUES (?, ?)', [adminRoleId, permissionId], function(err) {
            if (err) {
              console.error(`Error assigning ${permissionName} to Admin role:`, err.message);
              return;
            }
            console.log(`Successfully assigned ${permissionName} permission to Admin role`);
          });
        }
      });
    } else {
      // Permission doesn't exist, create it and assign to Admin
      db.run('INSERT INTO permissions_master (name, description) VALUES (?, ?)', [permissionName, permissionDescription], function(err) {
        if (err) {
          console.error(`Error creating ${permissionName} permission:`, err.message);
          return;
        }
        
        const permissionId = this.lastID;
        console.log(`Created ${permissionName} permission with ID: ${permissionId}`);
        
        // Assign new permission to Admin role
        db.run('INSERT INTO role_permissions_tx (role_id, permission_id) VALUES (?, ?)', [adminRoleId, permissionId], function(err) {
          if (err) {
            console.error(`Error assigning ${permissionName} to Admin role:`, err.message);
            return;
          }
          console.log(`Successfully assigned ${permissionName} permission to Admin role`);
        });
      });
    }
  });
}

// Close database connection and exit
function closeAndExit(code = 0) {
  db.close((err) => {
    if (err) {
      console.error('Error closing database connection:', err.message);
      process.exit(1);
    }
    console.log('Database connection closed.');
    process.exit(code);
  });
}

// Run the main function
ensurePaymentPermissionsForAdmin();

// Set a timeout to ensure the script exits even if async operations are pending
setTimeout(() => {
  console.log('Script execution timeout reached. Closing database connection.');
  closeAndExit(0);
}, 5000);
