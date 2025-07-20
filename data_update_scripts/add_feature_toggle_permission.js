/**
 * Add feature_toggle_view permission to Admin role
 */
const sqlite3 = require('sqlite3').verbose();
const path = require('path');
const fs = require('fs');

// Database path
const dbPath = path.join(__dirname, '..', 'db', 'employdex-base.db');

// Open database connection
const db = new sqlite3.Database(dbPath, (err) => {
  if (err) {
    console.error('Error connecting to database:', err.message);
    process.exit(1);
  }
  console.log('Connected to the database.');
});

// Check if feature_toggle_view permission exists, create if not
async function ensureFeatureTogglePermission() {
  try {
    // Get Admin role ID
    db.get('SELECT role_id FROM roles_master WHERE name = ?', ['Admin'], (err, adminRole) => {
      if (err) {
        console.error('Error getting Admin role:', err.message);
        return;
      }
      
      if (!adminRole) {
        console.error('Admin role not found');
        return;
      }
      
      const adminRoleId = adminRole.role_id;
      console.log(`Found Admin role with ID: ${adminRoleId}`);
      
      // Check if feature_toggle_view permission exists
      db.get('SELECT permission_id FROM permissions_master WHERE name = ?', 
        ['feature_toggle_view'], 
        (err, permission) => {
          if (err) {
            console.error('Error checking feature_toggle_view permission:', err.message);
            return;
          }
          
          if (!permission) {
            // Create permission if it doesn't exist
            db.run('INSERT INTO permissions_master (name, description) VALUES (?, ?)', 
              ['feature_toggle_view', 'View feature toggles'], 
              function(err) {
                if (err) {
                  console.error('Error creating feature_toggle_view permission:', err.message);
                  return;
                }
                
                const permissionId = this.lastID;
                console.log(`Created permission: feature_toggle_view with ID: ${permissionId}`);
                
                // Assign to Admin role
                assignPermissionToRole(adminRoleId, permissionId, 'feature_toggle_view');
              }
            );
          } else {
            console.log(`Found feature_toggle_view permission with ID: ${permission.permission_id}`);
            
            // Check if already assigned to Admin role
            checkAndAssignPermission(adminRoleId, permission.permission_id, 'feature_toggle_view');
          }
        }
      );
    });
  } catch (error) {
    console.error('Error ensuring feature_toggle_view permission:', error);
  }
}

// Function to check if permission is assigned to role and assign if not
function checkAndAssignPermission(roleId, permissionId, permissionName) {
  db.get('SELECT * FROM role_permissions_tx WHERE role_id = ? AND permission_id = ?', 
    [roleId, permissionId], 
    (err, rolePermission) => {
      if (err) {
        console.error(`Error checking if ${permissionName} is assigned to role:`, err.message);
        return;
      }
      
      if (!rolePermission) {
        // Assign permission to role
        assignPermissionToRole(roleId, permissionId, permissionName);
      } else {
        console.log(`Permission ${permissionName} already assigned to Admin role`);
      }
    }
  );
}

// Function to assign permission to role
function assignPermissionToRole(roleId, permissionId, permissionName) {
  db.run('INSERT INTO role_permissions_tx (role_id, permission_id) VALUES (?, ?)', 
    [roleId, permissionId], 
    function(err) {
      if (err) {
        console.error(`Error assigning ${permissionName} to Admin role:`, err.message);
        return;
      }
      
      console.log(`Assigned permission ${permissionName} to Admin role`);
    }
  );
}

// Run the function
ensureFeatureTogglePermission();

// Give enough time for async operations to complete before exiting
setTimeout(() => {
  console.log('Operations completed');
  process.exit(0);
}, 3000);
