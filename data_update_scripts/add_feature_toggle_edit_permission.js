/**
 * Add feature_toggle_edit permission to Admin role
 * Date: 2025-07-12
 */
const sqlite3 = require('sqlite3').verbose();
const path = require('path');

// Database path - using the correct path to the database
const dbPath = path.join(__dirname, '..', 'db', 'employdex-base.db');

// Open database connection
const db = new sqlite3.Database(dbPath, (err) => {
  if (err) {
    console.error('Error connecting to database:', err.message);
    process.exit(1);
  }
  console.log('Connected to the database.');
});

// Check if feature_toggle_edit permission exists, create if not
function ensureFeatureToggleEditPermission() {
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
      
      // Check if feature_toggle_edit permission exists
      db.get('SELECT permission_id FROM permissions_master WHERE name = ?', 
        ['feature_toggle_edit'], 
        (err, permission) => {
          if (err) {
            console.error('Error checking feature_toggle_edit permission:', err.message);
            return;
          }
          
          if (!permission) {
            console.error('feature_toggle_edit permission not found in permissions_master');
            return;
          }
          
          console.log(`Found feature_toggle_edit permission with ID: ${permission.permission_id}`);
          
          // Check if already assigned to Admin role
          checkAndAssignPermission(adminRoleId, permission.permission_id, 'feature_toggle_edit');
        }
      );
    });
  } catch (error) {
    console.error('Error ensuring feature_toggle_edit permission:', error);
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
ensureFeatureToggleEditPermission();

// Give enough time for async operations to complete before exiting
setTimeout(() => {
  console.log('Operations completed');
  db.close(() => {
    console.log('Database connection closed');
    process.exit(0);
  });
}, 3000);
