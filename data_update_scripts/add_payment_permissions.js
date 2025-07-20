/**
 * Script to add payment permissions to Admin role
 * Date: 2025-07-11
 */

const sqlite3 = require('sqlite3').verbose();

// Connect to the database
const db = new sqlite3.Database('./db/employdex-base.db', (err) => {
  if (err) {
    console.error('Error connecting to the database:', err.message);
    process.exit(1);
  }
  console.log('Connected to the database.');
});

// Payment permissions to add to Admin role
const paymentPermissions = [
  'payment_view',
  'payment_create',
  'payment_edit',
  'payment_delete'
];

// Function to ensure permissions exist in the database
function ensurePermissionsExist() {
  console.log('Checking if payment permissions exist...');
  
  // Get Admin role ID
  db.get('SELECT role_id FROM roles_master WHERE name = ?', ['Admin'], (err, adminRole) => {
    if (err) {
      console.error('Error getting Admin role:', err.message);
      db.close();
      return;
    }
    
    if (!adminRole) {
      console.error('Admin role not found in the database');
      db.close();
      return;
    }
    
    const adminRoleId = adminRole.role_id;
    console.log(`Found Admin role with ID: ${adminRoleId}`);
    
    // For each permission
    paymentPermissions.forEach(permissionName => {
      // Check if permission exists
      db.get('SELECT permission_id FROM permissions_master WHERE name = ?', [permissionName], (err, permission) => {
        if (err) {
          console.error(`Error checking ${permissionName}:`, err.message);
          return;
        }
        
        if (!permission) {
          // Create permission if it doesn't exist
          db.run('INSERT INTO permissions_master (name, description) VALUES (?, ?)', 
            [permissionName, `${permissionName} permission for payment module`], 
            function(err) {
              if (err) {
                console.error(`Error creating ${permissionName}:`, err.message);
                return;
              }
              
              const permissionId = this.lastID;
              console.log(`Created permission: ${permissionName} with ID: ${permissionId}`);
              
              // Assign to Admin role
              assignPermissionToRole(adminRoleId, permissionId, permissionName);
            }
          );
        } else {
          console.log(`Permission ${permissionName} already exists with ID: ${permission.permission_id}`);
          
          // Check if permission is assigned to Admin role
          checkAndAssignPermission(adminRoleId, permission.permission_id, permissionName);
        }
      });
    });
  });
}

// Function to check if permission is assigned to role and assign if not
function checkAndAssignPermission(roleId, permissionId, permissionName) {
  db.get('SELECT * FROM role_permissions_tx WHERE role_id = ? AND permission_id = ?', 
    [roleId, permissionId], 
    (err, rolePermission) => {
      if (err) {
        console.error(`Error checking role permission for ${permissionName}:`, err.message);
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

// Enable feature toggle for payment integration if not already enabled
function enablePaymentFeatureToggle() {
  console.log('Checking payment feature toggle...');
  
  db.get('SELECT * FROM feature_toggles WHERE feature_name = ?', ['payment_integration'], (err, toggle) => {
    if (err) {
      console.error('Error checking feature toggle:', err.message);
      return;
    }
    
    if (!toggle) {
      // Create feature toggle
      db.run('INSERT INTO feature_toggles (feature_name, description, is_enabled, feature) VALUES (?, ?, ?, ?)',
        ['payment_integration', 'Enable payment integration with QR code support', 1, 'payment'],
        function(err) {
          if (err) {
            console.error('Error creating feature toggle:', err.message);
            return;
          }
          
          console.log('Created and enabled payment_integration feature toggle');
        }
      );
    } else if (!toggle.is_enabled) {
      // Enable feature toggle
      db.run('UPDATE feature_toggles SET is_enabled = 1 WHERE feature_name = ?', ['payment_integration'], function(err) {
        if (err) {
          console.error('Error enabling feature toggle:', err.message);
          return;
        }
        
        console.log('Enabled payment_integration feature toggle');
      });
    } else {
      console.log('Payment integration feature toggle already enabled');
    }
  });
}

// Create necessary tables first
db.serialize(() => {
  // Create permissions table if it doesn't exist
  db.run(`
    CREATE TABLE IF NOT EXISTS permissions_master (
      permission_id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT UNIQUE NOT NULL,
      description TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `, (err) => {
    if (err) console.error('Error creating permissions table:', err.message);
    else console.log('Permissions table ready');
  });

  // Create role_permissions table if it doesn't exist
  db.run(`
    CREATE TABLE IF NOT EXISTS role_permissions_tx (
      role_permission_id INTEGER PRIMARY KEY AUTOINCREMENT,
      role_id INTEGER NOT NULL,
      permission_id INTEGER NOT NULL,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      UNIQUE (role_id, permission_id)
    )
  `, (err) => {
    if (err) console.error('Error creating role_permissions table:', err.message);
    else console.log('Role permissions table ready');
  });

  // Create feature_toggles table if it doesn't exist
  db.run(`
    CREATE TABLE IF NOT EXISTS feature_toggles (
      toggle_id INTEGER PRIMARY KEY AUTOINCREMENT,
      feature_name TEXT UNIQUE NOT NULL,
      description TEXT,
      enabled INTEGER DEFAULT 0,
      feature TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `, (err) => {
    if (err) console.error('Error creating feature_toggles table:', err.message);
    else console.log('Feature toggles table ready');
    
    // Run the main functions after tables are created
    ensurePermissionsExist();
    enablePaymentFeatureToggle();
  });
});

// We will NOT close the database in the timeout
// Instead, use process.exit after a reasonable delay to ensure all operations complete
setTimeout(() => {
  console.log('Operations completed');
  console.log('Done!');
  process.exit(0);
}, 5000);
