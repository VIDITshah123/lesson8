/**
 * Script to add feature_toggle_edit permission to Admin role
 * Date: 2025-07-12
 */

const sqlite3 = require('sqlite3').verbose();
const path = require('path');

// Database path
const dbPath = path.join(__dirname, '..', 'db', 'employdex-base.db');

// Connect to the database with extended timeout and busy handling
const db = new sqlite3.Database(dbPath, sqlite3.OPEN_READWRITE, (err) => {
  if (err) {
    console.error('Error connecting to the database:', err.message);
    process.exit(1);
  }
  console.log('Connected to the database.');
});

// Configure busy timeout to wait longer when database is locked
db.configure('busyTimeout', 5000); // 5 seconds

// Direct approach to add feature_toggle_edit permission to Admin role
function addFeatureToggleEditPermission() {
  console.log('Adding feature_toggle_edit permission to Admin role...');
  
  // First, get the Admin role ID
  db.get('SELECT role_id FROM roles_master WHERE name = ?', ['Admin'], (err, adminRole) => {
    if (err) {
      console.error('Error getting Admin role:', err.message);
      db.close();
      return;
    }
    
    if (!adminRole) {
      console.error('Admin role not found in database');
      db.close();
      return;
    }
    
    const adminRoleId = adminRole.role_id;
    console.log(`Found Admin role with ID: ${adminRoleId}`);
    
    // Next, get the feature_toggle_edit permission ID
    db.get('SELECT permission_id FROM permissions_master WHERE name = ?', ['feature_toggle_edit'], (err, permission) => {
      if (err) {
        console.error('Error getting feature_toggle_edit permission:', err.message);
        db.close();
        return;
      }
      
      if (!permission) {
        console.error('feature_toggle_edit permission not found in database');
        db.close();
        return;
      }
      
      const permissionId = permission.permission_id;
      console.log(`Found feature_toggle_edit permission with ID: ${permissionId}`);
      
      // Check if the permission is already assigned to the Admin role
      db.get(
        'SELECT * FROM role_permissions_tx WHERE role_id = ? AND permission_id = ?', 
        [adminRoleId, permissionId], 
        (err, existingAssignment) => {
          if (err) {
            console.error('Error checking existing permission assignment:', err.message);
            db.close();
            return;
          }
          
          if (existingAssignment) {
            console.log('Permission is already assigned to Admin role');
            db.close();
            return;
          }
          
          // Add the permission to the Admin role
          db.run(
            'INSERT INTO role_permissions_tx (role_id, permission_id) VALUES (?, ?)', 
            [adminRoleId, permissionId], 
            function(err) {
              if (err) {
                console.error('Error assigning permission to Admin role:', err.message);
                db.close();
                return;
              }
              
              console.log(`Successfully assigned feature_toggle_edit permission to Admin role. Row ID: ${this.lastID}`);
              console.log('Done!');
              db.close();
            }
          );
        }
      );
    });
  });
}

// Run the function
addFeatureToggleEditPermission();
