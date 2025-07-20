const sqlite3 = require('sqlite3').verbose();
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');

// Constants from the app
const JWT_SECRET = 'employdex-base-v1-secure-jwt-secret';
const JWT_EXPIRES_IN = '24h';

// Connect to the SQLite database
const db = new sqlite3.Database('./db/employdex-base.db', (err) => {
  if (err) {
    console.error('Error connecting to the database:', err.message);
    process.exit(1);
  }
  console.log('Connected to the database.');
});

// Admin credentials
const adminUser = {
  email: 'admin@employdex.com',
  password: 'Admin@123'
};

// Function to simulate admin login and verify permissions
async function testAdminLogin() {
  console.log('Testing admin login with credentials:');
  console.log(`- Email: ${adminUser.email}`);
  console.log(`- Password: ${adminUser.password}`);

  // Find admin by email (simulating login)
  db.get(
    'SELECT user_id, email, password_hash, first_name, last_name, is_active FROM users_master WHERE email = ?',
    [adminUser.email],
    async (err, user) => {
      if (err) {
        console.error('Database error:', err);
        db.close();
        return;
      }
      
      if (!user) {
        console.error('Admin user not found');
        db.close();
        return;
      }
      
      console.log(`Found admin user: ${user.first_name} ${user.last_name} (ID: ${user.user_id})`);
      
      // Verify password
      const isMatch = await bcrypt.compare(adminUser.password, user.password_hash);
      
      if (!isMatch) {
        console.error('Password verification failed!');
        db.close();
        return;
      }
      
      console.log('Password verification successful!');
      
      // Get admin roles and permissions (exactly as in the authentication module)
      db.all(
        `SELECT r.name as role_name, p.name as permission_name
         FROM user_roles_tx ur
         JOIN roles_master r ON ur.role_id = r.role_id
         LEFT JOIN role_permissions_tx rp ON r.role_id = rp.role_id
         LEFT JOIN permissions_master p ON rp.permission_id = p.permission_id
         WHERE ur.user_id = ?`,
        [user.user_id],
        (err, userRolesAndPermissions) => {
          if (err) {
            console.error('Error retrieving user roles:', err);
            db.close();
            return;
          }
          
          // Extract roles and permissions
          const roles = [...new Set(userRolesAndPermissions.map(item => item.role_name))];
          const permissions = userRolesAndPermissions
            .filter(item => item.permission_name)
            .map(item => item.permission_name);
          
          console.log('\nAdmin roles found:', roles);
          console.log('\nAdmin permissions found:', permissions);
          
          // Check for specific permissions
          const requiredPermissions = [
            'user_view', 'user_create', 'user_edit', 'user_delete',
            'role_view', 'role_create', 'role_edit', 'role_delete',
            'permission_view', 'permission_assign', 'activity_view',
            // Payment module permissions
            'payment_view', 'payment_create', 'payment_edit', 'payment_delete'
          ];
          
          console.log('\nChecking for required permissions:');
          const missingPermissions = requiredPermissions.filter(p => !permissions.includes(p));
          
          if (missingPermissions.length > 0) {
            console.error('Missing required permissions:', missingPermissions);
            console.log('\nFixing missing permissions for Admin role...');
            
            // Get Admin role ID
            db.get('SELECT role_id FROM roles_master WHERE name = ?', ['Admin'], (err, role) => {
              if (err || !role) {
                console.error('Error getting Admin role:', err ? err.message : 'Role not found');
                db.close();
                return;
              }
              
              // Get all missing permission IDs
              db.all(
                'SELECT permission_id FROM permissions_master WHERE name IN (' + 
                missingPermissions.map(() => '?').join(',') + ')',
                missingPermissions,
                (err, permissionsToAdd) => {
                  if (err) {
                    console.error('Error getting permission IDs:', err.message);
                    db.close();
                    return;
                  }
                  
                  console.log('Permissions to add:', permissionsToAdd);
                  
                  // Add missing permissions to Admin role
                  const insertPromises = permissionsToAdd.map(p => {
                    return new Promise((resolve, reject) => {
                      db.run(
                        'INSERT OR IGNORE INTO role_permissions_tx (role_id, permission_id) VALUES (?, ?)',
                        [role.role_id, p.permission_id],
                        function(err) {
                          if (err) reject(err);
                          else resolve();
                        }
                      );
                    });
                  });
                  
                  Promise.all(insertPromises)
                    .then(() => {
                      console.log('Successfully added missing permissions to Admin role');
                      
                      // Verify the fix
                      db.all(
                        `SELECT p.name
                         FROM role_permissions_tx rp
                         JOIN permissions_master p ON rp.permission_id = p.permission_id
                         WHERE rp.role_id = ?`,
                        [role.role_id],
                        (err, updatedPermissions) => {
                          if (err) {
                            console.error('Error verifying permissions:', err.message);
                          } else {
                            const updatedPermissionNames = updatedPermissions.map(p => p.name);
                            console.log('\nUpdated Admin permissions:', updatedPermissionNames);
                            
                            // Check if all required permissions are now assigned
                            const stillMissing = requiredPermissions.filter(p => !updatedPermissionNames.includes(p));
                            
                            if (stillMissing.length > 0) {
                              console.error('Still missing permissions:', stillMissing);
                            } else {
                              console.log('All required permissions are now assigned to Admin role!');
                              
                              // Create and print JWT token for verification
                              const payload = {
                                user: {
                                  id: user.user_id,
                                  email: user.email,
                                  first_name: user.first_name,
                                  last_name: user.last_name,
                                  roles: roles,
                                  permissions: updatedPermissionNames
                                }
                              };
                              
                              jwt.sign(payload, JWT_SECRET, { expiresIn: JWT_EXPIRES_IN }, (err, token) => {
                                if (err) {
                                  console.error('Error creating token:', err.message);
                                } else {
                                  console.log('\nFixed JWT token (for debugging):', token);
                                  console.log('\nToken payload:', payload);
                                }
                                
                                db.close();
                              });
                            }
                          }
                        }
                      );
                    })
                    .catch(err => {
                      console.error('Error adding permissions:', err.message);
                      db.close();
                    });
                }
              );
            });
          } else {
            console.log('All required permissions are assigned to Admin role!');
            
            // Create JWT token as in the auth module
            const payload = {
              user: {
                id: user.user_id,
                email: user.email,
                first_name: user.first_name,
                last_name: user.last_name,
                roles,
                permissions
              }
            };
            
            jwt.sign(payload, JWT_SECRET, { expiresIn: JWT_EXPIRES_IN }, (err, token) => {
              if (err) {
                console.error('Error creating token:', err.message);
              } else {
                console.log('\nJWT token (for debugging):', token);
                console.log('\nToken payload:', payload);
              }
              
              db.close();
            });
          }
        }
      );
    }
  );
}

// Execute the test
testAdminLogin();
