const sqlite3 = require('sqlite3').verbose();

// Connect to the SQLite database
const db = new sqlite3.Database('./db/employdex-base.db', (err) => {
  if (err) {
    console.error('Error connecting to the database:', err.message);
    process.exit(1);
  }
  console.log('Connected to the database.');
});

// Check permissions for Admin role
async function checkPermissions() {
  // Get Admin role ID
  db.get('SELECT role_id FROM roles_master WHERE name = ?', ['Admin'], (err, role) => {
    if (err || !role) {
      console.error('Error retrieving Admin role:', err ? err.message : 'Role not found');
      db.close();
      return;
    }
    
    const adminRoleId = role.role_id;
    console.log(`Found Admin role with ID: ${adminRoleId}`);
    
    // Get all available permissions
    db.all('SELECT * FROM permissions_master', [], (err, allPermissions) => {
      if (err) {
        console.error('Error retrieving permissions:', err.message);
        db.close();
        return;
      }
      
      console.log(`Available permissions in system: ${allPermissions.length}`);
      allPermissions.forEach(p => {
        console.log(`- ${p.permission_id}: ${p.name} (${p.description})`);
      });
      
      // Get permissions assigned to Admin role
      db.all(
        `SELECT rp.role_permission_id, p.permission_id, p.name, p.description
         FROM role_permissions_tx rp
         JOIN permissions_master p ON rp.permission_id = p.permission_id
         WHERE rp.role_id = ?`,
        [adminRoleId],
        (err, adminPermissions) => {
          if (err) {
            console.error('Error retrieving Admin permissions:', err.message);
            db.close();
            return;
          }
          
          console.log(`\nPermissions assigned to Admin role: ${adminPermissions.length}`);
          adminPermissions.forEach(p => {
            console.log(`- ${p.permission_id}: ${p.name} (${p.description})`);
          });
          
          // Calculate missing permissions
          const adminPermissionIds = adminPermissions.map(p => p.permission_id);
          const missingPermissions = allPermissions
            .filter(p => !adminPermissionIds.includes(p.permission_id));
          
          console.log(`\nMissing permissions for Admin role: ${missingPermissions.length}`);
          if (missingPermissions.length > 0) {
            missingPermissions.forEach(p => {
              console.log(`- ${p.permission_id}: ${p.name} (${p.description})`);
            });
          } else {
            console.log('Admin role has all available permissions.');
          }
          
          db.close();
        }
      );
    });
  });
}

// Execute the check
checkPermissions();
