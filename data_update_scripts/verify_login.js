const sqlite3 = require('sqlite3').verbose();
const bcrypt = require('bcryptjs');

// Connect to the SQLite database
const db = new sqlite3.Database('./db/employdex-base.db', (err) => {
  if (err) {
    console.error('Error connecting to the database:', err.message);
    process.exit(1);
  }
  console.log('Connected to the database.');
});

// Test credentials
const testUser = {
  email: 'john.doe@employdex.com',
  password: 'User@123'
};

async function verifyLogin() {
  try {
    console.log(`Verifying login for user: ${testUser.email}`);
    
    // Find user by email (simulating login)
    db.get(
      'SELECT user_id, email, password_hash, first_name, last_name, is_active FROM users_master WHERE email = ?',
      [testUser.email],
      async (err, user) => {
        if (err) {
          console.error('Database error:', err);
          db.close();
          return;
        }
        
        if (!user) {
          console.error('User not found');
          db.close();
          return;
        }
        
        console.log(`Found user: ${user.first_name} ${user.last_name} (ID: ${user.user_id})`);
        console.log(`Stored hash: ${user.password_hash}`);
        
        // Verify password (simulating login authentication)
        const isMatch = await bcrypt.compare(testUser.password, user.password_hash);
        
        if (isMatch) {
          console.log('SUCCESS: Password verification successful! Login would work');
          
          // Get user roles and permissions
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
              
              console.log('User roles:', roles);
              console.log('User permissions:', permissions);
              
              db.close();
            }
          );
        } else {
          console.error('ERROR: Password verification failed! Login would not work');
          console.log('Testing with direct hash method...');
          
          // For debugging: Try the direct hash method
          const salt = await bcrypt.genSalt(10);
          const testHash = await bcrypt.hash(testUser.password, salt);
          console.log(`Generated test hash: ${testHash}`);
          console.log(`Direct comparison would match: ${testHash === user.password_hash}`);
          console.log('Note: This should almost always be false as bcrypt generates unique salts');
          
          db.close();
        }
      }
    );
  } catch (error) {
    console.error('Error in verifyLogin function:', error.message);
    db.close();
  }
}

// Execute validation
verifyLogin();
