const sqlite3 = require('sqlite3').verbose();

// Connect to the SQLite database
const db = new sqlite3.Database('./db/employdex-base.db', (err) => {
  if (err) {
    console.error('Error connecting to the database:', err.message);
    process.exit(1);
  }
  console.log('Connected to the database.');
});

// Delete all users except the admin
async function resetUsers() {
  try {
    // Get admin user ID
    db.get('SELECT user_id FROM users_master WHERE email = ?', ['admin@employdex.com'], (err, adminUser) => {
      if (err) {
        console.error('Error retrieving admin user:', err.message);
        db.close();
        return;
      }
      
      if (!adminUser) {
        console.error('Admin user not found in database');
        db.close();
        return;
      }
      
      const adminId = adminUser.user_id;
      console.log(`Found admin user with ID: ${adminId}`);
      
      // Delete all user_roles for non-admin users
      db.run(
        'DELETE FROM user_roles_tx WHERE user_id != ?',
        [adminId],
        function(err) {
          if (err) {
            console.error('Error deleting user roles:', err.message);
            return;
          }
          
          console.log(`Deleted ${this.changes} user role assignments`);
          
          // Delete all non-admin users
          db.run(
            'DELETE FROM users_master WHERE user_id != ?',
            [adminId],
            function(err) {
              if (err) {
                console.error('Error deleting users:', err.message);
                return;
              }
              
              console.log(`Deleted ${this.changes} users`);
              
              // Close the connection
              db.close((err) => {
                if (err) {
                  console.error('Error closing database:', err.message);
                } else {
                  console.log('Database connection closed.');
                }
              });
            }
          );
        }
      );
    });
    
  } catch (error) {
    console.error('Error in resetUsers function:', error.message);
    db.close();
  }
}

// Execute the function
resetUsers();
