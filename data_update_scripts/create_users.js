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

// Define 5 new users with default password
const users = [
  {
    mobile_number: '9876543210',
    email: 'john.doe@employdex.com',
    password: 'User@123',
    first_name: 'John',
    last_name: 'Doe'
  },
  {
    mobile_number: '9876543211',
    email: 'jane.smith@employdex.com',
    password: 'User@123',
    first_name: 'Jane',
    last_name: 'Smith'
  },
  {
    mobile_number: '9876543212',
    email: 'robert.johnson@employdex.com',
    password: 'User@123',
    first_name: 'Robert',
    last_name: 'Johnson'
  },
  {
    mobile_number: '9876543213',
    email: 'emily.williams@employdex.com',
    password: 'User@123',
    first_name: 'Emily',
    last_name: 'Williams'
  },
  {
    mobile_number: '9876543214',
    email: 'michael.brown@employdex.com',
    password: 'User@123',
    first_name: 'Michael',
    last_name: 'Brown'
  }
];

// Function to create users and assign roles
async function createUsers() {
  try {
    // Get User role ID
    db.get('SELECT role_id FROM roles_master WHERE name = ?', ['User'], async (err, role) => {
      if (err) {
        console.error('Error retrieving User role:', err.message);
        db.close();
        return;
      }
      
      if (!role) {
        console.error('User role not found in database');
        db.close();
        return;
      }
      
      const userRoleId = role.role_id;
      console.log(`Found User role with ID: ${userRoleId}`);
      
      // Process each user
      for (const user of users) {
        try {
          // Hash the password with the same method used in authentication module
          const salt = await bcrypt.genSalt(10);
          const hashedPassword = await bcrypt.hash(user.password, salt);
          
          // Insert user
          db.run(
            'INSERT INTO users_master (mobile_number, email, password_hash, first_name, last_name) VALUES (?, ?, ?, ?, ?)',
            [user.mobile_number, user.email, hashedPassword, user.first_name, user.last_name],
            function(err) {
              if (err) {
                console.error(`Error creating user ${user.email}:`, err.message);
                return;
              }
              
              const userId = this.lastID;
              console.log(`Created user: ${user.first_name} ${user.last_name} (${user.email}) with ID: ${userId}`);
              
              // Assign User role
              db.run(
                'INSERT INTO user_roles_tx (user_id, role_id) VALUES (?, ?)',
                [userId, userRoleId],
                function(err) {
                  if (err) {
                    console.error(`Error assigning role to user ${user.email}:`, err.message);
                    return;
                  }
                  
                  console.log(`Assigned User role to: ${user.email}`);
                }
              );
            }
          );
        } catch (hashError) {
          console.error(`Error hashing password for user ${user.email}:`, hashError.message);
        }
      }
    });
    
    // Keep connection open for async operations to complete
    setTimeout(() => {
      console.log('Completed user creation process');
      db.close((err) => {
        if (err) {
          console.error('Error closing database:', err.message);
        } else {
          console.log('Database connection closed.');
        }
      });
    }, 2000);
    
  } catch (error) {
    console.error('Error in createUsers function:', error.message);
    db.close();
  }
}

// Execute the function
createUsers();
