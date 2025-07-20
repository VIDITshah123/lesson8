/**
 * Script to create a 'full_access' role with all permissions and a user 'FA' 
 * assigned to this role
 */
const sqlite3 = require('sqlite3').verbose();
const bcrypt = require('bcryptjs');
const path = require('path');
const fs = require('fs');

// Connect to SQLite database
const dbPath = path.join(__dirname, 'db', 'employdex-base.db');

// Check if database exists
if (!fs.existsSync(dbPath)) {
  console.error(`Database file not found at ${dbPath}`);
  process.exit(1);
}

console.log('Using database at path:', dbPath);

const db = new sqlite3.Database(dbPath, (err) => {
  if (err) {
    console.error('Error connecting to database:', err.message);
    process.exit(1);
  }
  console.log('Connected to database successfully');
});

// Enable foreign keys
db.run('PRAGMA foreign_keys = ON', (err) => {
  if (err) {
    console.error('Error enabling foreign keys:', err);
    process.exit(1);
  }
});

// Wrap db.run in a Promise
const run = (sql, params = []) => {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function(err) {
      if (err) return reject(err);
      resolve(this.lastID);
    });
  });
};

// Wrap db.get in a Promise
const get = (sql, params = []) => {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (err, row) => {
      if (err) return reject(err);
      resolve(row);
    });
  });
};

// Wrap db.all in a Promise
const all = (sql, params = []) => {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (err, rows) => {
      if (err) return reject(err);
      resolve(rows);
    });
  });
};

// Main function to create role and user
async function createFullAccessRoleAndUser() {
  try {
    console.log('Starting creation of full_access role and FA user...');
    
    // 1. Get all existing permissions
    const permissions = await all('SELECT * FROM permissions_master');
    console.log(`Found ${permissions.length} permissions in the system`);
    
    // 2. Create the full_access role
    console.log('Creating full_access role...');
    const roleExists = await get('SELECT * FROM roles_master WHERE name = ?', ['full_access']);
    
    let roleId;
    if (roleExists) {
      console.log('Role full_access already exists with ID:', roleExists.role_id);
      roleId = roleExists.role_id;
    } else {
      roleId = await run(
        'INSERT INTO roles_master (name, description, created_at, updated_at) VALUES (?, ?, datetime("now"), datetime("now"))',
        ['full_access', 'Role with access to all functionality']
      );
      console.log('Created new role full_access with ID:', roleId);
    }
    
    // 3. Assign all permissions to the role
    console.log('Assigning all permissions to full_access role...');
    // First, clear existing permissions
    await run('DELETE FROM role_permissions_tx WHERE role_id = ?', [roleId]);
    
    // Then assign all permissions
    for (const permission of permissions) {
      await run(
        'INSERT INTO role_permissions_tx (role_id, permission_id, created_at) VALUES (?, ?, datetime("now"))',
        [roleId, permission.permission_id]
      );
    }
    
    // 4. Create FA user if they don't exist
    console.log('Creating FA user...');
    const userExists = await get('SELECT * FROM users_master WHERE mobile_number = ?', ['8888888888']);
    
    let userId;
    if (userExists) {
      console.log('User FA already exists with ID:', userExists.user_id);
      userId = userExists.user_id;
      
      // Update password
      const passwordHash = await bcrypt.hash('User@123', 10);
      await run(
        'UPDATE users_master SET password_hash = ?, updated_at = datetime("now") WHERE user_id = ?',
        [passwordHash, userId]
      );
      console.log('Updated password for user FA');
    } else {
      // Hash the password
      const passwordHash = await bcrypt.hash('User@123', 10);
      
      // Insert new user
      userId = await run(
        `INSERT INTO users_master 
         (mobile_number, password_hash, email, first_name, last_name, is_active, created_at, updated_at) 
         VALUES (?, ?, ?, ?, ?, ?, datetime("now"), datetime("now"))`,
        ['8888888888', passwordHash, 'fa@employdex.com', 'FA', 'User', 1]
      );
      console.log('Created new user FA with ID:', userId);
    }
    
    // 5. Assign full_access role to FA user
    console.log('Assigning full_access role to FA user...');
    // Check if role assignment already exists
    const roleAssignmentExists = await get(
      'SELECT * FROM user_roles_tx WHERE user_id = ? AND role_id = ?', 
      [userId, roleId]
    );
    
    if (!roleAssignmentExists) {
      await run(
        'INSERT INTO user_roles_tx (user_id, role_id, created_at) VALUES (?, ?, datetime("now"))',
        [userId, roleId]
      );
      console.log('Assigned full_access role to user FA');
    } else {
      console.log('User FA already has full_access role assigned');
    }
    
    console.log('Successfully completed setup of full_access role and FA user!');
    console.log('FA user credentials:');
    console.log('Username: 8888888888 (mobile number)');
    console.log('Email: fa@employdex.com');
    console.log('Password: User@123');
    
  } catch (error) {
    console.error('Error:', error);
  } finally {
    // Close the database connection
    db.close();
  }
}

// Execute the main function
createFullAccessRoleAndUser();
