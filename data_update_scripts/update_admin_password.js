const sqlite3 = require('sqlite3').verbose();

// Connect to the SQLite database
const db = new sqlite3.Database('./db/employdex-base.db', (err) => {
  if (err) {
    console.error('Error connecting to the database:', err.message);
    process.exit(1);
  }
  console.log('Connected to the database.');
});

// New password hash for Admin@321
const newPasswordHash = '$2a$10$LjZl9CjeQFg1nrz8KvTYlOC.Nvsr5loM2qHbppZrbksSBPbFGVT5S';

// Update the admin user's password
db.run(
  'UPDATE users_master SET password_hash = ? WHERE email = ?',
  [newPasswordHash, 'admin@employdex.com'],
  function(err) {
    if (err) {
      console.error('Error updating password:', err.message);
    } else {
      console.log(`Password updated successfully for admin@employdex.com`);
      console.log(`Rows affected: ${this.changes}`);
    }
    
    // Close the database connection
    db.close((err) => {
      if (err) {
        console.error('Error closing database:', err.message);
      } else {
        console.log('Database connection closed.');
      }
    });
  }
);
