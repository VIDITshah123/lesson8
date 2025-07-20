const bcrypt = require('bcryptjs');

const password = 'Admin@321';
bcrypt.hash(password, 10, (err, hash) => {
  if (err) {
    console.error('Error generating hash:', err);
    return;
  }
  console.log('Password:', password);
  console.log('Generated Hash:', hash);
});
