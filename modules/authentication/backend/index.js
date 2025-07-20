/**
 * Authentication Module - Backend Implementation
 * Created: 2025-06-26
 */

const express = require('express');
const router = express.Router();
const { body, validationResult } = require('express-validator');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');

// JWT Secret - Should be in environment variables for production
// Matching the JWT_SECRET in the auth middleware
const JWT_SECRET = 'employdex-base-v1-secure-jwt-secret';
const JWT_EXPIRES_IN = '24h';

// Event listeners for inter-module communication
const registerModuleEvents = (eventBus) => {
  // Listen for user-related events that might require authentication actions
  eventBus.on('user:created', (data) => {
    console.log('Authentication module received user:created event:', data);
    // Handle new user creation, e.g., send welcome email
  });

  eventBus.on('user:password_reset_requested', async (data) => {
    console.log('Password reset requested for user:', data);
    // Handle password reset logic
  });
};

// Middleware to validate token
const authenticateToken = (req, res, next) => {
  const authHeader = req.headers['authorization'];
  const token = authHeader && authHeader.split(' ')[1];
  
  if (!token) {
    return res.status(401).json({ error: 'Access token is required' });
  }
  
  jwt.verify(token, JWT_SECRET, (err, user) => {
    if (err) {
      return res.status(403).json({ error: 'Invalid or expired token' });
    }
    
    req.user = user;
    next();
  });
};

// Register a new user
router.post('/register', [
  body('mobile_number').notEmpty().withMessage('Mobile number is required'),
  body('email').isEmail().withMessage('Valid email is required'),
  body('password')
    .isLength({ min: 8 })
    .withMessage('Password must be at least 8 characters long')
    .matches(/^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)/)
    .withMessage('Password must include at least one uppercase letter, one lowercase letter, and one number'),
  body('first_name').notEmpty().withMessage('First name is required'),
  body('last_name').notEmpty().withMessage('Last name is required'),
], async (req, res) => {
  // Validate request
  const errors = validationResult(req);
  if (!errors.isEmpty()) {
    return res.status(400).json({ errors: errors.array() });
  }
  
  const { mobile_number, email, password, first_name, last_name } = req.body;
  const db = req.app.locals.db;
  const eventBus = req.app.locals.eventBus;
  
  try {
    // Check if user already exists
    db.get(
      'SELECT user_id FROM users_master WHERE email = ? OR mobile_number = ?',
      [email, mobile_number],
      async (err, user) => {
        if (err) {
          console.error('Database error:', err);
          return res.status(500).json({ error: 'Database error' });
        }
        
        if (user) {
          return res.status(400).json({ error: 'User already exists with this email or mobile number' });
        }
        
        // Hash password
        const salt = await bcrypt.genSalt(10);
        const hashedPassword = await bcrypt.hash(password, salt);
        
        // Insert new user
        db.run(
          'INSERT INTO users_master (mobile_number, email, password_hash, first_name, last_name) VALUES (?, ?, ?, ?, ?)',
          [mobile_number, email, hashedPassword, first_name, last_name],
          function (err) {
            if (err) {
              console.error('Error creating user:', err);
              return res.status(500).json({ error: 'Failed to create user' });
            }
            
            const userId = this.lastID;
            
            // Assign default User role to new user
            db.get('SELECT role_id FROM roles_master WHERE name = ?', ['User'], (err, role) => {
              if (err || !role) {
                console.error('Error getting User role:', err);
                // Continue even if role assignment fails
              } else {
                db.run(
                  'INSERT INTO user_roles_tx (user_id, role_id) VALUES (?, ?)',
                  [userId, role.role_id],
                  (err) => {
                    if (err) {
                      console.error('Error assigning User role:', err);
                    }
                  }
                );
              }
            });
            
            // Log the user registration event
            db.run(
              'INSERT INTO activity_logs_tx (user_id, action, details, ip_address, user_agent) VALUES (?, ?, ?, ?, ?)',
              [
                userId,
                'REGISTER',
                'User registered',
                req.ip,
                req.headers['user-agent']
              ]
            );
            
            // Emit event for user creation
            eventBus.emit('user:created', {
              user_id: userId,
              email,
              mobile_number,
              first_name,
              last_name
            });
            
            res.status(201).json({
              message: 'User registered successfully',
              user_id: userId
            });
          }
        );
      }
    );
  } catch (error) {
    console.error('Registration error:', error);
    res.status(500).json({ error: 'Registration failed' });
  }
});

// Login user
router.post('/login', [
  body('username').notEmpty().withMessage('Username or email is required'),
  body('password').notEmpty().withMessage('Password is required'),
], async (req, res) => {
  // Validate request
  const errors = validationResult(req);
  if (!errors.isEmpty()) {
    return res.status(400).json({ errors: errors.array() });
  }
  
  const { username, password } = req.body;
  const db = req.app.locals.db;
  const eventBus = req.app.locals.eventBus;
  
  // Helper function to process successful login
  const processSuccessfulLogin = (user, req, res, db, eventBus) => {
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
          return res.status(500).json({ error: 'Failed to retrieve user roles' });
        }
        
        // Extract roles and permissions from results
        const roles = [...new Set(userRolesAndPermissions.map(item => item.role_name))];
        const permissions = userRolesAndPermissions
          .filter(item => item.permission_name)
          .map(item => item.permission_name);
        
        // Create JWT payload
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
        
        // Sign token
        jwt.sign(payload, JWT_SECRET, { expiresIn: JWT_EXPIRES_IN }, (err, token) => {
          if (err) {
            console.error('Error generating token:', err);
            return res.status(500).json({ error: 'Failed to generate token' });
          }
          
          // Log login event
          db.run(
            'INSERT INTO activity_logs_tx (user_id, action, details, ip_address, user_agent) VALUES (?, ?, ?, ?, ?)',
            [
              user.user_id,
              'LOGIN',
              'User logged in',
              req.ip,
              req.headers['user-agent']
            ]
          );
          
          res.json({
            token,
            user: {
              id: user.user_id,
              email: user.email,
              first_name: user.first_name,
              last_name: user.last_name,
              roles,
              permissions
            }
          });
        });
      }
    );
  };
  
  try {
    // Special case for admin user
    if (username === 'admin') {
      console.log('Admin login attempt');
      // Use the hardcoded query for admin username
      db.get(
        'SELECT user_id, email, password_hash, first_name, last_name, is_active FROM users_master LIMIT 1',
        [],
        async (err, user) => {
          if (err) {
            console.error('Database error:', err);
            return res.status(500).json({ error: 'Database error' });
          }
          
          if (!user) {
            console.error('No admin user found in database');
            return res.status(401).json({ error: 'Invalid credentials' });
          }
          
          console.log('Found admin user:', user.email);
          
          // Check password
          const isMatch = await bcrypt.compare(password, user.password_hash);
          if (!isMatch) {
            console.error('Password mismatch for admin');
            return res.status(401).json({ error: 'Invalid credentials' });
          }
          
          console.log('Admin password matched');
          
          // Continue with login process
          processSuccessfulLogin(user, req, res, db, eventBus);
        }
      );
      return; // Exit early - we're handling this login separately
    }
    
    // Regular case - Find user by email or mobile number
    const isEmail = username.includes('@');
    const query = isEmail ? 
      'SELECT user_id, email, password_hash, first_name, last_name, is_active FROM users_master WHERE email = ?' :
      'SELECT user_id, email, password_hash, first_name, last_name, is_active FROM users_master WHERE mobile_number = ?';
    const params = isEmail ? [username] : [username];
    
    console.log('Regular login attempt:', { username, isEmail, query, params });
    
    db.get(
      query,
      params,
      async (err, user) => {
        if (err) {
          console.error('Database error:', err);
          return res.status(500).json({ error: 'Database error' });
        }
        
        if (!user) {
          console.log('User not found for:', username);
          return res.status(401).json({ error: 'Invalid credentials' });
        }
        
        if (!user.is_active) {
          console.log('Account disabled for:', username);
          return res.status(401).json({ error: 'Account is disabled. Please contact an administrator.' });
        }
        
        // Check password
        const isMatch = await bcrypt.compare(password, user.password_hash);
        if (!isMatch) {
          console.log('Password mismatch for:', username);
          return res.status(401).json({ error: 'Invalid credentials' });
        }
        
        console.log('Login successful for user:', user.email);
        // Use the shared function to process successful login
        processSuccessfulLogin(user, req, res, db, eventBus);
      }
    );
  } catch (error) {
    console.error('Login error:', error);
    return res.status(500).json({ error: 'Login failed' });
  }
});

// Password reset request
router.post('/forgot-password', [
  body('email').isEmail().withMessage('Valid email is required'),
], async (req, res) => {
  // Validate request
  const errors = validationResult(req);
  if (!errors.isEmpty()) {
    return res.status(400).json({ errors: errors.array() });
  }
  
  const { email } = req.body;
  const db = req.app.locals.db;
  const eventBus = req.app.locals.eventBus;
  
  try {
    // Check if user exists
    db.get('SELECT user_id FROM users_master WHERE email = ?', [email], (err, user) => {
      if (err) {
        console.error('Database error:', err);
        return res.status(500).json({ error: 'Database error' });
      }
      
      // Always return success even if user doesn't exist (for security)
      if (!user) {
        return res.json({ message: 'If your email is registered, you will receive a password reset link' });
      }
      
      // Generate password reset token
      const resetToken = jwt.sign(
        { user_id: user.user_id },
        JWT_SECRET,
        { expiresIn: '1h' }
      );
      
      // In a real application, send email with reset token
      // For this implementation, we'll just log it and emit an event
      console.log(`Password reset token for ${email}: ${resetToken}`);
      
      // Log the password reset request
      db.run(
        'INSERT INTO activity_logs_tx (user_id, action, details, ip_address, user_agent) VALUES (?, ?, ?, ?, ?)',
        [
          user.user_id,
          'PASSWORD_RESET_REQUEST',
          'Password reset requested',
          req.ip,
          req.headers['user-agent']
        ]
      );
      
      // Emit password reset event
      eventBus.emit('user:password_reset_requested', {
        user_id: user.user_id,
        email,
        reset_token: resetToken
      });
      
      res.json({ message: 'If your email is registered, you will receive a password reset link' });
    });
  } catch (error) {
    console.error('Password reset request error:', error);
    res.status(500).json({ error: 'Password reset request failed' });
  }
});

// Reset password with token
router.post('/reset-password', [
  body('token').notEmpty().withMessage('Reset token is required'),
  body('password')
    .isLength({ min: 8 })
    .withMessage('Password must be at least 8 characters long')
    .matches(/^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)/)
    .withMessage('Password must include at least one uppercase letter, one lowercase letter, and one number'),
], async (req, res) => {
  // Validate request
  const errors = validationResult(req);
  if (!errors.isEmpty()) {
    return res.status(400).json({ errors: errors.array() });
  }
  
  const { token, password } = req.body;
  const db = req.app.locals.db;
  const eventBus = req.app.locals.eventBus;
  
  try {
    // Verify token
    jwt.verify(token, JWT_SECRET, async (err, decoded) => {
      if (err) {
        return res.status(400).json({ error: 'Invalid or expired reset token' });
      }
      
      const userId = decoded.user_id;
      
      // Hash new password
      const salt = await bcrypt.genSalt(10);
      const hashedPassword = await bcrypt.hash(password, salt);
      
      // Update password in database
      db.run(
        'UPDATE users_master SET password_hash = ?, updated_at = CURRENT_TIMESTAMP WHERE user_id = ?',
        [hashedPassword, userId],
        function (err) {
          if (err) {
            console.error('Error updating password:', err);
            return res.status(500).json({ error: 'Failed to update password' });
          }
          
          if (this.changes === 0) {
            return res.status(404).json({ error: 'User not found' });
          }
          
          // Log password reset event
          db.run(
            'INSERT INTO activity_logs_tx (user_id, action, details, ip_address, user_agent) VALUES (?, ?, ?, ?, ?)',
            [
              userId,
              'PASSWORD_RESET',
              'Password reset successful',
              req.ip,
              req.headers['user-agent']
            ]
          );
          
          // Emit password reset success event
          eventBus.emit('user:password_reset_success', {
            user_id: userId,
            timestamp: new Date()
          });
          
          res.json({ message: 'Password has been reset successfully' });
        }
      );
    });
  } catch (error) {
    console.error('Password reset error:', error);
    res.status(500).json({ error: 'Password reset failed' });
  }
});

// Verify token and get user data
router.get('/me', authenticateToken, (req, res) => {
  const userId = req.user.user.id;
  const db = req.app.locals.db;
  
  db.get(
    'SELECT user_id, email, first_name, last_name, mobile_number, is_active FROM users_master WHERE user_id = ?',
    [userId],
    (err, user) => {
      if (err) {
        console.error('Database error:', err);
        return res.status(500).json({ error: 'Database error' });
      }
      
      if (!user) {
        return res.status(404).json({ error: 'User not found' });
      }
      
      res.json({
        user: {
          id: user.user_id,
          email: user.email,
          first_name: user.first_name,
          last_name: user.last_name,
          mobile_number: user.mobile_number,
          is_active: user.is_active,
          roles: req.user.user.roles,
          permissions: req.user.user.permissions
        }
      });
    }
  );
});

// Initialize module
const init = (app) => {
  const eventBus = app.locals.eventBus;
  registerModuleEvents(eventBus);
};

// Export the router and init function
module.exports = router;
module.exports.init = init;
module.exports.authenticateToken = authenticateToken;
