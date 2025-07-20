/**
 * User Management Module - Backend Implementation
 * Created: 2025-06-27
 * Updated: 2025-07-09 - Added bulk user upload functionality
 */

const express = require('express');
const router = express.Router();
const { body, validationResult } = require('express-validator');
const { authenticateToken } = require('../../../middleware/auth');
const { checkPermissions } = require('../../../middleware/rbac');
const { dbMethods } = require('../../database/backend');
const multer = require('multer');
const csv = require('csv-parser');
const fs = require('fs');
const path = require('path');
const bcrypt = require('bcryptjs');

// Register module events
const registerModuleEvents = (eventBus) => {
  eventBus.on('user:updated', (data) => {
    console.log('User updated event received:', data);
    // Handle user update events
  });

  eventBus.on('user:deleted', (data) => {
    console.log('User deleted event received:', data);
    // Handle user deletion events
  });
};

// Set up multer storage configuration
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    const uploadDir = path.join(__dirname, '../../../uploads');
    // Create directory if it doesn't exist
    if (!fs.existsSync(uploadDir)) {
      fs.mkdirSync(uploadDir, { recursive: true });
    }
    cb(null, uploadDir);
  },
  filename: (req, file, cb) => {
    // Create unique filename with timestamp
    const uniquePrefix = Date.now() + '-' + Math.round(Math.random() * 1E9);
    cb(null, uniquePrefix + '-' + file.originalname);
  }
});

const fileFilter = (req, file, cb) => {
  // Accept only CSV files
  if (file.mimetype === 'text/csv' || file.originalname.endsWith('.csv')) {
    cb(null, true);
  } else {
    cb(new Error('Only CSV files are allowed'), false);
  }
};

const upload = multer({ 
  storage: storage,
  fileFilter: fileFilter,
  limits: {
    fileSize: 5 * 1024 * 1024 // 5MB max file size
  }
});

// Initialize the module
const init = (app) => {
  if (app.locals.eventBus) {
    registerModuleEvents(app.locals.eventBus);
  }
};

/**
 * @route GET /api/user_management/users
 * @description Get all users with optional filtering
 * @access Private - Requires user_view permission
 */
router.get('/users', authenticateToken, checkPermissions(['user_view']), async (req, res) => {
  try {
    const db = req.app.locals.db;
    const eventBus = req.app.locals.eventBus;
    
    // Query parameters for filtering and pagination
    const { isActive, search, searchTerm, role, limit, page = 1 } = req.query;
    const pageSize = parseInt(limit) || 10;
    const offset = (parseInt(page) - 1) * pageSize;
    
    // Use search parameter from frontend or fallback to searchTerm for backwards compatibility
    const searchQuery = search || searchTerm;
    
    console.log('User list request with params:', req.query);
    console.log('Authenticated user:', req.user);
    
    // Base SQL query - fetch users first
    let sql = `
      SELECT 
        u.user_id, 
        u.first_name, 
        u.last_name, 
        u.email, 
        u.mobile_number, 
        u.is_active,
        u.created_at,
        u.updated_at
      FROM users_master u
    `;
    
    // Build WHERE clause based on filters
    const whereConditions = [];
    const params = [];
    
    if (isActive !== undefined) {
      whereConditions.push('u.is_active = ?');
      params.push(isActive === 'true' ? 1 : 0);
    }
    
    if (searchQuery) {
      whereConditions.push('(u.first_name LIKE ? OR u.last_name LIKE ? OR u.email LIKE ?)');
      const searchPattern = `%${searchQuery}%`;
      params.push(searchPattern, searchPattern, searchPattern);
    }
    
    // Add WHERE clause if conditions exist
    if (whereConditions.length > 0) {
      sql += ' WHERE ' + whereConditions.join(' AND ');
    }
    
    // Count total users for pagination
    const countSql = `SELECT COUNT(*) as total FROM (${sql})`;
    const countResult = await dbMethods.get(db, countSql, params);
    const total = countResult.total;
    
    // Add ORDER BY and LIMIT for pagination
    sql += ' ORDER BY u.created_at DESC';
    sql += ` LIMIT ${pageSize} OFFSET ${offset}`;
    
    console.log('Executing SQL:', sql);
    
    // Get users
    const users = await dbMethods.all(db, sql, params);
    console.log(`Retrieved ${users.length} users from database`);
    
    // Fetch roles for each user
    for (const user of users) {
      const roles = await dbMethods.all(db, 
        `SELECT r.role_id, r.name 
         FROM roles_master r 
         JOIN user_roles_tx ur ON r.role_id = ur.role_id 
         WHERE ur.user_id = ?`,
        [user.user_id]
      );
      
      user.roles = roles;
    }
    
    // Log activity
    try {
      // Debug the structure of req.user
      console.log('User object for activity logging:', JSON.stringify(req.user, null, 2));
      
      // Use defensive coding to avoid undefined errors
      const userId = req.user && req.user.user_id ? req.user.user_id : 
                     (req.user && req.user.id ? req.user.id : null);
      
      eventBus.emit('log:activity', {
        user_id: userId,
        action: 'USER_LIST_VIEW',
        details: 'Retrieved list of users',
        ip_address: req.ip,
        user_agent: req.headers['user-agent']
      });
    } catch (logError) {
      console.error('Error logging activity:', logError);
      // Continue despite logging error
    }
    
    // Return users with roles and pagination info
    return res.json({
      users,
      total,
      page: parseInt(page),
      limit: pageSize,
      pages: Math.ceil(total / pageSize)
    });
  } catch (error) {
    console.error('Error in user listing endpoint:', error);
    return res.status(500).json({ error: 'Failed to fetch users: ' + error.message });
  }
});

/**
 * @route POST /api/user_management/users
 * @description Create a new user
 * @access Private - Requires user_create permission
 */
router.post('/users', [
  authenticateToken, 
  checkPermissions(['user_create']),
  body('first_name').notEmpty().withMessage('First name is required'),
  body('last_name').notEmpty().withMessage('Last name is required'),
  body('email').isEmail().withMessage('Valid email is required'),
  body('mobile_number').notEmpty().withMessage('Mobile number is required'),
  body('password').isLength({ min: 8 }).withMessage('Password must be at least 8 characters long')
    .matches(/[A-Z]/).withMessage('Password must contain at least one uppercase letter')
    .matches(/[a-z]/).withMessage('Password must contain at least one lowercase letter')
    .matches(/[0-9]/).withMessage('Password must contain at least one number'),
  body('roles').isArray().withMessage('Roles must be an array')
], async (req, res) => {
  try {
    // Validate request
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({ errors: errors.array() });
    }
    
    const { first_name, last_name, email, mobile_number, password, roles } = req.body;
    const db = req.app.locals.db;
    const eventBus = req.app.locals.eventBus;
    
    // Check if email or mobile already exists
    const existingUser = await dbMethods.get(db, 
      'SELECT user_id FROM users_master WHERE email = ? OR mobile_number = ?', 
      [email, mobile_number]
    );
    
    if (existingUser) {
      return res.status(409).json({ error: 'A user with this email or mobile number already exists' });
    }
    
    // Hash password
    const salt = await bcrypt.genSalt(10);
    const hashedPassword = await bcrypt.hash(password, salt);
    
    // Create new user
    const result = await dbMethods.run(db, 
      `INSERT INTO users_master (first_name, last_name, email, mobile_number, password_hash, is_active) 
       VALUES (?, ?, ?, ?, ?, 1)`, 
      [first_name, last_name, email, mobile_number, hashedPassword]
    );
    
    const newUserId = result.lastID;
    
    // Assign roles to user if provided
    if (roles && roles.length > 0) {
      for (const roleId of roles) {
        await dbMethods.run(db, 
          'INSERT INTO user_roles_tx (user_id, role_id) VALUES (?, ?)',
          [newUserId, roleId]
        );
      }
    } else {
      // Assign default 'User' role if no roles specified
      const defaultRole = await dbMethods.get(db, 'SELECT role_id FROM roles_master WHERE name = ?', ['User']);
      if (defaultRole) {
        await dbMethods.run(db, 
          'INSERT INTO user_roles_tx (user_id, role_id) VALUES (?, ?)',
          [newUserId, defaultRole.role_id]
        );
      }
    }
    
    // Log activity
    eventBus.emit('log:activity', {
      user_id: req.user.user_id,
      action: 'USER_CREATED',
      details: `Created new user: ${email}`,
      ip_address: req.ip,
      user_agent: req.headers['user-agent']
    });
    
    // Emit user created event
    eventBus.emit('user:created', {
      user_id: newUserId,
      email,
      created_by: req.user.user_id
    });
    
    return res.status(201).json({ 
      message: 'User created successfully',
      user_id: newUserId 
    });
  } catch (error) {
    console.error('Error creating user:', error);
    return res.status(500).json({ error: 'Failed to create user' });
  }
});

/**
 * @route GET /api/user_management/users/:id
 * @description Get user by ID
 * @access Private - Requires user_view permission
 */
router.get('/users/:id', authenticateToken, checkPermissions(['user_view']), async (req, res) => {
  try {
    const userId = req.params.id;
    const db = req.app.locals.db;
    const eventBus = req.app.locals.eventBus;
    
    // Get user data
    const user = await dbMethods.get(db, 
      `SELECT user_id, first_name, last_name, email, mobile_number, is_active, created_at, updated_at 
       FROM users_master WHERE user_id = ?`, 
      [userId]
    );
    
    if (!user) {
      return res.status(404).json({ error: 'User not found' });
    }
    
    // Get user roles
    const roles = await dbMethods.all(db, 
      `SELECT r.role_id, r.name 
       FROM roles_master r 
       JOIN user_roles_tx ur ON r.role_id = ur.role_id 
       WHERE ur.user_id = ?`,
      [userId]
    );
    
    // Log activity
    eventBus.emit('log:activity', {
      user_id: req.user.user_id,
      action: 'USER_DETAIL_VIEW',
      details: `Viewed details for user ID ${userId}`,
      ip_address: req.ip,
      user_agent: req.headers['user-agent']
    });
    
    // Return user with roles
    return res.status(200).json({
      ...user,
      roles: roles.map(role => ({
        role_id: role.role_id,
        name: role.name
      }))
    });
  } catch (error) {
    console.error(`Error fetching user ${req.params.id}:`, error);
    return res.status(500).json({ error: 'Failed to retrieve user details' });
  }
});

/**
 * @route PUT /api/user_management/users/:id
 * @description Update user information
 * @access Private - Requires user_edit permission
 */
router.put('/users/:id', [
  authenticateToken, 
  checkPermissions(['user_edit']),
  body('first_name').optional().notEmpty().withMessage('First name cannot be empty'),
  body('last_name').optional().notEmpty().withMessage('Last name cannot be empty'),
  body('email').optional().isEmail().withMessage('Valid email is required'),
  body('mobile_number').optional().notEmpty().withMessage('Mobile number cannot be empty'),
  body('roles').optional().isArray().withMessage('Roles must be an array')
], async (req, res) => {
  try {
    // Validate request
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({ errors: errors.array() });
    }
    
    const userId = req.params.id;
    const { first_name, last_name, email, mobile_number, roles } = req.body;
    const db = req.app.locals.db;
    const eventBus = req.app.locals.eventBus;
    
    // Check if user exists
    const existingUser = await dbMethods.get(db, 'SELECT user_id FROM users_master WHERE user_id = ?', [userId]);
    if (!existingUser) {
      return res.status(404).json({ error: 'User not found' });
    }
    
    // Check if email or mobile belongs to another user
    if (email) {
      const userWithEmail = await dbMethods.get(db, 
        'SELECT user_id FROM users_master WHERE email = ? AND user_id != ?', 
        [email, userId]
      );
      if (userWithEmail) {
        return res.status(409).json({ error: 'Email is already in use by another user' });
      }
    }
    
    if (mobile_number) {
      const userWithMobile = await dbMethods.get(db, 
        'SELECT user_id FROM users_master WHERE mobile_number = ? AND user_id != ?', 
        [mobile_number, userId]
      );
      if (userWithMobile) {
        return res.status(409).json({ error: 'Mobile number is already in use by another user' });
      }
    }
    
    // Build update query based on provided fields
    const updateFields = [];
    const updateValues = [];
    
    if (first_name) {
      updateFields.push('first_name = ?');
      updateValues.push(first_name);
    }
    
    if (last_name) {
      updateFields.push('last_name = ?');
      updateValues.push(last_name);
    }
    
    if (email) {
      updateFields.push('email = ?');
      updateValues.push(email);
    }
    
    if (mobile_number) {
      updateFields.push('mobile_number = ?');
      updateValues.push(mobile_number);
    }
    
    // Add updated_at timestamp
    updateFields.push('updated_at = CURRENT_TIMESTAMP');
    
    // Only update if there are fields to update
    if (updateFields.length > 0) {
      updateValues.push(userId);
      await dbMethods.run(db, 
        `UPDATE users_master SET ${updateFields.join(', ')} WHERE user_id = ?`, 
        updateValues
      );
    }
    
    // Update roles if provided
    if (roles) {
      // Delete existing roles first
      await dbMethods.run(db, 'DELETE FROM user_roles_tx WHERE user_id = ?', [userId]);
      
      // Add new roles
      for (const roleId of roles) {
        await dbMethods.run(db, 
          'INSERT INTO user_roles_tx (user_id, role_id) VALUES (?, ?)', 
          [userId, roleId]
        );
      }
    }
    
    // Log activity
    eventBus.emit('log:activity', {
      user_id: req.user.user_id,
      action: 'USER_UPDATED',
      details: `Updated user ID ${userId}`,
      ip_address: req.ip,
      user_agent: req.headers['user-agent']
    });
    
    // Emit user updated event
    eventBus.emit('user:updated', {
      user_id: userId,
      updated_by: req.user.user_id
    });
    
    return res.status(200).json({ 
      message: 'User updated successfully'
    });
  } catch (error) {
    console.error(`Error updating user ${req.params.id}:`, error);
    return res.status(500).json({ error: 'Failed to update user' });
  }
});

/**
 * @route PATCH /api/user_management/users/:id/status
 * @description Toggle user active status
 * @access Private - Requires user_edit permission
 */
router.patch('/users/:id/status', [
  authenticateToken, 
  checkPermissions(['user_edit']),
  body('is_active').isBoolean().withMessage('is_active must be a boolean')
], async (req, res) => {
  try {
    // Validate request
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({ errors: errors.array() });
    }
    
    const userId = req.params.id;
    const { is_active } = req.body;
    const db = req.app.locals.db;
    const eventBus = req.app.locals.eventBus;
    
    // Check if user exists
    const existingUser = await dbMethods.get(db, 'SELECT user_id FROM users_master WHERE user_id = ?', [userId]);
    if (!existingUser) {
      return res.status(404).json({ error: 'User not found' });
    }
    
    // Update user status
    await dbMethods.run(db, 
      'UPDATE users_master SET is_active = ?, updated_at = CURRENT_TIMESTAMP WHERE user_id = ?', 
      [is_active ? 1 : 0, userId]
    );
    
    // Log activity
    const action = is_active ? 'USER_ACTIVATED' : 'USER_DEACTIVATED';
    eventBus.emit('log:activity', {
      user_id: req.user.user_id,
      action,
      details: `${is_active ? 'Activated' : 'Deactivated'} user ID ${userId}`,
      ip_address: req.ip,
      user_agent: req.headers['user-agent']
    });
    
    // Emit user updated event
    eventBus.emit('user:updated', {
      user_id: userId,
      updated_by: req.user.user_id,
      status_changed: true,
      is_active
    });
    
    return res.status(200).json({ 
      message: `User ${is_active ? 'activated' : 'deactivated'} successfully`
    });
  } catch (error) {
    console.error(`Error updating user status ${req.params.id}:`, error);
    return res.status(500).json({ error: 'Failed to update user status' });
  }
});

/**
 * @route DELETE /api/user_management/users/:id
 * @description Delete a user
 * @access Private - Requires user_delete permission
 */
router.delete('/users/:id', authenticateToken, checkPermissions(['user_delete']), async (req, res) => {
  try {
    const userId = req.params.id;
    const db = req.app.locals.db;
    const eventBus = req.app.locals.eventBus;
    
    // Check if user exists
    const existingUser = await dbMethods.get(db, 
      'SELECT user_id, email FROM users_master WHERE user_id = ?', 
      [userId]
    );
    
    if (!existingUser) {
      return res.status(404).json({ error: 'User not found' });
    }
    
    // Don't allow deletion of the admin user
    const adminUser = await dbMethods.get(db, 
      `SELECT u.user_id FROM users_master u 
       JOIN user_roles_tx ur ON u.user_id = ur.user_id 
       JOIN roles_master r ON ur.role_id = r.role_id 
       WHERE u.user_id = ? AND r.name = 'Admin'`, 
      [userId]
    );
    
    if (adminUser && parseInt(userId) === 1) {
      return res.status(403).json({ error: 'Cannot delete the primary administrator account' });
    }
    
    // Because of ON DELETE CASCADE constraints, deleting from users_master 
    // will automatically delete related records in other tables
    await dbMethods.run(db, 'DELETE FROM users_master WHERE user_id = ?', [userId]);
    
    // Log activity
    eventBus.emit('log:activity', {
      user_id: req.user.user_id,
      action: 'USER_DELETED',
      details: `Deleted user ID ${userId} (${existingUser.email})`,
      ip_address: req.ip,
      user_agent: req.headers['user-agent']
    });
    
    // Emit user deleted event
    eventBus.emit('user:deleted', {
      user_id: userId,
      deleted_by: req.user.user_id,
      email: existingUser.email
    });
    
    return res.status(200).json({ 
      message: 'User deleted successfully'
    });
  } catch (error) {
    console.error(`Error deleting user ${req.params.id}:`, error);
    return res.status(500).json({ error: 'Failed to delete user' });
  }
});

/**
 * @route GET /api/user_management/users/template
 * @description Download a CSV template for bulk user upload
 * @access Private - Requires user_create permission
 */
router.get('/users/template', authenticateToken, checkPermissions(['user_create']), async (req, res) => {
  try {
    // CSV header row
    const header = 'firstName,lastName,email,password,roles,isActive\n';
    
    // Example row
    const exampleRow = 'John,Doe,john.doe@example.com,StrongP@ss1,User,true\n';
    
    // Set response headers
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', 'attachment; filename=user_template.csv');
    
    // Send the CSV content
    res.send(header + exampleRow);
    
    // Log activity
    const eventBus = req.app.locals.eventBus;
    eventBus.emit('log:activity', {
      user_id: req.user.user_id,
      action: 'USER_TEMPLATE_DOWNLOAD',
      details: 'Downloaded bulk user upload template',
      ip_address: req.ip,
      user_agent: req.headers['user-agent']
    });
    
  } catch (error) {
    console.error('Error creating template:', error);
    return res.status(500).json({ error: 'Failed to generate template file' });
  }
});

/**
 * @route POST /api/user_management/users/bulk
 * @description Upload and process multiple users from CSV file
 * @access Private - Requires user_create permission
 */
router.post('/users/bulk', authenticateToken, checkPermissions(['user_create']), upload.single('file'), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: 'No file uploaded' });
    }
    
    const db = req.app.locals.db;
    const eventBus = req.app.locals.eventBus;
    
    // Results tracking
    const results = {
      total: 0,
      successful: 0,
      failed: 0,
      errors: []
    };
    
    // Array to store all user processing promises
    const processPromises = [];
    
    // Process the CSV file
    const processFile = new Promise((resolve, reject) => {
      fs.createReadStream(req.file.path)
        .pipe(csv())
        .on('data', async (row) => {
          results.total++;
          
          // Process each row
          const processPromise = processUserRow(row, db, results, eventBus, req.user.user_id);
          processPromises.push(processPromise);
        })
        .on('end', async () => {
          try {
            // Wait for all user processing to complete
            await Promise.allSettled(processPromises);
            
            // Delete the temporary file
            fs.unlinkSync(req.file.path);
            
            // Log activity
            eventBus.emit('log:activity', {
              user_id: req.user.user_id,
              action: 'USER_BULK_UPLOAD',
              details: `Processed ${results.total} users, ${results.successful} created successfully, ${results.failed} failed`,
              ip_address: req.ip,
              user_agent: req.headers['user-agent']
            });
            
            resolve();
          } catch (error) {
            reject(error);
          }
        })
        .on('error', (error) => {
          reject(error);
        });
    });
    
    await processFile;
    
    // Return results
    return res.status(200).json(results);
    
  } catch (error) {
    console.error('Error processing bulk users:', error);
    return res.status(500).json({ error: 'Failed to process bulk user upload' });
  }
});

/**
 * Process a single user row from the CSV file
 * @param {Object} row - The CSV row data
 * @param {Object} db - Database connection
 * @param {Object} results - Results tracking object
 * @param {Object} eventBus - Event bus for logging
 * @param {number} createdBy - User ID of the creator
 */
async function processUserRow(row, db, results, eventBus, createdBy) {
  try {
    // Validate required fields
    if (!row.firstName || !row.lastName || !row.email || !row.password || !row.roles) {
      results.failed++;
      results.errors.push({
        row: results.total,
        email: row.email || 'Unknown',
        message: 'Missing required fields'
      });
      return;
    }
    
    // Check if email already exists
    const existingUser = await dbMethods.get(db, 
      'SELECT user_id FROM users_master WHERE email = ?', 
      [row.email]
    );
    
    if (existingUser) {
      results.failed++;
      results.errors.push({
        row: results.total,
        email: row.email,
        message: 'Email already exists'
      });
      return;
    }
    
    // Hash password
    const salt = await bcrypt.genSalt(10);
    const hashedPassword = await bcrypt.hash(row.password, salt);
    
    // Parse isActive value
    const isActive = row.isActive ? row.isActive.toLowerCase() === 'true' : true;
    
    // Create user
    const result = await dbMethods.run(db, 
      `INSERT INTO users_master (first_name, last_name, email, mobile_number, password_hash, is_active) 
       VALUES (?, ?, ?, ?, ?, ?)`, 
      [row.firstName, row.lastName, row.email, row.email, hashedPassword, isActive ? 1 : 0]
    );
    
    const newUserId = result.lastID;
    
    // Process roles
    const roleNames = row.roles.split(',').map(role => role.trim());
    
    // Find role IDs from role names
    for (const roleName of roleNames) {
      const roleRecord = await dbMethods.get(db, 
        'SELECT role_id FROM roles_master WHERE name = ?', 
        [roleName]
      );
      
      if (roleRecord) {
        await dbMethods.run(db, 
          'INSERT INTO user_roles_tx (user_id, role_id) VALUES (?, ?)',
          [newUserId, roleRecord.role_id]
        );
      } else {
        console.warn(`Role '${roleName}' not found for user ${row.email}`);
      }
    }
    
    // Emit user created event
    eventBus.emit('user:created', {
      user_id: newUserId,
      email: row.email,
      created_by: createdBy
    });
    
    results.successful++;
    
  } catch (error) {
    console.error(`Error processing user ${row.email}:`, error);
    results.failed++;
    results.errors.push({
      row: results.total,
      email: row.email || 'Unknown',
      message: error.message || 'Unknown error'
    });
  }
}

// Initialize module when imported
if (router && router.init) {
  router.init = init;
}

module.exports = router;
