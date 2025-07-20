/**
 * Role Management Module - Backend Implementation
 * Created: 2025-06-27
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

// Configure multer for file uploads
const storage = multer.diskStorage({
  destination: function(req, file, cb) {
    const uploadsDir = path.join(__dirname, '..', '..', '..', 'uploads');
    if (!fs.existsSync(uploadsDir)){
      fs.mkdirSync(uploadsDir, { recursive: true });
    }
    cb(null, uploadsDir);
  },
  filename: function(req, file, cb) {
    cb(null, `role-bulk-upload-${Date.now()}-${file.originalname}`);
  }
});

const fileFilter = (req, file, cb) => {
  // Accept only CSV files
  if (file.mimetype === 'text/csv' || 
      file.originalname.endsWith('.csv')) {
    cb(null, true);
  } else {
    cb(new Error('Only CSV files are allowed'), false);
  }
};

const upload = multer({
  storage: storage,
  fileFilter: fileFilter,
  limits: {
    fileSize: 1024 * 1024 * 5 // 5MB file size limit
  }
});

// Register module events
const registerModuleEvents = (eventBus) => {
  eventBus.on('role:updated', (data) => {
    console.log('Role updated event received:', data);
    // Handle role update events
  });

  eventBus.on('role:deleted', (data) => {
    console.log('Role deleted event received:', data);
    // Handle role deletion events
  });
};

// Initialize the module
const init = (app) => {
  if (app.locals.eventBus) {
    registerModuleEvents(app.locals.eventBus);
  }
  
  // Create uploads directory if it doesn't exist
  const uploadsDir = path.join(__dirname, '..', '..', '..', 'uploads');
  if (!fs.existsSync(uploadsDir)){
    fs.mkdirSync(uploadsDir, { recursive: true });
  }
};

/**
 * @route GET /api/role_management/roles
 * @description Get all roles
 * @access Private - Requires role_view permission
 */
router.get('/roles', authenticateToken, checkPermissions(['role_view']), async (req, res) => {
  try {
    const db = req.app.locals.db;
    const eventBus = req.app.locals.eventBus;
    
    // Query all roles
    const roles = await dbMethods.all(db, 
      'SELECT role_id, name, description, created_at, updated_at FROM roles_master ORDER BY name',
      []
    );
    
    // For each role, get its permissions
    for (const role of roles) {
      const permissions = await dbMethods.all(db, 
        `SELECT p.permission_id, p.name, p.description 
         FROM permissions_master p
         JOIN role_permissions_tx rp ON p.permission_id = rp.permission_id
         WHERE rp.role_id = ?
         ORDER BY p.name`,
        [role.role_id]
      );
      
      // Attach permissions to the role
      role.permissions = permissions;
      
      // Count users with this role
      const userCount = await dbMethods.get(db,
        'SELECT COUNT(DISTINCT user_id) as count FROM user_roles_tx WHERE role_id = ?',
        [role.role_id]
      );
      
      role.user_count = userCount.count;
    }
    
    // Log activity
    eventBus.emit('log:activity', {
      user_id: req.user.user_id,
      action: 'ROLE_LIST_VIEW',
      details: 'Retrieved list of roles',
      ip_address: req.ip,
      user_agent: req.headers['user-agent']
    });
    
    return res.status(200).json({
      count: roles.length,
      roles
    });
  } catch (error) {
    console.error('Error fetching roles:', error);
    return res.status(500).json({ error: 'Failed to retrieve roles' });
  }
});

/**
 * @route GET /api/role_management/roles/template
 * @description Download a CSV template for bulk role upload
 * @access Private - Requires role_create permission
 */
router.get('/roles/template', [
  authenticateToken,
  checkPermissions(['role_create'])
], (req, res) => {
  try {
    // Set headers for CSV download
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', 'attachment; filename=role-template.csv');
    
    // Create CSV template with header and example row
    const csvContent = 'name,description,permissions\n' +
                      'Example Role,Example description for role,permission1;permission2;permission3\n';
    
    // Log template download activity
    const eventBus = req.app.locals.eventBus;
    if (eventBus) {
      eventBus.emit('log:activity', {
        user_id: req.user.user_id,
        action: 'DOWNLOAD_TEMPLATE',
        details: 'Downloaded role CSV template',
        ip_address: req.ip,
        user_agent: req.headers['user-agent'],
        resource_type: 'role',
        module: 'role_management'
      });
    }
    
    res.send(csvContent);
  } catch (error) {
    console.error('Error generating role template:', error);
    res.status(500).json({ error: 'Failed to generate template' });
  }
});

/**
 * @route POST /api/role_management/roles/bulk
 * @description Upload and process multiple roles from CSV file
 * @access Private - Requires role_create permission
 */
router.post('/roles/bulk', [
  authenticateToken,
  checkPermissions(['role_create']),
  upload.single('file')
], async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: 'No file uploaded' });
    }
    
    const db = req.app.locals.db;
    const eventBus = req.app.locals.eventBus;
    const userId = req.user.user_id;
    
    // Results tracking
    const results = {
      total: 0,
      success: 0,
      failed: 0,
      errors: [],
      created: []
    };
    
    // Create a promise to process the CSV file
    const processFile = new Promise((resolve, reject) => {
      const stream = fs.createReadStream(req.file.path)
        .pipe(csv())
        .on('data', async (row) => {
          // Pause the stream while processing this row
          stream.pause();
          results.total++;
          
          try {
            await processRoleRow(row, db, results, eventBus, userId);
          } catch (error) {
            results.failed++;
            results.errors.push({
              row: results.total,
              name: row.name || 'Unknown',
              error: error.message
            });
          } finally {
            // Resume the stream to process next row
            stream.resume();
          }
        })
        .on('end', () => {
          // Clean up the uploaded file
          fs.unlink(req.file.path, (err) => {
            if (err) console.error('Error deleting temp file:', err);
          });
          resolve();
        })
        .on('error', (error) => {
          reject(error);
        });
    });
    
    // Wait for all rows to be processed
    await processFile;
    
    // Log bulk upload activity
    if (eventBus) {
      eventBus.emit('log:activity', {
        user_id: userId,
        action: 'BULK_UPLOAD',
        details: `Bulk uploaded ${results.total} roles, ${results.success} succeeded, ${results.failed} failed`,
        ip_address: req.ip,
        user_agent: req.headers['user-agent'],
        resource_type: 'role',
        module: 'role_management'
      });
    }
    
    res.status(200).json({
      message: 'Bulk upload completed',
      results
    });
  } catch (error) {
    console.error('Error processing bulk upload:', error);
    res.status(500).json({ error: 'Failed to process bulk upload' });
  }
});

/**
 * @route GET /api/role_management/roles/:id
 * @description Get a specific role by ID
 * @access Private - Requires role_view permission
 */
router.get('/roles/:id', authenticateToken, checkPermissions(['role_view']), async (req, res) => {
  try {
    const roleId = req.params.id;
    const db = req.app.locals.db;
    const eventBus = req.app.locals.eventBus;
    
    // Get role data
    const role = await dbMethods.get(db, 
      'SELECT role_id, name, description, created_at, updated_at FROM roles_master WHERE role_id = ?',
      [roleId]
    );
    
    if (!role) {
      return res.status(404).json({ error: 'Role not found' });
    }
    
    // Get role permissions
    const permissions = await dbMethods.all(db, 
      `SELECT p.permission_id, p.name, p.description 
       FROM permissions_master p
       JOIN role_permissions_tx rp ON p.permission_id = rp.permission_id
       WHERE rp.role_id = ?
       ORDER BY p.name`,
      [roleId]
    );
    
    // Get users with this role
    const users = await dbMethods.all(db, 
      `SELECT u.user_id, u.first_name, u.last_name, u.email 
       FROM users_master u
       JOIN user_roles_tx ur ON u.user_id = ur.user_id
       WHERE ur.role_id = ?
       LIMIT 100`,
      [roleId]
    );
    
    // Attach permissions and users to the role
    role.permissions = permissions;
    role.users = users;
    role.user_count = users.length;
    
    // Log activity
    eventBus.emit('log:activity', {
      user_id: req.user.user_id,
      action: 'ROLE_DETAIL_VIEW',
      details: `Viewed details for role ID ${roleId}`,
      ip_address: req.ip,
      user_agent: req.headers['user-agent']
    });
    
    return res.status(200).json(role);
  } catch (error) {
    console.error(`Error fetching role ${req.params.id}:`, error);
    return res.status(500).json({ error: 'Failed to retrieve role details' });
  }
});

/**
 * @route POST /api/role_management/roles
 * @description Create a new role
 * @access Private - Requires role_create permission
 */
router.post('/roles', [
  authenticateToken, 
  checkPermissions(['role_create']),
  body('name').notEmpty().withMessage('Role name is required'),
  body('description').optional(),
  body('permissions').isArray().withMessage('Permissions must be an array')
], async (req, res) => {
  try {
    // Validate request
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({ errors: errors.array() });
    }
    
    const { name, description, permissions } = req.body;
    const db = req.app.locals.db;
    const eventBus = req.app.locals.eventBus;
    
    // Check if role with the same name already exists
    const existingRole = await dbMethods.get(db, 
      'SELECT role_id FROM roles_master WHERE name = ?', 
      [name]
    );
    
    if (existingRole) {
      return res.status(409).json({ error: 'A role with this name already exists' });
    }
    
    // Create new role
    const result = await dbMethods.run(db, 
      'INSERT INTO roles_master (name, description) VALUES (?, ?)', 
      [name, description || '']
    );
    
    const newRoleId = result.lastID;
    
    // Assign permissions to role if provided
    if (permissions && permissions.length > 0) {
      for (const permissionId of permissions) {
        await dbMethods.run(db, 
          'INSERT INTO role_permissions_tx (role_id, permission_id) VALUES (?, ?)',
          [newRoleId, permissionId]
        );
      }
    }
    
    // Log activity
    eventBus.emit('log:activity', {
      user_id: req.user.user_id,
      action: 'ROLE_CREATED',
      details: `Created new role: ${name}`,
      ip_address: req.ip,
      user_agent: req.headers['user-agent']
    });
    
    // Emit role created event
    eventBus.emit('role:created', {
      role_id: newRoleId,
      name,
      created_by: req.user.user_id
    });
    
    return res.status(201).json({ 
      message: 'Role created successfully',
      role_id: newRoleId 
    });
  } catch (error) {
    console.error('Error creating role:', error);
    return res.status(500).json({ error: 'Failed to create role' });
  }
});

/**
 * @route PUT /api/role_management/roles/:id
 * @description Update role information
 * @access Private - Requires role_edit permission
 */
router.put('/roles/:id', [
  authenticateToken, 
  checkPermissions(['role_edit']),
  body('name').optional().notEmpty().withMessage('Role name cannot be empty'),
  body('description').optional(),
  body('permissions').optional().isArray().withMessage('Permissions must be an array')
], async (req, res) => {
  try {
    // Validate request
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({ errors: errors.array() });
    }
    
    const roleId = req.params.id;
    const { name, description, permissions } = req.body;
    const db = req.app.locals.db;
    const eventBus = req.app.locals.eventBus;
    
    // Check if role exists
    const existingRole = await dbMethods.get(db, 'SELECT role_id FROM roles_master WHERE role_id = ?', [roleId]);
    if (!existingRole) {
      return res.status(404).json({ error: 'Role not found' });
    }
    
    // Check if the role is a system role (Admin/User)
    if (roleId <= 2) {
      // Can update description but not name for system roles
      if (name) {
        return res.status(403).json({ error: 'Cannot change the name of system roles' });
      }
    } else if (name) {
      // For custom roles, check if name is unique
      const roleWithName = await dbMethods.get(db, 
        'SELECT role_id FROM roles_master WHERE name = ? AND role_id != ?', 
        [name, roleId]
      );
      if (roleWithName) {
        return res.status(409).json({ error: 'A role with this name already exists' });
      }
    }
    
    // Build update query based on provided fields
    const updateFields = [];
    const updateValues = [];
    
    if (name) {
      updateFields.push('name = ?');
      updateValues.push(name);
    }
    
    if (description !== undefined) {
      updateFields.push('description = ?');
      updateValues.push(description);
    }
    
    // Add updated_at timestamp
    updateFields.push('updated_at = CURRENT_TIMESTAMP');
    
    // Only update if there are fields to update
    if (updateFields.length > 0) {
      updateValues.push(roleId);
      await dbMethods.run(db, 
        `UPDATE roles_master SET ${updateFields.join(', ')} WHERE role_id = ?`, 
        updateValues
      );
    }
    
    // Update permissions if provided
    if (permissions) {
      // For Admin role, ensure it keeps all permissions
      if (roleId == 1) {
        const allPermissions = await dbMethods.all(db, 'SELECT permission_id FROM permissions_master');
        const allPermissionIds = allPermissions.map(p => p.permission_id);
        
        // Check if all permissions are included
        const hasAllPermissions = allPermissionIds.every(id => permissions.includes(id));
        
        if (!hasAllPermissions) {
          return res.status(403).json({ error: 'Admin role must have all permissions' });
        }
      }
      
      // Delete existing permissions first
      await dbMethods.run(db, 'DELETE FROM role_permissions_tx WHERE role_id = ?', [roleId]);
      
      // Add new permissions
      for (const permissionId of permissions) {
        await dbMethods.run(db, 
          'INSERT INTO role_permissions_tx (role_id, permission_id) VALUES (?, ?)', 
          [roleId, permissionId]
        );
      }
    }
    
    // Log activity
    eventBus.emit('log:activity', {
      user_id: req.user.user_id,
      action: 'ROLE_UPDATED',
      details: `Updated role ID ${roleId}`,
      ip_address: req.ip,
      user_agent: req.headers['user-agent']
    });
    
    // Emit role updated event
    eventBus.emit('role:updated', {
      role_id: roleId,
      updated_by: req.user.user_id
    });
    
    return res.status(200).json({ 
      message: 'Role updated successfully'
    });
  } catch (error) {
    console.error(`Error updating role ${req.params.id}:`, error);
    return res.status(500).json({ error: 'Failed to update role' });
  }
});

/**
 * @route DELETE /api/role_management/roles/:id
 * @description Delete a role
 * @access Private - Requires role_delete permission
 */
router.delete('/roles/:id', authenticateToken, checkPermissions(['role_delete']), async (req, res) => {
  try {
    const roleId = req.params.id;
    const db = req.app.locals.db;
    const eventBus = req.app.locals.eventBus;
    
    // Get role data
    const role = await dbMethods.get(db, 'SELECT role_id, name FROM roles_master WHERE role_id = ?', [roleId]);
    
    if (!role) {
      return res.status(404).json({ error: 'Role not found' });
    }
    
    // Don't allow deletion of the Admin or User roles (IDs 1 and 2)
    if (roleId <= 2) {
      return res.status(403).json({ error: 'Cannot delete system roles (Admin/User)' });
    }
    
    // Check if role has users
    const userCount = await dbMethods.get(db, 
      'SELECT COUNT(*) as count FROM user_roles_tx WHERE role_id = ?', 
      [roleId]
    );
    
    if (userCount.count > 0) {
      return res.status(400).json({ 
        error: 'Cannot delete role with assigned users',
        user_count: userCount.count
      });
    }
    
    // Because of ON DELETE CASCADE constraints, deleting from roles_master 
    // will automatically delete related records in role_permissions_tx
    await dbMethods.run(db, 'DELETE FROM roles_master WHERE role_id = ?', [roleId]);
    
    // Log activity
    eventBus.emit('log:activity', {
      user_id: req.user.user_id,
      action: 'ROLE_DELETED',
      details: `Deleted role ID ${roleId} (${role.name})`,
      ip_address: req.ip,
      user_agent: req.headers['user-agent']
    });
    
    // Emit role deleted event
    eventBus.emit('role:deleted', {
      role_id: roleId,
      name: role.name,
      deleted_by: req.user.user_id
    });
    
    return res.status(200).json({ 
      message: 'Role deleted successfully'
    });
  } catch (error) {
    console.error(`Error deleting role ${req.params.id}:`, error);
    return res.status(500).json({ error: 'Failed to delete role' });
  }
});

// Export router and init function

/**
 * Process a single role row from the CSV file
 * @param {Object} row - The CSV row data
 * @param {Object} db - Database connection
 * @param {Object} results - Results tracking object
 * @param {Object} eventBus - Event bus for logging
 * @param {number} createdBy - User ID of the creator
 */
async function processRoleRow(row, db, results, eventBus, createdBy) {
  // Validate required fields
  if (!row.name || row.name.trim() === '') {
    throw new Error('Role name is required');
  }
  
  const name = row.name.trim();
  const description = row.description ? row.description.trim() : '';
  const permissionsStr = row.permissions ? row.permissions.trim() : '';
  
  // Check if role already exists
  const existingRole = await dbMethods.get(db, 
    'SELECT role_id FROM roles_master WHERE name = ?', 
    [name]
  );
  
  if (existingRole) {
    throw new Error(`Role with name '${name}' already exists`);
  }
  
  // Start a transaction
  await dbMethods.run(db, 'BEGIN TRANSACTION');
  
  try {
    // Create the role
    const result = await dbMethods.run(db, 
      'INSERT INTO roles_master (name, description) VALUES (?, ?)', 
      [name, description]
    );
    
    const roleId = result.lastID;
    
    // Process permissions if provided
    if (permissionsStr) {
      const permissionNames = permissionsStr.split(';').map(p => p.trim()).filter(p => p);
      
      // Get permission IDs from names
      for (const permName of permissionNames) {
        const permission = await dbMethods.get(db, 
          'SELECT permission_id FROM permissions_master WHERE name = ?', 
          [permName]
        );
        
        if (permission) {
          // Assign permission to role
          await dbMethods.run(db, 
            'INSERT INTO role_permissions_tx (role_id, permission_id) VALUES (?, ?)', 
            [roleId, permission.permission_id]
          );
        }
      }
    }
    
    // Commit transaction
    await dbMethods.run(db, 'COMMIT');
    
    // Update results
    results.success++;
    results.created.push({
      role_id: roleId,
      name: name
    });
    
    // Log role creation event
    if (eventBus) {
      eventBus.emit('log:activity', {
        user_id: createdBy,
        action: 'CREATE',
        details: `Created role ${name} via bulk upload`,
        ip_address: 'bulk-upload',
        resource_type: 'role',
        resource_id: roleId,
        module: 'role_management'
      });
    }
  } catch (error) {
    // Rollback transaction on error
    await dbMethods.run(db, 'ROLLBACK');
    throw error;
  }
}

module.exports = router;
module.exports.init = init;
