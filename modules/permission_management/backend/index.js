/**
 * Permission Management Module - Backend Implementation
 * Created: 2025-06-27
 */

const express = require('express');
const router = express.Router();
const { body, validationResult } = require('express-validator');
const { authenticateToken } = require('../../../middleware/auth');
const { checkPermissions } = require('../../../middleware/rbac');
const { dbMethods } = require('../../database/backend');

// Register module events
const registerModuleEvents = (eventBus) => {
  eventBus.on('permission:updated', (data) => {
    console.log('Permission updated event received:', data);
    // Handle permission update events
  });
};

// Initialize the module
const init = (app) => {
  if (app.locals.eventBus) {
    registerModuleEvents(app.locals.eventBus);
  }
};

/**
 * @route GET /api/permission_management/permissions
 * @description Get all permissions
 * @access Private - Requires permission_view permission
 */
router.get('/permissions', authenticateToken, checkPermissions(['permission_view']), async (req, res) => {
  try {
    const db = req.app.locals.db;
    const eventBus = req.app.locals.eventBus;
    
    // Query all permissions
    const permissions = await dbMethods.all(db, 
      'SELECT permission_id, name, description, created_at, updated_at FROM permissions_master ORDER BY name',
      []
    );
    
    // For each permission, count roles that have it
    for (const permission of permissions) {
      const roleCount = await dbMethods.get(db,
        'SELECT COUNT(DISTINCT role_id) as count FROM role_permissions_tx WHERE permission_id = ?',
        [permission.permission_id]
      );
      
      permission.role_count = roleCount.count;
    }
    
    // Log activity
    eventBus.emit('log:activity', {
      user_id: req.user.user_id,
      action: 'PERMISSION_LIST_VIEW',
      details: 'Retrieved list of permissions',
      ip_address: req.ip,
      user_agent: req.headers['user-agent']
    });
    
    return res.status(200).json({
      count: permissions.length,
      permissions
    });
  } catch (error) {
    console.error('Error fetching permissions:', error);
    return res.status(500).json({ error: 'Failed to retrieve permissions' });
  }
});

/**
 * @route GET /api/permission_management/permissions/:id
 * @description Get permission by ID
 * @access Private - Requires permission_view permission
 */
router.get('/permissions/:id', authenticateToken, checkPermissions(['permission_view']), async (req, res) => {
  try {
    const permissionId = req.params.id;
    const db = req.app.locals.db;
    const eventBus = req.app.locals.eventBus;
    
    // Get permission data
    const permission = await dbMethods.get(db, 
      'SELECT permission_id, name, description, created_at, updated_at FROM permissions_master WHERE permission_id = ?',
      [permissionId]
    );
    
    if (!permission) {
      return res.status(404).json({ error: 'Permission not found' });
    }
    
    // Get roles that have this permission
    const roles = await dbMethods.all(db, 
      `SELECT r.role_id, r.name, r.description
       FROM roles_master r
       JOIN role_permissions_tx rp ON r.role_id = rp.role_id
       WHERE rp.permission_id = ?
       ORDER BY r.name`,
      [permissionId]
    );
    
    // Attach roles to the permission
    permission.roles = roles;
    permission.role_count = roles.length;
    
    // Log activity
    eventBus.emit('log:activity', {
      user_id: req.user.user_id,
      action: 'PERMISSION_DETAIL_VIEW',
      details: `Viewed details for permission ID ${permissionId}`,
      ip_address: req.ip,
      user_agent: req.headers['user-agent']
    });
    
    return res.status(200).json(permission);
  } catch (error) {
    console.error(`Error fetching permission ${req.params.id}:`, error);
    return res.status(500).json({ error: 'Failed to retrieve permission details' });
  }
});

/**
 * @route POST /api/permission_management/permissions
 * @description Create a new permission
 * @access Private - Requires permission_assign permission
 */
router.post('/permissions', [
  authenticateToken, 
  checkPermissions(['permission_assign']),
  body('name').notEmpty().withMessage('Permission name is required')
    .matches(/^[a-z_]+$/).withMessage('Permission name must be lowercase with underscores only'),
  body('description').notEmpty().withMessage('Description is required')
], async (req, res) => {
  try {
    // Validate request
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({ errors: errors.array() });
    }
    
    const { name, description } = req.body;
    const db = req.app.locals.db;
    const eventBus = req.app.locals.eventBus;
    
    // Check if permission with the same name already exists
    const existingPermission = await dbMethods.get(db, 
      'SELECT permission_id FROM permissions_master WHERE name = ?', 
      [name]
    );
    
    if (existingPermission) {
      return res.status(409).json({ error: 'A permission with this name already exists' });
    }
    
    // Create new permission
    const result = await dbMethods.run(db, 
      'INSERT INTO permissions_master (name, description) VALUES (?, ?)', 
      [name, description]
    );
    
    const newPermissionId = result.lastID;
    
    // Automatically assign to Admin role
    const adminRole = await dbMethods.get(db, 'SELECT role_id FROM roles_master WHERE name = ?', ['Admin']);
    if (adminRole) {
      await dbMethods.run(db, 
        'INSERT INTO role_permissions_tx (role_id, permission_id) VALUES (?, ?)',
        [adminRole.role_id, newPermissionId]
      );
    }
    
    // Log activity
    eventBus.emit('log:activity', {
      user_id: req.user.user_id,
      action: 'PERMISSION_CREATED',
      details: `Created new permission: ${name}`,
      ip_address: req.ip,
      user_agent: req.headers['user-agent']
    });
    
    // Emit permission created event
    eventBus.emit('permission:created', {
      permission_id: newPermissionId,
      name,
      created_by: req.user.user_id
    });
    
    return res.status(201).json({ 
      message: 'Permission created successfully',
      permission_id: newPermissionId 
    });
  } catch (error) {
    console.error('Error creating permission:', error);
    return res.status(500).json({ error: 'Failed to create permission' });
  }
});

/**
 * @route PUT /api/permission_management/permissions/:id
 * @description Update permission information
 * @access Private - Requires permission_assign permission
 */
router.put('/permissions/:id', [
  authenticateToken, 
  checkPermissions(['permission_assign']),
  body('description').notEmpty().withMessage('Description is required')
], async (req, res) => {
  try {
    // Validate request
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({ errors: errors.array() });
    }
    
    const permissionId = req.params.id;
    const { description } = req.body;
    const db = req.app.locals.db;
    const eventBus = req.app.locals.eventBus;
    
    // Check if permission exists
    const existingPermission = await dbMethods.get(db, 
      'SELECT permission_id, name FROM permissions_master WHERE permission_id = ?', 
      [permissionId]
    );
    
    if (!existingPermission) {
      return res.status(404).json({ error: 'Permission not found' });
    }
    
    // Update permission (only description can be updated, name is immutable)
    await dbMethods.run(db, 
      'UPDATE permissions_master SET description = ?, updated_at = CURRENT_TIMESTAMP WHERE permission_id = ?', 
      [description, permissionId]
    );
    
    // Log activity
    eventBus.emit('log:activity', {
      user_id: req.user.user_id,
      action: 'PERMISSION_UPDATED',
      details: `Updated permission ID ${permissionId} (${existingPermission.name})`,
      ip_address: req.ip,
      user_agent: req.headers['user-agent']
    });
    
    // Emit permission updated event
    eventBus.emit('permission:updated', {
      permission_id: permissionId,
      name: existingPermission.name,
      updated_by: req.user.user_id
    });
    
    return res.status(200).json({ 
      message: 'Permission updated successfully'
    });
  } catch (error) {
    console.error(`Error updating permission ${req.params.id}:`, error);
    return res.status(500).json({ error: 'Failed to update permission' });
  }
});

/**
 * @route POST /api/permission_management/assign
 * @description Assign or remove permissions from a role
 * @access Private - Requires permission_assign permission
 */
router.post('/assign', [
  authenticateToken, 
  checkPermissions(['permission_assign']),
  body('role_id').isNumeric().withMessage('Valid role ID is required'),
  body('permissions').isArray().withMessage('Permissions must be an array'),
], async (req, res) => {
  try {
    // Validate request
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({ errors: errors.array() });
    }
    
    const { role_id, permissions } = req.body;
    const db = req.app.locals.db;
    const eventBus = req.app.locals.eventBus;
    
    // Check if role exists
    const role = await dbMethods.get(db, 'SELECT role_id, name FROM roles_master WHERE role_id = ?', [role_id]);
    if (!role) {
      return res.status(404).json({ error: 'Role not found' });
    }
    
    // For Admin role, ensure it keeps all permissions
    if (role_id == 1) {
      const allPermissions = await dbMethods.all(db, 'SELECT permission_id FROM permissions_master');
      const allPermissionIds = allPermissions.map(p => p.permission_id);
      
      // Check if all permissions are included
      const hasAllPermissions = allPermissionIds.every(id => 
        permissions.includes(id) || permissions.includes(id.toString())
      );
      
      if (!hasAllPermissions) {
        return res.status(403).json({ error: 'Admin role must have all permissions' });
      }
    }
    
    // Delete existing permissions first
    await dbMethods.run(db, 'DELETE FROM role_permissions_tx WHERE role_id = ?', [role_id]);
    
    // Add new permissions
    for (const permissionId of permissions) {
      await dbMethods.run(db, 
        'INSERT INTO role_permissions_tx (role_id, permission_id) VALUES (?, ?)', 
        [role_id, permissionId]
      );
    }
    
    // Log activity
    eventBus.emit('log:activity', {
      user_id: req.user.user_id,
      action: 'PERMISSIONS_ASSIGNED',
      details: `Updated permissions for role ${role.name} (${role_id})`,
      ip_address: req.ip,
      user_agent: req.headers['user-agent']
    });
    
    // Emit role updated event
    eventBus.emit('role:updated', {
      role_id,
      updated_by: req.user.user_id,
      permissions_changed: true
    });
    
    return res.status(200).json({ 
      message: 'Permissions updated successfully'
    });
  } catch (error) {
    console.error('Error assigning permissions:', error);
    return res.status(500).json({ error: 'Failed to update permissions' });
  }
});

// Export router and init function
module.exports = router;
module.exports.init = init;
