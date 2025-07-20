/**
 * Logging Module - Backend Implementation
 * Created: 2025-06-27
 */

const express = require('express');
const router = express.Router();
const { body, query, validationResult } = require('express-validator');
const { authenticateToken } = require('../../../middleware/auth');
const { checkPermissions } = require('../../../middleware/rbac');
const { dbMethods } = require('../../database/backend');

// Register module events
const registerModuleEvents = (eventBus) => {
  // Listen for activity log events
  eventBus.on('log:activity', async (data) => {
    try {
      const db = global.app.locals.db;
      
      // Prepare log entry
      const logEntry = {
        user_id: data.user_id || null,
        action: data.action || 'UNKNOWN_ACTION',
        details: data.details || null,
        ip_address: data.ip_address || null,
        user_agent: data.user_agent || null
      };
      
      // Insert log entry into database
      await dbMethods.run(db, 
        `INSERT INTO activity_logs_tx (user_id, action, details, ip_address, user_agent) 
         VALUES (?, ?, ?, ?, ?)`,
        [logEntry.user_id, logEntry.action, logEntry.details, logEntry.ip_address, logEntry.user_agent]
      );
      
      // Log to console for development
      console.log(`[ACTIVITY LOG] ${logEntry.action}: ${logEntry.details}`);
    } catch (error) {
      console.error('Error logging activity:', error);
    }
  });
  
  // Listen for error log events
  eventBus.on('log:error', async (data) => {
    try {
      const db = global.app.locals.db;
      
      // Prepare log entry
      const logEntry = {
        user_id: data.user_id || null,
        action: 'ERROR',
        details: `${data.message}\n${data.stack || ''}`,
        ip_address: data.ip_address || null,
        user_agent: data.user_agent || null
      };
      
      // Insert log entry into database
      await dbMethods.run(db, 
        `INSERT INTO activity_logs_tx (user_id, action, details, ip_address, user_agent) 
         VALUES (?, ?, ?, ?, ?)`,
        [logEntry.user_id, logEntry.action, logEntry.details, logEntry.ip_address, logEntry.user_agent]
      );
      
      // Log to console for development
      console.error(`[ERROR LOG] ${data.message}`);
    } catch (error) {
      console.error('Error logging error:', error);
    }
  });
};

// Initialize the module
const init = (app) => {
  if (app.locals.eventBus) {
    // Store app reference globally for event handlers
    global.app = app;
    registerModuleEvents(app.locals.eventBus);
    console.log('Logging module initialized');
  }
};

/**
 * @route GET /api/logging/activity
 * @description Get activity logs with pagination and filtering
 * @access Private - Requires activity_view permission or admin/full_access role
 */
router.get('/activity', [
  authenticateToken, 
  (req, res, next) => {
    // Allow access if user has activity_view permission OR is admin/full_access
    const userRoles = req.user.roles || [];
    const userPermissions = req.user.permissions || [];
    
    if (userPermissions.includes('activity_view') ||
        userRoles.includes('Admin') ||
        userRoles.includes('full_access')) {
      next();
    } else {
      return res.status(403).json({ 
        error: 'Access denied: insufficient permissions',
        required: ['activity_view']
      });
    }
  },
  query('page').optional().isInt({ min: 1 }).withMessage('Page must be a positive integer'),
  query('limit').optional().isInt({ min: 1, max: 100 }).withMessage('Limit must be between 1 and 100'),
  query('action').optional().isString(),
  query('user_id').optional().isInt({ min: 1 }),
  query('start_date').optional().isISO8601(),
  query('end_date').optional().isISO8601()
], async (req, res) => {
  try {
    // Validate request
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({ errors: errors.array() });
    }
    
    const db = req.app.locals.db;
    const eventBus = req.app.locals.eventBus;
    
    // Pagination parameters
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 50;
    const offset = (page - 1) * limit;
    
    // Filter parameters
    const { action, user_id, start_date, end_date } = req.query;
    
    // Build base query and count query
    let logQuery = `
      SELECT 
        al.activity_log_id, 
        al.user_id, 
        CASE WHEN u.first_name IS NOT NULL 
          THEN u.first_name || ' ' || u.last_name 
          ELSE 'Unknown User' 
        END as user_name,
        al.action, 
        al.details, 
        al.ip_address, 
        al.user_agent, 
        al.created_at
      FROM activity_logs_tx al
      LEFT JOIN users_master u ON al.user_id = u.user_id
    `;
    
    let countQuery = 'SELECT COUNT(*) as total FROM activity_logs_tx al';
    
    // Build WHERE clause based on filters
    const whereConditions = [];
    const params = [];
    const countParams = [];
    
    if (action) {
      whereConditions.push('al.action = ?');
      params.push(action);
      countParams.push(action);
    }
    
    if (user_id) {
      whereConditions.push('al.user_id = ?');
      params.push(user_id);
      countParams.push(user_id);
    }
    
    if (start_date) {
      whereConditions.push('al.created_at >= ?');
      params.push(start_date);
      countParams.push(start_date);
    }
    
    if (end_date) {
      whereConditions.push('al.created_at <= ?');
      params.push(end_date);
      countParams.push(end_date);
    }
    
    // Add WHERE clause if conditions exist
    if (whereConditions.length > 0) {
      const whereClause = 'WHERE ' + whereConditions.join(' AND ');
      logQuery += ' ' + whereClause;
      countQuery += ' ' + whereClause;
    }
    
    // Add ORDER BY and LIMIT
    logQuery += ' ORDER BY al.created_at DESC LIMIT ? OFFSET ?';
    params.push(limit, offset);
    
    // Execute queries
    const logs = await dbMethods.all(db, logQuery, params);
    const countResult = await dbMethods.get(db, countQuery, countParams);
    const total = countResult.total;
    
    // Calculate pagination metadata
    const totalPages = Math.ceil(total / limit);
    const hasNext = page < totalPages;
    const hasPrev = page > 1;
    
    // Log activity (viewing logs is also an activity)
    eventBus.emit('log:activity', {
      user_id: req.user.user_id,
      action: 'LOGS_VIEWED',
      details: `Viewed activity logs (page ${page})`,
      ip_address: req.ip,
      user_agent: req.headers['user-agent']
    });
    
    return res.status(200).json({
      logs,
      pagination: {
        page,
        limit,
        total,
        total_pages: totalPages,
        has_next: hasNext,
        has_prev: hasPrev
      }
    });
  } catch (error) {
    console.error('Error fetching activity logs:', error);
    return res.status(500).json({ error: 'Failed to retrieve activity logs' });
  }
});

/**
 * @route GET /api/logging/actions
 * @description Get all unique action types for filtering
 * @access Private - Requires permission_view permission
 */
router.get('/actions', authenticateToken, checkPermissions(['permission_view']), async (req, res) => {
  try {
    const db = req.app.locals.db;
    
    // Get unique action types
    const actions = await dbMethods.all(db, 
      'SELECT DISTINCT action FROM activity_logs_tx ORDER BY action',
      []
    );
    
    return res.status(200).json({
      actionTypes: actions.map(a => a.action)
    });
  } catch (error) {
    console.error('Error fetching action types:', error);
    return res.status(500).json({ error: 'Failed to retrieve action types' });
  }
});

/**
 * @route GET /api/logging/entities
 * @description Get all unique entity types for filtering
 * @access Private - Requires permission_view permission
 */
router.get('/entities', authenticateToken, checkPermissions(['permission_view']), async (req, res) => {
  try {
    const db = req.app.locals.db;
    
    // Extract entity types from activity logs details where possible
    // This is an approximation based on common patterns in log details
    const entityTypes = await dbMethods.all(db, 
      `SELECT DISTINCT
         CASE 
           WHEN details LIKE '%user%' THEN 'User'
           WHEN details LIKE '%role%' THEN 'Role'
           WHEN details LIKE '%permission%' THEN 'Permission'
           WHEN details LIKE '%payment%' THEN 'Payment'
           WHEN details LIKE '%feature%' THEN 'Feature Toggle'
           ELSE 'System'
         END as entity_type
       FROM activity_logs_tx
       ORDER BY entity_type`,
      []
    );
    
    return res.status(200).json({
      entityTypes: entityTypes.map(e => e.entity_type)
    });
  } catch (error) {
    console.error('Error fetching entity types:', error);
    return res.status(500).json({ error: 'Failed to retrieve entity types' });
  }
});

/**
 * @route GET /api/logging/stats
 * @description Get activity statistics (counts by action type, recent activity)
 * @access Private - Requires permission_view permission
 */
/**
 * @route GET /api/logging/stats
 * @description Get activity log statistics
 * @access Private - Requires activity_view permission or admin/full_access role
 */
router.get('/stats', authenticateToken, (req, res, next) => {
    // Allow access if user has activity_view permission OR is admin/full_access
    const userRoles = req.user.roles || [];
    const userPermissions = req.user.permissions || [];
    
    if (userPermissions.includes('activity_view') ||
        userRoles.includes('Admin') ||
        userRoles.includes('full_access')) {
      next();
    } else {
      return res.status(403).json({ 
        error: 'Access denied: insufficient permissions',
        required: ['activity_view']
      });
    }
  }, async (req, res) => {
  try {
    const db = req.app.locals.db;
    
    // Get action counts
    const actionCounts = await dbMethods.all(db, 
      'SELECT action, COUNT(*) as count FROM activity_logs_tx GROUP BY action ORDER BY count DESC',
      []
    );
    
    // Generate a complete date range for the last 7 days
    const dailyActivityMap = new Map();
    
    // Initialize the last 7 days with zero counts
    for (let i = 0; i < 7; i++) {
      const date = new Date();
      date.setDate(date.getDate() - i);
      const formattedDate = date.toISOString().split('T')[0]; // YYYY-MM-DD format
      dailyActivityMap.set(formattedDate, 0);
    }
    
    // Get daily activity for last 7 days
    const activityResults = await dbMethods.all(db, 
      `SELECT 
        DATE(created_at) as date,
        COUNT(*) as count
      FROM activity_logs_tx
      WHERE created_at >= DATE('now', '-7 days')
      GROUP BY DATE(created_at)
      ORDER BY date`,
      []
    );
    
    // Merge actual counts into the map
    activityResults.forEach(row => {
      if (dailyActivityMap.has(row.date)) {
        dailyActivityMap.set(row.date, row.count);
      }
    });
    
    // Convert map to array for response
    const dailyActivity = Array.from(dailyActivityMap, ([date, count]) => ({ date, count }));
    
    // Sort by date (ascending)
    dailyActivity.sort((a, b) => new Date(a.date) - new Date(b.date));
    
    // Get top users by activity
    const topUsers = await dbMethods.all(db, 
      `SELECT 
        al.user_id,
        CASE WHEN u.first_name IS NOT NULL 
          THEN u.first_name || ' ' || u.last_name 
          ELSE 'Unknown User' 
        END as user_name,
        COUNT(*) as action_count
      FROM activity_logs_tx al
      LEFT JOIN users_master u ON al.user_id = u.user_id
      GROUP BY al.user_id
      ORDER BY action_count DESC
      LIMIT 5`,
      []
    );
    
    return res.status(200).json({
      action_counts: actionCounts,
      daily_activity: dailyActivity,
      top_users: topUsers,
      total_logs: actionCounts.reduce((sum, item) => sum + item.count, 0)
    });
  } catch (error) {
    console.error('Error fetching activity statistics:', error);
    return res.status(500).json({ error: 'Failed to retrieve activity statistics' });
  }
});

// Export router and init function
module.exports = router;
module.exports.init = init;
