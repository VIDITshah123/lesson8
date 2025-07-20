/**
 * Database Utility Module
 * Provides database connection and common database operations
 */

const express = require('express');
const router = express.Router();
const { authenticateToken } = require('../../../middleware/auth');
const { checkPermissions } = require('../../../middleware/rbac');

/**
 * Get database health status
 * @route GET /api/database/status
 * @access Private - Admin only
 */
router.get('/status', authenticateToken, checkPermissions(['permission_view']), async (req, res) => {
  try {
    const db = req.app.locals.db;
    
    // Check if database is connected and working
    db.get("SELECT 1", (err) => {
      if (err) {
        return res.status(500).json({ 
          status: 'error', 
          message: 'Database connection error',
          error: err.message
        });
      }
      
      return res.status(200).json({ 
        status: 'connected',
        message: 'Database is connected and operational',
        timestamp: new Date()
      });
    });
  } catch (error) {
    console.error('Database status error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

/**
 * Run an SQL query (admin only)
 * @route POST /api/database/query
 * @access Private - Admin only
 */
router.post('/query', authenticateToken, checkPermissions(['permission_view']), async (req, res) => {
  try {
    const { query } = req.body;
    
    if (!query) {
      return res.status(400).json({ error: 'Query is required' });
    }
    
    // Only allow SELECT queries for security
    if (!query.trim().toUpperCase().startsWith('SELECT')) {
      return res.status(403).json({ error: 'Only SELECT queries are allowed' });
    }
    
    const db = req.app.locals.db;
    
    db.all(query, [], (err, rows) => {
      if (err) {
        return res.status(400).json({ 
          error: 'Query execution failed',
          details: err.message
        });
      }
      
      res.status(200).json({ results: rows });
    });
  } catch (error) {
    console.error('Database query error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

/**
 * Promisify database operations for easier use in other modules
 */
const dbMethods = {
  /**
   * Run SQL query that doesn't return data
   * @param {Object} db - Database connection object
   * @param {String} sql - SQL query
   * @param {Array} params - Query parameters
   * @returns {Promise} Promise object represents the query execution
   */
  run: (db, sql, params = []) => {
    return new Promise((resolve, reject) => {
      db.run(sql, params, function (err) {
        if (err) {
          reject(err);
          return;
        }
        resolve({ lastID: this.lastID, changes: this.changes });
      });
    });
  },
  
  /**
   * Get a single row from the database
   * @param {Object} db - Database connection object
   * @param {String} sql - SQL query
   * @param {Array} params - Query parameters
   * @returns {Promise} Promise object represents the single row
   */
  get: (db, sql, params = []) => {
    return new Promise((resolve, reject) => {
      db.get(sql, params, (err, row) => {
        if (err) {
          reject(err);
          return;
        }
        resolve(row);
      });
    });
  },
  
  /**
   * Get all rows from the database
   * @param {Object} db - Database connection object
   * @param {String} sql - SQL query
   * @param {Array} params - Query parameters
   * @returns {Promise} Promise object represents the array of rows
   */
  all: (db, sql, params = []) => {
    return new Promise((resolve, reject) => {
      db.all(sql, params, (err, rows) => {
        if (err) {
          reject(err);
          return;
        }
        resolve(rows);
      });
    });
  }
};

// Export router for API endpoints
module.exports = router;

// Export database methods for use in other modules
module.exports.dbMethods = dbMethods;
