/**
 * Payment Integration Module - Backend Implementation
 * Created: 2025-07-11
 */

const express = require('express');
const router = express.Router();
const { body, validationResult } = require('express-validator');
const { authenticateToken } = require('../../../middleware/auth');
const { checkPermissions } = require('../../../middleware/rbac');
const { dbMethods } = require('../../database/backend');
const multer = require('multer');
const fs = require('fs');
const path = require('path');
const { v4: uuidv4 } = require('uuid');

// Register module events
const registerModuleEvents = (eventBus) => {
  eventBus.on('payment:qrcode-uploaded', (data) => {
    console.log('QR code uploaded event received:', data);
    // Handle QR code upload events
  });

  eventBus.on('payment:qrcode-activated', (data) => {
    console.log('QR code activated event received:', data);
    // Handle QR code activation events
  });
};

// Set up multer storage configuration for QR code images
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    const uploadDir = path.join(__dirname, '../../../uploads/qr');
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
  // Accept only image files
  if (file.mimetype.startsWith('image/')) {
    cb(null, true);
  } else {
    cb(new Error('Only image files are allowed for QR codes'), false);
  }
};

const upload = multer({ 
  storage: storage,
  fileFilter: fileFilter,
  limits: {
    fileSize: 2 * 1024 * 1024 // 2MB max file size
  }
});

// Feature toggle check middleware
const checkPaymentFeatureEnabled = async (req, res, next) => {
  try {
    const db = req.app.locals.db;
    const featureToggle = await dbMethods.get(db, 
      'SELECT is_enabled FROM feature_toggles WHERE feature_name = ?', 
      ['payment_integration']
    );

    // Check if feature toggle exists and is enabled (1)
    // SQLite stores boolean as integers 0/1
    if (!featureToggle || featureToggle.is_enabled !== 1) {
      return res.status(403).json({ error: 'Payment integration feature is not enabled' });
    }
    next();
  } catch (error) {
    console.error('Error checking payment feature toggle:', error);
    return res.status(500).json({ error: 'Error checking payment feature' });
  }
};

// Initialize the module
const init = (app) => {
  if (app.locals.eventBus) {
    registerModuleEvents(app.locals.eventBus);
  }
  
  // Initialize database tables
  if (app.locals.db) {
    initializeDatabase(app.locals.db);
  }
};

// Create necessary database tables if they don't exist
const initializeDatabase = async (db) => {
  try {
    // Create payment_qr_codes table if it doesn't exist
    await dbMethods.run(db, `
      CREATE TABLE IF NOT EXISTS payment_qr_codes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        description TEXT,
        image_url TEXT NOT NULL,
        image_data BLOB,
        payment_type TEXT NOT NULL,
        active INTEGER DEFAULT 0,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
      )
    `);
    
    // Create payment_transactions table if it doesn't exist
    await dbMethods.run(db, `
      CREATE TABLE IF NOT EXISTS payment_transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        transaction_reference TEXT UNIQUE NOT NULL,
        amount REAL NOT NULL,
        currency TEXT DEFAULT 'INR',
        payment_status TEXT NOT NULL,
        qr_code_id INTEGER,
        user_id INTEGER,
        transaction_notes TEXT,
        transaction_metadata TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (qr_code_id) REFERENCES payment_qr_codes(id),
        FOREIGN KEY (user_id) REFERENCES users(id)
      )
    `);
    
    console.log('Payment module database tables initialized successfully');
  } catch (error) {
    console.error('Error initializing payment module database tables:', error);
  }
};

/**
 * @route GET /api/payment/status
 * @description Check if payment integration feature is enabled
 * @access Public
 */
router.get('/status', async (req, res) => {
  try {
    const db = req.app.locals.db;
    const featureToggle = await dbMethods.get(db, 
      'SELECT is_enabled FROM feature_toggles WHERE feature_name = ?', 
      ['payment_integration']
    );

    return res.json({ 
      enabled: featureToggle && featureToggle.is_enabled === 1 
    });
  } catch (error) {
    console.error('Error checking payment feature status:', error);
    return res.status(500).json({ error: 'Error checking payment feature status' });
  }
});

/**
 * @route GET /api/payment/qr-codes
 * @description Get all QR codes
 * @access Private - Requires payment_view permission
 */
router.get('/qr-codes', [
  authenticateToken, 
  checkPermissions(['payment_view']),
  checkPaymentFeatureEnabled
], async (req, res) => {
  try {
    const db = req.app.locals.db;
    const eventBus = req.app.locals.eventBus;
    
    // Get all QR codes
    const qrCodes = await dbMethods.all(db, 
      `SELECT 
        id, 
        name, 
        description, 
        image_url, 
        payment_type, 
        active, 
        created_at,
        updated_at
       FROM payment_qr_codes
       ORDER BY created_at DESC`, 
      []
    );
    
    // Log activity
    eventBus.emit('log:activity', {
      user_id: req.user.user_id,
      action: 'QR_CODE_LIST_VIEW',
      details: 'Retrieved list of QR codes',
      ip_address: req.ip,
      user_agent: req.headers['user-agent']
    });
    
    return res.json(qrCodes);
  } catch (error) {
    console.error('Error fetching QR codes:', error);
    return res.status(500).json({ error: 'Failed to fetch QR codes' });
  }
});

/**
 * @route GET /api/payment/qr-codes/:id
 * @description Get a specific QR code
 * @access Private - Requires payment_view permission
 */
router.get('/qr-codes/:id', [
  authenticateToken, 
  checkPermissions(['payment_view']),
  checkPaymentFeatureEnabled
], async (req, res) => {
  try {
    const qrCodeId = req.params.id;
    const db = req.app.locals.db;
    const eventBus = req.app.locals.eventBus;
    
    // Get QR code
    const qrCode = await dbMethods.get(db, 
      `SELECT 
        id, 
        name, 
        description, 
        image_url, 
        payment_type, 
        active, 
        created_at,
        updated_at
       FROM payment_qr_codes
       WHERE id = ?`, 
      [qrCodeId]
    );
    
    if (!qrCode) {
      return res.status(404).json({ error: 'QR code not found' });
    }
    
    // Log activity
    eventBus.emit('log:activity', {
      user_id: req.user.user_id,
      action: 'QR_CODE_VIEW',
      details: `Viewed QR code ID ${qrCodeId}`,
      ip_address: req.ip,
      user_agent: req.headers['user-agent']
    });
    
    return res.json(qrCode);
  } catch (error) {
    console.error(`Error fetching QR code ${req.params.id}:`, error);
    return res.status(500).json({ error: 'Failed to fetch QR code' });
  }
});

/**
 * @route POST /api/payment/qr-codes
 * @description Upload a new QR code
 * @access Private - Requires payment_create permission
 */
router.post('/qr-codes', [
  authenticateToken, 
  checkPermissions(['payment_create']),
  checkPaymentFeatureEnabled,
  upload.single('qr_code_image'),
  body('name').notEmpty().withMessage('QR code name is required'),
  body('payment_type').notEmpty().withMessage('Payment type is required')
], async (req, res) => {
  try {
    // Validate request
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      // Remove uploaded file if validation fails
      if (req.file && req.file.path) {
        fs.unlinkSync(req.file.path);
      }
      return res.status(400).json({ errors: errors.array() });
    }
    
    if (!req.file) {
      return res.status(400).json({ error: 'QR code image is required' });
    }
    
    const { payment_name, payment_description, payment_type } = req.body;
    const db = req.app.locals.db;
    const eventBus = req.app.locals.eventBus;
    
    // Read file for blob storage
    const qrCodeImage = fs.readFileSync(req.file.path);
    
    // Create new QR code
    const result = await dbMethods.run(db, 
      `INSERT INTO payment_qr_codes (
        name, 
        description, 
        image_url, 
        payment_type, 
        active
      ) VALUES (?, ?, ?, ?, ?)`, 
      [
        payment_name, 
        payment_description || null, 
        req.file.path,
        payment_type,
        0,  // Set as inactive by default
        //req.user.user_id
      ]
    );
    
    const newQrCodeId = result.lastID;
    
    // Log activity
    eventBus.emit('log:activity', {
      user_id: req.user.user_id,
      action: 'QR_CODE_CREATED',
      details: `Uploaded new QR code: ${payment_name}`,
      ip_address: req.ip,
      user_agent: req.headers['user-agent']
    });
    
    // Emit QR code uploaded event
    eventBus.emit('payment:qrcode-uploaded', {
      qr_code_id: newQrCodeId,
      payment_name,
      created_by: req.user.user_id
    });
    
    return res.status(201).json({ 
      message: 'QR code uploaded successfully',
      qr_code_id: newQrCodeId
    });
  } catch (error) {
    // Remove uploaded file if operation fails
    if (req.file && req.file.path) {
      fs.unlinkSync(req.file.path);
    }
    console.error('Error uploading QR code:', error);
    return res.status(500).json({ error: 'Failed to upload QR code' });
  }
});

/**
 * @route PUT /api/payment/qr-codes/:id
 * @description Update QR code details
 * @access Private - Requires payment_edit permission
 */
router.put('/qr-codes/:id', [
  authenticateToken, 
  checkPermissions(['payment_edit']),
  checkPaymentFeatureEnabled,
  body('payment_name').optional().notEmpty().withMessage('QR code name cannot be empty'),
  body('payment_type').optional().notEmpty().withMessage('Payment type cannot be empty')
], async (req, res) => {
  try {
    // Validate request
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({ errors: errors.array() });
    }
    
    const qrCodeId = req.params.id;
    const { payment_name, payment_description, payment_type } = req.body;
    const db = req.app.locals.db;
    const eventBus = req.app.locals.eventBus;
    
    // Check if QR code exists
    const existingQrCode = await dbMethods.get(db, 'SELECT id FROM payment_qr_codes WHERE id = ?', [qrCodeId]);
    if (!existingQrCode) {
      return res.status(404).json({ error: 'QR code not found' });
    }
    
    // Build update query based on provided fields
    const updateFields = [];
    const updateValues = [];
    
    if (payment_name) {
      updateFields.push('payment_name = ?');
      updateValues.push(payment_name);
    }
    
    if (payment_description !== undefined) {
      updateFields.push('payment_description = ?');
      updateValues.push(payment_description);
    }
    
    if (payment_type) {
      updateFields.push('payment_type = ?');
      updateValues.push(payment_type);
    }
    
    // Update timestamp
    updateFields.push('updated_at = CURRENT_TIMESTAMP');
    
    if (updateFields.length === 0) {
      return res.status(400).json({ error: 'No fields to update' });
    }
    
    // Add ID to update values
    updateValues.push(qrCodeId);
    
    // Update QR code
    await dbMethods.run(db, 
      `UPDATE payment_qr_codes SET ${updateFields.join(', ')} WHERE id = ?`, 
      updateValues
    );
    
    // Log activity
    eventBus.emit('log:activity', {
      user_id: req.user.user_id,
      action: 'QR_CODE_UPDATED',
      details: `Updated QR code ID ${qrCodeId}`,
      ip_address: req.ip,
      user_agent: req.headers['user-agent']
    });
    
    // Get updated QR code
    const updatedQrCode = await dbMethods.get(db, 
      `SELECT 
        id, 
        payment_name, 
        payment_description, 
        qr_code_path, 
        payment_type, 
        is_active, 
        created_by,
        created_at,
        updated_at
       FROM payment_qr_codes
       WHERE id = ?`, 
      [qrCodeId]
    );
    
    return res.json(updatedQrCode);
  } catch (error) {
    console.error(`Error updating QR code ${req.params.id}:`, error);
    return res.status(500).json({ error: 'Failed to update QR code' });
  }
});

/**
 * @route DELETE /api/payment/qr-codes/:id
 * @description Delete a QR code
 * @access Private - Requires payment_delete permission
 */
router.delete('/qr-codes/:id', [
  authenticateToken, 
  checkPermissions(['payment_delete']),
  checkPaymentFeatureEnabled
], async (req, res) => {
  try {
    const qrCodeId = req.params.id;
    const db = req.app.locals.db;
    const eventBus = req.app.locals.eventBus;
    
    // Get QR code to check if it exists and to get the file path
    const qrCode = await dbMethods.get(db, 
      'SELECT id, qr_code_path FROM payment_qr_codes WHERE id = ?', 
      [qrCodeId]
    );
    
    if (!qrCode) {
      return res.status(404).json({ error: 'QR code not found' });
    }
    
    // Check if QR code is being used in any transactions
    const transactionsUsingQrCode = await dbMethods.get(db, 
      'SELECT COUNT(*) as count FROM payment_transactions WHERE qr_code_id = ?', 
      [qrCodeId]
    );
    
    if (transactionsUsingQrCode && transactionsUsingQrCode.count > 0) {
      return res.status(400).json({ 
        error: 'QR code cannot be deleted as it is associated with transactions' 
      });
    }
    
    // Delete QR code
    await dbMethods.run(db, 'DELETE FROM payment_qr_codes WHERE id = ?', [qrCodeId]);
    
    // Delete file from filesystem if it exists
    if (qrCode.qr_code_path && fs.existsSync(qrCode.qr_code_path)) {
      fs.unlinkSync(qrCode.qr_code_path);
    }
    
    // Log activity
    eventBus.emit('log:activity', {
      user_id: req.user.user_id,
      action: 'QR_CODE_DELETED',
      details: `Deleted QR code ID ${qrCodeId}`,
      ip_address: req.ip,
      user_agent: req.headers['user-agent']
    });
    
    return res.json({ message: 'QR code deleted successfully' });
  } catch (error) {
    console.error(`Error deleting QR code ${req.params.id}:`, error);
    return res.status(500).json({ error: 'Failed to delete QR code' });
  }
});

/**
 * @route PATCH /api/payment/qr-codes/:id/activate
 * @description Activate a QR code
 * @access Private - Requires payment_edit permission
 */
router.patch('/qr-codes/:id/activate', [
  authenticateToken, 
  checkPermissions(['payment_edit']),
  checkPaymentFeatureEnabled
], async (req, res) => {
  try {
    const qrCodeId = req.params.id;
    const db = req.app.locals.db;
    const eventBus = req.app.locals.eventBus;
    
    // Check if QR code exists
    const existingQrCode = await dbMethods.get(db, 'SELECT id FROM payment_qr_codes WHERE id = ?', [qrCodeId]);
    if (!existingQrCode) {
      return res.status(404).json({ error: 'QR code not found' });
    }
    
    // Start transaction to ensure atomic operations
    await dbMethods.run(db, 'BEGIN TRANSACTION');
    
    try {
      // Deactivate all QR codes
      await dbMethods.run(db, 'UPDATE payment_qr_codes SET is_active = 0');
      
      // Activate the selected QR code
      await dbMethods.run(db, 'UPDATE payment_qr_codes SET is_active = 1 WHERE id = ?', [qrCodeId]);
      
      // Commit transaction
      await dbMethods.run(db, 'COMMIT');
    } catch (err) {
      // Rollback transaction on error
      await dbMethods.run(db, 'ROLLBACK');
      throw err;
    }
    
    // Log activity
    eventBus.emit('log:activity', {
      user_id: req.user.user_id,
      action: 'QR_CODE_ACTIVATED',
      details: `Activated QR code ID ${qrCodeId}`,
      ip_address: req.ip,
      user_agent: req.headers['user-agent']
    });
    
    // Emit QR code activated event
    eventBus.emit('payment:qrcode-activated', {
      qr_code_id: qrCodeId,
      activated_by: req.user.user_id
    });
    
    return res.json({ message: 'QR code activated successfully' });
  } catch (error) {
    console.error(`Error activating QR code ${req.params.id}:`, error);
    return res.status(500).json({ error: 'Failed to activate QR code' });
  }
});

/**
 * @route PATCH /api/payment/qr-codes/:id/deactivate
 * @description Deactivate a QR code
 * @access Private - Requires payment_edit permission
 */
router.patch('/qr-codes/:id/deactivate', [
  authenticateToken, 
  checkPermissions(['payment_edit']),
  checkPaymentFeatureEnabled
], async (req, res) => {
  try {
    const qrCodeId = req.params.id;
    const db = req.app.locals.db;
    const eventBus = req.app.locals.eventBus;
    
    // Check if QR code exists
    const existingQrCode = await dbMethods.get(db, 'SELECT id, is_active FROM payment_qr_codes WHERE id = ?', [qrCodeId]);
    if (!existingQrCode) {
      return res.status(404).json({ error: 'QR code not found' });
    }
    
    // Check if QR code is already inactive
    if (existingQrCode.is_active === 0) {
      return res.status(400).json({ error: 'QR code is already inactive' });
    }
    
    // Deactivate the QR code
    await dbMethods.run(db, 'UPDATE payment_qr_codes SET is_active = 0 WHERE id = ?', [qrCodeId]);
    
    // Log activity
    eventBus.emit('log:activity', {
      user_id: req.user.user_id,
      action: 'QR_CODE_DEACTIVATED',
      details: `Deactivated QR code ID ${qrCodeId}`,
      ip_address: req.ip,
      user_agent: req.headers['user-agent']
    });
    
    return res.json({ message: 'QR code deactivated successfully' });
  } catch (error) {
    console.error(`Error deactivating QR code ${req.params.id}:`, error);
    return res.status(500).json({ error: 'Failed to deactivate QR code' });
  }
});

/**
 * @route GET /api/payment/transactions
 * @description Get all transactions
 * @access Private - Requires payment_view permission
 */
router.get('/transactions', [
  authenticateToken, 
  checkPermissions(['payment_view']),
  checkPaymentFeatureEnabled
], async (req, res) => {
  try {
    const db = req.app.locals.db;
    const eventBus = req.app.locals.eventBus;
    
    // Query parameters for filtering and pagination
    const { status, search, limit, page = 1 } = req.query;
    const pageSize = parseInt(limit) || 10;
    const offset = (parseInt(page) - 1) * pageSize;
    
    // Base SQL query
    let sql = `
      SELECT 
        t.id,
        t.transaction_reference,
        t.amount,
        t.currency,
        t.payment_status,
        t.qr_code_id,
        t.user_id,
        t.transaction_date,
        t.transaction_notes,
        t.transaction_metadata,
        q.payment_name as qr_code_name,
        u.first_name || ' ' || u.last_name as user_name
      FROM payment_transactions t
      LEFT JOIN payment_qr_codes q ON t.qr_code_id = q.id
      LEFT JOIN users_master u ON t.user_id = u.user_id
    `;
    
    // Build WHERE clause based on filters
    const whereConditions = [];
    const params = [];
    
    if (status) {
      whereConditions.push('t.payment_status = ?');
      params.push(status);
    }
    
    if (search) {
      whereConditions.push('(t.transaction_reference LIKE ? OR q.payment_name LIKE ?)');
      const searchPattern = `%${search}%`;
      params.push(searchPattern, searchPattern);
    }
    
    // Add WHERE clause if conditions exist
    if (whereConditions.length > 0) {
      sql += ' WHERE ' + whereConditions.join(' AND ');
    }
    
    // Count total transactions for pagination
    const countSql = `SELECT COUNT(*) as total FROM (${sql})`;
    const countResult = await dbMethods.get(db, countSql, params);
    const total = countResult.total;
    
    // Add ORDER BY and LIMIT for pagination
    sql += ' ORDER BY t.transaction_date DESC';
    sql += ` LIMIT ${pageSize} OFFSET ${offset}`;
    
    // Get transactions
    const transactions = await dbMethods.all(db, sql, params);
    
    // Log activity
    eventBus.emit('log:activity', {
      user_id: req.user.user_id,
      action: 'TRANSACTIONS_LIST_VIEW',
      details: 'Retrieved list of payment transactions',
      ip_address: req.ip,
      user_agent: req.headers['user-agent']
    });
    
    // Return transactions with pagination info
    return res.json({
      transactions,
      total,
      page: parseInt(page),
      limit: pageSize,
      pages: Math.ceil(total / pageSize)
    });
  } catch (error) {
    console.error('Error fetching transactions:', error);
    return res.status(500).json({ error: 'Failed to fetch transactions' });
  }
});

/**
 * @route GET /api/payment/qr-codes/:id/image
 * @description Get QR code image
 * @access Private - Requires payment_view permission
 */
router.get('/qr-codes/:id/image', [
  authenticateToken, 
  checkPermissions(['payment_view']),
  checkPaymentFeatureEnabled
], async (req, res) => {
  try {
    const qrCodeId = req.params.id;
    const db = req.app.locals.db;
    
    // Get QR code image data
    const qrCode = await dbMethods.get(db, 
      'SELECT qr_code_image, qr_code_path FROM payment_qr_codes WHERE id = ?', 
      [qrCodeId]
    );
    
    if (!qrCode) {
      return res.status(404).json({ error: 'QR code not found' });
    }
    
    // If qr_code_image exists in database (stored as BLOB)
    if (qrCode.qr_code_image) {
      res.set('Content-Type', 'image/png');
      return res.send(Buffer.from(qrCode.qr_code_image));
    } 
    // If we have a file path, try to serve that
    else if (qrCode.qr_code_path && fs.existsSync(qrCode.qr_code_path)) {
      return res.sendFile(qrCode.qr_code_path);
    } 
    // No image found
    else {
      return res.status(404).json({ error: 'QR code image not found' });
    }
  } catch (error) {
    console.error(`Error retrieving QR code image ${req.params.id}:`, error);
    return res.status(500).json({ error: 'Failed to retrieve QR code image' });
  }
});

/**
 * @route GET /api/payment/transactions/:id
 * @description Get a specific transaction
 * @access Private - Requires payment_view permission
 */
router.get('/transactions/:id', [
  authenticateToken, 
  checkPermissions(['payment_view']),
  checkPaymentFeatureEnabled
], async (req, res) => {
  try {
    const transactionId = req.params.id;
    const db = req.app.locals.db;
    const eventBus = req.app.locals.eventBus;
    
    // Get transaction
    const transaction = await dbMethods.get(db, 
      `SELECT 
        t.id,
        t.transaction_reference,
        t.amount,
        t.currency,
        t.payment_status,
        t.qr_code_id,
        t.user_id,
        t.transaction_date,
        t.transaction_notes,
        t.transaction_metadata,
        q.payment_name as qr_code_name,
        u.first_name || ' ' || u.last_name as user_name
      FROM payment_transactions t
      LEFT JOIN payment_qr_codes q ON t.qr_code_id = q.id
      LEFT JOIN users_master u ON t.user_id = u.user_id
      WHERE t.id = ?`, 
      [transactionId]
    );
    
    if (!transaction) {
      return res.status(404).json({ error: 'Transaction not found' });
    }
    
    // Log activity
    eventBus.emit('log:activity', {
      user_id: req.user.user_id,
      action: 'TRANSACTION_VIEW',
      details: `Viewed transaction ID ${transactionId}`,
      ip_address: req.ip,
      user_agent: req.headers['user-agent']
    });
    
    return res.json(transaction);
  } catch (error) {
    console.error(`Error fetching transaction ${req.params.id}:`, error);
    return res.status(500).json({ error: 'Failed to fetch transaction' });
  }
});

/**
 * @route POST /api/payment/transactions
 * @description Create a new transaction
 * @access Private - Requires payment_create permission
 */
router.post('/transactions', [
  authenticateToken, 
  checkPermissions(['payment_create']),
  checkPaymentFeatureEnabled,
  body('transaction_reference').notEmpty().withMessage('Transaction reference is required'),
  body('amount').isFloat({ min: 0.01 }).withMessage('Valid amount is required'),
  body('payment_status').notEmpty().withMessage('Payment status is required')
], async (req, res) => {
  try {
    // Validate request
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({ errors: errors.array() });
    }
    
    const { 
      transaction_reference,
      amount,
      currency = 'INR',
      payment_status,
      qr_code_id,
      transaction_notes,
      transaction_metadata
    } = req.body;
    
    const db = req.app.locals.db;
    const eventBus = req.app.locals.eventBus;
    
    // Verify QR code exists if provided
    if (qr_code_id) {
      const qrCode = await dbMethods.get(db, 'SELECT id FROM payment_qr_codes WHERE id = ?', [qr_code_id]);
      if (!qrCode) {
        return res.status(404).json({ error: 'QR code not found' });
      }
    }
    
    // Check for duplicate transaction reference
    const existingTransaction = await dbMethods.get(db, 
      'SELECT id FROM payment_transactions WHERE transaction_reference = ?', 
      [transaction_reference]
    );
    
    if (existingTransaction) {
      return res.status(409).json({ error: 'Transaction reference already exists' });
    }
    
    // Create new transaction
    const result = await dbMethods.run(db, 
      `INSERT INTO payment_transactions (
        transaction_reference,
        amount,
        currency,
        payment_status,
        qr_code_id,
        user_id,
        transaction_notes,
        transaction_metadata
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, 
      [
        transaction_reference,
        amount,
        currency,
        payment_status,
        qr_code_id || null,
        req.user.user_id,
        transaction_notes || null,
        transaction_metadata || null
      ]
    );
    
    const newTransactionId = result.lastID;
    
    // Log activity
    eventBus.emit('log:activity', {
      user_id: req.user.user_id,
      action: 'TRANSACTION_CREATED',
      details: `Created new transaction: ${transaction_reference}`,
      ip_address: req.ip,
      user_agent: req.headers['user-agent']
    });
    
    return res.status(201).json({ 
      message: 'Transaction created successfully',
      transaction_id: newTransactionId
    });
  } catch (error) {
    console.error('Error creating transaction:', error);
    return res.status(500).json({ error: 'Failed to create transaction' });
  }
});

module.exports = router;
module.exports.init = init;
