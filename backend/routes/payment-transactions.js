const express = require('express');
const router = express.Router();
// const db = require('../database');
const { authenticateToken, checkPermission } = require('../../middleware/auth');

/**
 * @route   POST /api/payment/transactions
 * @desc    Create a new payment transaction record
 * @access  Private (requires authentication)
 */
router.post('/', authenticateToken, (req, res) => {
  try {
    const { qrCodeId, transactionRef, userId } = req.body;
    
    if (!qrCodeId || !transactionRef || !userId) {
      return res.status(400).json({ 
        error: 'Required fields missing: qrCodeId, transactionRef, userId' 
      });
    }
    
    // Get the current timestamp
    const timestamp = new Date().toISOString();
    
    // Check if QR code exists
    db.get('SELECT * FROM payment_qr_codes WHERE id = ?', [qrCodeId], (err, qrCode) => {
      if (err) {
        console.error('Database error:', err);
        return res.status(500).json({ error: 'Database error' });
      }
      
      if (!qrCode) {
        return res.status(404).json({ error: 'QR Code not found' });
      }
      
      // Check if user exists
      db.get('SELECT * FROM users WHERE id = ?', [userId], (err, user) => {
        if (err) {
          console.error('Database error:', err);
          return res.status(500).json({ error: 'Database error' });
        }
        
        if (!user) {
          return res.status(404).json({ error: 'User not found' });
        }
        
        // Create transaction in database
        const sql = `
          INSERT INTO payment_transactions 
          (qr_code_id, transaction_ref, user_id, verified, created_at) 
          VALUES (?, ?, ?, 0, ?)
        `;
        
        db.run(sql, [qrCodeId, transactionRef, userId, timestamp], function(err) {
          if (err) {
            console.error('Error creating transaction:', err);
            return res.status(500).json({ error: 'Could not create transaction record' });
          }
          
          const transactionId = this.lastID;
          
          return res.status(201).json({
            id: transactionId,
            qrCodeId,
            transactionRef,
            userId,
            verified: 0,
            createdAt: timestamp
          });
        });
      });
    });
  } catch (error) {
    console.error('Server error:', error);
    return res.status(500).json({ error: 'Server error' });
  }
});

/**
 * @route   GET /api/payment/transactions
 * @desc    Get all payment transactions (with pagination and filtering)
 * @access  Private (requires authentication and payment_view permission)
 */
router.get('/', authenticateToken, checkPermission('payment_view'), (req, res) => {
  try {
    const { search = '', page = 0, limit = 10 } = req.query;
    const offset = page * limit;
    
    // Base query for count
    let countSql = `
      SELECT COUNT(*) as total 
      FROM payment_transactions
    `;
    
    // Base query for data
    let dataSql = `
      SELECT 
        pt.id,
        pt.transaction_ref,
        pt.qr_code_id,
        pt.user_id,
        pt.verified,
        pt.created_at,
        pt.updated_at,
        u.first_name,
        u.last_name,
        u.email,
        qr.name AS qr_name,
        qr.description AS qr_description,
        qr.amount AS qr_amount
      FROM payment_transactions pt
      LEFT JOIN users u ON pt.user_id = u.id
      LEFT JOIN payment_qr_codes qr ON pt.qr_code_id = qr.id
    `;
    
    // Add search condition if provided
    if (search) {
      const searchParam = `%${search}%`;
      
      countSql += `
        WHERE pt.transaction_ref LIKE ?
        OR u.first_name LIKE ?
        OR u.last_name LIKE ?
        OR u.email LIKE ?
      `;
      
      dataSql += `
        WHERE pt.transaction_ref LIKE ?
        OR u.first_name LIKE ?
        OR u.last_name LIKE ?
        OR u.email LIKE ?
      `;
    }
    
    // Add ordering and pagination to data query
    dataSql += `
      ORDER BY pt.created_at DESC
      LIMIT ? OFFSET ?
    `;
    
    // Execute count query
    db.get(countSql, search ? [searchParam, searchParam, searchParam, searchParam] : [], (err, countResult) => {
      if (err) {
        console.error('Database error:', err);
        return res.status(500).json({ error: 'Database error' });
      }
      
      const totalCount = countResult ? countResult.total : 0;
      
      // Prepare parameters for data query
      let params = [];
      if (search) {
        params = [searchParam, searchParam, searchParam, searchParam];
      }
      params.push(parseInt(limit), parseInt(offset));
      
      // Execute data query
      db.all(dataSql, params, (err, rows) => {
        if (err) {
          console.error('Database error:', err);
          return res.status(500).json({ error: 'Database error' });
        }
        
        // Process the rows to format the response
        const transactions = rows.map(row => ({
          id: row.id,
          transactionRef: row.transaction_ref,
          qrCodeId: row.qr_code_id,
          userId: row.user_id,
          verified: Boolean(row.verified),
          createdAt: row.created_at,
          updatedAt: row.updated_at,
          user: {
            id: row.user_id,
            firstName: row.first_name,
            lastName: row.last_name,
            email: row.email
          },
          qrCode: {
            id: row.qr_code_id,
            name: row.qr_name,
            description: row.qr_description,
            amount: row.qr_amount
          }
        }));
        
        return res.json({
          transactions,
          totalCount,
          page: parseInt(page),
          limit: parseInt(limit)
        });
      });
    });
  } catch (error) {
    console.error('Server error:', error);
    return res.status(500).json({ error: 'Server error' });
  }
});

/**
 * @route   GET /api/payment/transactions/:id
 * @desc    Get a specific transaction by ID
 * @access  Private (requires authentication and payment_view permission)
 */
router.get('/:id', authenticateToken, checkPermission('payment_view'), (req, res) => {
  try {
    const { id } = req.params;
    
    const sql = `
      SELECT 
        pt.id,
        pt.transaction_ref,
        pt.qr_code_id,
        pt.user_id,
        pt.verified,
        pt.created_at,
        pt.updated_at,
        u.first_name,
        u.last_name,
        u.email,
        qr.name AS qr_name,
        qr.description AS qr_description,
        qr.amount AS qr_amount
      FROM payment_transactions pt
      LEFT JOIN users u ON pt.user_id = u.id
      LEFT JOIN payment_qr_codes qr ON pt.qr_code_id = qr.id
      WHERE pt.id = ?
    `;
    
    db.get(sql, [id], (err, row) => {
      if (err) {
        console.error('Database error:', err);
        return res.status(500).json({ error: 'Database error' });
      }
      
      if (!row) {
        return res.status(404).json({ error: 'Transaction not found' });
      }
      
      const transaction = {
        id: row.id,
        transactionRef: row.transaction_ref,
        qrCodeId: row.qr_code_id,
        userId: row.user_id,
        verified: Boolean(row.verified),
        createdAt: row.created_at,
        updatedAt: row.updated_at,
        user: {
          id: row.user_id,
          firstName: row.first_name,
          lastName: row.last_name,
          email: row.email
        },
        qrCode: {
          id: row.qr_code_id,
          name: row.qr_name,
          description: row.qr_description,
          amount: row.qr_amount
        }
      };
      
      return res.json(transaction);
    });
  } catch (error) {
    console.error('Server error:', error);
    return res.status(500).json({ error: 'Server error' });
  }
});

/**
 * @route   GET /api/payment/qr-codes/active
 * @desc    Get the currently active QR code for payments
 * @access  Private (requires authentication)
 */
router.get('/qr-codes/active', authenticateToken, (req, res) => {
  try {
    const sql = `
      SELECT * FROM payment_qr_codes
      WHERE active = 1
      ORDER BY updated_at DESC
      LIMIT 1
    `;
    
    db.get(sql, [], (err, row) => {
      if (err) {
        console.error('Database error:', err);
        return res.status(500).json({ error: 'Database error' });
      }
      
      if (!row) {
        return res.status(404).json({ error: 'No active QR code found' });
      }
      
      const qrCode = {
        id: row.id,
        name: row.name,
        description: row.description,
        imageUrl: row.image_url,
        amount: row.amount,
        active: Boolean(row.active),
        createdAt: row.created_at,
        updatedAt: row.updated_at
      };
      
      return res.json(qrCode);
    });
  } catch (error) {
    console.error('Server error:', error);
    return res.status(500).json({ error: 'Server error' });
  }
});

/**
 * @route   PUT /api/payment/transactions/:id/verify
 * @desc    Verify a transaction
 * @access  Private (requires authentication and payment_edit permission)
 */
router.put('/:id/verify', authenticateToken, checkPermission('payment_edit'), (req, res) => {
  try {
    const { id } = req.params;
    const timestamp = new Date().toISOString();
    
    // Update the transaction verification status
    const sql = `
      UPDATE payment_transactions
      SET verified = 1, updated_at = ?
      WHERE id = ?
    `;
    
    db.run(sql, [timestamp, id], function(err) {
      if (err) {
        console.error('Database error:', err);
        return res.status(500).json({ error: 'Database error' });
      }
      
      if (this.changes === 0) {
        return res.status(404).json({ error: 'Transaction not found or already verified' });
      }
      
      return res.json({
        id: parseInt(id),
        verified: true,
        updatedAt: timestamp,
        message: 'Transaction verified successfully'
      });
    });
  } catch (error) {
    console.error('Server error:', error);
    return res.status(500).json({ error: 'Server error' });
  }
});

module.exports = router;
