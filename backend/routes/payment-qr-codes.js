const express = require('express');
const router = express.Router();
const multer = require('multer');
const path = require('path');
const fs = require('fs');
//const { authenticateToken, checkPermission } = require('../../modules/authentication/backend/middleware');
const { authenticateToken, checkPermission } = require('../../middleware/auth');
// Set up storage for QR code images
const storage = multer.diskStorage({
  destination: function (req, file, cb) {
    // Store in backend/uploads/qr-codes to match the static middleware in app.js
    const uploadDir = path.join(__dirname, '..', 'uploads', 'qr-codes');
    
    // Create directory if it doesn't exist
    if (!fs.existsSync(uploadDir)) {
      fs.mkdirSync(uploadDir, { recursive: true });
    }
    
    cb(null, uploadDir);
  },
  filename: function (req, file, cb) {
    const uniqueSuffix = Date.now() + '-' + Math.round(Math.random() * 1E9);
    const ext = path.extname(file.originalname);
    cb(null, 'qrcode-' + uniqueSuffix + ext);
  }
});

// Set up upload middleware with file filtering
const upload = multer({
  storage: storage,
  limits: {
    fileSize: 2 * 1024 * 1024 // 2MB limit
  },
  fileFilter: (req, file, cb) => {
    // Only accept images
    if (!file) {
      cb(new Error('No file provided'), false);
    } else if (!file.mimetype.startsWith('image/')) {
      cb(new Error('Only image files are allowed!'), false);
    } else if (!['.jpg', '.jpeg', '.png'].includes(
      path.extname(file.originalname).toLowerCase()
    )) {
      cb(new Error('Only JPG, JPEG and PNG files are allowed!'), false);
    } else {
      cb(null, true);
    }
  }
});

/**
 * @route   GET /api/payment/qr-codes
 * @desc    Get all QR codes
 * @access  Private (requires authentication and payment_view permission)
 */
router.get('/', authenticateToken, checkPermission('payment_view'), (req, res) => {
  try {
    const db = req.app.locals.db;
    const sql = `
      SELECT * FROM payment_qr_codes
      ORDER BY created_at DESC
    `;
    
    db.all(sql, [], (err, rows) => {
      if (err) {
        console.error('Database error:', err);
        return res.status(500).json({ error: 'Database error' });
      }
      
      const qrCodes = rows.map(row => ({
        id: row.id,
        name: row.name,
        description: row.description,
        paymentType: row.payment_type,
        imageUrl: row.image_url,
        active: Boolean(row.active),
        createdAt: row.created_at,
        updatedAt: row.updated_at
      }));
      
      return res.json(qrCodes);
    });
  } catch (error) {
    console.error('Server error:', error);
    return res.status(500).json({ error: 'Server error' });
  }
});

/**
 * @route   GET /api/payment/qr-codes/:id
 * @desc    Get a specific QR code by ID
 * @access  Private (requires authentication and payment_view permission)
 */
router.get('/:id', authenticateToken, checkPermission('payment_view'), (req, res) => {
  try {
    const db = req.app.locals.db;
    const { id } = req.params;
    
    const sql = 'SELECT * FROM payment_qr_codes WHERE id = ?';
    
    db.get(sql, [id], (err, row) => {
      if (err) {
        console.error('Database error:', err);
        return res.status(500).json({ error: 'Database error' });
      }
      
      if (!row) {
        return res.status(404).json({ error: 'QR code not found' });
      }
      
      const qrCode = {
        id: row.id,
        name: row.name,
        description: row.description,
        paymentType: row.payment_type,
        imageUrl: row.image_url,
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

// Error handling middleware for multer
const handleMulterError = (err, req, res, next) => {
  if (err instanceof multer.MulterError) {
    // A Multer error occurred when uploading
    console.error('Multer error:', err);
    if (err.code === 'LIMIT_FILE_SIZE') {
      return res.status(400).json({ error: 'File is too large. Maximum size is 2MB.' });
    }
    return res.status(400).json({ error: `Upload error: ${err.message}` });
  } else if (err) {
    // An unknown error occurred
    console.error('Unknown upload error:', err);
    return res.status(500).json({ error: err.message || 'An unknown error occurred during upload' });
  }
  next();
};

/**
 * @route   POST /api/payment/qr-codes
 * @desc    Upload a new QR code
 * @access  Private (requires authentication and payment_edit permission)
 */
router.post('/', authenticateToken, checkPermission('payment_edit'), upload.single('qr_code_image'), handleMulterError, (req, res) => {
  try {
    const db = req.app.locals.db;
    const { payment_name, payment_description, payment_type } = req.body;
    
    // Validate required fields
    if (!payment_name || !payment_type) {
      return res.status(400).json({ error: 'QR code name and payment type are required' });
    }
    
    // Check if file was uploaded
    if (!req.file) {
      return res.status(400).json({ error: 'QR code image is required' });
    }
    
    // Get the relative path to the uploaded image - ensure it matches the Express static serving path
    const imageUrl = `/uploads/qr-codes/${path.basename(req.file.path)}`;
    
    // Get the current timestamp
    const timestamp = new Date().toISOString();
    
    // Insert into database
    const sql = `
      INSERT INTO payment_qr_codes 
      (name, description, payment_type, image_url, active, created_at, updated_at) 
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `;
    
    db.run(
      sql, 
      [payment_name, payment_description || '', payment_type, imageUrl, 0, timestamp, timestamp], 
      function(err) {
        if (err) {
          console.error('Error creating QR code:', err);
          return res.status(500).json({ error: 'Could not create QR code' });
        }
        
        const qrCodeId = this.lastID;
        
        return res.status(201).json({
          id: qrCodeId,
          name: payment_name,
          description: payment_description || '',
          paymentType: payment_type,
          imageUrl: imageUrl,
          active: false,
          createdAt: timestamp,
          updatedAt: timestamp
        });
      }
    );
  } catch (error) {
    console.error('Server error:', error);
    return res.status(500).json({ error: 'Server error' });
  }
});

/**
 * @route   POST /api/payment/qr-codes/:id/activate
 * @desc    Activate a QR code (and deactivate all others)
 * @access  Private (requires authentication and payment_edit permission)
 */
router.post('/:id/activate', authenticateToken, checkPermission('payment_edit'), (req, res) => {
  try {
    const db = req.app.locals.db;
    const { id } = req.params;
    const timestamp = new Date().toISOString();
    
    // Start transaction
    db.serialize(() => {
      // Begin transaction
      db.run('BEGIN TRANSACTION');
      
      // First deactivate all QR codes
      db.run('UPDATE payment_qr_codes SET active = 0, updated_at = ?', [timestamp], (err) => {
        if (err) {
          console.error('Error deactivating QR codes:', err);
          db.run('ROLLBACK');
          return res.status(500).json({ error: 'Database error' });
        }
        
        // Then activate the specific QR code
        db.run(
          'UPDATE payment_qr_codes SET active = 1, updated_at = ? WHERE id = ?', 
          [timestamp, id], 
          function(err) {
            if (err) {
              console.error('Error activating QR code:', err);
              db.run('ROLLBACK');
              return res.status(500).json({ error: 'Database error' });
            }
            
            if (this.changes === 0) {
              db.run('ROLLBACK');
              return res.status(404).json({ error: 'QR code not found' });
            }
            
            // Commit the transaction
            db.run('COMMIT');
            
            return res.json({
              id: parseInt(id),
              active: true,
              updatedAt: timestamp,
              message: 'QR code activated successfully'
            });
          }
        );
      });
    });
  } catch (error) {
    console.error('Server error:', error);
    return res.status(500).json({ error: 'Server error' });
  }
});

/**
 * @route   DELETE /api/payment/qr-codes/:id
 * @desc    Delete a QR code
 * @access  Private (requires authentication and payment_edit permission)
 */
router.delete('/:id', authenticateToken, checkPermission('payment_edit'), (req, res) => {
  try {
    const db = req.app.locals.db;
    const { id } = req.params;
    
    // Get the QR code details first to delete the image file
    db.get('SELECT image_url FROM payment_qr_codes WHERE id = ?', [id], (err, row) => {
      if (err) {
        console.error('Database error:', err);
        return res.status(500).json({ error: 'Database error' });
      }
      
      if (!row) {
        return res.status(404).json({ error: 'QR code not found' });
      }
      
      // Delete the record from the database
      db.run('DELETE FROM payment_qr_codes WHERE id = ?', [id], function(err) {
        if (err) {
          console.error('Error deleting QR code:', err);
          return res.status(500).json({ error: 'Could not delete QR code' });
        }
        
        if (this.changes === 0) {
          return res.status(404).json({ error: 'QR code not found' });
        }
        
        // Try to delete the image file if it exists
        if (row.image_url) {
          try {
            const imagePath = path.join(__dirname, '..', row.image_url);
            if (fs.existsSync(imagePath)) {
              fs.unlinkSync(imagePath);
            }
          } catch (fileError) {
            console.error('Error deleting QR code image file:', fileError);
            // Continue even if file deletion fails
          }
        }
        
        return res.json({
          message: 'QR code deleted successfully',
          id: parseInt(id)
        });
      });
    });
  } catch (error) {
    console.error('Server error:', error);
    return res.status(500).json({ error: 'Server error' });
  }
});

module.exports = router;
