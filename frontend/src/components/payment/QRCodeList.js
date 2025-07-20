import React, { useState, useEffect } from 'react';
import {
  Box,
  Paper,
  Typography,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Button,
  IconButton,
  Chip,
  Dialog,
  DialogActions,
  DialogContent,
  DialogContentText,
  DialogTitle,
  CircularProgress,
  Alert,
  Tooltip
} from '@mui/material';
import {
  Edit as EditIcon,
  Delete as DeleteIcon,
  CheckCircle as CheckCircleIcon,
  Cancel as CancelIcon,
  Visibility as VisibilityIcon
} from '@mui/icons-material';
import { paymentAPI } from '../../services/api';
import { useAuth } from '../../contexts/AuthContext';
import { format } from 'date-fns';

const QRCodeList = ({ onEdit, onRefreshNeeded, refreshTrigger }) => {
  const [qrCodes, setQrCodes] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [selectedQrCode, setSelectedQrCode] = useState(null);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [activateDialogOpen, setActivateDialogOpen] = useState(false);
  const [previewDialogOpen, setPreviewDialogOpen] = useState(false);
  const [actionLoading, setActionLoading] = useState(false);
  const [actionMessage, setActionMessage] = useState({ text: '', type: '' });

  const { token } = useAuth();

  // Fetch QR codes from the API
  const fetchQrCodes = async () => {
    setLoading(true);
    setError(null);
    
    try {
      const response = await paymentAPI.getQrCodes();
      
      setQrCodes(response.data);
    } catch (err) {
      console.error('Error fetching QR codes:', err);
      setError(
        err.response?.data?.error || 
        'Failed to fetch QR codes. Please try again.'
      );
    } finally {
      setLoading(false);
    }
  };

  // Initial load and refresh when triggered
  useEffect(() => {
    fetchQrCodes();
  }, [refreshTrigger]);

  // Handle QR code deletion
  const handleDelete = async () => {
    if (!selectedQrCode) return;
    
    setActionLoading(true);
    
    try {
      await paymentAPI.deleteQrCode(selectedQrCode.id);
      
      // Remove the deleted QR code from the list
      setQrCodes(prevCodes => prevCodes.filter(code => code.id !== selectedQrCode.id));
      
      setActionMessage({
        text: 'QR code deleted successfully',
        type: 'success'
      });
      
      // Notify parent component if needed
      if (onRefreshNeeded && typeof onRefreshNeeded === 'function') {
        onRefreshNeeded();
      }
      
    } catch (err) {
      console.error('Error deleting QR code:', err);
      setActionMessage({
        text: err.response?.data?.error || 'Failed to delete QR code',
        type: 'error'
      });
    } finally {
      setActionLoading(false);
      setDeleteDialogOpen(false);
      setSelectedQrCode(null);
      
      // Clear action message after 3 seconds
      setTimeout(() => {
        setActionMessage({ text: '', type: '' });
      }, 3000);
    }
  };

  // Handle QR code activation
  const handleActivate = async () => {
    if (!selectedQrCode) return;
    
    setActionLoading(true);
    
    try {
      await paymentAPI.activateQrCode(selectedQrCode.id);
      
      // Update QR codes list to reflect the change
      setQrCodes(prevCodes => 
        prevCodes.map(code => ({
          ...code,
          is_active: code.id === selectedQrCode.id ? 1 : 0
        }))
      );
      
      setActionMessage({
        text: 'QR code activated successfully',
        type: 'success'
      });
      
      // Notify parent component if needed
      if (onRefreshNeeded && typeof onRefreshNeeded === 'function') {
        onRefreshNeeded();
      }
      
    } catch (err) {
      console.error('Error activating QR code:', err);
      setActionMessage({
        text: err.response?.data?.error || 'Failed to activate QR code',
        type: 'error'
      });
    } finally {
      setActionLoading(false);
      setActivateDialogOpen(false);
      setSelectedQrCode(null);
      
      // Clear action message after 3 seconds
      setTimeout(() => {
        setActionMessage({ text: '', type: '' });
      }, 3000);
    }
  };

  return (
    <Paper elevation={3} sx={{ p: 3 }}>
      <Typography variant="h6" component="h2" gutterBottom>
        Manage QR Codes
      </Typography>
      
      {actionMessage.text && (
        <Alert severity={actionMessage.type} sx={{ mb: 2 }}>
          {actionMessage.text}
        </Alert>
      )}
      
      {loading ? (
        <Box sx={{ display: 'flex', justifyContent: 'center', p: 3 }}>
          <CircularProgress />
        </Box>
      ) : error ? (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      ) : qrCodes.length === 0 ? (
        <Alert severity="info" sx={{ mb: 2 }}>
          No QR codes found. Use the upload form to add a QR code.
        </Alert>
      ) : (
        <TableContainer>
          <Table>
            <TableHead>
              <TableRow>
                <TableCell>Name</TableCell>
                <TableCell>Type</TableCell>
                <TableCell>Status</TableCell>
                <TableCell>Created At</TableCell>
                <TableCell align="center">Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {qrCodes.map((qrCode) => (
                <TableRow key={qrCode.id}>
                  <TableCell>{qrCode.payment_name}</TableCell>
                  <TableCell>{qrCode.payment_type}</TableCell>
                  <TableCell>
                    {qrCode.is_active === 1 ? (
                      <Chip 
                        label="Active" 
                        color="success" 
                        size="small" 
                        icon={<CheckCircleIcon />} 
                      />
                    ) : (
                      <Chip 
                        label="Inactive" 
                        color="default" 
                        size="small" 
                        icon={<CancelIcon />} 
                      />
                    )}
                  </TableCell>
                  <TableCell>
                    {format(new Date(qrCode.created_at), 'dd MMM yyyy')}
                  </TableCell>
                  <TableCell align="center">
                    <Box sx={{ display: 'flex', justifyContent: 'center' }}>
                      <Tooltip title="Preview QR Code">
                        <IconButton
                          color="primary"
                          onClick={() => {
                            setSelectedQrCode(qrCode);
                            setPreviewDialogOpen(true);
                          }}
                        >
                          <VisibilityIcon />
                        </IconButton>
                      </Tooltip>
                      
                      <Tooltip title="Edit QR Code">
                        <IconButton
                          color="primary"
                          onClick={() => {
                            if (onEdit && typeof onEdit === 'function') {
                              onEdit(qrCode);
                            }
                          }}
                        >
                          <EditIcon />
                        </IconButton>
                      </Tooltip>
                      
                      {qrCode.is_active === 0 && (
                        <Tooltip title="Activate QR Code">
                          <IconButton
                            color="success"
                            onClick={() => {
                              setSelectedQrCode(qrCode);
                              setActivateDialogOpen(true);
                            }}
                          >
                            <CheckCircleIcon />
                          </IconButton>
                        </Tooltip>
                      )}
                      
                      <Tooltip title="Delete QR Code">
                        <IconButton
                          color="error"
                          onClick={() => {
                            setSelectedQrCode(qrCode);
                            setDeleteDialogOpen(true);
                          }}
                        >
                          <DeleteIcon />
                        </IconButton>
                      </Tooltip>
                    </Box>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      )}
      
      {/* Delete Confirmation Dialog */}
      <Dialog
        open={deleteDialogOpen}
        onClose={() => setDeleteDialogOpen(false)}
      >
        <DialogTitle>Confirm Deletion</DialogTitle>
        <DialogContent>
          <DialogContentText>
            Are you sure you want to delete the QR code "{selectedQrCode?.payment_name}"? 
            This action cannot be undone.
          </DialogContentText>
        </DialogContent>
        <DialogActions>
          <Button 
            onClick={() => setDeleteDialogOpen(false)} 
            disabled={actionLoading}
          >
            Cancel
          </Button>
          <Button 
            onClick={handleDelete} 
            color="error" 
            autoFocus
            disabled={actionLoading}
          >
            {actionLoading ? <CircularProgress size={24} /> : 'Delete'}
          </Button>
        </DialogActions>
      </Dialog>
      
      {/* Activate Confirmation Dialog */}
      <Dialog
        open={activateDialogOpen}
        onClose={() => setActivateDialogOpen(false)}
      >
        <DialogTitle>Confirm Activation</DialogTitle>
        <DialogContent>
          <DialogContentText>
            Are you sure you want to activate the QR code "{selectedQrCode?.payment_name}"? 
            This will deactivate any currently active QR code.
          </DialogContentText>
        </DialogContent>
        <DialogActions>
          <Button 
            onClick={() => setActivateDialogOpen(false)} 
            disabled={actionLoading}
          >
            Cancel
          </Button>
          <Button 
            onClick={handleActivate} 
            color="success" 
            autoFocus
            disabled={actionLoading}
          >
            {actionLoading ? <CircularProgress size={24} /> : 'Activate'}
          </Button>
        </DialogActions>
      </Dialog>
      
      {/* QR Code Preview Dialog */}
      <Dialog
        open={previewDialogOpen}
        onClose={() => setPreviewDialogOpen(false)}
      >
        <DialogTitle>QR Code Preview</DialogTitle>
        <DialogContent>
          <Box sx={{ textAlign: 'center', p: 2 }}>
            <Typography variant="h6" gutterBottom>
              {selectedQrCode?.payment_name}
            </Typography>
            
            <Typography variant="body2" color="textSecondary" gutterBottom>
              {selectedQrCode?.payment_description}
            </Typography>
            
            <Box sx={{ my: 2 }}>
              <img
                src={selectedQrCode ? `/api/payment/qr-codes/${selectedQrCode.id}/image` : ''}
                alt={`QR Code for ${selectedQrCode?.payment_name}`}
                style={{ maxWidth: '100%', maxHeight: 300 }}
                onError={(e) => {
                  e.target.src = '/images/qr-placeholder.png';
                  e.target.alt = 'QR Code image not available';
                }}
              />
            </Box>
            
            <Chip 
              label={`Type: ${selectedQrCode?.payment_type}`}
              variant="outlined"
              sx={{ mr: 1 }}
            />
            
            <Chip 
              label={selectedQrCode?.is_active === 1 ? 'Active' : 'Inactive'}
              color={selectedQrCode?.is_active === 1 ? 'success' : 'default'}
            />
          </Box>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setPreviewDialogOpen(false)}>Close</Button>
        </DialogActions>
      </Dialog>
    </Paper>
  );
};

export default QRCodeList;
