import React, { useState, useEffect } from 'react';
import {
  Container,
  Paper,
  Typography,
  Box,
  Button,
  TextField,
  Grid,
  Divider,
  Alert,
  CircularProgress,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions
} from '@mui/material';
import { useAuth } from '../../contexts/AuthContext';
import axios from 'axios';

const PaymentPage = () => {
  const { token, currentUser } = useAuth();
  const [activeQrCode, setActiveQrCode] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [transactionRef, setTransactionRef] = useState('');
  const [transactionDialogOpen, setTransactionDialogOpen] = useState(false);
  const [submitLoading, setSubmitLoading] = useState(false);
  const [submitSuccess, setSubmitSuccess] = useState(false);
  const [submitError, setSubmitError] = useState(null);

  // Fetch active QR code on component mount
  useEffect(() => {
    const fetchActiveQrCode = async () => {
      setLoading(true);
      setError(null);
      
      try {
        const response = await axios.get('/api/payment/qr-codes/active', {
          headers: {
            'Authorization': `Bearer ${token}`
          }
        });
        
        setActiveQrCode(response.data);
      } catch (err) {
        console.error('Error fetching active QR code:', err);
        setError(
          err.response?.data?.error || 
          'Failed to fetch payment QR code. Please try again later.'
        );
      } finally {
        setLoading(false);
      }
    };

    fetchActiveQrCode();
  }, [token]);

  const handleOpenTransactionDialog = () => {
    setTransactionDialogOpen(true);
    setTransactionRef('');
    setSubmitSuccess(false);
    setSubmitError(null);
  };

  const handleCloseTransactionDialog = () => {
    setTransactionDialogOpen(false);
  };

  const handleSubmitTransaction = async () => {
    if (!transactionRef.trim()) {
      setSubmitError('Transaction reference is required');
      return;
    }

    setSubmitLoading(true);
    setSubmitError(null);

    try {
      await axios.post(
        '/api/payment/transactions',
        {
          qrCodeId: activeQrCode.id,
          transactionRef,
          userId: currentUser.id
        },
        {
          headers: {
            'Authorization': `Bearer ${token}`
          }
        }
      );

      setSubmitSuccess(true);
    } catch (err) {
      console.error('Error submitting transaction:', err);
      setSubmitError(
        err.response?.data?.error || 
        'Failed to record transaction. Please try again.'
      );
    } finally {
      setSubmitLoading(false);
    }
  };

  if (loading) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" minHeight="80vh">
        <CircularProgress />
      </Box>
    );
  }

  if (error) {
    return (
      <Container maxWidth="md" sx={{ mt: 4 }}>
        <Alert severity="error">{error}</Alert>
      </Container>
    );
  }

  if (!activeQrCode) {
    return (
      <Container maxWidth="md" sx={{ mt: 4 }}>
        <Paper sx={{ p: 3 }}>
          <Typography variant="h5" component="h1" gutterBottom>
            Payment
          </Typography>
          <Alert severity="info">
            No payment method is currently available. Please contact your administrator.
          </Alert>
        </Paper>
      </Container>
    );
  }

  return (
    <Container maxWidth="md" sx={{ mt: 4 }}>
      <Paper sx={{ p: 3 }}>
        <Typography variant="h5" component="h1" gutterBottom>
          Make a Payment
        </Typography>
        <Divider sx={{ my: 2 }} />
        
        <Grid container spacing={3}>
          <Grid item xs={12} md={6} sx={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
            <Typography variant="subtitle1" gutterBottom>
              Scan this QR code to make a payment:
            </Typography>
            <Box 
              component="img" 
              src={activeQrCode.imageUrl} 
              alt="Payment QR Code" 
              sx={{ 
                maxWidth: '100%', 
                maxHeight: 300, 
                border: '1px solid #ddd', 
                borderRadius: 1,
                p: 1,
                mb: 2
              }} 
            />
            <Typography variant="body2" color="text.secondary" align="center">
              {activeQrCode.description}
            </Typography>
          </Grid>
          
          <Grid item xs={12} md={6}>
            <Box sx={{ p: 2, border: '1px solid #eee', borderRadius: 1 }}>
              <Typography variant="h6" gutterBottom>
                Payment Instructions
              </Typography>
              <Typography variant="body2" paragraph>
                1. Scan the QR code with your payment app
              </Typography>
              <Typography variant="body2" paragraph>
                2. Complete the payment for the required amount
              </Typography>
              <Typography variant="body2" paragraph>
                3. Note down the transaction reference number
              </Typography>
              <Typography variant="body2" paragraph>
                4. Submit the reference number to record your payment
              </Typography>
              
              <Button 
                variant="contained" 
                color="primary"
                fullWidth
                sx={{ mt: 2 }}
                onClick={handleOpenTransactionDialog}
              >
                I've Made the Payment
              </Button>
            </Box>
          </Grid>
        </Grid>
      </Paper>

      {/* Transaction Reference Dialog */}
      <Dialog open={transactionDialogOpen} onClose={handleCloseTransactionDialog} maxWidth="sm" fullWidth>
        <DialogTitle>
          {submitSuccess ? "Payment Recorded" : "Enter Transaction Reference"}
        </DialogTitle>
        <DialogContent>
          {submitSuccess ? (
            <Alert severity="success" sx={{ mt: 2 }}>
              Your payment has been successfully recorded. Thank you!
            </Alert>
          ) : (
            <>
              <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
                Please enter the transaction reference number from your payment app:
              </Typography>
              <TextField
                label="Transaction Reference"
                fullWidth
                value={transactionRef}
                onChange={(e) => setTransactionRef(e.target.value)}
                disabled={submitLoading}
                error={!!submitError}
                helperText={submitError}
                margin="normal"
                autoFocus
              />
            </>
          )}
        </DialogContent>
        <DialogActions>
          {submitSuccess ? (
            <Button onClick={handleCloseTransactionDialog}>Close</Button>
          ) : (
            <>
              <Button onClick={handleCloseTransactionDialog} disabled={submitLoading}>
                Cancel
              </Button>
              <Button 
                onClick={handleSubmitTransaction} 
                color="primary" 
                variant="contained"
                disabled={submitLoading}
                startIcon={submitLoading ? <CircularProgress size={20} /> : null}
              >
                Submit
              </Button>
            </>
          )}
        </DialogActions>
      </Dialog>
    </Container>
  );
};

export default PaymentPage;
