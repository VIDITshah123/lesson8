import React, { useState, useEffect } from 'react';
import {
  Box,
  Paper,
  Typography,
  Switch,
  FormControlLabel,
  CircularProgress,
  Alert
} from '@mui/material';
import axios from 'axios';
import { useAuth } from '../../contexts/AuthContext';

const PaymentFeatureToggle = ({ onToggle }) => {
  const [isEnabled, setIsEnabled] = useState(false);
  const [loading, setLoading] = useState(true);
  const [updating, setUpdating] = useState(false);
  const [error, setError] = useState(null);
  const [message, setMessage] = useState(null);
  
  const { token } = useAuth();
  
  // Fetch current feature toggle status
  const fetchToggleStatus = async () => {
    try {
      const response = await axios.get('/api/payment/status', {
        headers: {
          'Authorization': `Bearer ${token}`
        }
      });
      
      setIsEnabled(response.data.enabled);
    } catch (err) {
      console.error('Error fetching payment feature toggle status:', err);
      setError('Failed to fetch payment feature status');
    } finally {
      setLoading(false);
    }
  };
  
  // Load toggle status on component mount
  useEffect(() => {
    fetchToggleStatus();
  }, []);
  
  // Handle toggle change
  const handleToggle = async (event) => {
    const newStatus = event.target.checked;
    
    setUpdating(true);
    setError(null);
    setMessage(null);
    
    try {
      // Call API to update feature toggle status
      await axios.patch(
        '/api/feature-toggles/update', 
        {
          name: 'payment_integration',
          is_enabled: newStatus
        },
        {
          headers: {
            'Authorization': `Bearer ${token}`
          }
        }
      );
      
      // Update local state
      setIsEnabled(newStatus);
      
      // Show success message
      setMessage({
        text: `Payment integration feature ${newStatus ? 'enabled' : 'disabled'} successfully`,
        type: 'success'
      });
      
      // Call parent component callback if provided
      if (onToggle && typeof onToggle === 'function') {
        onToggle(newStatus);
      }
      
    } catch (err) {
      console.error('Error updating payment feature toggle:', err);
      setError(
        err.response?.data?.error || 
        'Failed to update payment feature status'
      );
      // Revert the switch to previous state
      setIsEnabled(!newStatus);
    } finally {
      setUpdating(false);
      
      // Clear messages after 3 seconds
      setTimeout(() => {
        setMessage(null);
        setError(null);
      }, 3000);
    }
  };
  
  return (
    <Paper elevation={3} sx={{ p: 3, mb: 3 }}>
      <Typography variant="h6" component="h2" gutterBottom>
        Payment Integration Settings
      </Typography>
      
      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}
      
      {message && (
        <Alert severity={message.type} sx={{ mb: 2 }}>
          {message.text}
        </Alert>
      )}
      
      <Box sx={{ display: 'flex', alignItems: 'center' }}>
        <FormControlLabel
          control={
            <Switch
              checked={isEnabled}
              onChange={handleToggle}
              disabled={loading || updating}
            />
          }
          label={isEnabled ? 'Enabled' : 'Disabled'}
        />
        
        {(loading || updating) && (
          <CircularProgress size={24} sx={{ ml: 2 }} />
        )}
      </Box>
      
      <Typography variant="body2" color="textSecondary" sx={{ mt: 1 }}>
        {isEnabled 
          ? 'Payment integration is currently enabled. Users can make payments using the active QR code.'
          : 'Payment integration is currently disabled. Enable this feature to allow users to make payments.'}
      </Typography>
    </Paper>
  );
};

export default PaymentFeatureToggle;
