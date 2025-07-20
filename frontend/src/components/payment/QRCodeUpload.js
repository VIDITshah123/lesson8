import React, { useState } from 'react';
import { 
  Button, 
  Box, 
  TextField, 
  FormControl, 
  InputLabel, 
  Select, 
  MenuItem, 
  Typography, 
  Paper, 
  Grid,
  CircularProgress,
  Alert
} from '@mui/material';
import { useDropzone } from 'react-dropzone';
import { paymentAPI } from '../../services/api';
import { useAuth } from '../../contexts/AuthContext';

const QRCodeUpload = ({ onSuccess }) => {
  const [formData, setFormData] = useState({
    payment_name: '',
    payment_description: '',
    payment_type: '',
    qr_code_image: null
  });
  
  const [previewUrl, setPreviewUrl] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [success, setSuccess] = useState(false);
  
  const { token } = useAuth();
  
  const paymentTypes = [
    { value: 'UPI', label: 'UPI Payment' },
    { value: 'BANK', label: 'Bank Transfer' },
    { value: 'WALLET', label: 'Digital Wallet' },
    { value: 'CARD', label: 'Credit/Debit Card' },
    { value: 'CASH', label: 'Cash Payment' }
  ];
  
  const handleChange = (e) => {
    const { name, value } = e.target;
    setFormData(prev => ({
      ...prev,
      [name]: value
    }));
  };
  
  const onDrop = (acceptedFiles) => {
    // Only accept the first file
    const file = acceptedFiles[0];
    if (file) {
      setFormData(prev => ({
        ...prev,
        qr_code_image: file
      }));
      
      // Create preview URL
      const fileUrl = URL.createObjectURL(file);
      setPreviewUrl(fileUrl);
      
      // Clear any previous errors
      setError(null);
    }
  };
  
  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    accept: {
      'image/*': ['.jpeg', '.jpg', '.png']
    },
    maxSize: 2097152, // 2MB max file size
    multiple: false
  });
  
  const handleSubmit = async (e) => {
    e.preventDefault();
    setLoading(true);
    setError(null);
    
    // Validate required fields
    if (!formData.payment_name || !formData.payment_type || !formData.qr_code_image) {
      setError('Please fill all required fields and upload a QR code image');
      setLoading(false);
      return;
    }
    
    // Prepare form data for upload
    const uploadData = new FormData();
    uploadData.append('payment_name', formData.payment_name);
    uploadData.append('payment_description', formData.payment_description || '');
    uploadData.append('payment_type', formData.payment_type);
    uploadData.append('qr_code_image', formData.qr_code_image);
    
    try {
      const response = await paymentAPI.uploadQrCode(uploadData);
      
      // Handle success
      setSuccess(true);
      setFormData({
        payment_name: '',
        payment_description: '',
        payment_type: '',
        qr_code_image: null
      });
      setPreviewUrl(null);
      
      // Call the success callback if provided
      if (onSuccess && typeof onSuccess === 'function') {
        onSuccess(response.data);
      }
      
      // Reset success message after 3 seconds
      setTimeout(() => {
        setSuccess(false);
      }, 3000);
      
    } catch (err) {
      console.error('Error uploading QR code:', err);
      setError(
        err.response?.data?.error || 
        'Failed to upload QR code. Please try again.'
      );
    } finally {
      setLoading(false);
    }
  };
  
  return (
    <Paper elevation={3} sx={{ p: 3, mb: 3 }}>
      <Typography variant="h6" component="h2" gutterBottom>
        Upload QR Code
      </Typography>
      
      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}
      
      {success && (
        <Alert severity="success" sx={{ mb: 2 }}>
          QR code uploaded successfully!
        </Alert>
      )}
      
      <form onSubmit={handleSubmit}>
        <Grid container spacing={2}>
          <Grid item xs={12} md={6}>
            <TextField
              fullWidth
              required
              label="QR Code Name"
              name="payment_name"
              value={formData.payment_name}
              onChange={handleChange}
              margin="normal"
              variant="outlined"
            />
            
            <TextField
              fullWidth
              label="Description"
              name="payment_description"
              value={formData.payment_description}
              onChange={handleChange}
              margin="normal"
              variant="outlined"
              multiline
              rows={3}
            />
            
            <FormControl fullWidth margin="normal" required>
              <InputLabel>Payment Type</InputLabel>
              <Select
                name="payment_type"
                value={formData.payment_type}
                onChange={handleChange}
                label="Payment Type"
              >
                {paymentTypes.map(type => (
                  <MenuItem key={type.value} value={type.value}>
                    {type.label}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          </Grid>
          
          <Grid item xs={12} md={6}>
            <Box 
              {...getRootProps()} 
              sx={{ 
                border: '2px dashed #ccc',
                borderRadius: 2,
                p: 2,
                textAlign: 'center',
                cursor: 'pointer',
                bgcolor: isDragActive ? 'rgba(0, 0, 0, 0.05)' : 'transparent',
                height: 200,
                display: 'flex',
                flexDirection: 'column',
                justifyContent: 'center',
                alignItems: 'center',
                mb: 2
              }}
            >
              <input {...getInputProps()} />
              {previewUrl ? (
                <img 
                  src={previewUrl} 
                  alt="QR code preview" 
                  style={{ maxHeight: '100%', maxWidth: '100%' }} 
                />
              ) : (
                <Typography>
                  {isDragActive ? 
                    'Drop the QR code image here...' : 
                    'Drag and drop a QR code image here, or click to select a file'}
                </Typography>
              )}
            </Box>
            <Typography variant="caption" color="textSecondary">
              * Supported formats: JPG, PNG. Max size: 2MB
            </Typography>
          </Grid>
          
          <Grid item xs={12}>
            <Box sx={{ display: 'flex', justifyContent: 'flex-end', mt: 2 }}>
              <Button 
                type="submit" 
                variant="contained" 
                color="primary"
                disabled={loading}
              >
                {loading ? <CircularProgress size={24} /> : 'Upload QR Code'}
              </Button>
            </Box>
          </Grid>
        </Grid>
      </form>
    </Paper>
  );
};

export default QRCodeUpload;
