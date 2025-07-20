import axios from 'axios';
import { toast } from 'react-toastify';

// Create axios instance with default config
const api = axios.create({
  baseURL: '/api',
  headers: {
    'Content-Type': 'application/json',
  }
});

// Set the JWT token for authenticated requests
export const setAuthToken = (token) => {
  if (token) {
    api.defaults.headers.common['Authorization'] = `Bearer ${token}`;
    localStorage.setItem('token', token);
  } else {
    delete api.defaults.headers.common['Authorization'];
    localStorage.removeItem('token');
  }
};

// Initialize token from localStorage if it exists
const token = localStorage.getItem('token');
if (token) {
  setAuthToken(token);
}

// Add a response interceptor for error handling
api.interceptors.response.use(
  (response) => response,
  (error) => {
    // Handle common error scenarios
    const { response } = error;

    if (response) {
      // Handle specific status codes
      switch (response.status) {
        case 401:
          // Unauthorized - token expired or invalid
          toast.error('Authentication session expired. Please log in again.');
          // Clear token and redirect to login
          setAuthToken(null);
          window.location.href = '/login';
          break;
        case 403:
          // Forbidden - insufficient permissions
          toast.error('You do not have permission to perform this action.');
          break;
        case 404:
          // Not found
          toast.error('The requested resource was not found.');
          break;
        case 422:
          // Validation errors
          if (response.data?.errors) {
            const errors = response.data.errors;
            errors.forEach(err => toast.error(err.msg));
          } else {
            toast.error('Validation error. Please check your input.');
          }
          break;
        case 500:
          // Server error
          toast.error('Server error. Please try again later or contact support.');
          break;
        default:
          // Other errors
          toast.error(response.data?.error || 'An error occurred. Please try again.');
      }
    } else {
      // Network error or server not responding
      toast.error('Unable to connect to server. Please check your internet connection.');
    }

    return Promise.reject(error);
  }
);

// Authentication API
export const authAPI = {
  login: (credentials) => api.post('/authentication/login', credentials),
  register: (userData) => api.post('/authentication/register', userData),
  forgotPassword: (email) => api.post('/authentication/forgot-password', { email }),
  resetPassword: (token, password) => api.post('/authentication/reset-password', { token, password }),
};

// User Management API
export const userAPI = {
  getUsers: (params) => api.get('/user_management/users', { params }),
  getUser: (id) => api.get(`/user_management/users/${id}`),
  createUser: (userData) => api.post('/user_management/users', userData),
  updateUser: (id, userData) => api.put(`/user_management/users/${id}`, userData),
  toggleUserStatus: (id, isActive) => api.patch(`/user_management/users/${id}/status`, { is_active: isActive }),
  deleteUser: (id) => api.delete(`/user_management/users/${id}`),
  uploadBulkUsers: (formData) => api.post('/user_management/users/bulk', formData, {
    headers: {
      'Content-Type': 'multipart/form-data'
    }
  }),
  downloadUserTemplate: () => api.get('/user_management/users/template', {
    responseType: 'blob'
  }),
};

// Role Management API
export const roleAPI = {
  getRoles: () => api.get('/role_management/roles'),
  getRole: (id) => api.get(`/role_management/roles/${id}`),
  createRole: (roleData) => api.post('/role_management/roles', roleData),
  updateRole: (id, roleData) => api.put(`/role_management/roles/${id}`, roleData),
  deleteRole: (id) => api.delete(`/role_management/roles/${id}`),
  uploadBulkRoles: (formData, onUploadProgress) => {
    return api.post('/role_management/roles/bulk', formData, {
      headers: {
        'Content-Type': 'multipart/form-data'
      },
      onUploadProgress
    });
  },
  downloadRoleTemplate: () => {
    return api.get('/role_management/roles/template', {
      responseType: 'blob'
    }).then(response => {
      // Create blob link to download
      const url = window.URL.createObjectURL(new Blob([response.data]));
      const link = document.createElement('a');
      link.href = url;
      link.setAttribute('download', 'role-template.csv');
      
      // Append to html page
      document.body.appendChild(link);
      
      // Force download
      link.click();
      
      // Clean up and remove the link
      link.parentNode.removeChild(link);
    });
  }
};

// Permission Management API
export const permissionAPI = {
  getPermissions: () => api.get('/permission_management/permissions'),
  getPermission: (id) => api.get(`/permission_management/permissions/${id}`),
  createPermission: (permissionData) => api.post('/permission_management/permissions', permissionData),
  updatePermission: (id, permissionData) => api.put(`/permission_management/permissions/${id}`, permissionData),
  assignPermissions: (roleId, permissions) => api.post('/permission_management/assign', { role_id: roleId, permissions }),
};

// Logging API
export const loggingAPI = {
  getLogs: (params) => api.get('/logging/activity', { params }),
  getActionTypes: () => api.get('/logging/actions'),
  getEntityTypes: () => api.get('/logging/entities'),
  getStats: () => api.get('/logging/stats'),
};

// Feature Toggles API
export const featureToggleAPI = {
  getToggles: () => api.get('/feature-toggles'),
  getToggle: (name) => api.get(`/feature-toggles/${name}`),
  updateToggle: (name, isEnabled) => api.patch('/feature-toggles/update', { name, is_enabled: isEnabled }),
};

// Payment API
export const paymentAPI = {
  // QR Code operations
  getQrCodes: () => api.get('/payment/qr-codes'),
  getQrCode: (id) => api.get(`/payment/qr-codes/${id}`),
  deleteQrCode: (id) => api.delete(`/payment/qr-codes/${id}`),
  activateQrCode: (id) => api.post(`/payment/qr-codes/${id}/activate`),
  // Transactions
  getTransactions: (params) => api.get('/payment/transactions', { params }),
  getTransaction: (id) => api.get(`/payment/transactions/${id}`),
  // Special handling for file uploads with authentication
  uploadQrCode: (formData) => {
    const token = localStorage.getItem('token');
    return api.post('/payment/qr-codes', formData, {
      headers: {
        'Content-Type': 'multipart/form-data',
        'Authorization': `Bearer ${token}`
      }
    });
  },
  // Feature toggle status
  getPaymentStatus: () => api.get('/payment/status')
};

export default api;
