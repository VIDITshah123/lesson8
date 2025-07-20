import React from 'react';
import { Navigate } from 'react-router-dom';
import { useAuth } from '../../contexts/AuthContext';

/**
 * Component to protect routes that require authentication
 * 
 * If the user is authenticated, it renders the children components
 * Otherwise, it redirects to the login page
 */
const ProtectedRoute = ({ children, requiredPermissions = [], requiredRoles = [] }) => {
  // Use a try-catch to safely access auth context
  try {
    const auth = useAuth();
    
    // If auth context is null or undefined, redirect to login
    if (!auth) {
      console.warn('Auth context is null or undefined');
      return <Navigate to="/login" />;
    }
    
    const { isAuthenticated = false, isLoading = false, hasPermission = () => false, hasRole = () => false } = auth;
    
    // Show loading indicator while checking authentication
    if (isLoading) {
      return (
        <div className="d-flex justify-content-center align-items-center" style={{ height: '100vh' }}>
          <div className="spinner-border text-primary" role="status">
            <span className="visually-hidden">Loading...</span>
          </div>
        </div>
      );
    }
    
    // Check if user is authenticated
    if (!isAuthenticated) {
      return <Navigate to="/login" state={{ from: window.location.pathname }} />;
    }
    
    // Check if user has required permissions (if any)
    if (requiredPermissions.length > 0) {
      // Make sure hasPermission is a function
      if (typeof hasPermission !== 'function') {
        console.error('hasPermission is not a function');
        return <Navigate to="/unauthorized" />;
      }
      
      const hasRequiredPermission = hasPermission(requiredPermissions);
      if (!hasRequiredPermission) {
        return <Navigate to="/unauthorized" />;
      }
    }
    
    // Check if user has required roles (if any)
    if (requiredRoles.length > 0) {
      // Make sure hasRole is a function
      if (typeof hasRole !== 'function') {
        console.error('hasRole is not a function');
        return <Navigate to="/unauthorized" />;
      }
      
      const hasRequiredRole = hasRole(requiredRoles);
      if (!hasRequiredRole) {
        return <Navigate to="/unauthorized" />;
      }
    }
    
    // User is authenticated and has required permissions/roles, render children
    return children;
  } catch (error) {
    console.error('Error in ProtectedRoute:', error);
    return <Navigate to="/login" />;
  }
};

export default ProtectedRoute;
