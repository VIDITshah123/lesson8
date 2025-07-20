/**
 * Role-Based Access Control (RBAC) Middleware
 * Provides permission-based access control for routes
 */

/**
 * Check if user has required permissions
 * @param {Array} requiredPermissions - Array of permission names required for access
 * @returns {Function} Express middleware function
 */
const checkPermissions = (requiredPermissions) => {
  return (req, res, next) => {
    // User should be provided by authenticateToken middleware
    if (!req.user) {
      return res.status(401).json({ error: 'Authentication required' });
    }
    
    // If no specific permissions are required, allow access
    if (!requiredPermissions || requiredPermissions.length === 0) {
      return next();
    }
    
    // User permissions should be in the token (added during login)
    const userPermissions = req.user.permissions || [];
    
    // Check if user has at least one of the required permissions
    const hasPermission = requiredPermissions.some(permission => 
      userPermissions.includes(permission)
    );
    
    if (!hasPermission) {
      return res.status(403).json({ 
        error: 'Access denied: insufficient permissions',
        required: requiredPermissions
      });
    }
    
    next();
  };
};

/**
 * Check if user has specific role
 * @param {Array} requiredRoles - Array of role names required for access
 * @returns {Function} Express middleware function
 */
const checkRoles = (requiredRoles) => {
  return (req, res, next) => {
    // User should be provided by authenticateToken middleware
    if (!req.user) {
      return res.status(401).json({ error: 'Authentication required' });
    }
    
    // If no specific roles are required, allow access
    if (!requiredRoles || requiredRoles.length === 0) {
      return next();
    }
    
    // User roles should be in the token (added during login)
    const userRoles = req.user.roles || [];
    
    // Check if user has at least one of the required roles
    const hasRole = requiredRoles.some(role => 
      userRoles.includes(role)
    );
    
    if (!hasRole) {
      return res.status(403).json({ 
        error: 'Access denied: insufficient role',
        required: requiredRoles
      });
    }
    
    next();
  };
};

module.exports = {
  checkPermissions,
  checkRoles
};
