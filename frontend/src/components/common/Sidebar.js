import React, { useState, useEffect } from 'react';
import { Link, useLocation } from 'react-router-dom';
import { 
  FaHome, 
  FaUsers, 
  FaUserTag, 
  FaShieldAlt, 
  FaList, 
  FaChartLine, 
  FaToggleOn,
  FaCreditCard
} from 'react-icons/fa';
import { useAuth } from '../../contexts/AuthContext';
import { featureToggleAPI } from '../../services/api';

const Sidebar = ({ collapsed }) => {
  const location = useLocation();
  const { hasPermission, hasRole, currentUser } = useAuth();
  const token = localStorage.getItem('token');
  const [featureToggles, setFeatureToggles] = useState({});
  const [loading, setLoading] = useState(true);
  
  // Fetch feature toggles when component mounts
  useEffect(() => {
    const fetchFeatureToggles = async () => {
      try {
        const response = await featureToggleAPI.getToggles();
        
        console.log('Feature toggles response:', response.data);
        
        // Convert to a map for easier checking
        const togglesMap = {};
        response.data.forEach(toggle => {
          // Check if the toggle has feature_name or name property
          const name = toggle.feature_name || toggle.name;
          const isEnabled = toggle.enabled === 1 || toggle.enabled === true;
          console.log(`Toggle ${name} is ${isEnabled ? 'enabled' : 'disabled'}`);
          togglesMap[name] = isEnabled;
        });
        
        console.log('Feature toggles map:', togglesMap);
        setFeatureToggles(togglesMap);
      } catch (error) {
        console.error('Failed to fetch feature toggles:', error);
      } finally {
        setLoading(false);
      }
    };
    
    if (token) {
      fetchFeatureToggles();
    }
  }, [token]);
  
  // Define base menu items that will be available to everyone with permissions
  const baseMenuItems = [
    {
      name: 'Dashboard',
      path: '/dashboard',
      icon: <FaHome />,
      permission: null // Everyone can access dashboard
    },
    {
      name: 'Users',
      path: '/users',
      icon: <FaUsers />,
      permission: 'user_view'
    },
    {
      name: 'Roles',
      path: '/roles',
      icon: <FaUserTag />,
      permission: 'role_view'
    },
    {
      name: 'File Upload Settings',
      path: '/admin/file-upload-settings',
      icon: <FaCreditCard />,
      permission: 'role_view'
    },
    {
      name: 'Permissions',
      path: '/permissions',
      icon: <FaShieldAlt />,
      permission: 'permission_view'
    },
    {
      name: 'Activity Logs',
      path: '/logs',
      icon: <FaList />,
      permission: 'permission_view'
    },
    {
      name: 'Analytics',
      path: '/analytics',
      icon: <FaChartLine />,
      permission: 'dashboard_view'
    }
  ];
  
  // Start with base menu items
  const menuItems = [...baseMenuItems];
  
  // For admin users, always add the payment module regardless of feature toggles
  if (hasRole && hasRole(['Admin', 'admin'])) {
    console.log('Admin user detected - adding Payment module');
    menuItems.push({
      name: 'Payment',
      path: '/payment',
      icon: <FaCreditCard />,
      permission: null // No permission check for admins
    });
    
    // Also add Feature Toggles menu item for admin
    menuItems.push({
      name: 'Feature Toggles',
      path: '/roles/feature-toggles',
      icon: <FaToggleOn />,
      permission: null
    });
  } 
  // For non-admin users who have payment_view permission, show payment module if feature toggle is enabled
  else if (hasPermission(['payment_view']) && featureToggles['payment_integration']) {
    console.log('Non-admin user with payment_view permission - adding Payment module');
    menuItems.push({
      name: 'Payment',
      path: '/payment',
      icon: <FaCreditCard />,
      permission: 'payment_view',
      featureToggle: 'payment_integration'
    });
  }
  
  // Add Feature Toggles for full_access users who are not admins
  if (hasRole && hasRole(['full_access']) && !hasRole(['Admin', 'admin'])) {
    menuItems.push({
      name: 'Feature Toggles',
      path: '/roles/feature-toggles',
      icon: <FaToggleOn />,
      permission: null
    });
  }

  return (
    <div className={`sidebar ${collapsed ? 'collapsed' : ''}`} style={{
      width: collapsed ? '70px' : '250px',
      minHeight: '100vh',
      position: 'fixed',
      top: 0,
      left: 0,
      backgroundColor: '#343a40',
      transition: 'width 0.3s',
      boxShadow: '0 0 10px rgba(0, 0, 0, 0.1)',
      padding: '20px 0',
      zIndex: 1030
    }}>
      <div className="d-flex justify-content-center align-items-center py-3 mb-4" style={{ color: '#fff' }}>
        {!collapsed && <h4 className="m-0">EmployDEX</h4>}
        {collapsed && <h4 className="m-0">E</h4>}
      </div>
      <ul className="nav flex-column">
        {menuItems.map((item) => {
          // Skip rendering menu item if user doesn't have permission
          if (item.permission && !hasPermission([item.permission])) {
            return null;
          }
          
          // Skip rendering menu item if its feature toggle is disabled
          // But always show payment module to admin users regardless of feature toggle
          if (item.featureToggle && !featureToggles[item.featureToggle]) {
            // Special case for payment module - always show to admin users
            if (item.name === 'Payment' && hasRole(['Admin'])) {
              console.log('Showing Payment module to Admin user regardless of feature toggle');
            } else {
              console.log(`Hiding ${item.name} module due to disabled feature toggle ${item.featureToggle}`);
              return null;
            }
          }
          
          const isActive = location.pathname.startsWith(item.path);
          
          return (
            <li 
              key={item.name} 
              className="nav-item mb-2"
              title={collapsed ? item.name : ''}
            >
              <Link 
                to={item.path} 
                className={`nav-link ${isActive ? 'active' : ''}`} 
                style={{
                  color: isActive ? '#fff' : '#ced4da',
                  backgroundColor: isActive ? 'rgba(255, 255, 255, 0.1)' : 'transparent',
                  padding: collapsed ? '10px 0' : '10px 20px',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: collapsed ? 'center' : 'flex-start',
                  borderRadius: '4px',
                  margin: '0 10px',
                  transition: 'all 0.3s'
                }}
              >
                <span className="me-2">{item.icon}</span>
                {!collapsed && <span>{item.name}</span>}
              </Link>
            </li>
          );
        })}
      </ul>
      <div className="mt-auto text-center p-3" style={{ color: '#6c757d', fontSize: '0.8rem' }}>
        {!collapsed && <span>EmployDEX &copy; 2025</span>}
      </div>
    </div>
  );
};

export default Sidebar;
