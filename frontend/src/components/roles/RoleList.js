import React, { useState, useEffect } from 'react';
import styles from './RoleList.module.css';
import { Container, Row, Col, Card, Table, Button, Form, InputGroup, Badge, Modal, Alert } from 'react-bootstrap';
import { Link, useNavigate } from 'react-router-dom';
import { FaPlus, FaSearch, FaEdit, FaTrash, FaUserTag, FaShieldAlt, FaCloudUploadAlt, FaKey } from 'react-icons/fa';
import { toast } from 'react-toastify';
import { roleAPI, permissionAPI } from '../../services/api';
import { useAuth } from '../../contexts/AuthContext';

const RoleList = () => {
  const [roles, setRoles] = useState([]);
  const [loading, setLoading] = useState(true);
  const [searchTerm, setSearchTerm] = useState('');
  const [filteredRoles, setFilteredRoles] = useState([]);
  const [permissions, setPermissions] = useState([]);
  const [selectedRole, setSelectedRole] = useState(null);
  const [showPermissionModal, setShowPermissionModal] = useState(false);
  const [savingPermissions, setSavingPermissions] = useState(false);
  const [selectedPermissionIds, setSelectedPermissionIds] = useState([]);
  const [permissionsLoading, setPermissionsLoading] = useState(false);
  
  const { hasPermission } = useAuth();
  const navigate = useNavigate();

  const canCreateRole = hasPermission(['role_create']);
  const canEditRole = hasPermission(['role_update']);
  const canDeleteRole = hasPermission(['role_delete']);

  useEffect(() => {
    fetchRoles();
    fetchPermissions();
  }, []);
  
  // Fetch all available permissions
  const fetchPermissions = async () => {
    try {
      setPermissionsLoading(true);
      const response = await permissionAPI.getPermissions();
      if (response.data && Array.isArray(response.data)) {
        setPermissions(response.data);
      } else if (response.data && Array.isArray(response.data.permissions)) {
        setPermissions(response.data.permissions);
      } else {
        setPermissions([]);
      }
    } catch (error) {
      console.error('Error fetching permissions:', error);
      toast.error('Failed to load permissions');
    } finally {
      setPermissionsLoading(false);
    }
  };

  useEffect(() => {
    if (roles.length > 0 && searchTerm) {
      const filtered = roles.filter(role => 
        role.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
        role.description.toLowerCase().includes(searchTerm.toLowerCase())
      );
      setFilteredRoles(filtered);
    } else {
      setFilteredRoles(roles);
    }
  }, [roles, searchTerm]);

  const fetchRoles = async () => {
    try {
      setLoading(true);
      const response = await roleAPI.getRoles();
      console.log('Roles API response:', response.data);
      if (response.data && response.data.roles) {
        // Backend returns { count, roles } structure
        setRoles(response.data.roles);
        setFilteredRoles(response.data.roles);
      } else if (Array.isArray(response.data)) {
        // Handle case where response might be an array directly
        setRoles(response.data);
        setFilteredRoles(response.data);
      } else {
        console.error('Unexpected response format:', response.data);
        toast.error('Received invalid data format from server');
        setRoles([]);
        setFilteredRoles([]);
      }
    } catch (error) {
      console.error('Error fetching roles:', error);
      toast.error('Failed to load roles. Please try again.');
    } finally {
      setLoading(false);
    }
  };

  const handleDeleteRole = async (roleId, roleName) => {
    // Prevent deletion of system roles
    if (['Admin', 'System'].includes(roleName)) {
      toast.error(`The ${roleName} role cannot be deleted as it is a system role.`);
      return;
    }
    
    if (window.confirm(`Are you sure you want to delete the "${roleName}" role? This action cannot be undone.`)) {
      try {
        await roleAPI.deleteRole(roleId);
        toast.success(`Role "${roleName}" deleted successfully`);
        
        // Remove role from local state
        setRoles(roles.filter(role => role.role_id !== roleId));
      } catch (error) {
        console.error('Error deleting role:', error);
        toast.error('Failed to delete role. It may be assigned to users or have other dependencies.');
      }
    }
  };

  const handleSearch = (e) => {
    setSearchTerm(e.target.value);
  };

  // Handle opening permission edit modal
  const handleShowPermissionModal = (role) => {
    setSelectedRole(role);
    
    // Initialize selected permissions from the role
    const permissionIds = role.permissions ? 
      role.permissions.map(permission => permission.permission_id) : 
      [];
    
    setSelectedPermissionIds(permissionIds);
    setShowPermissionModal(true);
  };
  
  // Group permissions by category for better organization in modal
  const groupedPermissions = permissions.reduce((acc, permission) => {
    const category = permission.name.split('_')[0]; // Assuming permissions are named like "user_create"
    if (!acc[category]) {
      acc[category] = [];
    }
    acc[category].push(permission);
    return acc;
  }, {});
  
  // Handle permission checkbox change
  const handlePermissionChange = (permissionId, isChecked) => {
    if (isChecked) {
      setSelectedPermissionIds(prev => [...prev, permissionId]);
    } else {
      setSelectedPermissionIds(prev => prev.filter(id => id !== permissionId));
    }
  };
  
  // Handle saving role permissions
  const handleSavePermissions = async () => {
    if (!selectedRole) return;
    
    try {
      setSavingPermissions(true);
      
      await permissionAPI.assignPermissions(selectedRole.role_id, selectedPermissionIds);
      
      // Update the role in local state with new permissions
      const updatedPermissions = permissions.filter(p => selectedPermissionIds.includes(p.permission_id));
      
      setRoles(roles.map(role => {
        if (role.role_id === selectedRole.role_id) {
          return { ...role, permissions: updatedPermissions };
        }
        return role;
      }));
      
      // Also update filtered roles
      setFilteredRoles(filteredRoles.map(role => {
        if (role.role_id === selectedRole.role_id) {
          return { ...role, permissions: updatedPermissions };
        }
        return role;
      }));
      
      toast.success(`Permissions updated for ${selectedRole.name}`);
      setShowPermissionModal(false);
    } catch (error) {
      console.error('Error saving permissions:', error);
      toast.error('Failed to update permissions');
    } finally {
      setSavingPermissions(false);
    }
  };

  return (
    <Container fluid>
      <Card className="mb-4 glass-card">
        <Card.Body>
          <Row className="mb-4">
            <Col md={6}>
              <h2>Role Management</h2>
              <p className="text-muted">Manage system roles and their permissions</p>
            </Col>
            <Col md={6} className="text-md-end">
              {canCreateRole && (
                <>
                  <Button 
                    variant="outline-primary" 
                    as={Link} 
                    to="/roles/bulk-upload"
                    className="d-inline-flex align-items-center me-2 glass-btn"
                  >
                    <FaCloudUploadAlt className="me-2" /> Bulk Upload
                  </Button>
                  <Button 
                    variant="primary" 
                    as={Link} 
                    to="/roles/create"
                    className="d-inline-flex align-items-center glass-btn glass-btn-primary"
                  >
                    <FaPlus className="me-2" /> Add New Role
                  </Button>
                </>
              )}
            </Col>
          </Row>
          <Row>
            <Col md={6}>
              <h6>Search Roles</h6>
              <InputGroup>
                <Form.Control 
                  type="search" 
                  placeholder="Search roles..."
                  value={searchTerm}
                  onChange={handleSearch}
                />
                <Button variant="outline-secondary">
                  <FaSearch />
                </Button>
              </InputGroup>
            </Col>
          </Row>
          
          {loading ? (
            <div className="text-center py-5">
              <div className="spinner-border text-primary" role="status">
                <span className="visually-hidden">Loading...</span>
              </div>
            </div>
          ) : (
            <div className="table-responsive table-container glass-table">
              <Table hover className="align-middle">
                <thead>
                  <tr>
                    <th width="5%">#</th>
                    <th width="20%">Role Name</th>
                    <th width="30%">Description</th>
                    <th width="25%">Permissions</th>
                    <th style={{minWidth: '135px'}}>Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredRoles.length > 0 ? (
                    filteredRoles.map((role, index) => (
                      <tr key={role.role_id}>
                        <td>{index + 1}</td>
                        <td className={styles.roleNameCell}>
  <Link to={`/roles/${role.role_id}`} className={styles.roleName} style={{ textDecoration: 'underline', color: '#0d6efd', cursor: 'pointer' }}>
    {role.name}
  </Link>
  {['Admin', 'System'].includes(role.name) && (
    <Badge bg="secondary" className="ms-2">System</Badge>
  )}
</td>
                        <td>{role.description}</td>
                        <td>
  {role.permissions && role.permissions.length > 0 ? (
    <div className={styles.permissionBadges}>
      {role.permissions.slice(0, 3).map(permission => (
        <Badge key={permission.permission_id} bg="secondary" className="badge-permission glass-badge">
          {permission.name}
        </Badge>
      ))}
      {role.permissions.length > 3 && (
        <Badge className={styles.badgeMore}>
          +{role.permissions.length - 3} more
        </Badge>
      )}
    </div>
  ) : (
    <span className="text-muted">No permissions</span>
  )}
</td>
                        <td>
  <div className={styles.actionButtons}>
    {canEditRole ? (
      <>
        <Button 
          variant="primary" 
          size="sm" 
          className="glass-btn" 
          onClick={() => navigate(`/roles/edit/${role.role_id}`)} 
          title="Edit role"
        >
          <FaEdit className="me-1" /> Edit Role
        </Button>
        <Button 
          variant="outline-info" 
          size="sm" 
          className="glass-btn" 
          onClick={() => handleShowPermissionModal(role)} 
          title="Edit permissions"
        >
          <FaKey />
        </Button>
      </>
    ) : null}
    {canDeleteRole && !['Admin', 'System'].includes(role.name) ? (
      <Button 
        variant="outline-danger" 
        size="sm" 
        onClick={() => handleDeleteRole(role.role_id, role.name)}
        title="Delete role"
        className="glass-btn"
      >
        <FaTrash />
      </Button>
    ) : null}
    {!(canEditRole || (canDeleteRole && !['Admin', 'System'].includes(role.name))) && (
      <span style={{minWidth: '36px', display: 'inline-block'}}></span>
    )}
  </div>
</td>
                      </tr>
                    ))
                  ) : (
                    <tr>
                      <td colSpan="5" className="text-center py-4">
                        {searchTerm ? 'No roles match your search criteria.' : 'No roles found.'}
                      </td>
                    </tr>
                  )}
                </tbody>
              </Table>
            </div>
          )}
        </Card.Body>
      </Card>
      
      <Card className="glass-card mb-4">
        <Card.Header>
          <h5 className="mb-0">About Roles</h5>
        </Card.Header>
        <Card.Body>
          <Row>
            <Col md={6}>
              <h6>What are roles?</h6>
              <p className="text-muted">
                Roles are collections of permissions that define what actions users can perform in the system.
                Each user can be assigned multiple roles, and their effective permissions are the combination
                of all permissions from their assigned roles.
              </p>
            </Col>
            <Col md={6}>
              <h6>System Roles</h6>
              <p className="text-muted">
                System roles like "Admin" and "System" cannot be deleted as they are essential for system operation.
                The Admin role has all permissions by default. Be careful when modifying system roles as it may affect system functionality.
              </p>
            </Col>
          </Row>
        </Card.Body>
      </Card>
      
      {/* Permission Edit Modal */}
      <Modal 
        show={showPermissionModal} 
        onHide={() => setShowPermissionModal(false)}
        size="lg"
        backdrop="static"
        className="glass-modal-container"
      >
        <div className="glass-modal">
        <Modal.Header closeButton>
          <Modal.Title>
            Edit Permissions for {selectedRole?.name}
          </Modal.Title>
        </Modal.Header>
        <Modal.Body style={{ maxHeight: '60vh', overflowY: 'auto' }}>
          {permissionsLoading ? (
            <div className="text-center py-4">
              <div className="spinner-border text-primary" role="status">
                <span className="visually-hidden">Loading...</span>
              </div>
            </div>
          ) : (
            <>
              {selectedRole && selectedRole.name && ['Admin', 'System'].includes(selectedRole.name) && (
                <Alert variant="warning" className="mb-3">
                  <FaShieldAlt className="me-2" />
                  This is a system role. Be careful when modifying its permissions as it may affect system functionality.
                </Alert>
              )}
              
              {Object.entries(groupedPermissions).sort().map(([category, categoryPermissions]) => (
                <div key={category} className="mb-4">
                  <h5 className="text-capitalize">{category}</h5>
                  <hr />
                  <Row>
                    {categoryPermissions.map(permission => {
                      const isChecked = selectedPermissionIds.includes(permission.permission_id);
                      return (
                        <Col md={4} key={permission.permission_id} className="mb-2">
                          <Form.Check 
                            type="checkbox"
                            id={`permission-${permission.permission_id}`}
                            label={permission.name}
                            checked={isChecked}
                            className="glass-checkbox"
                            onChange={(e) => handlePermissionChange(permission.permission_id, e.target.checked)}
                          />
                        </Col>
                      );
                    })}
                  </Row>
                </div>
              ))}
              
              {Object.keys(groupedPermissions).length === 0 && (
                <Alert variant="info">
                  No permissions available.
                </Alert>
              )}
            </>
          )}
        </Modal.Body>
        <Modal.Footer>
          <div className="d-flex justify-content-between w-100">
            <div>
              <Badge bg="primary" className="me-2 glass-badge">
                {selectedPermissionIds.length} permissions selected
              </Badge>
            </div>
            <div>
              <Button 
                variant="secondary" 
                onClick={() => setShowPermissionModal(false)}
                className="me-2 glass-btn"
              >
                Cancel
              </Button>
              <Button 
                variant="primary" 
                onClick={handleSavePermissions}
                disabled={savingPermissions}
                className="glass-btn glass-btn-primary"
              >
                {savingPermissions ? 'Saving...' : 'Save Permissions'}
              </Button>
            </div>
          </div>
        </Modal.Footer>
        </div>
      </Modal>
    </Container>
  );
};

export default RoleList;
