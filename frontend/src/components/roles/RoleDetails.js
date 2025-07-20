import React, { useState, useEffect } from 'react';
import { Container, Row, Col, Card, Button, Badge, Alert, Tabs, Tab } from 'react-bootstrap';
import { Link, useParams, useNavigate } from 'react-router-dom';
import { FaUserTag, FaEdit, FaTrash, FaArrowLeft, FaShieldAlt, FaUser } from 'react-icons/fa';
import { toast } from 'react-toastify';
import { roleAPI, userAPI } from '../../services/api';
import { useAuth } from '../../contexts/AuthContext';

const RoleDetails = () => {
  const { id } = useParams();
  const navigate = useNavigate();
  const { hasPermission } = useAuth();
  
  const [role, setRole] = useState(null);
  const [usersWithRole, setUsersWithRole] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  
  const canEditRole = hasPermission(['role_update']);
  const canDeleteRole = hasPermission(['role_delete']);

  useEffect(() => {
    const fetchRoleDetails = async () => {
      try {
        setLoading(true);
        const response = await roleAPI.getRole(id);
        setRole(response.data);
      } catch (error) {
        console.error('Error fetching role details:', error);
        setError('Failed to load role details. The role might not exist or you may not have permission to view it.');
      } finally {
        setLoading(false);
      }
    };

    fetchRoleDetails();
  }, [id]);

  useEffect(() => {
    const fetchUsersWithRole = async () => {
      if (role) {
        try {
          // This would typically have a filter parameter to only get users with this role
          // For demo purposes we'll just fetch all users and filter client-side
          const response = await userAPI.getUsers();
          const filteredUsers = response.data.users.filter(user => 
            user.roles && user.roles.some(r => r.role_id === parseInt(id))
          );
          setUsersWithRole(filteredUsers);
        } catch (error) {
          console.error('Error fetching users with role:', error);
          // Not setting error state here as this is secondary information
        }
      }
    };

    fetchUsersWithRole();
  }, [role, id]);

  const handleDeleteRole = async () => {
    // Prevent deletion of system roles
    if (['Admin', 'System'].includes(role.name)) {
      toast.error(`The ${role.name} role cannot be deleted as it is a system role.`);
      return;
    }
    
    if (window.confirm(`Are you sure you want to delete the "${role.name}" role? This action cannot be undone.`)) {
      try {
        await roleAPI.deleteRole(id);
        toast.success(`Role "${role.name}" deleted successfully`);
        navigate('/roles');
      } catch (error) {
        console.error('Error deleting role:', error);
        toast.error('Failed to delete role. It may be assigned to users or have other dependencies.');
      }
    }
  };

  const isSystemRole = role && ['Admin', 'System'].includes(role.name);

  if (loading) {
    return (
      <Container className="mt-4 text-center">
        <div className="spinner-border text-primary" role="status">
          <span className="visually-hidden">Loading...</span>
        </div>
        <p className="mt-2">Loading role details...</p>
      </Container>
    );
  }

  if (error) {
    return (
      <Container className="mt-4">
        <Alert variant="danger">{error}</Alert>
        <Button as={Link} to="/roles" variant="primary">
          <FaArrowLeft className="me-2" /> Back to Roles
        </Button>
      </Container>
    );
  }

  if (!role) {
    return (
      <Container className="mt-4">
        <Alert variant="warning">Role not found.</Alert>
        <Button as={Link} to="/roles" variant="primary">
          <FaArrowLeft className="me-2" /> Back to Roles
        </Button>
      </Container>
    );
  }

  return (
    <Container fluid>
      <Row className="mb-4">
        <Col>
          <Button as={Link} to="/roles" variant="light" className="mb-3">
            <FaArrowLeft className="me-2" /> Back to Roles
          </Button>
          <h2>Role Details</h2>
        </Col>
      </Row>

      <Row>
        <Col lg={4}>
          <Card className="mb-4">
            <Card.Body>
              <div className="text-center mb-4">
                <div className="rounded-circle bg-light p-4 d-inline-flex mb-3">
                  <FaUserTag size={40} className="text-primary" />
                </div>
                <h4>{role.name}</h4>
                {isSystemRole && (
                  <Badge bg="secondary" className="mb-2">System Role</Badge>
                )}
                <p className="text-muted">{role.description}</p>
              </div>

              {isSystemRole && (
                <Alert variant="info" className="mb-3">
                  This is a system role with special privileges. Modifications may affect system functionality.
                </Alert>
              )}

              <div className="d-grid gap-2 mt-3">
                {canEditRole && (
                  <Button as={Link} to={`/roles/edit/${role.role_id}`} variant="outline-primary">
                    <FaEdit className="me-2" /> Edit Role
                  </Button>
                )}
                {canDeleteRole && !isSystemRole && (
                  <Button variant="outline-danger" onClick={handleDeleteRole}>
                    <FaTrash className="me-2" /> Delete Role
                  </Button>
                )}
              </div>
            </Card.Body>
          </Card>

          <Card>
            <Card.Header className="bg-white">
              <h5 className="mb-0">Role Information</h5>
            </Card.Header>
            <Card.Body>
              <div className="mb-3">
                <h6 className="text-muted mb-1">Role ID</h6>
                <p>{role.role_id}</p>
              </div>
              <div className="mb-3">
                <h6 className="text-muted mb-1">Created On</h6>
                <p>{new Date(role.created_at).toLocaleString()}</p>
              </div>
              <div className="mb-3">
                <h6 className="text-muted mb-1">Users with this Role</h6>
                <p>{usersWithRole.length}</p>
              </div>
            </Card.Body>
          </Card>
        </Col>

        <Col lg={8}>
          <Card className="mb-4">
            <Card.Body>
              <Tabs defaultActiveKey="permissions" className="mb-3">
                <Tab eventKey="permissions" title="Permissions">
                  <h5 className="mb-3">Role Permissions</h5>
                  
                  {role.permissions && role.permissions.length > 0 ? (
                    <div className="d-flex flex-wrap">
                      {role.permissions.map(permission => (
                        <Badge 
                          key={permission.permission_id} 
                          bg="secondary" 
                          className="me-2 mb-2 p-2 badge-permission"
                        >
                          <FaShieldAlt className="me-1" /> {permission.name}
                        </Badge>
                      ))}
                    </div>
                  ) : (
                    <Alert variant="info">
                      This role has no permissions assigned.
                    </Alert>
                  )}
                </Tab>
                
                <Tab eventKey="users" title="Assigned Users">
                  <h5 className="mb-3">Users with this Role</h5>
                  
                  {usersWithRole.length > 0 ? (
                    <div className="table-responsive">
                      <table className="table table-hover">
                        <thead>
                          <tr>
                            <th>Name</th>
                            <th>Email</th>
                            <th>Status</th>
                            <th>Action</th>
                          </tr>
                        </thead>
                        <tbody>
                          {usersWithRole.map(user => (
                            <tr key={user.user_id}>
                              <td>
                                <div className="d-flex align-items-center">
                                  <FaUser className="text-primary me-2" />
                                  {user.first_name} {user.last_name}
                                </div>
                              </td>
                              <td>{user.email}</td>
                              <td>
                                <Badge bg={user.is_active ? 'success' : 'danger'}>
                                  {user.is_active ? 'Active' : 'Inactive'}
                                </Badge>
                              </td>
                              <td>
                                <Button 
                                  as={Link} 
                                  to={`/users/${user.user_id}`} 
                                  variant="outline-primary" 
                                  size="sm"
                                >
                                  View Profile
                                </Button>
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : (
                    <Alert variant="info">
                      No users are currently assigned this role.
                    </Alert>
                  )}
                </Tab>
              </Tabs>
            </Card.Body>
          </Card>
        </Col>
      </Row>
    </Container>
  );
};

export default RoleDetails;
