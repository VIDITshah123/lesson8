import React, { useState, useEffect } from 'react';
import { Container, Row, Col, Card, Button, Badge, Tabs, Tab, Spinner, Alert } from 'react-bootstrap';
import { Link, useParams, useNavigate } from 'react-router-dom';
import { FaUser, FaEdit, FaTrash, FaArrowLeft, FaToggleOn, FaToggleOff } from 'react-icons/fa';
import { toast } from 'react-toastify';
import { userAPI, loggingAPI } from '../../services/api';
import { useAuth } from '../../contexts/AuthContext';

const UserDetails = () => {
  const { id } = useParams();
  const navigate = useNavigate();
  const { hasPermission } = useAuth();
  
  const [user, setUser] = useState(null);
  const [userActivities, setUserActivities] = useState([]);
  const [loading, setLoading] = useState(true);
  const [activitiesLoading, setActivitiesLoading] = useState(true);
  const [error, setError] = useState(null);
  
  const canEditUser = hasPermission(['user_update']);
  const canDeleteUser = hasPermission(['user_delete']);

  useEffect(() => {
    const fetchUserDetails = async () => {
      try {
        setLoading(true);
        const response = await userAPI.getUser(id);
        setUser(response.data);
      } catch (error) {
        console.error('Error fetching user details:', error);
        setError('Failed to load user details. The user might not exist or you may not have permission to view it.');
      } finally {
        setLoading(false);
      }
    };

    fetchUserDetails();
  }, [id]);

  useEffect(() => {
    const fetchUserActivities = async () => {
      try {
        setActivitiesLoading(true);
        const response = await loggingAPI.getLogs({ user_id: id, limit: 20 });
        setUserActivities(response.data.activities || []);
      } catch (error) {
        console.error('Error fetching user activities:', error);
        // Not setting error state here as this is secondary information
      } finally {
        setActivitiesLoading(false);
      }
    };

    if (user) {
      fetchUserActivities();
    }
  }, [user, id]);

  const handleToggleStatus = async () => {
    try {
      await userAPI.toggleUserStatus(id, !user.is_active);
      toast.success(`User ${user.is_active ? 'deactivated' : 'activated'} successfully`);
      setUser({ ...user, is_active: !user.is_active });
    } catch (error) {
      console.error('Error toggling user status:', error);
      toast.error('Failed to update user status');
    }
  };

  const handleDeleteUser = async () => {
    if (window.confirm('Are you sure you want to delete this user? This action cannot be undone.')) {
      try {
        await userAPI.deleteUser(id);
        toast.success('User deleted successfully');
        navigate('/users');
      } catch (error) {
        console.error('Error deleting user:', error);
        toast.error('Failed to delete user');
      }
    }
  };

  if (loading) {
    return (
      <Container className="mt-4 text-center">
        <Spinner animation="border" variant="primary" />
        <p>Loading user details...</p>
      </Container>
    );
  }

  if (error) {
    return (
      <Container className="mt-4">
        <Alert variant="danger">{error}</Alert>
        <Button as={Link} to="/users" variant="primary">
          <FaArrowLeft className="me-2" /> Back to Users
        </Button>
      </Container>
    );
  }

  return (
    <Container fluid>
      <Row className="mb-4">
        <Col>
          <Button as={Link} to="/users" variant="light" className="mb-3">
            <FaArrowLeft className="me-2" /> Back to Users
          </Button>
          <h2>User Details</h2>
        </Col>
      </Row>

      <Row>
        <Col lg={4}>
          <Card className="mb-4">
            <Card.Body className="text-center">
              <div className="mb-3">
                <div className="d-inline-flex align-items-center justify-content-center bg-light rounded-circle p-4 mb-3">
                  <FaUser size={50} className="text-primary" />
                </div>
                <h4>{user.first_name} {user.last_name}</h4>
                <p className="text-muted">{user.email}</p>
                <Badge bg={user.is_active ? 'success' : 'danger'} className="mb-3">
                  {user.is_active ? 'Active' : 'Inactive'}
                </Badge>
              </div>

              <div className="d-grid gap-2 mt-3">
                {canEditUser && (
                  <>
                    <Button as={Link} to={`/users/edit/${user.user_id}`} variant="outline-primary">
                      <FaEdit className="me-2" /> Edit User
                    </Button>
                    <Button variant={user.is_active ? 'outline-warning' : 'outline-success'} onClick={handleToggleStatus}>
                      {user.is_active ? (
                        <>
                          <FaToggleOff className="me-2" /> Deactivate User
                        </>
                      ) : (
                        <>
                          <FaToggleOn className="me-2" /> Activate User
                        </>
                      )}
                    </Button>
                  </>
                )}
                {canDeleteUser && (
                  <Button variant="outline-danger" onClick={handleDeleteUser}>
                    <FaTrash className="me-2" /> Delete User
                  </Button>
                )}
              </div>
            </Card.Body>
          </Card>

          <Card>
            <Card.Header className="bg-white">
              <h5 className="mb-0">User Information</h5>
            </Card.Header>
            <Card.Body>
              <div className="mb-3">
                <h6 className="text-muted mb-1">User ID</h6>
                <p>{user.user_id}</p>
              </div>
              <div className="mb-3">
                <h6 className="text-muted mb-1">Full Name</h6>
                <p>{user.first_name} {user.last_name}</p>
              </div>
              <div className="mb-3">
                <h6 className="text-muted mb-1">Email</h6>
                <p>{user.email}</p>
              </div>
              <div className="mb-3">
                <h6 className="text-muted mb-1">Mobile Number</h6>
                <p>{user.mobile_number || <span className="text-muted">Not set</span>}</p>
              </div>
              <div className="mb-3">
                <h6 className="text-muted mb-1">Created On</h6>
                <p>{new Date(user.created_at).toLocaleString()}</p>
              </div>
              {user.last_login && (
                <div className="mb-3">
                  <h6 className="text-muted mb-1">Last Login</h6>
                  <p>{new Date(user.last_login).toLocaleString()}</p>
                </div>
              )}
              <div className="mb-3">
                <h6 className="text-muted mb-1">Roles</h6>
                <div>
                  {user.roles && user.roles.length > 0 ? (
                    user.roles.map(role => (
                      <Badge 
                        key={role.role_id}
                        bg="primary" 
                        className="me-1 mb-1 badge-role"
                      >
                        {role.name}
                      </Badge>
                    ))
                  ) : (
                    <p className="text-muted">No roles assigned</p>
                  )}
                </div>
              </div>
            </Card.Body>
          </Card>
        </Col>

        <Col lg={8}>
          <Card className="mb-4">
            <Card.Body>
              <Tabs defaultActiveKey="activity" className="mb-3">
                <Tab eventKey="activity" title="Activity Log">
                  {activitiesLoading ? (
                    <div className="text-center py-4">
                      <Spinner animation="border" size="sm" />
                      <p className="mt-2">Loading activities...</p>
                    </div>
                  ) : (
                    <>
                      {userActivities.length > 0 ? (
                        <div className="table-responsive">
                          <table className="table">
                            <thead>
                              <tr>
                                <th>Action</th>
                                <th>Module</th>
                                <th>Description</th>
                                <th>Date</th>
                              </tr>
                            </thead>
                            <tbody>
                              {userActivities.map(activity => (
                                <tr key={activity.log_id}>
                                  <td>{activity.action_type}</td>
                                  <td>{activity.module}</td>
                                  <td>{activity.description}</td>
                                  <td>{new Date(activity.timestamp).toLocaleString()}</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      ) : (
                        <Alert variant="info">No activity records found for this user.</Alert>
                      )}
                    </>
                  )}
                </Tab>
                <Tab eventKey="permissions" title="Effective Permissions">
                  <div className="mb-4">
                    <h6 className="mb-3">Permissions by Role</h6>
                    {user.roles && user.roles.length > 0 ? (
                      user.roles.map(role => (
                        <div key={role.role_id} className="mb-3">
                          <h6 className="border-bottom pb-2">{role.name}</h6>
                          <div className="mt-2">
                            {role.permissions && role.permissions.length > 0 ? (
                              role.permissions.map(permission => (
                                <Badge 
                                  key={permission.permission_id} 
                                  bg="secondary" 
                                  className="me-2 mb-2 badge-permission"
                                >
                                  {permission.name}
                                </Badge>
                              ))
                            ) : (
                              <p className="text-muted">No permissions for this role</p>
                            )}
                          </div>
                        </div>
                      ))
                    ) : (
                      <Alert variant="info">
                        This user has no roles assigned and therefore no permissions.
                      </Alert>
                    )}
                  </div>
                </Tab>
              </Tabs>
            </Card.Body>
          </Card>
        </Col>
      </Row>
    </Container>
  );
};

export default UserDetails;
