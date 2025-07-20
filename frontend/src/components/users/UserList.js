import React, { useState, useEffect } from 'react';
import { Container, Row, Col, Card, Table, Button, Form, InputGroup, Badge, Pagination, Dropdown, Modal } from 'react-bootstrap';
import { Link, useNavigate } from 'react-router-dom';
import { FaPlus, FaSearch, FaEdit, FaTrash, FaUser, FaToggleOn, FaToggleOff, FaUserTag } from 'react-icons/fa';
import { toast } from 'react-toastify';
import { userAPI, roleAPI } from '../../services/api';
import { useAuth } from '../../contexts/AuthContext';

const UserList = () => {
  const [users, setUsers] = useState([]);
  const [loading, setLoading] = useState(true);
  const [searchTerm, setSearchTerm] = useState('');
  const [currentPage, setCurrentPage] = useState(1);
  const [totalPages, setTotalPages] = useState(1);
  const [totalUsers, setTotalUsers] = useState(0);
  const [roles, setRoles] = useState([]);
  const [selectedUser, setSelectedUser] = useState(null);
  const [showRoleModal, setShowRoleModal] = useState(false);
  const [savingRole, setSavingRole] = useState(false);
  const pageSize = 10;
  
  const { hasPermission } = useAuth();
  const navigate = useNavigate();

  const canCreateUser = hasPermission(['user_create']);
  const canEditUser = hasPermission(['user_update']);
  const canDeleteUser = hasPermission(['user_delete']);
  const canBulkUpload = hasPermission(['user_create']);

  useEffect(() => {
    fetchUsers();
    fetchRoles();
  }, [currentPage, searchTerm]);
  
  // Fetch all available roles
  const fetchRoles = async () => {
    try {
      const response = await roleAPI.getRoles();
      if (response.data && Array.isArray(response.data)) {
        setRoles(response.data);
      } else if (response.data && Array.isArray(response.data.roles)) {
        setRoles(response.data.roles);
      }
    } catch (error) {
      console.error('Error fetching roles:', error);
    }
  };

  const fetchUsers = async () => {
    try {
      setLoading(true);
      const params = {
        page: currentPage,
        limit: pageSize,
        mobile_number: searchTerm || undefined
      };
      
      const response = await userAPI.getUsers(params);
      
      setUsers(response.data.users);
      setTotalUsers(response.data.total);
      setTotalPages(Math.ceil(response.data.total / pageSize));
    } catch (error) {
      console.error('Error fetching users:', error);
      toast.error('Failed to load users. Please try again.');
    } finally {
      setLoading(false);
    }
  };

  const handleToggleStatus = async (userId, currentStatus) => {
    try {
      await userAPI.toggleUserStatus(userId, !currentStatus);
      toast.success('User status updated successfully');
      
      // Update user in the local state
      setUsers(users.map(user => 
        user.user_id === userId 
          ? { ...user, is_active: !currentStatus } 
          : user
      ));
    } catch (error) {
      console.error('Error toggling user status:', error);
      toast.error('Failed to update user status');
    }
  };

  const handleDeleteUser = async (userId) => {
    if (window.confirm('Are you sure you want to delete this user? This action cannot be undone.')) {
      try {
        await userAPI.deleteUser(userId);
        toast.success('User deleted successfully');
        
        // Remove user from local state
        setUsers(users.filter(user => user.user_id !== userId));
        setTotalUsers(totalUsers - 1);
        setTotalPages(Math.ceil((totalUsers - 1) / pageSize));
        
        // If we deleted the last item on this page and there are other pages, go to previous page
        if (users.length === 1 && currentPage > 1) {
          setCurrentPage(currentPage - 1);
        }
      } catch (error) {
        console.error('Error deleting user:', error);
        toast.error('Failed to delete user');
      }
    }
  };

  const handleSearch = (e) => {
    e.preventDefault();
    setCurrentPage(1); // Reset to first page on new search
    fetchUsers();
  };

  const handlePageChange = (pageNumber) => {
    setCurrentPage(pageNumber);
  };

  // Handle opening role edit modal
  const handleShowRoleModal = (user) => {
    setSelectedUser(user);
    setShowRoleModal(true);
  };
  
  // Handle role update
  const handleUpdateUserRoles = async (selectedRoleIds) => {
    if (!selectedUser) return;
    
    try {
      setSavingRole(true);
      
      // Get existing user data and just update the roles
      const userData = {
        roles: selectedRoleIds
      };
      
      await userAPI.updateUser(selectedUser.user_id, userData);
      
      // Update user in local state
      const updatedRoles = roles.filter(role => selectedRoleIds.includes(role.role_id));
      
      setUsers(users.map(user => {
        if (user.user_id === selectedUser.user_id) {
          return { ...user, roles: updatedRoles };
        }
        return user;
      }));
      
      toast.success('User roles updated successfully');
      setShowRoleModal(false);
    } catch (error) {
      console.error('Error updating user roles:', error);
      toast.error('Failed to update user roles');
    } finally {
      setSavingRole(false);
    }
  };

  return (
    <Container fluid>
      <Row className="mb-4">
        <Col md={6}>
          <h2>User Management</h2>
          <p className="text-muted">Manage system users and their access</p>
        </Col>
        <Col md={6} className="text-md-end">
          {canCreateUser && (
            <div className="d-flex gap-2 justify-content-end">
              <Button 
                variant="primary" 
                as={Link} 
                to="/users/create"
                className="d-inline-flex align-items-center"
              >
                <FaPlus className="me-2" /> Add New User
              </Button>
              
              {canBulkUpload && (
                <Button 
                  variant="outline-primary" 
                  as={Link} 
                  to="/users/bulk-upload"
                  className="d-inline-flex align-items-center"
                >
                  <FaPlus className="me-2" /> Bulk Upload
                </Button>
              )}
            </div>
          )}
        </Col>
      </Row>
      
      <Card className="mb-4">
        <Card.Body>
          <Row className="mb-3">
            <Col md={6}>
              <Form onSubmit={handleSearch}>
                <InputGroup>
                  <Form.Control 
                    placeholder="Search users..."
                    value={searchTerm}
                    onChange={(e) => setSearchTerm(e.target.value)}
                  />
                  <Button variant="outline-secondary" type="submit">
                    <FaSearch />
                  </Button>
                </InputGroup>
              </Form>
            </Col>
          </Row>
          
          {loading ? (
            <div className="text-center py-5">
              <div className="spinner-border text-primary" role="status">
                <span className="visually-hidden">Loading...</span>
              </div>
            </div>
          ) : (
            <>
              <div className="table-responsive">
                <Table hover bordered className="align-middle">
                  <thead>
                    <tr>
                      <th>#</th>
                      <th>Name</th>
                      <th>Email</th>
                      <th>Mobile</th>
                      <th>Roles</th>
                      <th>Status</th>
                      <th>Created Date</th>
                      <th>Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {users.length > 0 ? (
                      users.map((user, index) => (
                        <tr key={user.user_id}>
                          <td>{(currentPage - 1) * pageSize + index + 1}</td>
                          <td>
                            <Link to={`/users/${user.user_id}`} className="d-flex align-items-center text-decoration-none">
                              <FaUser className="me-2 text-primary" />
                              {user.first_name} {user.last_name}
                            </Link>
                          </td>
                          <td>{user.email}</td>
                          <td>{user.mobile_number || '-'}</td>
                          <td>
                            <div className="d-flex align-items-center">
                              <div>
                                {user.roles && user.roles.map(role => (
                                  <Badge key={role.role_id} pill bg="primary" className="me-1">
                                    {role.name}
                                  </Badge>
                                ))}
                              </div>
                              {canEditUser && (
                                <Button 
                                  variant="link" 
                                  className="text-decoration-none p-0 ms-2"
                                  onClick={() => handleShowRoleModal(user)}
                                  title="Edit roles"
                                >
                                  <FaUserTag size={14} />
                                </Button>
                              )}
                            </div>
                          </td>
                          <td>
                            <Badge bg={user.is_active ? 'success' : 'danger'}>
                              {user.is_active ? 'Active' : 'Inactive'}
                            </Badge>
                          </td>
                          <td>{new Date(user.created_at).toLocaleDateString()}</td>
                          <td>
                            <div className="d-flex">
                              <Button 
                                variant="outline-info" 
                                size="sm" 
                                className="me-2" 
                                onClick={() => navigate(`/users/edit/${user.user_id}`)} 
                                title="Edit user"
                              >
                                <FaEdit />
                              </Button>
                              
                              {canEditUser && (
                                <>
                                  <Button 
                                    variant="outline-info" 
                                    size="sm" 
                                    className="me-2" 
                                    onClick={() => navigate(`/users/edit/${user.user_id}`)} 
                                    title="Edit user"
                                  >
                                    <FaEdit />
                                  </Button>
                                  
                                  <Button 
                                    variant={user.is_active ? 'outline-warning' : 'outline-success'} 
                                    size="sm" 
                                    className="me-2"
                                    onClick={() => handleToggleStatus(user.user_id, user.is_active)}
                                    title={user.is_active ? 'Deactivate user' : 'Activate user'}
                                  >
                                    {user.is_active ? <FaToggleOff /> : <FaToggleOn />}
                                  </Button>
                                </>
                              )}
                              
                              {canDeleteUser && (
                                <Button 
                                  variant="outline-danger" 
                                  size="sm" 
                                  onClick={() => handleDeleteUser(user.user_id)}
                                  title="Delete user"
                                >
                                  <FaTrash />
                                </Button>
                              )}
                            </div>
                          </td>
                        </tr>
                      ))
                    ) : (
                      <tr>
                        <td colSpan="7" className="text-center py-4">
                          No users found.
                        </td>
                      </tr>
                    )}
                  </tbody>
                </Table>
              </div>
              
              <div className="d-flex justify-content-between align-items-center mt-3">
                <div>
                  Showing {users.length} of {totalUsers} users
                </div>
                
                {totalPages > 1 && (
                  <Pagination className="mb-0">
                    <Pagination.First disabled={currentPage === 1} onClick={() => handlePageChange(1)} />
                    <Pagination.Prev disabled={currentPage === 1} onClick={() => handlePageChange(currentPage - 1)} />
                    
                    {[...Array(totalPages).keys()].map(page => {
                      const pageNumber = page + 1;
                      // Only show a few pages around current page for better UI
                      if (
                        pageNumber === 1 || 
                        pageNumber === totalPages || 
                        Math.abs(pageNumber - currentPage) <= 2
                      ) {
                        return (
                          <Pagination.Item 
                            key={pageNumber} 
                            active={pageNumber === currentPage}
                            onClick={() => handlePageChange(pageNumber)}
                          >
                            {pageNumber}
                          </Pagination.Item>
                        );
                      } else if (
                        pageNumber === 2 || 
                        pageNumber === totalPages - 1
                      ) {
                        // Show ellipsis for better pagination UI
                        return <Pagination.Ellipsis key={pageNumber} />;
                      }
                      return null;
                    })}
                    
                    <Pagination.Next disabled={currentPage === totalPages} onClick={() => handlePageChange(currentPage + 1)} />
                    <Pagination.Last disabled={currentPage === totalPages} onClick={() => handlePageChange(totalPages)} />
                  </Pagination>
                )}
              </div>
            </>
          )}
        </Card.Body>
      </Card>
      
      {/* Role Edit Modal */}
      <Modal show={showRoleModal} onHide={() => setShowRoleModal(false)}>
        <Modal.Header closeButton>
          <Modal.Title>Edit User Roles</Modal.Title>
        </Modal.Header>
        <Modal.Body>
          {selectedUser && (
            <>
              <p>
                <strong>User:</strong> {selectedUser.first_name} {selectedUser.last_name}
              </p>
              <Form>
                <Form.Group className="mb-3">
                  <Form.Label>Assign Roles</Form.Label>
                  {roles.map(role => {
                    const isChecked = selectedUser.roles?.some(userRole => 
                      userRole.role_id === role.role_id
                    );
                    
                    return (
                      <Form.Check 
                        key={role.role_id}
                        type="checkbox"
                        id={`role-${role.role_id}`}
                        label={role.name}
                        defaultChecked={isChecked}
                        onChange={(e) => {
                          // Update selected user roles when checkbox is toggled
                          if (e.target.checked) {
                            // Add role if not already added
                            if (!selectedUser.roles.some(r => r.role_id === role.role_id)) {
                              setSelectedUser({
                                ...selectedUser,
                                roles: [...selectedUser.roles, role]
                              });
                            }
                          } else {
                            // Remove role
                            setSelectedUser({
                              ...selectedUser,
                              roles: selectedUser.roles.filter(r => r.role_id !== role.role_id)
                            });
                          }
                        }}
                      />
                    );
                  })}
                </Form.Group>
              </Form>
            </>
          )}
        </Modal.Body>
        <Modal.Footer>
          <Button variant="secondary" onClick={() => setShowRoleModal(false)}>
            Cancel
          </Button>
          <Button 
            variant="primary" 
            onClick={() => {
              if (selectedUser) {
                const selectedRoleIds = selectedUser.roles.map(role => role.role_id);
                handleUpdateUserRoles(selectedRoleIds);
              }
            }}
            disabled={savingRole}
          >
            {savingRole ? 'Saving...' : 'Save Changes'}
          </Button>
        </Modal.Footer>
      </Modal>
    </Container>
  );
};

export default UserList;
