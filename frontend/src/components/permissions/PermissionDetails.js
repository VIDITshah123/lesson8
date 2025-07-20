import React, { useState, useEffect } from 'react';
import { Container, Row, Col, Card, Button, Alert, Badge, ListGroup } from 'react-bootstrap';
import { Link, useParams, useNavigate } from 'react-router-dom';
import { FaArrowLeft, FaEdit, FaTrash, FaShieldAlt, FaUsers } from 'react-icons/fa';
import { toast } from 'react-toastify';
import { permissionAPI, roleAPI } from '../../services/api';
import { useAuth } from '../../contexts/AuthContext';
import ConfirmModal from '../common/ConfirmModal';

const PermissionDetails = () => {
  const { id } = useParams();
  const navigate = useNavigate();
  const { hasPermission } = useAuth();
  
  const [permission, setPermission] = useState(null);
  const [rolesWithPermission, setRolesWithPermission] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [showDeleteModal, setShowDeleteModal] = useState(false);
  
  const canViewPermission = hasPermission(['permission_view']);
  const canEditPermission = hasPermission(['permission_edit']);
  const canDeletePermission = hasPermission(['permission_delete']);

  useEffect(() => {
    if (canViewPermission) {
      fetchPermissionDetails();
    } else {
      setLoading(false);
    }
  }, [id, canViewPermission]);

  const fetchPermissionDetails = async () => {
    try {
      setLoading(true);
      const permResponse = await permissionAPI.getPermission(id);
      setPermission(permResponse.data);
      
      // Fetch roles that have this permission
      const rolesResponse = await roleAPI.getRoles();
      const roles = rolesResponse.data || [];
      
      const rolesWithThisPermission = roles.filter(role => 
        role.permissions && role.permissions.some(perm => perm.permission_id === parseInt(id))
      );
      
      setRolesWithPermission(rolesWithThisPermission);
    } catch (error) {
      console.error('Error fetching permission details:', error);
      setError('Failed to load permission details. Please try again.');
    } finally {
      setLoading(false);
    }
  };

  const handleDelete = async () => {
    try {
      await permissionAPI.deletePermission(id);
      toast.success('Permission deleted successfully');
      navigate('/permissions');
    } catch (error) {
      console.error('Error deleting permission:', error);
      toast.error('Failed to delete permission. It may be in use by roles.');
    }
  };

  const isSystemPermission = () => {
    return permission && permission.is_system === 1;
  };

  if (!canViewPermission) {
    return (
      <Container fluid>
        <Card className="text-center p-5">
          <Card.Body>
            <div className="text-danger mb-3">
              <FaShieldAlt size={48} />
            </div>
            <h3>Access Restricted</h3>
            <p className="text-muted">
              You don't have permission to view permission details. Please contact your administrator if you 
              believe you should have access.
            </p>
          </Card.Body>
        </Card>
      </Container>
    );
  }

  if (loading) {
    return (
      <Container className="mt-4 text-center">
        <div className="spinner-border text-primary" role="status">
          <span className="visually-hidden">Loading...</span>
        </div>
        <p className="mt-2">Loading permission details...</p>
      </Container>
    );
  }

  if (error) {
    return (
      <Container className="mt-4">
        <Alert variant="danger">{error}</Alert>
        <Button as={Link} to="/permissions" variant="primary">
          <FaArrowLeft className="me-2" /> Back to Permissions
        </Button>
      </Container>
    );
  }

  if (!permission) {
    return (
      <Container className="mt-4">
        <Alert variant="warning">Permission not found.</Alert>
        <Button as={Link} to="/permissions" variant="primary">
          <FaArrowLeft className="me-2" /> Back to Permissions
        </Button>
      </Container>
    );
  }

  return (
    <Container fluid>
      <Row className="mb-4">
        <Col>
          <Button as={Link} to="/permissions" variant="light" className="mb-3">
            <FaArrowLeft className="me-2" /> Back to Permissions
          </Button>
          <h2>Permission Details</h2>
        </Col>
        <Col xs="auto">
          {canEditPermission && !isSystemPermission() && (
            <Button 
              as={Link} 
              to={`/permissions/edit/${permission.permission_id}`}
              variant="primary"
              className="me-2"
            >
              <FaEdit className="me-2" /> Edit
            </Button>
          )}
          
          {canDeletePermission && !isSystemPermission() && (
            <Button 
              variant="danger"
              onClick={() => setShowDeleteModal(true)}
            >
              <FaTrash className="me-2" /> Delete
            </Button>
          )}
        </Col>
      </Row>
      
      {isSystemPermission() && canEditPermission && (
        <Alert variant="info">
          <FaShieldAlt className="me-2" />
          This is a system permission. It cannot be modified or deleted.
        </Alert>
      )}
      
      <Row>
        <Col lg={4}>
          <Card className="mb-4">
            <Card.Header className="bg-white">
              <h5 className="mb-0">Permission Information</h5>
            </Card.Header>
            <Card.Body>
              <ListGroup variant="flush">
                <ListGroup.Item>
                  <div className="fw-bold">Name</div>
                  <div>{permission.name}</div>
                </ListGroup.Item>
                <ListGroup.Item>
                  <div className="fw-bold">Code</div>
                  <div>
                    <code>{permission.code}</code>
                    {permission.is_system === 1 && (
                      <Badge bg="secondary" className="ms-2">System</Badge>
                    )}
                  </div>
                </ListGroup.Item>
                <ListGroup.Item>
                  <div className="fw-bold">Category</div>
                  <div>{permission.category || 'Uncategorized'}</div>
                </ListGroup.Item>
                <ListGroup.Item>
                  <div className="fw-bold">Created At</div>
                  <div>{new Date(permission.created_at).toLocaleString()}</div>
                </ListGroup.Item>
                {permission.updated_at && (
                  <ListGroup.Item>
                    <div className="fw-bold">Updated At</div>
                    <div>{new Date(permission.updated_at).toLocaleString()}</div>
                  </ListGroup.Item>
                )}
              </ListGroup>
            </Card.Body>
          </Card>
        </Col>
        
        <Col lg={8}>
          <Card className="mb-4">
            <Card.Header className="bg-white">
              <h5 className="mb-0">Description</h5>
            </Card.Header>
            <Card.Body>
              {permission.description ? (
                <p>{permission.description}</p>
              ) : (
                <p className="text-muted">No description available.</p>
              )}
            </Card.Body>
          </Card>
          
          <Card>
            <Card.Header className="bg-white">
              <h5 className="mb-0">
                <FaUsers className="me-2" /> Roles Using This Permission
              </h5>
            </Card.Header>
            <Card.Body>
              {rolesWithPermission.length > 0 ? (
                <ListGroup>
                  {rolesWithPermission.map(role => (
                    <ListGroup.Item 
                      key={role.role_id}
                      className="d-flex justify-content-between align-items-center"
                    >
                      <div>
                        <h6 className="mb-0">{role.name}</h6>
                        <small className="text-muted">{role.description}</small>
                      </div>
                      <Button 
                        as={Link} 
                        to={`/roles/${role.role_id}`}
                        variant="outline-primary" 
                        size="sm"
                      >
                        View
                      </Button>
                    </ListGroup.Item>
                  ))}
                </ListGroup>
              ) : (
                <Alert variant="info">
                  This permission is not assigned to any roles.
                </Alert>
              )}
            </Card.Body>
          </Card>
        </Col>
      </Row>
      
      <ConfirmModal 
        show={showDeleteModal}
        title="Delete Permission"
        message={`Are you sure you want to delete the permission "${permission.name}"? This action cannot be undone.`}
        confirmText="Delete"
        confirmVariant="danger"
        onConfirm={handleDelete}
        onCancel={() => setShowDeleteModal(false)}
      />
    </Container>
  );
};

export default PermissionDetails;
