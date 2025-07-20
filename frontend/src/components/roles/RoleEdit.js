import React, { useState, useEffect } from 'react';
import { Container, Row, Col, Card, Form, Button, Alert } from 'react-bootstrap';
import { Link, useParams, useNavigate } from 'react-router-dom';
import { Formik } from 'formik';
import * as Yup from 'yup';
import { FaUserTag, FaArrowLeft, FaSave, FaShieldAlt } from 'react-icons/fa';
import { toast } from 'react-toastify';
import { roleAPI, permissionAPI } from '../../services/api';

const RoleEdit = () => {
  const { id } = useParams();
  const navigate = useNavigate();
  
  const [role, setRole] = useState(null);
  const [permissions, setPermissions] = useState([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');

  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true);
        
        // Fetch role details
        const roleResponse = await roleAPI.getRole(id);
        setRole(roleResponse.data);
        
        // Fetch all permissions
        const permissionsResponse = await permissionAPI.getPermissions();
        setPermissions(permissionsResponse.data);
        
      } catch (error) {
        console.error('Error fetching data:', error);
        setError('Failed to load role data. Please try again.');
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, [id]);

  // Group permissions by category
  const groupedPermissions = permissions.reduce((acc, permission) => {
    const category = permission.name.split('_')[0]; // Assuming permissions are named like "user_create"
    if (!acc[category]) {
      acc[category] = [];
    }
    acc[category].push(permission);
    return acc;
  }, {});

  // Validation schema
  const validationSchema = Yup.object({
    name: Yup.string()
      .required('Role name is required')
      .max(50, 'Role name must be at most 50 characters'),
    description: Yup.string()
      .required('Description is required')
      .max(255, 'Description must be at most 255 characters'),
    permissionIds: Yup.array()
      .min(1, 'At least one permission must be assigned')
  });

  const handleSubmit = async (values, { setSubmitting }) => {
    // Check if this is a system role with name changes
    if (role && ['Admin', 'System'].includes(role.name) && values.name !== role.name) {
      toast.error(`Cannot change the name of system role "${role.name}"`);
      setSubmitting(false);
      return;
    }
    
    try {
      setSaving(true);
      setError('');
      
      const roleData = {
        name: values.name,
        description: values.description,
        permission_ids: values.permissionIds
      };

      await roleAPI.updateRole(id, roleData);
      toast.success('Role updated successfully');
      navigate(`/roles/${id}`);
    } catch (error) {
      console.error('Error updating role:', error);
      setError(error.response?.data?.error || 'Failed to update role. Please try again.');
      toast.error('Failed to update role');
    } finally {
      setSaving(false);
      setSubmitting(false);
    }
  };

  if (loading) {
    return (
      <Container className="mt-4 text-center">
        <div className="spinner-border text-primary" role="status">
          <span className="visually-hidden">Loading...</span>
        </div>
        <p className="mt-2">Loading role information...</p>
      </Container>
    );
  }

  if (!role) {
    return (
      <Container className="mt-4">
        <Alert variant="danger">Role not found or you don't have permission to edit this role.</Alert>
        <Button as={Link} to="/roles" variant="primary">
          <FaArrowLeft className="me-2" /> Back to Roles
        </Button>
      </Container>
    );
  }

  const isSystemRole = ['Admin', 'System'].includes(role.name);
  
  // Map role permissions to permission IDs
  const initialPermissionIds = role.permissions 
    ? role.permissions.map(permission => permission.permission_id) 
    : [];

  return (
    <Container fluid>
      <Row className="mb-4">
        <Col>
          <Button as={Link} to={`/roles/${id}`} variant="light" className="mb-3">
            <FaArrowLeft className="me-2" /> Back to Role Details
          </Button>
          <h2>Edit Role</h2>
        </Col>
      </Row>

      <Row>
        <Col lg={8}>
          <Card>
            <Card.Body>
              {error && <Alert variant="danger">{error}</Alert>}
              
              {isSystemRole && (
                <Alert variant="warning" className="mb-4">
                  <strong>Warning:</strong> You are editing a system role. Modifications may affect system functionality.
                </Alert>
              )}

              <Formik
                initialValues={{
                  name: role.name || '',
                  description: role.description || '',
                  permissionIds: initialPermissionIds
                }}
                validationSchema={validationSchema}
                onSubmit={handleSubmit}
                enableReinitialize
              >
                {({
                  values,
                  errors,
                  touched,
                  handleChange,
                  handleBlur,
                  handleSubmit,
                  setFieldValue,
                  isSubmitting
                }) => (
                  <Form onSubmit={handleSubmit}>
                    <Form.Group className="mb-3">
                      <Form.Label>Role Name</Form.Label>
                      <Form.Control
                        type="text"
                        name="name"
                        value={values.name}
                        onChange={handleChange}
                        onBlur={handleBlur}
                        isInvalid={touched.name && errors.name}
                        disabled={isSystemRole}
                      />
                      {isSystemRole && (
                        <Form.Text className="text-muted">
                          System role names cannot be changed.
                        </Form.Text>
                      )}
                      <Form.Control.Feedback type="invalid">
                        {errors.name}
                      </Form.Control.Feedback>
                    </Form.Group>

                    <Form.Group className="mb-3">
                      <Form.Label>Description</Form.Label>
                      <Form.Control
                        as="textarea"
                        name="description"
                        value={values.description}
                        onChange={handleChange}
                        onBlur={handleBlur}
                        isInvalid={touched.description && errors.description}
                        rows={3}
                      />
                      <Form.Control.Feedback type="invalid">
                        {errors.description}
                      </Form.Control.Feedback>
                    </Form.Group>

                    <h5 className="mt-4 mb-3">Manage Permissions</h5>
                    {touched.permissionIds && errors.permissionIds && (
                      <Alert variant="danger" className="py-2">
                        {errors.permissionIds}
                      </Alert>
                    )}
                    
                    {Object.keys(groupedPermissions).length > 0 ? (
                      <div className="mb-4">
                        {Object.entries(groupedPermissions).map(([category, categoryPermissions]) => (
                          <div key={category} className="mb-4">
                            <h6 className="text-capitalize mb-3">{category} Permissions</h6>
                            <div className="d-flex flex-wrap mb-2">
                              <Button
                                variant="outline-secondary"
                                size="sm"
                                className="me-2 mb-2"
                                onClick={() => {
                                  const categoryIds = categoryPermissions.map(p => p.permission_id);
                                  const allSelected = categoryPermissions.every(p => 
                                    values.permissionIds.includes(p.permission_id)
                                  );
                                  
                                  if (allSelected) {
                                    // If all already selected, deselect all in category
                                    setFieldValue(
                                      'permissionIds',
                                      values.permissionIds.filter(id => !categoryIds.includes(id))
                                    );
                                  } else {
                                    // Otherwise, select all in category
                                    const newIds = [...values.permissionIds];
                                    categoryPermissions.forEach(p => {
                                      if (!newIds.includes(p.permission_id)) {
                                        newIds.push(p.permission_id);
                                      }
                                    });
                                    setFieldValue('permissionIds', newIds);
                                  }
                                }}
                              >
                                {categoryPermissions.every(p => values.permissionIds.includes(p.permission_id))
                                  ? 'Deselect All'
                                  : 'Select All'}
                              </Button>
                            </div>
                            <div className="d-flex flex-wrap">
                              {categoryPermissions.map(permission => (
                                <div key={permission.permission_id} className="me-3 mb-3">
                                  <Form.Check
                                    type="checkbox"
                                    id={`permission-${permission.permission_id}`}
                                    label={<>
                                      <FaShieldAlt className="me-1" /> 
                                      <span className="text-capitalize">{permission.name.replace(`${category}_`, '')}</span>
                                    </>}
                                    checked={values.permissionIds.includes(permission.permission_id)}
                                    onChange={(e) => {
                                      if (e.target.checked) {
                                        setFieldValue('permissionIds', [...values.permissionIds, permission.permission_id]);
                                      } else {
                                        setFieldValue(
                                          'permissionIds',
                                          values.permissionIds.filter(id => id !== permission.permission_id)
                                        );
                                      }
                                    }}
                                    // For Admin role, some critical permissions cannot be removed
                                    disabled={isSystemRole && role.name === 'Admin' && 
                                      ['role_create', 'role_update', 'role_delete', 'permission_view'].includes(permission.name)}
                                  />
                                </div>
                              ))}
                            </div>
                          </div>
                        ))}
                      </div>
                    ) : (
                      <Alert variant="info">Loading permissions...</Alert>
                    )}

                    <div className="d-grid gap-2 d-md-flex justify-content-md-end">
                      <Button
                        variant="secondary"
                        as={Link}
                        to={`/roles/${id}`}
                        className="me-md-2"
                      >
                        Cancel
                      </Button>
                      <Button
                        variant="primary"
                        type="submit"
                        disabled={isSubmitting || saving}
                      >
                        {saving ? (
                          <>
                            <span className="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>
                            Saving...
                          </>
                        ) : (
                          <>
                            <FaSave className="me-2" /> Save Changes
                          </>
                        )}
                      </Button>
                    </div>
                  </Form>
                )}
              </Formik>
            </Card.Body>
          </Card>
        </Col>

        <Col lg={4}>
          <Card>
            <Card.Body>
              <div className="text-center mb-4">
                <div className="rounded-circle bg-light p-3 d-inline-flex mb-3">
                  <FaUserTag size={40} className="text-primary" />
                </div>
                <h5>{role.name}</h5>
                <p className="text-muted">{role.description}</p>
              </div>
              <hr />
              <h6>Edit Instructions:</h6>
              <ul className="small text-muted">
                <li>All fields marked with an asterisk (*) are required.</li>
                <li>System role names cannot be modified, but their descriptions and permissions can be adjusted.</li>
                <li>The Admin role must maintain certain critical permissions to ensure system functionality.</li>
                <li>Changes to permissions will affect all users assigned to this role.</li>
              </ul>
              <hr />
              <div className="small text-muted">
                <strong>Role ID:</strong> {role.role_id}<br />
                <strong>Created:</strong> {new Date(role.created_at).toLocaleDateString()}<br />
                <strong>Current Permissions:</strong> {initialPermissionIds.length}
              </div>
            </Card.Body>
          </Card>
        </Col>
      </Row>
    </Container>
  );
};

export default RoleEdit;
