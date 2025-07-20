import React, { useState, useEffect } from 'react';
import { Container, Row, Col, Card, Form, Button, Alert } from 'react-bootstrap';
import { Link, useNavigate } from 'react-router-dom';
import { Formik } from 'formik';
import * as Yup from 'yup';
import { FaUserTag, FaArrowLeft, FaSave, FaShieldAlt } from 'react-icons/fa';
import { toast } from 'react-toastify';
import { roleAPI, permissionAPI } from '../../services/api';

const RoleCreate = () => {
  const [permissions, setPermissions] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const navigate = useNavigate();

  useEffect(() => {
    const fetchPermissions = async () => {
      try {
        const response = await permissionAPI.getPermissions();
        // Handle the nested structure where permissions are in response.data.permissions
        if (response.data && response.data.permissions) {
          setPermissions(response.data.permissions);
        } else {
          console.error('Unexpected API response format:', response.data);
          toast.error('Failed to load permissions: unexpected data format');
          setPermissions([]);
        }
      } catch (error) {
        console.error('Error fetching permissions:', error);
        toast.error('Failed to load permissions');
        setPermissions([]);
      }
    };

    fetchPermissions();
  }, []);

  // Group permissions by category
  const groupedPermissions = Array.isArray(permissions) ? permissions.reduce((acc, permission) => {
    const category = permission.name.split('_')[0]; // Assuming permissions are named like "user_create"
    if (!acc[category]) {
      acc[category] = [];
    }
    acc[category].push(permission);
    return acc;
  }, {}) : {};

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
    try {
      setError('');
      setLoading(true);
      
      const roleData = {
        name: values.name,
        description: values.description,
        permission_ids: values.permissionIds
      };

      await roleAPI.createRole(roleData);
      toast.success('Role created successfully');
      navigate('/roles');
    } catch (error) {
      console.error('Error creating role:', error);
      setError(error.response?.data?.error || 'Failed to create role. Please try again.');
      toast.error('Failed to create role');
    } finally {
      setLoading(false);
      setSubmitting(false);
    }
  };

  return (
    <Container fluid>
      <Row className="mb-4">
        <Col>
          <Button as={Link} to="/roles" variant="light" className="mb-3">
            <FaArrowLeft className="me-2" /> Back to Roles
          </Button>
          <h2>Create New Role</h2>
        </Col>
      </Row>

      <Row>
        <Col lg={8}>
          <Card>
            <Card.Body>
              {error && <Alert variant="danger">{error}</Alert>}

              <Formik
                initialValues={{
                  name: '',
                  description: '',
                  permissionIds: []
                }}
                validationSchema={validationSchema}
                onSubmit={handleSubmit}
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
                      />
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

                    <h5 className="mt-4 mb-3">Assign Permissions</h5>
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
                        to="/roles"
                        className="me-md-2"
                      >
                        Cancel
                      </Button>
                      <Button
                        variant="primary"
                        type="submit"
                        disabled={isSubmitting || loading}
                      >
                        {loading ? (
                          <>
                            <span className="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>
                            Creating...
                          </>
                        ) : (
                          <>
                            <FaSave className="me-2" /> Create Role
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
          <Card className="mb-4">
            <Card.Body>
              <div className="text-center mb-4">
                <div className="rounded-circle bg-light p-3 d-inline-flex mb-3">
                  <FaUserTag size={40} className="text-primary" />
                </div>
                <h5>New Role Information</h5>
                <p className="text-muted">Fill in the form to create a new role with permissions.</p>
              </div>
              <hr />
              <h6>Instructions:</h6>
              <ul className="small text-muted">
                <li>All fields marked with an asterisk (*) are required.</li>
                <li>Role names should be concise and descriptive.</li>
                <li>Assign appropriate permissions to the role based on its intended purpose.</li>
                <li>You can select/deselect all permissions in a category using the Select All button.</li>
                <li>Assigning too many permissions may create security risks.</li>
              </ul>
            </Card.Body>
          </Card>

          <Card>
            <Card.Header className="bg-white">
              <h5 className="mb-0">Permission Categories</h5>
            </Card.Header>
            <Card.Body>
              <div className="small">
                <p><strong>User:</strong> Manage user accounts and profiles</p>
                <p><strong>Role:</strong> Manage system roles and assignments</p>
                <p><strong>Permission:</strong> Manage granular system permissions</p>
                <p><strong>Dashboard:</strong> Access to analytics and reporting</p>
                <p><strong>System:</strong> Core system functionality access</p>
              </div>
            </Card.Body>
          </Card>
        </Col>
      </Row>
    </Container>
  );
};

export default RoleCreate;
