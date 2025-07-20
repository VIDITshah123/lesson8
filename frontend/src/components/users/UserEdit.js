import React, { useState, useEffect } from 'react';
import { Container, Row, Col, Card, Form, Button, Alert } from 'react-bootstrap';
import { Link, useParams, useNavigate } from 'react-router-dom';
import { Formik } from 'formik';
import * as Yup from 'yup';
import { FaUser, FaArrowLeft, FaSave } from 'react-icons/fa';
import { toast } from 'react-toastify';
import { userAPI, roleAPI } from '../../services/api';

const UserEdit = () => {
  const { id } = useParams();
  const navigate = useNavigate();
  
  const [user, setUser] = useState(null);
  const [roles, setRoles] = useState([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');

  useEffect(() => {
    const fetchUserAndRoles = async () => {
      try {
        setLoading(true);
        
        // Fetch user details
        const userResponse = await userAPI.getUser(id);
        setUser(userResponse.data);
        
        // Fetch all roles
        const rolesResponse = await roleAPI.getRoles();
        if (rolesResponse.data && Array.isArray(rolesResponse.data)) {
          setRoles(rolesResponse.data);
        } else if (rolesResponse.data && Array.isArray(rolesResponse.data.roles)) {
          setRoles(rolesResponse.data.roles);
        } else {
          setRoles([]);
        }
        
      } catch (error) {
        console.error('Error fetching data:', error);
        setError('Failed to load user data. Please try again.');
      } finally {
        setLoading(false);
      }
    };

    fetchUserAndRoles();
  }, [id]);

  // Validation schema
  const validationSchema = Yup.object({
    firstName: Yup.string()
      .required('First name is required')
      .max(50, 'First name must be at most 50 characters'),
    lastName: Yup.string()
      .required('Last name is required')
      .max(50, 'Last name must be at most 50 characters'),
    mobileNumber: Yup.string()
      .required('Mobile number is required')
      .matches(/^\d{10}$/, 'Mobile number must be 10 digits'),
    email: Yup.string()
      .email('Invalid email address')
      .required('Email is required')
      .max(100, 'Email must be at most 100 characters'),
    password: Yup.string()
      .nullable()
      .test('password', 'Password must be at least 8 characters', function(value) {
        // Skip validation if password is empty (no password change)
        if (!value) return true;
        
        return value.length >= 8;
      })
      .test('password-lowercase', 'Password must contain at least one lowercase letter', function(value) {
        if (!value) return true;
        return /[a-z]/.test(value);
      })
      .test('password-uppercase', 'Password must contain at least one uppercase letter', function(value) {
        if (!value) return true;
        return /[A-Z]/.test(value);
      })
      .test('password-number', 'Password must contain at least one number', function(value) {
        if (!value) return true;
        return /[0-9]/.test(value);
      })
      .test('password-special', 'Password must contain at least one special character', function(value) {
        if (!value) return true;
        return /[@$!%*?&]/.test(value);
      }),
    confirmPassword: Yup.string()
      .test('passwords-match', 'Passwords must match', function(value) {
        return this.parent.password === value;
      }),
    roleIds: Yup.array()
      .min(1, 'At least one role must be assigned')
      .required('Role selection is required'),
    isActive: Yup.boolean()
  });

  const handleSubmit = async (values, { setSubmitting }) => {
    try {
      setSaving(true);
      setError('');
      
      // Prepare user data for update
      const userData = {
        first_name: values.firstName,
        last_name: values.lastName,
        email: values.email,
        mobile_number: values.mobileNumber,
        roles: values.roleIds, // Changed from role_ids to roles to match backend API
        is_active: values.isActive
      };
      
      // Only include password if it's provided (for password change)
      if (values.password) {
        userData.password = values.password;
      }

      await userAPI.updateUser(id, userData);
      toast.success('User updated successfully');
      navigate(`/users/${id}`);
    } catch (error) {
      console.error('Error updating user:', error);
      setError(error.response?.data?.error || 'Failed to update user. Please try again.');
      toast.error('Failed to update user');
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
        <p className="mt-2">Loading user information...</p>
      </Container>
    );
  }

  if (!user) {
    return (
      <Container className="mt-4">
        <Alert variant="danger">User not found or you don't have permission to edit this user.</Alert>
        <Button as={Link} to="/users" variant="primary">
          <FaArrowLeft className="me-2" /> Back to Users
        </Button>
      </Container>
    );
  }

  // Extract role IDs from user object
  const userRoleIds = user.roles ? user.roles.map(role => role.role_id) : [];

  const initialValues = {
    firstName: user.first_name || '',
    lastName: user.last_name || '',
    email: user.email || '',
    mobileNumber: user.mobile_number || '',
    password: '',
    confirmPassword: '',
    roleIds: userRoleIds,
    isActive: user.is_active
  };

  return (
    <Container fluid>
      <Row className="mb-4">
        <Col>
          <Button as={Link} to={`/users/${id}`} variant="light" className="mb-3">
            <FaArrowLeft className="me-2" /> Back to User Details
          </Button>
          <h2>Edit User</h2>
        </Col>
      </Row>

      <Row>
        <Col lg={8}>
          <Card>
            <Card.Body>
              {error && <Alert variant="danger">{error}</Alert>}

              <Formik
                initialValues={initialValues}
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
                    <Row>
                      <Col md={6}>
                        <Form.Group className="mb-3">
                          <Form.Label>First Name</Form.Label>
                          <Form.Control
                            type="text"
                            name="firstName"
                            value={values.firstName}
                            onChange={handleChange}
                            onBlur={handleBlur}
                            isInvalid={touched.firstName && errors.firstName}
                          />
                          <Form.Control.Feedback type="invalid">
                            {errors.firstName}
                          </Form.Control.Feedback>
                        </Form.Group>
                      </Col>
                      <Col md={6}>
                        <Form.Group className="mb-3">
                          <Form.Label>Mobile Number</Form.Label>
                          <Form.Control
                            type="text"
                            name="mobileNumber"
                            value={values.mobileNumber}
                            onChange={handleChange}
                            onBlur={handleBlur}
                            isInvalid={touched.mobileNumber && errors.mobileNumber}
                          />
                          <Form.Control.Feedback type="invalid">
                            {errors.mobileNumber}
                          </Form.Control.Feedback>
                        </Form.Group>
                      </Col>
                      <Col md={6}>
                        <Form.Group className="mb-3">
                          <Form.Label>Last Name</Form.Label>
                          <Form.Control
                            type="text"
                            name="lastName"
                            value={values.lastName}
                            onChange={handleChange}
                            onBlur={handleBlur}
                            isInvalid={touched.lastName && errors.lastName}
                          />
                          <Form.Control.Feedback type="invalid">
                            {errors.lastName}
                          </Form.Control.Feedback>
                        </Form.Group>
                      </Col>
                    </Row>

                    <Form.Group className="mb-3">
                      <Form.Label>Email Address</Form.Label>
                      <Form.Control
                        type="email"
                        name="email"
                        value={values.email}
                        onChange={handleChange}
                        onBlur={handleBlur}
                        isInvalid={touched.email && errors.email}
                      />
                      <Form.Control.Feedback type="invalid">
                        {errors.email}
                      </Form.Control.Feedback>
                    </Form.Group>

                    <Card className="mb-4 bg-light">
                      <Card.Body>
                        <Card.Title as="h6">Change Password (Optional)</Card.Title>
                        <Card.Text className="text-muted mb-3">
                          Leave blank to keep the current password
                        </Card.Text>

                        <Row>
                          <Col md={6}>
                            <Form.Group className="mb-3">
                              <Form.Label>New Password</Form.Label>
                              <Form.Control
                                type="password"
                                name="password"
                                value={values.password}
                                onChange={handleChange}
                                onBlur={handleBlur}
                                isInvalid={touched.password && errors.password}
                              />
                              <Form.Control.Feedback type="invalid">
                                {errors.password}
                              </Form.Control.Feedback>
                              <Form.Text className="text-muted">
                                Password must be at least 8 characters and include lowercase, uppercase, 
                                number, and special character.
                              </Form.Text>
                            </Form.Group>
                          </Col>
                          <Col md={6}>
                            <Form.Group className="mb-3">
                              <Form.Label>Confirm New Password</Form.Label>
                              <Form.Control
                                type="password"
                                name="confirmPassword"
                                value={values.confirmPassword}
                                onChange={handleChange}
                                onBlur={handleBlur}
                                isInvalid={touched.confirmPassword && errors.confirmPassword}
                              />
                              <Form.Control.Feedback type="invalid">
                                {errors.confirmPassword}
                              </Form.Control.Feedback>
                            </Form.Group>
                          </Col>
                        </Row>
                      </Card.Body>
                    </Card>

                    <Form.Group className="mb-4">
                      <Form.Label>Assign Roles</Form.Label>
                      {roles.length > 0 ? (
                        <div>
                          {roles.map(role => (
                            <Form.Check
                              key={role.role_id}
                              type="checkbox"
                              id={`role-${role.role_id}`}
                              label={role.name}
                              checked={values.roleIds.includes(role.role_id)}
                              onChange={(e) => {
                                if (e.target.checked) {
                                  setFieldValue('roleIds', [...values.roleIds, role.role_id]);
                                } else {
                                  setFieldValue(
                                    'roleIds',
                                    values.roleIds.filter(id => id !== role.role_id)
                                  );
                                }
                              }}
                              isInvalid={touched.roleIds && errors.roleIds}
                              className="mb-2"
                            />
                          ))}
                          {touched.roleIds && errors.roleIds && (
                            <div className="text-danger small mt-1">{errors.roleIds}</div>
                          )}
                        </div>
                      ) : (
                        <p className="text-muted">No roles available</p>
                      )}
                    </Form.Group>

                    <Form.Group className="mb-4">
                      <Form.Check
                        type="switch"
                        id="is-active"
                        label="Active User"
                        checked={values.isActive}
                        onChange={(e) => setFieldValue('isActive', e.target.checked)}
                      />
                      <Form.Text className="text-muted">
                        Inactive users cannot log in to the system.
                      </Form.Text>
                    </Form.Group>

                    <div className="d-grid gap-2 d-md-flex justify-content-md-end">
                      <Button
                        variant="secondary"
                        as={Link}
                        to={`/users/${id}`}
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
                  <FaUser size={40} className="text-primary" />
                </div>
                <h5>{user.first_name} {user.last_name}</h5>
                <p className="text-muted">{user.email}</p>
              </div>
              <hr />
              <h6>Edit Instructions:</h6>
              <ul className="small text-muted">
                <li>All fields marked with an asterisk (*) are required.</li>
                <li>Leave the password fields blank to keep the current password.</li>
                <li>At least one role must be assigned to the user.</li>
                <li>Toggle the active status to enable or disable user login.</li>
                <li>System roles like "Admin" have special privileges.</li>
              </ul>
              <hr />
              <div className="small text-muted">
                <strong>User ID:</strong> {user.user_id}<br />
                <strong>Created:</strong> {new Date(user.created_at).toLocaleDateString()}<br />
                {user.last_login && (
                  <><strong>Last Login:</strong> {new Date(user.last_login).toLocaleString()}</>
                )}
              </div>
            </Card.Body>
          </Card>
        </Col>
      </Row>
    </Container>
  );
};

export default UserEdit;
