import React, { useState, useEffect } from 'react';
import { Container, Row, Col, Card, Form, Button, Alert } from 'react-bootstrap';
import { Link, useNavigate } from 'react-router-dom';
import { Formik } from 'formik';
import * as Yup from 'yup';
import { FaUser, FaArrowLeft, FaSave } from 'react-icons/fa';
import { toast } from 'react-toastify';
import { userAPI, roleAPI } from '../../services/api';

const UserCreate = () => {
  const [roles, setRoles] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const navigate = useNavigate();

  useEffect(() => {
    const fetchRoles = async () => {
      try {
        const response = await roleAPI.getRoles();
        console.log('Roles API response:', response.data);
        
        // Handle different response structures
        if (response.data && Array.isArray(response.data)) {
          setRoles(response.data);
        } else if (response.data && Array.isArray(response.data.roles)) {
          setRoles(response.data.roles);
        } else {
          console.error('Unexpected roles data structure:', response.data);
          setRoles([]);
        }
      } catch (error) {
        console.error('Error fetching roles:', error);
        toast.error('Failed to load roles');
      }
    };

    fetchRoles();
  }, []);

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
      .required('Password is required')
      .min(8, 'Password must be at least 8 characters')
      .matches(/[a-z]/, 'Password must contain at least one lowercase letter')
      .matches(/[A-Z]/, 'Password must contain at least one uppercase letter')
      .matches(/[0-9]/, 'Password must contain at least one number')
      .matches(/[@$!%*?&]/, 'Password must contain at least one special character'),
    confirmPassword: Yup.string()
      .oneOf([Yup.ref('password'), null], 'Passwords must match')
      .required('Confirm password is required'),
    roleIds: Yup.array()
      .min(1, 'At least one role must be assigned')
      .required('Role selection is required'),
    isActive: Yup.boolean()
  });

  const handleSubmit = async (values, { setSubmitting }) => {
    try {
      setError('');
      setLoading(true);
      
      const userData = {
        first_name: values.firstName,
        last_name: values.lastName,
        email: values.email,
        mobile_number: values.mobileNumber,
        mobile_number: values.mobileNumber,
        password: values.password,
        mobile_number: values.email, // Add mobile_number field as required by backend
        roles: values.roleIds,       // Changed from role_ids to roles to match backend expectation
        is_active: values.isActive
      };

      await userAPI.createUser(userData);
      toast.success('User created successfully');
      navigate('/users');
    } catch (error) {
      console.error('Error creating user:', error);
      setError(error.response?.data?.error || 'Failed to create user. Please try again.');
      toast.error('Failed to create user');
    } finally {
      setLoading(false);
      setSubmitting(false);
    }
  };

  return (
    <Container fluid>
      <Row className="mb-4">
        <Col>
          <Button as={Link} to="/users" variant="light" className="mb-3">
            <FaArrowLeft className="me-2" /> Back to Users
          </Button>
          <h2>Create New User</h2>
        </Col>
      </Row>

      <Row>
        <Col lg={8}>
          <Card>
            <Card.Body>
              {error && <Alert variant="danger">{error}</Alert>}

              <Formik
                initialValues={{
                  firstName: '',
                  lastName: '',
                  email: '',
    mobileNumber: '',
                  mobileNumber: '',
                  password: '',
                  confirmPassword: '',
                  roleIds: [],
    isActive: true
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

                    <Row>
                      <Col md={6}>
                        <Form.Group className="mb-3">
                          <Form.Label>Password</Form.Label>
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
                          <Form.Label>Confirm Password</Form.Label>
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
                        <p className="text-muted">Loading roles...</p>
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
                        to="/users"
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
                            <FaSave className="me-2" /> Create User
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
                  <FaUser size={40} className="text-primary" />
                </div>
                <h5>New User Information</h5>
                <p className="text-muted">Fill in the form to create a new user account.</p>
              </div>
              <hr />
              <h6>Instructions:</h6>
              <ul className="small text-muted">
                <li>All fields marked with an asterisk (*) are required.</li>
                <li>Email address must be unique in the system.</li>
                <li>Password must meet security requirements.</li>
                <li>At least one role must be assigned to the user.</li>
                <li>By default, new users are set as active.</li>
              </ul>
            </Card.Body>
          </Card>
        </Col>
      </Row>
    </Container>
  );
};

export default UserCreate;
