import React, { useState } from 'react';
import { Container, Card, Form, Button, Alert, Row, Col } from 'react-bootstrap';
import { Link, useNavigate } from 'react-router-dom';
import { Formik } from 'formik';
import * as Yup from 'yup';
import { useAuth } from '../../contexts/AuthContext';
import { FaUser, FaEnvelope, FaKey, FaUserPlus } from 'react-icons/fa';

const Register = () => {
  const [registerError, setRegisterError] = useState('');
  const [registerSuccess, setRegisterSuccess] = useState('');
  const [loading, setLoading] = useState(false);
  const { register } = useAuth();
  const navigate = useNavigate();

  // Validation schema for registration form
  const validationSchema = Yup.object({
    firstName: Yup.string()
      .required('First name is required'),
    lastName: Yup.string()
      .required('Last name is required'),
    email: Yup.string()
      .email('Invalid email address')
      .required('Email is required'),
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
  });

  const handleSubmit = async (values) => {
    try {
      setRegisterError('');
      setRegisterSuccess('');
      setLoading(true);
      
      const userData = {
        first_name: values.firstName,
        last_name: values.lastName,
        email: values.email,
        password: values.password,
      };
      
      const result = await register(userData);
      
      if (result.success) {
        setRegisterSuccess('Registration successful! Please check your email for confirmation.');
        // Redirect to login page after 2 seconds
        setTimeout(() => {
          navigate('/login');
        }, 2000);
      } else {
        setRegisterError(result.message || 'Registration failed. Please try again.');
      }
    } catch (error) {
      setRegisterError('An unexpected error occurred. Please try again.');
      console.error('Registration error:', error);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="auth-page">
      <Container>
        <Row className="justify-content-center">
          <Col md={8} lg={6} xl={5}>
            <Card className="auth-card shadow-lg">
              <Card.Body className="p-4">
                <div className="text-center mb-4">
                  <h2>Create Account</h2>
                  <p className="text-muted">Sign up to get started with EmployDEX</p>
                </div>
                
                {registerError && <Alert variant="danger">{registerError}</Alert>}
                {registerSuccess && <Alert variant="success">{registerSuccess}</Alert>}
                
                <Formik
                  initialValues={{
                    firstName: '',
                    lastName: '',
                    email: '',
                    password: '',
                    confirmPassword: ''
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
                    isSubmitting
                  }) => (
                    <Form onSubmit={handleSubmit}>
                      <Row>
                        <Col md={6}>
                          <Form.Group className="mb-3">
                            <Form.Label>
                              <FaUser className="me-2" />
                              First Name
                            </Form.Label>
                            <Form.Control
                              type="text"
                              name="firstName"
                              placeholder="Enter first name"
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
                            <Form.Label>
                              <FaUser className="me-2" />
                              Last Name
                            </Form.Label>
                            <Form.Control
                              type="text"
                              name="lastName"
                              placeholder="Enter last name"
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
                        <Form.Label>
                          <FaEnvelope className="me-2" />
                          Email Address
                        </Form.Label>
                        <Form.Control
                          type="email"
                          name="email"
                          placeholder="Enter email"
                          value={values.email}
                          onChange={handleChange}
                          onBlur={handleBlur}
                          isInvalid={touched.email && errors.email}
                        />
                        <Form.Control.Feedback type="invalid">
                          {errors.email}
                        </Form.Control.Feedback>
                      </Form.Group>
                      
                      <Form.Group className="mb-3">
                        <Form.Label>
                          <FaKey className="me-2" />
                          Password
                        </Form.Label>
                        <Form.Control
                          type="password"
                          name="password"
                          placeholder="Enter password"
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
                      
                      <Form.Group className="mb-4">
                        <Form.Label>
                          <FaKey className="me-2" />
                          Confirm Password
                        </Form.Label>
                        <Form.Control
                          type="password"
                          name="confirmPassword"
                          placeholder="Confirm password"
                          value={values.confirmPassword}
                          onChange={handleChange}
                          onBlur={handleBlur}
                          isInvalid={touched.confirmPassword && errors.confirmPassword}
                        />
                        <Form.Control.Feedback type="invalid">
                          {errors.confirmPassword}
                        </Form.Control.Feedback>
                      </Form.Group>
                      
                      <div className="d-grid mb-4">
                        <Button 
                          variant="primary" 
                          type="submit" 
                          disabled={loading || registerSuccess}
                        >
                          {loading ? (
                            <>
                              <span className="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>
                              Registering...
                            </>
                          ) : (
                            <>
                              <FaUserPlus className="me-2" />
                              Register
                            </>
                          )}
                        </Button>
                      </div>
                    </Form>
                  )}
                </Formik>
                
                <div className="text-center mt-3">
                  <p className="text-muted">
                    Already have an account? <Link to="/login">Sign in</Link>
                  </p>
                </div>
              </Card.Body>
            </Card>
            
            <div className="text-center mt-3 text-muted">
              <small>&copy; 2025 EmployDEX. All rights reserved.</small>
            </div>
          </Col>
        </Row>
      </Container>
    </div>
  );
};

export default Register;
