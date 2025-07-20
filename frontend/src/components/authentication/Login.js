import React, { useState } from 'react';
import { Container, Card, Form, Button, Alert, Row, Col } from 'react-bootstrap';
import { Link, useNavigate, useLocation } from 'react-router-dom';
import { Formik } from 'formik';
import * as Yup from 'yup';
import { useAuth } from '../../contexts/AuthContext';
import { FaUser, FaKey, FaSignInAlt } from 'react-icons/fa';

const Login = () => {
  const [loginError, setLoginError] = useState('');
  const [loading, setLoading] = useState(false);
  const { login } = useAuth();
  const navigate = useNavigate();
  const location = useLocation();
  
  // Get the redirect path from the location state or default to dashboard
  const from = location.state?.from || '/dashboard';

  // Validation schema for login form
  const validationSchema = Yup.object({
    username: Yup.string()
      .required('Username is required'),
    password: Yup.string()
      .required('Password is required')
  });

  const handleSubmit = async (values) => {
    try {
      setLoginError('');
      setLoading(true);
      
      const result = await login(values.username, values.password);
      
      if (result.success) {
        navigate(from);
      } else {
        setLoginError(result.message || 'Login failed. Please check your credentials.');
      }
    } catch (error) {
      setLoginError('An unexpected error occurred. Please try again.');
      console.error('Login error:', error);
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
                  <h2>EmployDEX</h2>
                  <p className="text-muted">Sign in to continue to the dashboard</p>
                </div>
                
                {loginError && <Alert variant="danger">{loginError}</Alert>}
                
                <Formik
                  initialValues={{ username: '', password: '' }}
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
                      <Form.Group className="mb-3">
                        <Form.Label>
                          <FaUser className="me-2" />
                          Username
                        </Form.Label>
                        <Form.Control
                          type="text"
                          name="username"
                          placeholder="Enter username"
                          value={values.username}
                          onChange={handleChange}
                          onBlur={handleBlur}
                          isInvalid={touched.username && errors.username}
                        />
                        <Form.Control.Feedback type="invalid">
                          {errors.username}
                        </Form.Control.Feedback>
                      </Form.Group>
                      
                      <Form.Group className="mb-4">
                        <div className="d-flex justify-content-between">
                          <Form.Label>
                            <FaKey className="me-2" />
                            Password
                          </Form.Label>
                          <Link to="/forgot-password" className="text-muted small">Forgot password?</Link>
                        </div>
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
                      </Form.Group>
                      
                      <div className="d-grid mb-4">
                        <Button 
                          variant="primary" 
                          type="submit" 
                          disabled={loading}
                        >
                          {loading ? (
                            <>
                              <span className="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>
                              Signing in...
                            </>
                          ) : (
                            <>
                              <FaSignInAlt className="me-2" />
                              Sign In
                            </>
                          )}
                        </Button>
                      </div>
                    </Form>
                  )}
                </Formik>
                
                <div className="text-center mt-3">
                  <p className="text-muted">
                    Don't have an account? <Link to="/register">Sign up</Link>
                  </p>
                </div>
              </Card.Body>
            </Card>
            
            <div className="text-center mt-3 text-muted">
              <small>&copy; 2025 HiringTests. All rights reserved.</small>
            </div>
          </Col>
        </Row>
      </Container>
    </div>
  );
};

export default Login;
