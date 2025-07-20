import React, { useState } from 'react';
import { Container, Card, Form, Button, Alert, Row, Col } from 'react-bootstrap';
import { Link, useParams, useNavigate } from 'react-router-dom';
import { Formik } from 'formik';
import * as Yup from 'yup';
import { useAuth } from '../../contexts/AuthContext';
import { FaKey, FaCheck } from 'react-icons/fa';

const ResetPassword = () => {
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');
  const [loading, setLoading] = useState(false);
  const { resetPassword } = useAuth();
  const { token } = useParams();
  const navigate = useNavigate();

  // Validation schema
  const validationSchema = Yup.object({
    password: Yup.string()
      .required('Password is required')
      .min(8, 'Password must be at least 8 characters')
      .matches(/[a-z]/, 'Password must contain at least one lowercase letter')
      .matches(/[A-Z]/, 'Password must contain at least one uppercase letter')
      .matches(/[0-9]/, 'Password must contain at least one number')
      .matches(/[@$!%*?&]/, 'Password must contain at least one special character'),
    confirmPassword: Yup.string()
      .oneOf([Yup.ref('password'), null], 'Passwords must match')
      .required('Confirm password is required')
  });

  const handleSubmit = async (values) => {
    try {
      setError('');
      setSuccess('');
      setLoading(true);
      
      const result = await resetPassword(token, values.password);
      
      if (result.success) {
        setSuccess(result.message || 'Password has been reset successfully.');
        // Redirect to login page after successful password reset
        setTimeout(() => {
          navigate('/login');
        }, 2000);
      } else {
        setError(result.message || 'Failed to reset password. The link may be expired or invalid.');
      }
    } catch (error) {
      setError('An unexpected error occurred. Please try again.');
      console.error('Reset password error:', error);
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
                  <h2>Set New Password</h2>
                  <p className="text-muted">Enter your new password below</p>
                </div>
                
                {error && <Alert variant="danger">{error}</Alert>}
                {success && <Alert variant="success">{success}</Alert>}
                
                <Formik
                  initialValues={{
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
                      <Form.Group className="mb-3">
                        <Form.Label>
                          <FaKey className="me-2" />
                          New Password
                        </Form.Label>
                        <Form.Control
                          type="password"
                          name="password"
                          placeholder="Enter new password"
                          value={values.password}
                          onChange={handleChange}
                          onBlur={handleBlur}
                          isInvalid={touched.password && errors.password}
                          disabled={success}
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
                          Confirm New Password
                        </Form.Label>
                        <Form.Control
                          type="password"
                          name="confirmPassword"
                          placeholder="Confirm new password"
                          value={values.confirmPassword}
                          onChange={handleChange}
                          onBlur={handleBlur}
                          isInvalid={touched.confirmPassword && errors.confirmPassword}
                          disabled={success}
                        />
                        <Form.Control.Feedback type="invalid">
                          {errors.confirmPassword}
                        </Form.Control.Feedback>
                      </Form.Group>
                      
                      <div className="d-grid mb-4">
                        <Button 
                          variant="primary" 
                          type="submit" 
                          disabled={loading || success}
                        >
                          {loading ? (
                            <>
                              <span className="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>
                              Resetting...
                            </>
                          ) : (
                            <>
                              <FaCheck className="me-2" />
                              Reset Password
                            </>
                          )}
                        </Button>
                      </div>
                    </Form>
                  )}
                </Formik>
                
                <div className="text-center mt-3">
                  <p className="text-muted">
                    Remember your password? <Link to="/login">Back to Login</Link>
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

export default ResetPassword;
