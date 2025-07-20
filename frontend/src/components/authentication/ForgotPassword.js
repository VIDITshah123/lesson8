import React, { useState } from 'react';
import { Container, Card, Form, Button, Alert, Row, Col } from 'react-bootstrap';
import { Link } from 'react-router-dom';
import { Formik } from 'formik';
import * as Yup from 'yup';
import { useAuth } from '../../contexts/AuthContext';
import { FaEnvelope, FaPaperPlane } from 'react-icons/fa';

const ForgotPassword = () => {
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');
  const [loading, setLoading] = useState(false);
  const { forgotPassword } = useAuth();

  // Validation schema
  const validationSchema = Yup.object({
    email: Yup.string()
      .email('Invalid email address')
      .required('Email is required')
  });

  const handleSubmit = async (values) => {
    try {
      setError('');
      setSuccess('');
      setLoading(true);
      
      const result = await forgotPassword(values.email);
      
      if (result.success) {
        setSuccess(result.message || 'Password reset link has been sent to your email.');
      } else {
        setError(result.message || 'Failed to send password reset email.');
      }
    } catch (error) {
      setError('An unexpected error occurred. Please try again.');
      console.error('Forgot password error:', error);
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
                  <h2>Reset Password</h2>
                  <p className="text-muted">Enter your email to receive a password reset link</p>
                </div>
                
                {error && <Alert variant="danger">{error}</Alert>}
                {success && <Alert variant="success">{success}</Alert>}
                
                <Formik
                  initialValues={{ email: '' }}
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
                      <Form.Group className="mb-4">
                        <Form.Label>
                          <FaEnvelope className="me-2" />
                          Email Address
                        </Form.Label>
                        <Form.Control
                          type="email"
                          name="email"
                          placeholder="Enter your registered email"
                          value={values.email}
                          onChange={handleChange}
                          onBlur={handleBlur}
                          isInvalid={touched.email && errors.email}
                        />
                        <Form.Control.Feedback type="invalid">
                          {errors.email}
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
                              Sending...
                            </>
                          ) : (
                            <>
                              <FaPaperPlane className="me-2" />
                              Send Reset Link
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

export default ForgotPassword;
