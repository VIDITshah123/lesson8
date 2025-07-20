import React, { useState, useEffect } from 'react';
import { Container, Row, Col, Card, Form, Button, Alert, Tab, Nav } from 'react-bootstrap';
import { Formik } from 'formik';
import * as Yup from 'yup';
import { toast } from 'react-toastify';
import { FaUser, FaKey, FaSave, FaShieldAlt, FaHistory, FaCog } from 'react-icons/fa';
import { userAPI, loggingAPI } from '../../services/api';
import { useAuth } from '../../contexts/AuthContext';

const UserProfile = () => {
  const { currentUser, updateUserProfile } = useAuth();
  const [loading, setLoading] = useState(false);
  const [userLogs, setUserLogs] = useState([]);
  const [logsLoading, setLogsLoading] = useState(false);

  // Fetch user activity logs
  useEffect(() => {
    const fetchUserLogs = async () => {
      if (!currentUser?.userId) return;
      
      try {
        setLogsLoading(true);
        const response = await loggingAPI.getActivityLogs({
          user_id: currentUser.userId,
          limit: 10
        });
        setUserLogs(response.data.logs || []);
      } catch (error) {
        console.error('Error fetching user logs:', error);
      } finally {
        setLogsLoading(false);
      }
    };

    fetchUserLogs();
  }, [currentUser?.userId]);

  // Profile form validation schema
  const profileSchema = Yup.object({
    firstName: Yup.string()
      .required('First name is required')
      .max(50, 'First name must be at most 50 characters'),
    lastName: Yup.string()
      .required('Last name is required')
      .max(50, 'Last name must be at most 50 characters'),
    email: Yup.string()
      .email('Invalid email address')
      .required('Email is required')
      .max(100, 'Email must be at most 100 characters'),
  });

  // Password form validation schema
  const passwordSchema = Yup.object({
    currentPassword: Yup.string()
      .required('Current password is required'),
    newPassword: Yup.string()
      .required('New password is required')
      .min(8, 'Password must be at least 8 characters')
      .matches(/[a-z]/, 'Password must contain at least one lowercase letter')
      .matches(/[A-Z]/, 'Password must contain at least one uppercase letter')
      .matches(/[0-9]/, 'Password must contain at least one number')
      .matches(/[@$!%*?&]/, 'Password must contain at least one special character'),
    confirmPassword: Yup.string()
      .oneOf([Yup.ref('newPassword'), null], 'Passwords must match')
      .required('Confirm password is required')
  });

  // Handle profile update
  const handleProfileUpdate = async (values, { setSubmitting, setFieldError }) => {
    try {
      setLoading(true);
      
      const userData = {
        first_name: values.firstName,
        last_name: values.lastName,
        email: values.email
      };

      const response = await userAPI.updateUserProfile(currentUser.userId, userData);
      
      // Update auth context with new user data
      updateUserProfile({
        ...currentUser,
        firstName: values.firstName,
        lastName: values.lastName,
        email: values.email
      });
      
      toast.success('Profile updated successfully');
    } catch (error) {
      console.error('Error updating profile:', error);
      
      if (error.response?.data?.error === 'Email already exists') {
        setFieldError('email', 'This email is already in use');
      } else {
        toast.error('Failed to update profile. Please try again.');
      }
    } finally {
      setLoading(false);
      setSubmitting(false);
    }
  };

  // Handle password change
  const handlePasswordChange = async (values, { setSubmitting, setFieldError, resetForm }) => {
    try {
      setLoading(true);
      
      const passwordData = {
        current_password: values.currentPassword,
        new_password: values.newPassword
      };

      await userAPI.changePassword(currentUser.userId, passwordData);
      toast.success('Password changed successfully');
      resetForm();
    } catch (error) {
      console.error('Error changing password:', error);
      
      if (error.response?.data?.error === 'Current password is incorrect') {
        setFieldError('currentPassword', 'Current password is incorrect');
      } else {
        toast.error('Failed to change password. Please try again.');
      }
    } finally {
      setLoading(false);
      setSubmitting(false);
    }
  };

  // Format timestamp to readable date and time
  const formatTimestamp = (timestamp) => {
    const date = new Date(timestamp);
    return date.toLocaleString();
  };

  // Get badge color based on action type
  const getActionBadgeColor = (action) => {
    switch (action?.toLowerCase()) {
      case 'create':
        return 'success';
      case 'update':
        return 'primary';
      case 'delete':
        return 'danger';
      case 'login':
        return 'info';
      case 'logout':
        return 'secondary';
      default:
        return 'dark';
    }
  };

  if (!currentUser) {
    return (
      <Container className="mt-4 text-center">
        <Alert variant="warning">
          Please log in to view your profile.
        </Alert>
      </Container>
    );
  }

  return (
    <Container fluid>
      <h2 className="mb-4">My Profile</h2>
      
      <Tab.Container defaultActiveKey="profile">
        <Row>
          <Col lg={3} className="mb-4">
            <Card>
              <Card.Body className="p-0">
                <div className="user-profile-sidebar">
                  <div className="text-center p-4">
                    <div className="rounded-circle bg-light p-3 d-inline-flex mb-3">
                      <FaUser size={48} className="text-primary" />
                    </div>
                    <h4>{currentUser.firstName} {currentUser.lastName}</h4>
                    <p className="text-muted mb-0">{currentUser.email}</p>
                  </div>
                  
                  <hr className="my-0" />
                  
                  <Nav variant="pills" className="flex-column profile-nav">
                    <Nav.Item>
                      <Nav.Link eventKey="profile" className="d-flex align-items-center">
                        <FaUser className="me-3" /> Personal Information
                      </Nav.Link>
                    </Nav.Item>
                    <Nav.Item>
                      <Nav.Link eventKey="password" className="d-flex align-items-center">
                        <FaKey className="me-3" /> Change Password
                      </Nav.Link>
                    </Nav.Item>
                    <Nav.Item>
                      <Nav.Link eventKey="activity" className="d-flex align-items-center">
                        <FaHistory className="me-3" /> Activity Log
                      </Nav.Link>
                    </Nav.Item>
                    <Nav.Item>
                      <Nav.Link eventKey="security" className="d-flex align-items-center">
                        <FaShieldAlt className="me-3" /> Security
                      </Nav.Link>
                    </Nav.Item>
                    <Nav.Item>
                      <Nav.Link eventKey="preferences" className="d-flex align-items-center">
                        <FaCog className="me-3" /> Preferences
                      </Nav.Link>
                    </Nav.Item>
                  </Nav>
                </div>
              </Card.Body>
            </Card>
          </Col>
          
          <Col lg={9}>
            <Card>
              <Card.Body>
                <Tab.Content>
                  {/* Profile Information Tab */}
                  <Tab.Pane eventKey="profile">
                    <h4 className="mb-4">Personal Information</h4>
                    
                    <Formik
                      initialValues={{
                        firstName: currentUser.firstName || '',
                        lastName: currentUser.lastName || '',
                        email: currentUser.email || '',
                      }}
                      validationSchema={profileSchema}
                      onSubmit={handleProfileUpdate}
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
                          
                          <Form.Group className="mb-4">
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
                          
                          <div className="d-grid gap-2 d-md-flex justify-content-md-end">
                            <Button
                              variant="primary"
                              type="submit"
                              disabled={isSubmitting || loading}
                            >
                              {loading ? (
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
                  </Tab.Pane>
                  
                  {/* Password Tab */}
                  <Tab.Pane eventKey="password">
                    <h4 className="mb-4">Change Password</h4>
                    
                    <Formik
                      initialValues={{
                        currentPassword: '',
                        newPassword: '',
                        confirmPassword: ''
                      }}
                      validationSchema={passwordSchema}
                      onSubmit={handlePasswordChange}
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
                            <Form.Label>Current Password</Form.Label>
                            <Form.Control
                              type="password"
                              name="currentPassword"
                              value={values.currentPassword}
                              onChange={handleChange}
                              onBlur={handleBlur}
                              isInvalid={touched.currentPassword && errors.currentPassword}
                            />
                            <Form.Control.Feedback type="invalid">
                              {errors.currentPassword}
                            </Form.Control.Feedback>
                          </Form.Group>
                          
                          <Form.Group className="mb-3">
                            <Form.Label>New Password</Form.Label>
                            <Form.Control
                              type="password"
                              name="newPassword"
                              value={values.newPassword}
                              onChange={handleChange}
                              onBlur={handleBlur}
                              isInvalid={touched.newPassword && errors.newPassword}
                            />
                            <Form.Control.Feedback type="invalid">
                              {errors.newPassword}
                            </Form.Control.Feedback>
                            <Form.Text className="text-muted">
                              Password must be at least 8 characters and include lowercase, uppercase, 
                              number, and special character.
                            </Form.Text>
                          </Form.Group>
                          
                          <Form.Group className="mb-4">
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
                          
                          <div className="d-grid gap-2 d-md-flex justify-content-md-end">
                            <Button
                              variant="primary"
                              type="submit"
                              disabled={isSubmitting || loading}
                            >
                              {loading ? (
                                <>
                                  <span className="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>
                                  Changing...
                                </>
                              ) : (
                                <>
                                  <FaKey className="me-2" /> Update Password
                                </>
                              )}
                            </Button>
                          </div>
                        </Form>
                      )}
                    </Formik>
                  </Tab.Pane>
                  
                  {/* Activity Log Tab */}
                  <Tab.Pane eventKey="activity">
                    <h4 className="mb-4">Your Activity Log</h4>
                    
                    {logsLoading ? (
                      <div className="text-center py-4">
                        <div className="spinner-border text-primary" role="status">
                          <span className="visually-hidden">Loading...</span>
                        </div>
                        <p className="mt-2">Loading activity logs...</p>
                      </div>
                    ) : userLogs.length > 0 ? (
                      <div className="table-responsive">
                        <table className="table table-hover">
                          <thead className="table-light">
                            <tr>
                              <th>Timestamp</th>
                              <th>Action</th>
                              <th>Entity</th>
                              <th>Description</th>
                            </tr>
                          </thead>
                          <tbody>
                            {userLogs.map((log) => (
                              <tr key={log.log_id}>
                                <td className="text-nowrap">{formatTimestamp(log.timestamp)}</td>
                                <td>
                                  <span className={`badge bg-${getActionBadgeColor(log.action_type)}`}>
                                    {log.action_type}
                                  </span>
                                </td>
                                <td>{log.entity_type}</td>
                                <td>{log.description}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    ) : (
                      <Alert variant="info">No recent activity found.</Alert>
                    )}
                  </Tab.Pane>
                  
                  {/* Security Tab */}
                  <Tab.Pane eventKey="security">
                    <h4 className="mb-4">Security Settings</h4>
                    
                    <Card className="mb-3">
                      <Card.Body>
                        <h5>Two-Factor Authentication</h5>
                        <p className="text-muted">
                          Add an extra layer of security to your account by enabling two-factor authentication.
                        </p>
                        <Button variant="outline-primary" disabled>
                          <FaShieldAlt className="me-2" /> Enable 2FA
                        </Button>
                        <small className="d-block mt-2 text-muted">
                          (Feature coming soon)
                        </small>
                      </Card.Body>
                    </Card>
                    
                    <Card>
                      <Card.Body>
                        <h5>Active Sessions</h5>
                        <p className="text-muted">
                          View and manage your currently active sessions across different devices.
                        </p>
                        <div className="d-flex align-items-center mb-2 pb-2 border-bottom">
                          <div className="flex-grow-1">
                            <strong>Current Session</strong>
                            <div className="text-muted small">
                              {navigator.userAgent}
                            </div>
                            <div className="text-success small">
                              Active Now
                            </div>
                          </div>
                          <div>
                            <Button variant="outline-danger" size="sm" disabled>
                              Revoke
                            </Button>
                          </div>
                        </div>
                        <small className="text-muted">
                          Session management coming soon.
                        </small>
                      </Card.Body>
                    </Card>
                  </Tab.Pane>
                  
                  {/* Preferences Tab */}
                  <Tab.Pane eventKey="preferences">
                    <h4 className="mb-4">User Preferences</h4>
                    
                    <Card>
                      <Card.Body>
                        <Form>
                          <h5 className="mb-3">Notifications</h5>
                          
                          <Form.Check 
                            type="switch"
                            id="email-notifications"
                            label="Email Notifications"
                            defaultChecked
                            className="mb-3"
                          />
                          <Form.Text className="text-muted mb-4 d-block">
                            Receive email notifications for important system events and account activities.
                          </Form.Text>
                          
                          <h5 className="mb-3 mt-4">Display Settings</h5>
                          
                          <Form.Group className="mb-3">
                            <Form.Label>Theme</Form.Label>
                            <Form.Select defaultValue="light">
                              <option value="light">Light</option>
                              <option value="dark">Dark</option>
                              <option value="system">System Default</option>
                            </Form.Select>
                          </Form.Group>
                          
                          <div className="d-grid gap-2 d-md-flex justify-content-md-end">
                            <Button variant="primary" disabled>
                              <FaSave className="me-2" /> Save Preferences
                            </Button>
                          </div>
                          <div className="text-muted text-end mt-2">
                            <small>(Preferences feature coming soon)</small>
                          </div>
                        </Form>
                      </Card.Body>
                    </Card>
                  </Tab.Pane>
                </Tab.Content>
              </Card.Body>
            </Card>
          </Col>
        </Row>
      </Tab.Container>
    </Container>
  );
};

export default UserProfile;
