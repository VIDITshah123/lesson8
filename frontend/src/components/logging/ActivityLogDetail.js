import React, { useState, useEffect } from 'react';
import { Container, Row, Col, Card, Button, Badge, Alert } from 'react-bootstrap';
import { Link, useParams, useNavigate } from 'react-router-dom';
import { FaArrowLeft, FaCalendarAlt, FaUser, FaShieldAlt, FaDatabase } from 'react-icons/fa';
import { toast } from 'react-toastify';
import { loggingAPI } from '../../services/api';
import { useAuth } from '../../contexts/AuthContext';

const ActivityLogDetail = () => {
  const { id } = useParams();
  const navigate = useNavigate();
  const { hasPermission } = useAuth();
  
  const [log, setLog] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  
  const canViewLogs = hasPermission(['log_view']);

  useEffect(() => {
    if (canViewLogs) {
      fetchLogDetail();
    } else {
      setLoading(false);
    }
  }, [canViewLogs, id]);

  const fetchLogDetail = async () => {
    try {
      setLoading(true);
      const response = await loggingAPI.getActivityLog(id);
      setLog(response.data);
    } catch (error) {
      console.error('Error fetching log details:', error);
      setError('Failed to load log details. The log might not exist or you may not have permission to view it.');
    } finally {
      setLoading(false);
    }
  };

  // Get badge color based on action type
  const getActionBadgeColor = (action) => {
    if (!action) return 'secondary';
    
    switch (action.toLowerCase()) {
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

  // Get badge color for entity type
  const getEntityBadgeColor = (entity) => {
    if (!entity) return 'light';
    
    switch (entity.toLowerCase()) {
      case 'user':
        return 'info';
      case 'role':
        return 'warning';
      case 'permission':
        return 'secondary';
      case 'auth':
        return 'dark';
      default:
        return 'light';
    }
  };

  // Format timestamp to readable date and time
  const formatTimestamp = (timestamp) => {
    if (!timestamp) return 'N/A';
    const date = new Date(timestamp);
    return date.toLocaleString();
  };

  // Format JSON data for display
  const formatJsonData = (jsonData) => {
    if (!jsonData) return null;
    
    try {
      const data = typeof jsonData === 'string' ? JSON.parse(jsonData) : jsonData;
      return (
        <pre className="bg-light p-3 rounded">
          {JSON.stringify(data, null, 2)}
        </pre>
      );
    } catch (error) {
      return <div className="text-danger">Invalid JSON data</div>;
    }
  };

  if (!canViewLogs) {
    return (
      <Container fluid>
        <Card className="text-center p-5 glass-card">
          <Card.Body>
            <div className="text-danger mb-3">
              <i className="fas fa-exclamation-circle fa-3x"></i>
            </div>
            <h3>Access Restricted</h3>
            <p className="text-muted">
              You don't have permission to view activity logs. Please contact your administrator if you 
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
        <p className="mt-2">Loading log details...</p>
      </Container>
    );
  }

  if (error) {
    return (
      <Container className="mt-4">
        <Alert variant="danger">{error}</Alert>
        <Button as={Link} to="/logs" variant="primary">
          <FaArrowLeft className="me-2" /> Back to Logs
        </Button>
      </Container>
    );
  }

  if (!log) {
    return (
      <Container className="mt-4">
        <Alert variant="warning">Log entry not found.</Alert>
        <Button as={Link} to="/logs" variant="primary">
          <FaArrowLeft className="me-2" /> Back to Logs
        </Button>
      </Container>
    );
  }

  return (
    <Container fluid className="py-4">
      <Row className="mb-3">
        <Col>
          <Button as={Link} to="/logs" variant="outline-primary" className="glass-btn">
            <FaArrowLeft className="me-2" /> Back to Logs
          </Button>
        </Col>
      </Row>

      <Row>
        <Col>
          <h2>Activity Log Details</h2>
        </Col>
      </Row>

      <Row>
        <Col lg={4}>
          <Card className="mb-4 glass-card">
            <Card.Body>
              <div className="text-center mb-4">
                <div className="rounded-circle bg-light p-3 d-inline-flex mb-3">
                  <FaDatabase size={30} className="text-primary" />
                </div>
                <h4>Log Entry #{log.log_id}</h4>
                <Badge bg={getActionBadgeColor(log.action_type)} className="mx-1 fw-normal fs-6 glass-badge">
                  {log.action_type}
                </Badge>
                <Badge bg={getEntityBadgeColor(log.entity_type)} className="mx-1 fw-normal fs-6 text-capitalize glass-badge">
                  {log.entity_type}
                </Badge>
              </div>

              <hr />

              <div className="mb-3">
                <small className="text-muted d-block">Timestamp</small>
                <div className="d-flex align-items-center">
                  <FaCalendarAlt className="me-2 text-primary" />
                  {formatTimestamp(log.timestamp)}
                </div>
              </div>

              {log.entity_id && (
                <div className="mb-3">
                  <small className="text-muted d-block">Entity ID</small>
                  <div className="fw-bold">{log.entity_id}</div>
                </div>
              )}

              <div className="mb-3">
                <small className="text-muted d-block">Performed By</small>
                <div className="d-flex align-items-center">
                  <FaUser className="me-2 text-primary" />
                  {log.user_id ? (
                    <Link to={`/users/${log.user_id}`} className="text-decoration-none">
                      {log.user_name}
                    </Link>
                  ) : (
                    <span>System</span>
                  )}
                </div>
              </div>

              {log.ip_address && (
                <div className="mb-3">
                  <small className="text-muted d-block">IP Address</small>
                  <div className="fw-bold">{log.ip_address}</div>
                </div>
              )}
            </Card.Body>
          </Card>

          <Card className="glass-card">
            <Card.Header>
              <h5 className="mb-0">Related Information</h5>
            </Card.Header>
            <Card.Body>
              {log.entity_type === 'user' && log.entity_id && (
                <Button 
                  as={Link}
                  to={`/users/${log.entity_id}`}
                  variant="outline-primary"
                  className="mb-2 d-block glass-btn"
                >
                  <FaUser className="me-2" /> View User Profile
                </Button>
              )}

              {log.entity_type === 'role' && log.entity_id && (
                <Button 
                  as={Link}
                  to={`/roles/${log.entity_id}`}
                  variant="outline-primary"
                  className="mb-2 d-block glass-btn"
                >
                  <FaShieldAlt className="me-2" /> View Role Details
                </Button>
              )}

              {log.user_id && (
                <Button 
                  variant="outline-secondary"
                  className="d-block"
                  as={Link}
                  to={`/logs?user=${log.user_id}`}
                >
                  View All Logs By This User
                </Button>
              )}
            </Card.Body>
          </Card>
        </Col>

        <Col lg={8}>
          <Card className="mb-4">
            <Card.Header className="bg-white">
              <h5 className="mb-0">Description</h5>
            </Card.Header>
            <Card.Body>
              <p className="lead">{log.description}</p>
            </Card.Body>
          </Card>

          {log.details && (
            <Card>
              <Card.Header className="bg-white d-flex justify-content-between align-items-center">
                <h5 className="mb-0">Log Details</h5>
              </Card.Header>
              <Card.Body>
                {formatJsonData(log.details)}
              </Card.Body>
            </Card>
          )}

          {log.before_state && log.after_state && (
            <Card className="mt-4">
              <Card.Header className="bg-white">
                <h5 className="mb-0">Change Comparison</h5>
              </Card.Header>
              <Card.Body>
                <Row>
                  <Col md={6}>
                    <h6 className="mb-3">Before</h6>
                    {formatJsonData(log.before_state)}
                  </Col>
                  <Col md={6}>
                    <h6 className="mb-3">After</h6>
                    {formatJsonData(log.after_state)}
                  </Col>
                </Row>
              </Card.Body>
            </Card>
          )}
        </Col>
      </Row>
    </Container>
  );
};

export default ActivityLogDetail;
