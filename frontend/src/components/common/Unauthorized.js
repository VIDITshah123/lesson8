import React from 'react';
import { Link } from 'react-router-dom';
import { Container, Alert, Button } from 'react-bootstrap';

/**
 * Unauthorized component to display when a user tries to access a page without proper permissions
 */
const Unauthorized = () => {
  return (
    <Container className="mt-5">
      <Alert variant="danger">
        <Alert.Heading>Unauthorized Access</Alert.Heading>
        <p>
          You do not have the necessary permissions to access this resource.
          Please contact your administrator if you believe this is an error.
        </p>
        <hr />
        <div className="d-flex justify-content-end">
          <Button as={Link} to="/dashboard" variant="outline-danger">
            Return to Dashboard
          </Button>
        </div>
      </Alert>
    </Container>
  );
};

export default Unauthorized;
