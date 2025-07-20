import React, { useState, useEffect } from 'react';
import { Container, Row, Col, Card, Form, Button, Alert } from 'react-bootstrap';
import { Formik } from 'formik';
import * as Yup from 'yup';
import { Link, useParams, useNavigate } from 'react-router-dom';
import { toast } from 'react-toastify';
import { FaArrowLeft, FaSave, FaShieldAlt } from 'react-icons/fa';
import { permissionAPI } from '../../services/api';
import { useAuth } from '../../contexts/AuthContext';

const PermissionEdit = () => {
  const { id } = useParams();
  const navigate = useNavigate();
  const { hasPermission } = useAuth();
  
  const [permission, setPermission] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [categories, setCategories] = useState([]);
  
  const canEditPermission = hasPermission(['permission_edit']);
  const canViewPermission = hasPermission(['permission_view']);

  useEffect(() => {
    if (canViewPermission || canEditPermission) {
      fetchPermissionDetails();
      fetchPermissionCategories();
    } else {
      setLoading(false);
    }
  }, [id, canViewPermission, canEditPermission]);

  const fetchPermissionDetails = async () => {
    try {
      setLoading(true);
      const response = await permissionAPI.getPermission(id);
      setPermission(response.data);
    } catch (error) {
      console.error('Error fetching permission:', error);
      setError('Failed to load permission details. The permission might not exist or you might not have permission to view it.');
    } finally {
      setLoading(false);
    }
  };

  const fetchPermissionCategories = async () => {
    try {
      const response = await permissionAPI.getPermissionCategories();
      setCategories(response.data || []);
    } catch (error) {
      console.error('Error fetching permission categories:', error);
      // Non-critical error, can proceed without categories
    }
  };

  const isSystemPermission = () => {
    return permission && permission.is_system === 1;
  };

  const validationSchema = Yup.object({
    name: Yup.string()
      .required('Permission name is required')
      .max(50, 'Permission name must be at most 50 characters'),
    code: Yup.string()
      .required('Permission code is required')
      .max(50, 'Permission code must be at most 50 characters')
      .matches(/^[a-z_]+$/, 'Permission code must only contain lowercase letters and underscores'),
    category: Yup.string()
      .required('Category is required'),
    description: Yup.string()
      .max(500, 'Description must be at most 500 characters')
  });

  const handleSubmit = async (values, { setSubmitting }) => {
    try {
      if (isSystemPermission()) {
        // For system permissions, only allow updating the name and description
        await permissionAPI.updatePermission(id, {
          name: values.name,
          description: values.description
        });
      } else {
        await permissionAPI.updatePermission(id, values);
      }
      
      toast.success('Permission updated successfully');
      navigate(`/permissions/${id}`);
    } catch (error) {
      console.error('Error updating permission:', error);
      toast.error('Failed to update permission. Please try again.');
    } finally {
      setSubmitting(false);
    }
  };

  if (!canEditPermission) {
    return (
      <Container fluid>
        <Card className="text-center p-5">
          <Card.Body>
            <div className="text-danger mb-3">
              <FaShieldAlt size={48} />
            </div>
            <h3>Access Restricted</h3>
            <p className="text-muted">
              You don't have permission to edit permissions. Please contact your administrator if you 
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
        <p className="mt-2">Loading permission details...</p>
      </Container>
    );
  }

  if (error) {
    return (
      <Container className="mt-4">
        <Alert variant="danger">{error}</Alert>
        <Button as={Link} to="/permissions" variant="primary">
          <FaArrowLeft className="me-2" /> Back to Permissions
        </Button>
      </Container>
    );
  }

  if (!permission) {
    return (
      <Container className="mt-4">
        <Alert variant="warning">Permission not found.</Alert>
        <Button as={Link} to="/permissions" variant="primary">
          <FaArrowLeft className="me-2" /> Back to Permissions
        </Button>
      </Container>
    );
  }

  return (
    <Container fluid>
      <Row className="mb-4">
        <Col>
          <Button as={Link} to={`/permissions/${id}`} variant="light" className="mb-3">
            <FaArrowLeft className="me-2" /> Back to Permission Details
          </Button>
          <h2>Edit Permission</h2>
        </Col>
      </Row>
      
      {isSystemPermission() && (
        <Alert variant="info" className="mb-4">
          <FaShieldAlt className="me-2" />
          This is a system permission. Only the name and description can be edited.
        </Alert>
      )}
      
      <Card>
        <Card.Body>
          <Formik
            initialValues={{
              name: permission.name || '',
              code: permission.code || '',
              category: permission.category || '',
              description: permission.description || ''
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
                      <Form.Label>Permission Name</Form.Label>
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
                  </Col>
                  
                  <Col md={6}>
                    <Form.Group className="mb-3">
                      <Form.Label>Permission Code</Form.Label>
                      <Form.Control
                        type="text"
                        name="code"
                        value={values.code}
                        onChange={handleChange}
                        onBlur={handleBlur}
                        isInvalid={touched.code && errors.code}
                        disabled={isSystemPermission()}
                      />
                      <Form.Control.Feedback type="invalid">
                        {errors.code}
                      </Form.Control.Feedback>
                      <Form.Text className="text-muted">
                        Code should be lowercase with underscores (e.g., user_create).
                      </Form.Text>
                    </Form.Group>
                  </Col>
                </Row>
                
                <Form.Group className="mb-3">
                  <Form.Label>Category</Form.Label>
                  <Form.Select
                    name="category"
                    value={values.category}
                    onChange={handleChange}
                    onBlur={handleBlur}
                    isInvalid={touched.category && errors.category}
                    disabled={isSystemPermission()}
                  >
                    <option value="">Select a category</option>
                    {categories.length > 0 ? (
                      categories.map((category, index) => (
                        <option key={index} value={category}>
                          {category}
                        </option>
                      ))
                    ) : (
                      <option value="general">General</option>
                    )}
                    <option value="custom">Custom</option>
                  </Form.Select>
                  <Form.Control.Feedback type="invalid">
                    {errors.category}
                  </Form.Control.Feedback>
                </Form.Group>
                
                <Form.Group className="mb-4">
                  <Form.Label>Description</Form.Label>
                  <Form.Control
                    as="textarea"
                    rows={3}
                    name="description"
                    value={values.description}
                    onChange={handleChange}
                    onBlur={handleBlur}
                    isInvalid={touched.description && errors.description}
                  />
                  <Form.Control.Feedback type="invalid">
                    {errors.description}
                  </Form.Control.Feedback>
                </Form.Group>
                
                <div className="d-flex justify-content-end">
                  <Button 
                    variant="secondary" 
                    as={Link} 
                    to={`/permissions/${id}`}
                    className="me-2"
                  >
                    Cancel
                  </Button>
                  <Button 
                    variant="primary" 
                    type="submit"
                    disabled={isSubmitting}
                  >
                    {isSubmitting ? (
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
    </Container>
  );
};

export default PermissionEdit;
