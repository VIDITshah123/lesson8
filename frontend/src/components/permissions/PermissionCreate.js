import React, { useState, useEffect } from 'react';
import { Container, Row, Col, Card, Form, Button, Alert } from 'react-bootstrap';
import { Formik } from 'formik';
import * as Yup from 'yup';
import { Link, useNavigate } from 'react-router-dom';
import { toast } from 'react-toastify';
import { FaArrowLeft, FaPlus, FaShieldAlt } from 'react-icons/fa';
import { permissionAPI } from '../../services/api';
import { useAuth } from '../../contexts/AuthContext';

const PermissionCreate = () => {
  const navigate = useNavigate();
  const { hasPermission } = useAuth();
  
  const [loading, setLoading] = useState(false);
  const [categories, setCategories] = useState([]);
  
  const canCreatePermission = hasPermission(['permission_create']);

  useEffect(() => {
    if (canCreatePermission) {
      fetchPermissionCategories();
    }
  }, [canCreatePermission]);

  const fetchPermissionCategories = async () => {
    try {
      const response = await permissionAPI.getPermissionCategories();
      setCategories(response.data || []);
    } catch (error) {
      console.error('Error fetching permission categories:', error);
      // Non-critical error, can proceed without categories
    }
  };

  const initialValues = {
    name: '',
    code: '',
    category: '',
    description: ''
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

  const handleSubmit = async (values, { setSubmitting, resetForm }) => {
    try {
      setLoading(true);
      
      const response = await permissionAPI.createPermission(values);
      
      toast.success('Permission created successfully');
      navigate(`/permissions/${response.data.permission_id}`);
    } catch (error) {
      console.error('Error creating permission:', error);
      
      if (error.response?.data?.error === 'Permission code already exists') {
        toast.error('Permission code already exists. Please use a unique code.');
      } else {
        toast.error('Failed to create permission. Please try again.');
      }
    } finally {
      setLoading(false);
      setSubmitting(false);
    }
  };

  if (!canCreatePermission) {
    return (
      <Container fluid>
        <Card className="text-center p-5">
          <Card.Body>
            <div className="text-danger mb-3">
              <FaShieldAlt size={48} />
            </div>
            <h3>Access Restricted</h3>
            <p className="text-muted">
              You don't have permission to create new permissions. Please contact your administrator if you 
              believe you should have access.
            </p>
          </Card.Body>
        </Card>
      </Container>
    );
  }

  return (
    <Container fluid>
      <Row className="mb-4">
        <Col>
          <Button as={Link} to="/permissions" variant="light" className="mb-3">
            <FaArrowLeft className="me-2" /> Back to Permissions
          </Button>
          <h2>Create New Permission</h2>
        </Col>
      </Row>
      
      <Card>
        <Card.Body>
          <Formik
            initialValues={initialValues}
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
                        placeholder="e.g., Create User"
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
                        placeholder="e.g., user_create"
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
                  >
                    <option value="">Select a category</option>
                    {categories.length > 0 ? (
                      categories.map((category, index) => (
                        <option key={index} value={category}>
                          {category}
                        </option>
                      ))
                    ) : (
                      <>
                        <option value="user_management">User Management</option>
                        <option value="role_management">Role Management</option>
                        <option value="permission_management">Permission Management</option>
                        <option value="dashboard">Dashboard</option>
                        <option value="reports">Reports</option>
                        <option value="settings">Settings</option>
                        <option value="general">General</option>
                      </>
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
                    placeholder="Describe what this permission allows a user to do"
                  />
                  <Form.Control.Feedback type="invalid">
                    {errors.description}
                  </Form.Control.Feedback>
                </Form.Group>
                
                <div className="d-flex justify-content-end">
                  <Button 
                    variant="secondary" 
                    as={Link} 
                    to="/permissions"
                    className="me-2"
                  >
                    Cancel
                  </Button>
                  <Button 
                    variant="primary" 
                    type="submit"
                    disabled={isSubmitting || loading}
                  >
                    {isSubmitting || loading ? (
                      <>
                        <span className="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>
                        Creating...
                      </>
                    ) : (
                      <>
                        <FaPlus className="me-2" /> Create Permission
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

export default PermissionCreate;
