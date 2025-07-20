import React, { useState, useEffect } from 'react';
import { Container, Row, Col, Card, Table, Button, Form, InputGroup, Badge } from 'react-bootstrap';
import { FaSearch, FaShieldAlt, FaInfoCircle, FaSort, FaSortUp, FaSortDown } from 'react-icons/fa';
import { toast } from 'react-toastify';
import { permissionAPI } from '../../services/api';
import { useAuth } from '../../contexts/AuthContext';

const PermissionList = () => {
  const [permissions, setPermissions] = useState([]);
  const [loading, setLoading] = useState(true);
  const [searchTerm, setSearchTerm] = useState('');
  const [filteredPermissions, setFilteredPermissions] = useState([]);
  const [sortConfig, setSortConfig] = useState({ key: 'name', direction: 'asc' });
  const [categories, setCategories] = useState([]);
  const [selectedCategory, setSelectedCategory] = useState('all');
  
  // Safely access auth context with fallbacks to prevent null reference errors
  const auth = useAuth() || {};
  const { hasPermission = () => false } = auth;
  const canViewPermissions = typeof hasPermission === 'function' && hasPermission(['permission_view']);

  // Fetch permissions on component load
  useEffect(() => {
    if (canViewPermissions) {
      fetchPermissions();
    } else {
      setLoading(false);
    }
  }, [canViewPermissions]);

  // Filter permissions based on search term and category
  useEffect(() => {
    if (permissions.length > 0) {
      let filtered = [...permissions];
      
      // Filter by search term
      if (searchTerm) {
        filtered = filtered.filter(permission => 
          permission.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
          permission.description.toLowerCase().includes(searchTerm.toLowerCase())
        );
      }
      
      // Filter by category
      if (selectedCategory !== 'all') {
        filtered = filtered.filter(permission => 
          permission.name.startsWith(selectedCategory + '_')
        );
      }
      
      // Apply sorting
      if (sortConfig.key) {
        filtered.sort((a, b) => {
          if (a[sortConfig.key] < b[sortConfig.key]) {
            return sortConfig.direction === 'asc' ? -1 : 1;
          }
          if (a[sortConfig.key] > b[sortConfig.key]) {
            return sortConfig.direction === 'asc' ? 1 : -1;
          }
          return 0;
        });
      }
      
      setFilteredPermissions(filtered);
    }
  }, [permissions, searchTerm, selectedCategory, sortConfig]);

  // Extract unique categories from permissions
  useEffect(() => {
    if (permissions.length > 0) {
      const uniqueCategories = [...new Set(
        permissions.map(permission => permission.name.split('_')[0])
      )];
      setCategories(uniqueCategories);
    }
  }, [permissions]);

  const fetchPermissions = async () => {
    try {
      setLoading(true);
      const response = await permissionAPI.getPermissions();
      // The backend returns data in the format { count, permissions }, so we need to access the permissions array
      if (response.data && response.data.permissions) {
        setPermissions(response.data.permissions);
        setFilteredPermissions(response.data.permissions);
      } else {
        console.error('Unexpected response format:', response.data);
        toast.error('Failed to load permissions: unexpected data format');
        setPermissions([]);
        setFilteredPermissions([]);
      }
    } catch (error) {
      console.error('Error fetching permissions:', error);
      toast.error('Failed to load permissions. Please try again.');
    } finally {
      setLoading(false);
    }
  };

  const handleSearch = (e) => {
    setSearchTerm(e.target.value);
  };

  const handleCategoryChange = (e) => {
    setSelectedCategory(e.target.value);
  };

  const requestSort = (key) => {
    let direction = 'asc';
    if (sortConfig.key === key && sortConfig.direction === 'asc') {
      direction = 'desc';
    }
    setSortConfig({ key, direction });
  };

  const getSortIcon = (columnName) => {
    if (sortConfig.key !== columnName) {
      return <FaSort className="ms-1 text-muted" />;
    }
    return sortConfig.direction === 'asc' ? 
      <FaSortUp className="ms-1 text-primary" /> : 
      <FaSortDown className="ms-1 text-primary" />;
  };

  if (!canViewPermissions) {
    return (
      <Container fluid>
        <Card className="text-center p-5">
          <Card.Body>
            <FaShieldAlt size={48} className="text-muted mb-3" />
            <h3>Access Restricted</h3>
            <p className="text-muted">
              You don't have permission to view this page. Please contact your administrator if you 
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
          <h2>System Permissions</h2>
          <p className="text-muted">View all system permissions and their assignments</p>
        </Col>
      </Row>
      
      <Card className="mb-4">
        <Card.Body>
          <Row className="mb-3 align-items-end">
            <Col md={6}>
              <InputGroup>
                <Form.Control 
                  placeholder="Search permissions..."
                  value={searchTerm}
                  onChange={handleSearch}
                />
                <Button variant="outline-secondary">
                  <FaSearch />
                </Button>
              </InputGroup>
            </Col>
            <Col md={3}>
              <Form.Group>
                <Form.Label>Filter by Category</Form.Label>
                <Form.Select
                  value={selectedCategory}
                  onChange={handleCategoryChange}
                >
                  <option value="all">All Categories</option>
                  {categories.map(category => (
                    <option key={category} value={category}>
                      {category.charAt(0).toUpperCase() + category.slice(1)}
                    </option>
                  ))}
                </Form.Select>
              </Form.Group>
            </Col>
            <Col md={3} className="text-end">
              <span className="text-muted">
                {filteredPermissions.length} {filteredPermissions.length === 1 ? 'permission' : 'permissions'} found
              </span>
            </Col>
          </Row>
          
          {loading ? (
            <div className="text-center py-5">
              <div className="spinner-border text-primary" role="status">
                <span className="visually-hidden">Loading...</span>
              </div>
              <p className="mt-2">Loading permissions...</p>
            </div>
          ) : (
            <div className="table-responsive">
              <Table hover bordered className="align-middle">
                <thead>
                  <tr>
                    <th width="5%">#</th>
                    <th 
                      width="25%"
                      onClick={() => requestSort('name')}
                      className="cursor-pointer"
                    >
                      Permission Name {getSortIcon('name')}
                    </th>
                    <th width="30%">Description</th>
                    <th width="20%">Category</th>
                    <th 
                      width="20%"
                      onClick={() => requestSort('created_at')}
                      className="cursor-pointer"
                    >
                      Created {getSortIcon('created_at')}
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {filteredPermissions.length > 0 ? (
                    filteredPermissions.map((permission, index) => {
                      const category = permission.name.split('_')[0];
                      const action = permission.name.substring(category.length + 1);
                      
                      return (
                        <tr key={permission.permission_id}>
                          <td>{index + 1}</td>
                          <td>
                            <div className="d-flex align-items-center">
                              <FaShieldAlt className="me-2 text-primary" />
                              {permission.name}
                            </div>
                          </td>
                          <td>{permission.description}</td>
                          <td>
                            <Badge 
                              bg="secondary" 
                              className="text-capitalize"
                            >
                              {category}
                            </Badge>
                          </td>
                          <td>
                            {new Date(permission.created_at).toLocaleDateString()}
                          </td>
                        </tr>
                      );
                    })
                  ) : (
                    <tr>
                      <td colSpan="5" className="text-center py-4">
                        {searchTerm || selectedCategory !== 'all' ? 'No permissions match your search criteria.' : 'No permissions found.'}
                      </td>
                    </tr>
                  )}
                </tbody>
              </Table>
            </div>
          )}
        </Card.Body>
      </Card>
      
      <Card>
        <Card.Header className="bg-white">
          <h5 className="mb-0">
            <FaInfoCircle className="me-2 text-primary" />
            Understanding Permissions
          </h5>
        </Card.Header>
        <Card.Body>
          <Row>
            <Col md={6}>
              <h6>What are Permissions?</h6>
              <p className="text-muted">
                Permissions are granular access controls that define what actions users can perform
                in the system. Each permission represents a specific capability, such as creating
                users or viewing reports. Permissions are grouped by category and are 
                typically named using the format <code>category_action</code>.
              </p>
            </Col>
            <Col md={6}>
              <h6>How Permissions Work</h6>
              <p className="text-muted">
                Permissions are not directly assigned to users. Instead, they are grouped into roles,
                which are then assigned to users. A user's effective permissions are the combined
                permissions of all roles they have. Permissions cannot be created or modified through
                the interface as they are defined by system functionality.
              </p>
            </Col>
          </Row>
        </Card.Body>
      </Card>
    </Container>
  );
};

export default PermissionList;
