import React, { useState, useEffect } from 'react';
import { Container, Row, Col, Card, Table, Button, Form, InputGroup, Badge, Dropdown } from 'react-bootstrap';
import { Link } from 'react-router-dom';
import { FaSearch, FaFilter, FaFileExport, FaCalendarAlt, FaChevronLeft, FaChevronRight, FaEye } from 'react-icons/fa';
import { toast } from 'react-toastify';
import DatePicker from 'react-datepicker';
import 'react-datepicker/dist/react-datepicker.css';
import { loggingAPI } from '../../services/api';
import { useAuth } from '../../contexts/AuthContext';

const ActivityLogList = () => {
  const [logs, setLogs] = useState([]);
  const [loading, setLoading] = useState(true);
  const [searchTerm, setSearchTerm] = useState('');
  const [filteredLogs, setFilteredLogs] = useState([]);
  const [currentPage, setCurrentPage] = useState(1);
  const [totalPages, setTotalPages] = useState(1);
  const [pageSize, setPageSize] = useState(10);
  const [startDate, setStartDate] = useState(null);
  const [endDate, setEndDate] = useState(null);
  const [actionType, setActionType] = useState('all');
  const [entityType, setEntityType] = useState('all');
  const [actionTypes, setActionTypes] = useState([]);
  const [entityTypes, setEntityTypes] = useState([]);
  
  const { hasPermission } = useAuth();
  const canViewLogs = hasPermission(['log_view']);
  const canExportLogs = hasPermission(['log_export']);

  useEffect(() => {
    if (canViewLogs) {
      fetchLogs();
      fetchFilters();
    } else {
      setLoading(false);
    }
  }, [canViewLogs, currentPage, pageSize]);

  // Filter logs based on filters when they change
  useEffect(() => {
    if (logs.length > 0) {
      applyFilters();
    }
  }, [searchTerm, startDate, endDate, actionType, entityType, logs]);

  // Fetch activity logs from the API
  const fetchLogs = async () => {
    try {
      setLoading(true);
      const response = await loggingAPI.getActivityLogs({
        page: currentPage,
        limit: pageSize,
        start_date: startDate ? startDate.toISOString() : undefined,
        end_date: endDate ? endDate.toISOString() : undefined,
        action_type: actionType !== 'all' ? actionType : undefined,
        entity_type: entityType !== 'all' ? entityType : undefined,
        search: searchTerm || undefined
      });
      
      setLogs(response.data.logs);
      setFilteredLogs(response.data.logs);
      setTotalPages(Math.ceil(response.data.total / pageSize));
    } catch (error) {
      console.error('Error fetching activity logs:', error);
      toast.error('Failed to load activity logs. Please try again.');
    } finally {
      setLoading(false);
    }
  };

  // Fetch available filters (action types and entity types)
  const fetchFilters = async () => {
    try {
      const response = await loggingAPI.getLogFilters();
      setActionTypes(response.data.action_types);
      setEntityTypes(response.data.entity_types);
    } catch (error) {
      console.error('Error fetching log filters:', error);
    }
  };

  // Apply client-side filtering
  const applyFilters = () => {
    let filtered = [...logs];
    
    if (searchTerm) {
      filtered = filtered.filter(log => 
        log.description.toLowerCase().includes(searchTerm.toLowerCase()) ||
        log.user_name.toLowerCase().includes(searchTerm.toLowerCase()) ||
        log.action_type.toLowerCase().includes(searchTerm.toLowerCase()) ||
        log.entity_type.toLowerCase().includes(searchTerm.toLowerCase())
      );
    }
    
    if (startDate) {
      filtered = filtered.filter(log => new Date(log.timestamp) >= startDate);
    }
    
    if (endDate) {
      filtered = filtered.filter(log => new Date(log.timestamp) <= new Date(endDate.setHours(23, 59, 59, 999)));
    }
    
    if (actionType !== 'all') {
      filtered = filtered.filter(log => log.action_type === actionType);
    }
    
    if (entityType !== 'all') {
      filtered = filtered.filter(log => log.entity_type === entityType);
    }
    
    setFilteredLogs(filtered);
  };

  // Handle searching
  const handleSearch = () => {
    setCurrentPage(1);
    fetchLogs();
  };

  // Clear all filters
  const clearFilters = () => {
    setSearchTerm('');
    setStartDate(null);
    setEndDate(null);
    setActionType('all');
    setEntityType('all');
    setCurrentPage(1);
  };
  
  // Export logs as CSV
  const handleExport = async () => {
    try {
      toast.info('Preparing log export...');
      
      const response = await loggingAPI.exportLogs({
        start_date: startDate ? startDate.toISOString() : undefined,
        end_date: endDate ? endDate.toISOString() : undefined,
        action_type: actionType !== 'all' ? actionType : undefined,
        entity_type: entityType !== 'all' ? entityType : undefined,
        search: searchTerm || undefined
      });
      
      // Create CSV content
      const csvContent = 'data:text/csv;charset=utf-8,' + response.data;
      
      // Create download link
      const encodedUri = encodeURI(csvContent);
      const link = document.createElement('a');
      link.setAttribute('href', encodedUri);
      link.setAttribute('download', `activity_logs_${new Date().toISOString().split('T')[0]}.csv`);
      document.body.appendChild(link);
      
      // Trigger download
      link.click();
      
      toast.success('Activity logs exported successfully');
    } catch (error) {
      console.error('Error exporting logs:', error);
      toast.error('Failed to export logs. Please try again.');
    }
  };

  // Get badge color based on action type
  const getActionBadgeColor = (action) => {
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

  // Get background color for entity type
  const getEntityBadgeColor = (entity) => {
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
    if (isNaN(date.getTime())) {
      // Optionally log for debugging
      // console.warn('Invalid timestamp:', timestamp);
      return 'N/A';
    }
    return date.toLocaleString();
  };

  // Handle page change
  const handlePageChange = (page) => {
    setCurrentPage(page);
  };

  if (!canViewLogs) {
    return (
      <Container fluid>
        <Card className="text-center p-5">
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

  return (
    <Container fluid>
      <Row className="mb-4">
        <Col md={6}>
          <h2>Activity Logs</h2>
          <p className="text-muted">View system activity and user actions</p>
        </Col>
        <Col md={6} className="text-md-end">
          {canExportLogs && (
            <Button 
              variant="outline-primary" 
              onClick={handleExport}
              className="d-inline-flex align-items-center"
            >
              <FaFileExport className="me-2" /> Export Logs
            </Button>
          )}
        </Col>
      </Row>
      
      <Card className="mb-4">
        <Card.Header className="bg-white">
          <h5 className="mb-0">
            <FaFilter className="me-2" /> Filter Logs
          </h5>
        </Card.Header>
        <Card.Body>
          <Row className="mb-3">
            <Col md={4}>
              <Form.Group className="mb-3">
                <Form.Label>Search</Form.Label>
                <InputGroup>
                  <Form.Control 
                    placeholder="Search logs..."
                    value={searchTerm}
                    onChange={(e) => setSearchTerm(e.target.value)}
                    onKeyPress={(e) => e.key === 'Enter' && handleSearch()}
                  />
                  <Button variant="outline-secondary" onClick={handleSearch}>
                    <FaSearch />
                  </Button>
                </InputGroup>
              </Form.Group>
            </Col>
            <Col md={4}>
              <Form.Group className="mb-3">
                <Form.Label>Action Type</Form.Label>
                <Form.Select
                  value={actionType}
                  onChange={(e) => setActionType(e.target.value)}
                >
                  <option value="all">All Actions</option>
                  {actionTypes.map(type => (
                    <option key={type} value={type}>{type}</option>
                  ))}
                </Form.Select>
              </Form.Group>
            </Col>
            <Col md={4}>
              <Form.Group className="mb-3">
                <Form.Label>Entity Type</Form.Label>
                <Form.Select
                  value={entityType}
                  onChange={(e) => setEntityType(e.target.value)}
                >
                  <option value="all">All Entities</option>
                  {entityTypes.map(type => (
                    <option key={type} value={type}>{type}</option>
                  ))}
                </Form.Select>
              </Form.Group>
            </Col>
          </Row>
          
          <Row>
            <Col md={4}>
              <Form.Group className="mb-3">
                <Form.Label>Start Date</Form.Label>
                <InputGroup>
                  <DatePicker
                    selected={startDate}
                    onChange={(date) => setStartDate(date)}
                    selectsStart
                    startDate={startDate}
                    endDate={endDate}
                    dateFormat="yyyy-MM-dd"
                    className="form-control"
                    placeholderText="Select start date"
                  />
                  <Button 
                    variant="outline-secondary"
                    onClick={() => setStartDate(null)}
                  >
                    Clear
                  </Button>
                </InputGroup>
              </Form.Group>
            </Col>
            <Col md={4}>
              <Form.Group className="mb-3">
                <Form.Label>End Date</Form.Label>
                <InputGroup>
                  <DatePicker
                    selected={endDate}
                    onChange={(date) => setEndDate(date)}
                    selectsEnd
                    startDate={startDate}
                    endDate={endDate}
                    minDate={startDate}
                    dateFormat="yyyy-MM-dd"
                    className="form-control"
                    placeholderText="Select end date"
                  />
                  <Button 
                    variant="outline-secondary"
                    onClick={() => setEndDate(null)}
                  >
                    Clear
                  </Button>
                </InputGroup>
              </Form.Group>
            </Col>
            <Col md={4} className="d-flex align-items-end">
              <div className="d-grid w-100">
                <Button 
                  variant="secondary" 
                  onClick={clearFilters}
                >
                  Clear All Filters
                </Button>
              </div>
            </Col>
          </Row>
        </Card.Body>
      </Card>
      
      <Card>
        <Card.Body>
          {loading ? (
            <div className="text-center py-5">
              <div className="spinner-border text-primary" role="status">
                <span className="visually-hidden">Loading...</span>
              </div>
              <p className="mt-2">Loading activity logs...</p>
            </div>
          ) : (
            <>
              <div className="table-responsive">
                <Table hover className="align-middle">
                  <thead className="table-light">
                    <tr>
                      <th>Timestamp</th>
<th>User</th>
<th>Action</th>
<th>Entity</th>
<th>Description</th>
<th>IP Address / Port</th>
<th>Details</th>
                    </tr>
                  </thead>
                  <tbody>
                    {filteredLogs.length > 0 ? (
                      filteredLogs.map((log) => (
                        <tr key={log.log_id}>
                          <td className="text-nowrap">
                            <div className="d-flex align-items-center">
                              <FaCalendarAlt className="me-2 text-muted" />
                              {formatTimestamp(log.timestamp)}
                            </div>
                          </td>
                          <td>
                            {log.user_id ? (
                              <Link to={`/users/${log.user_id}`} className="text-decoration-none">
                                {log.user_name}
                              </Link>
                            ) : (
                              <span>System</span>
                            )}
                          </td>
                          <td>
                            <Badge bg={getActionBadgeColor(log.action_type)} className="glass-badge">
                              {log.action_type}
                            </Badge>
                          </td>
                          <td>
                            <Badge bg={getEntityBadgeColor(log.entity_type)} className="text-capitalize glass-badge">
                              {log.entity_type}
                            </Badge>
                          </td>
                          <td className="text-truncate" style={{ maxWidth: '350px' }}>
  {log.description}
</td>
<td>
  {(() => {
    const ip = log.ip_address || log.ip || log.source_ip;
    const port = log.ip_port;
    if (ip && port) return `${ip}:${port}`;
    if (ip) return ip;
    if (port) return port;
    return 'N/A';
  })()}
</td>
<td>
  <Link to={`/logs/${log.log_id}`}>
    <Button size="sm" variant="outline-primary" className="glass-btn">
      <FaEye className="me-1" /> View
    </Button>
  </Link>
</td>
                        </tr>
                      ))
                    ) : (
                      <tr>
                        <td colSpan="6" className="text-center py-4">
                          No activity logs found matching your criteria.
                        </td>
                      </tr>
                    )}
                  </tbody>
                </Table>
              </div>

              {/* Pagination */}
              {filteredLogs.length > 0 && (
                <div className="d-flex justify-content-between align-items-center mt-4">
                  <div>
                    <span className="me-2">Show</span>
                    <Form.Select 
                      style={{ width: 'auto', display: 'inline-block' }}
                      value={pageSize}
                      className="glass-input"
                      onChange={(e) => {
                        setPageSize(Number(e.target.value));
                        setCurrentPage(1);
                      }}
                    >
                      <option value="10">10</option>
                      <option value="25">25</option>
                      <option value="50">50</option>
                      <option value="100">100</option>
                    </Form.Select>
                    <span className="ms-2">entries</span>
                    <span className="ms-3 text-muted">
                      {(() => {
                        const total = isNaN(Number(totalPages * pageSize)) ? 0 : Number(totalPages * pageSize);
                        const start = (currentPage - 1) * pageSize + 1;
                        const end = Math.min(currentPage * pageSize, filteredLogs.length ? total : 0);
                        return `Showing ${filteredLogs.length ? start : 0} to ${filteredLogs.length ? end : 0} of ${filteredLogs.length ? total : 0} entries`;
                      })()}
                    </span>
                  </div>
                  
                  <div>
                    <Button
                      variant="outline-primary"
                      size="sm"
                      className="me-2 glass-btn"
                      disabled={currentPage === 1}
                      onClick={() => handlePageChange(currentPage - 1)}
                    >
                      <FaChevronLeft /> Previous
                    </Button>
                    
                    <span className="mx-2">
                      Page {currentPage} of {totalPages}
                    </span>
                    
                    <Button
                      variant="outline-primary"
                      size="sm"
                      className="glass-btn"
                      disabled={currentPage === totalPages}
                      onClick={() => handlePageChange(currentPage + 1)}
                    >
                      Next <FaChevronRight />
                    </Button>
                  </div>
                </div>
              )}
            </>
          )}
        </Card.Body>
      </Card>
    </Container>
  );
};

export default ActivityLogList;
