import React, { useState, useEffect } from 'react';
import { Container, Row, Col, Card, Form, Button, Alert } from 'react-bootstrap';
import { FaChartBar, FaCalendarAlt, FaFilter, FaDownload } from 'react-icons/fa';
import DatePicker from 'react-datepicker';
import 'react-datepicker/dist/react-datepicker.css';
import { toast } from 'react-toastify';
import { loggingAPI } from '../../services/api';
import { useAuth } from '../../contexts/AuthContext';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  ArcElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js';
import { Line, Bar, Pie, Doughnut } from 'react-chartjs-2';

// Register Chart.js components
ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  ArcElement,
  Title,
  Tooltip,
  Legend
);

const Analytics = () => {
  const { hasPermission } = useAuth();
  
  const [loading, setLoading] = useState(true);
  const [startDate, setStartDate] = useState(new Date(new Date().setDate(new Date().getDate() - 30))); // Last 30 days
  const [endDate, setEndDate] = useState(new Date());
  const [entityType, setEntityType] = useState('all');
  const [chartType, setChartType] = useState('daily');
  const [entityTypes, setEntityTypes] = useState([]);
  
  const [activityByTimeData, setActivityByTimeData] = useState({
    labels: [],
    datasets: []
  });
  
  const [activityByEntityData, setActivityByEntityData] = useState({
    labels: [],
    datasets: []
  });
  
  const [activityByUserData, setActivityByUserData] = useState({
    labels: [],
    datasets: []
  });
  
  const [actionDistributionData, setActionDistributionData] = useState({
    labels: [],
    datasets: []
  });

  const canViewAnalytics = hasPermission(['log_view', 'dashboard_view']);

  useEffect(() => {
    if (canViewAnalytics) {
      fetchEntityTypes();
      fetchAnalyticsData();
    } else {
      setLoading(false);
    }
  }, [canViewAnalytics]);

  const fetchEntityTypes = async () => {
    try {
      const response = await loggingAPI.getLogFilters();
      setEntityTypes(response.data.entity_types || []);
    } catch (error) {
      console.error('Error fetching entity types:', error);
    }
  };

  const fetchAnalyticsData = async () => {
    try {
      setLoading(true);
      
      const filters = {
        start_date: startDate.toISOString(),
        end_date: endDate.toISOString(),
        entity_type: entityType !== 'all' ? entityType : undefined,
        group_by: chartType
      };
      
      const response = await loggingAPI.getAnalyticsData(filters);
      processAnalyticsData(response.data);
      
    } catch (error) {
      console.error('Error fetching analytics data:', error);
      toast.error('Failed to load analytics data');
    } finally {
      setLoading(false);
    }
  };

  const processAnalyticsData = (data) => {
    // Process activity by time (day, week, or month)
    if (data.activity_by_time) {
      const labels = data.activity_by_time.map(item => item.period);
      const counts = data.activity_by_time.map(item => item.count);
      
      setActivityByTimeData({
        labels,
        datasets: [
          {
            label: 'Activity Count',
            data: counts,
            fill: true,
            backgroundColor: 'rgba(75, 192, 192, 0.2)',
            borderColor: 'rgba(75, 192, 192, 1)',
            tension: 0.4
          }
        ]
      });
    }
    
    // Process activity by entity type
    if (data.activity_by_entity) {
      const labels = data.activity_by_entity.map(item => item.entity_type);
      const counts = data.activity_by_entity.map(item => item.count);
      
      setActivityByEntityData({
        labels,
        datasets: [
          {
            label: 'Activities by Entity',
            data: counts,
            backgroundColor: [
              'rgba(255, 99, 132, 0.7)',
              'rgba(54, 162, 235, 0.7)',
              'rgba(255, 206, 86, 0.7)',
              'rgba(75, 192, 192, 0.7)',
              'rgba(153, 102, 255, 0.7)',
              'rgba(255, 159, 64, 0.7)'
            ],
            borderWidth: 1
          }
        ]
      });
    }
    
    // Process activity by user
    if (data.activity_by_user) {
      const labels = data.activity_by_user.map(item => item.user_name || 'System');
      const counts = data.activity_by_user.map(item => item.count);
      
      setActivityByUserData({
        labels,
        datasets: [
          {
            label: 'Activities by User',
            data: counts,
            backgroundColor: 'rgba(54, 162, 235, 0.7)',
            borderColor: 'rgba(54, 162, 235, 1)',
            borderWidth: 1
          }
        ]
      });
    }
    
    // Process action type distribution
    if (data.action_distribution) {
      const labels = data.action_distribution.map(item => item.action_type);
      const counts = data.action_distribution.map(item => item.count);
      
      setActionDistributionData({
        labels,
        datasets: [
          {
            label: 'Action Types',
            data: counts,
            backgroundColor: [
              'rgba(75, 192, 192, 0.7)',
              'rgba(153, 102, 255, 0.7)',
              'rgba(255, 159, 64, 0.7)',
              'rgba(255, 99, 132, 0.7)',
              'rgba(54, 162, 235, 0.7)',
              'rgba(255, 206, 86, 0.7)'
            ],
            borderWidth: 1
          }
        ]
      });
    }
  };

  const handleApplyFilters = () => {
    fetchAnalyticsData();
  };

  const handleExportReport = async () => {
    try {
      toast.info('Preparing analytics report for export...');
      
      const filters = {
        start_date: startDate.toISOString(),
        end_date: endDate.toISOString(),
        entity_type: entityType !== 'all' ? entityType : undefined,
        format: 'pdf'
      };
      
      const response = await loggingAPI.exportAnalyticsReport(filters);
      
      // Create a download link for the PDF
      const url = window.URL.createObjectURL(new Blob([response.data]));
      const link = document.createElement('a');
      link.href = url;
      link.setAttribute('download', `analytics_report_${new Date().toISOString().split('T')[0]}.pdf`);
      document.body.appendChild(link);
      link.click();
      
      toast.success('Analytics report exported successfully');
    } catch (error) {
      console.error('Error exporting analytics report:', error);
      toast.error('Failed to export analytics report');
    }
  };

  // Chart options
  const lineChartOptions = {
    responsive: true,
    plugins: {
      legend: {
        position: 'top',
      },
      title: {
        display: true,
        text: `Activity Frequency (${chartType === 'daily' ? 'Daily' : chartType === 'weekly' ? 'Weekly' : 'Monthly'})`
      },
      tooltip: {
        mode: 'index',
        intersect: false,
      }
    },
    scales: {
      y: {
        beginAtZero: true,
        title: {
          display: true,
          text: 'Number of Activities'
        }
      },
      x: {
        title: {
          display: true,
          text: chartType === 'daily' ? 'Date' : chartType === 'weekly' ? 'Week' : 'Month'
        }
      }
    }
  };

  const barChartOptions = {
    responsive: true,
    plugins: {
      legend: {
        position: 'top',
      },
      title: {
        display: true,
        text: 'Top Active Users'
      }
    },
    scales: {
      y: {
        beginAtZero: true,
        title: {
          display: true,
          text: 'Number of Activities'
        }
      }
    }
  };

  const pieChartOptions = {
    responsive: true,
    plugins: {
      legend: {
        position: 'right',
      },
      title: {
        display: true,
        text: 'Activity by Entity Type'
      }
    }
  };

  const doughnutChartOptions = {
    responsive: true,
    plugins: {
      legend: {
        position: 'right',
      },
      title: {
        display: true,
        text: 'Action Type Distribution'
      }
    }
  };

  if (!canViewAnalytics) {
    return (
      <Container fluid>
        <Card className="text-center p-5">
          <Card.Body>
            <div className="text-danger mb-3">
              <FaChartBar size={48} />
            </div>
            <h3>Access Restricted</h3>
            <p className="text-muted">
              You don't have permission to view analytics. Please contact your administrator if you 
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
          <h2>Activity Analytics</h2>
          <p className="text-muted">Analyze system activity patterns and trends</p>
        </Col>
        <Col md={6} className="text-md-end">
          <Button 
            variant="outline-primary" 
            onClick={handleExportReport}
            className="d-inline-flex align-items-center"
          >
            <FaDownload className="me-2" /> Export Report
          </Button>
        </Col>
      </Row>
      
      <Card className="mb-4">
        <Card.Header className="bg-white">
          <h5 className="mb-0">
            <FaFilter className="me-2" /> Analytics Filters
          </h5>
        </Card.Header>
        <Card.Body>
          <Row>
            <Col md={3}>
              <Form.Group className="mb-3">
                <Form.Label className="d-flex align-items-center">
                  <FaCalendarAlt className="me-2" /> Start Date
                </Form.Label>
                <DatePicker
                  selected={startDate}
                  onChange={(date) => setStartDate(date)}
                  selectsStart
                  startDate={startDate}
                  endDate={endDate}
                  maxDate={new Date()}
                  dateFormat="yyyy-MM-dd"
                  className="form-control"
                />
              </Form.Group>
            </Col>
            <Col md={3}>
              <Form.Group className="mb-3">
                <Form.Label className="d-flex align-items-center">
                  <FaCalendarAlt className="me-2" /> End Date
                </Form.Label>
                <DatePicker
                  selected={endDate}
                  onChange={(date) => setEndDate(date)}
                  selectsEnd
                  startDate={startDate}
                  endDate={endDate}
                  minDate={startDate}
                  maxDate={new Date()}
                  dateFormat="yyyy-MM-dd"
                  className="form-control"
                />
              </Form.Group>
            </Col>
            <Col md={3}>
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
            <Col md={3}>
              <Form.Group className="mb-3">
                <Form.Label>Time Grouping</Form.Label>
                <Form.Select
                  value={chartType}
                  onChange={(e) => setChartType(e.target.value)}
                >
                  <option value="daily">Daily</option>
                  <option value="weekly">Weekly</option>
                  <option value="monthly">Monthly</option>
                </Form.Select>
              </Form.Group>
            </Col>
          </Row>
          <div className="d-grid gap-2 d-md-flex justify-content-md-end">
            <Button 
              variant="primary"
              onClick={handleApplyFilters}
              disabled={loading}
            >
              {loading ? (
                <>
                  <span className="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>
                  Loading...
                </>
              ) : (
                <>Apply Filters</>
              )}
            </Button>
          </div>
        </Card.Body>
      </Card>
      
      <Row>
        <Col lg={8} className="mb-4">
          <Card className="h-100">
            <Card.Header className="bg-white">
              <h5 className="mb-0">Activity Frequency over Time</h5>
            </Card.Header>
            <Card.Body>
              {loading ? (
                <div className="text-center py-5">
                  <div className="spinner-border text-primary" role="status">
                    <span className="visually-hidden">Loading...</span>
                  </div>
                </div>
              ) : (
                activityByTimeData.labels.length > 0 ? (
                  <Line options={lineChartOptions} data={activityByTimeData} />
                ) : (
                  <Alert variant="info">No activity data available for the selected period.</Alert>
                )
              )}
            </Card.Body>
          </Card>
        </Col>
        
        <Col lg={4} className="mb-4">
          <Card className="h-100">
            <Card.Header className="bg-white">
              <h5 className="mb-0">Entity Distribution</h5>
            </Card.Header>
            <Card.Body>
              {loading ? (
                <div className="text-center py-5">
                  <div className="spinner-border text-primary" role="status">
                    <span className="visually-hidden">Loading...</span>
                  </div>
                </div>
              ) : (
                activityByEntityData.labels.length > 0 ? (
                  <Pie options={pieChartOptions} data={activityByEntityData} />
                ) : (
                  <Alert variant="info">No entity data available for the selected period.</Alert>
                )
              )}
            </Card.Body>
          </Card>
        </Col>
      </Row>
      
      <Row>
        <Col lg={6} className="mb-4">
          <Card className="h-100">
            <Card.Header className="bg-white">
              <h5 className="mb-0">Top Active Users</h5>
            </Card.Header>
            <Card.Body>
              {loading ? (
                <div className="text-center py-5">
                  <div className="spinner-border text-primary" role="status">
                    <span className="visually-hidden">Loading...</span>
                  </div>
                </div>
              ) : (
                activityByUserData.labels.length > 0 ? (
                  <Bar options={barChartOptions} data={activityByUserData} />
                ) : (
                  <Alert variant="info">No user activity data available for the selected period.</Alert>
                )
              )}
            </Card.Body>
          </Card>
        </Col>
        
        <Col lg={6} className="mb-4">
          <Card className="h-100">
            <Card.Header className="bg-white">
              <h5 className="mb-0">Action Type Distribution</h5>
            </Card.Header>
            <Card.Body>
              {loading ? (
                <div className="text-center py-5">
                  <div className="spinner-border text-primary" role="status">
                    <span className="visually-hidden">Loading...</span>
                  </div>
                </div>
              ) : (
                actionDistributionData.labels.length > 0 ? (
                  <Doughnut options={doughnutChartOptions} data={actionDistributionData} />
                ) : (
                  <Alert variant="info">No action data available for the selected period.</Alert>
                )
              )}
            </Card.Body>
          </Card>
        </Col>
      </Row>
      
      <Card>
        <Card.Header className="bg-white">
          <h5 className="mb-0">Analytics Summary</h5>
        </Card.Header>
        <Card.Body>
          <Row>
            <Col md={6}>
              <h6 className="mb-3">Activity Insights</h6>
              <p className="text-muted">
                The analytics dashboard provides a visualization of system activities, helping you understand usage patterns
                and detect anomalies. Use the filters to customize the date range and focus on specific entity types.
              </p>
              <p className="text-muted">
                The time-based chart shows activity frequency trends, allowing you to identify peak usage periods and assess
                the impact of system changes or events.
              </p>
            </Col>
            <Col md={6}>
              <h6 className="mb-3">User Behavior Analysis</h6>
              <p className="text-muted">
                The user activity chart highlights the most active users in the system, which can help identify power users
                or potential security concerns if activity levels are unexpectedly high.
              </p>
              <p className="text-muted">
                Action and entity distribution charts show what types of operations are most common and which parts of the
                system receive the most attention, helping prioritize optimization efforts.
              </p>
            </Col>
          </Row>
        </Card.Body>
      </Card>
    </Container>
  );
};

export default Analytics;
