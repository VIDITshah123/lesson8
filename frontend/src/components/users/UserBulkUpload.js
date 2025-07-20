import React, { useState, useRef } from 'react';
import { Container, Row, Col, Card, Form, Button, Alert, ProgressBar, ListGroup } from 'react-bootstrap';
import { Link } from 'react-router-dom';
import { FaUpload, FaDownload, FaArrowLeft, FaFileAlt } from 'react-icons/fa';
import { toast } from 'react-toastify';
import { userAPI } from '../../services/api';

const UserBulkUpload = () => {
  const [file, setFile] = useState(null);
  const [isDragging, setIsDragging] = useState(false);
  const [uploadProgress, setUploadProgress] = useState(0);
  const [isUploading, setIsUploading] = useState(false);
  const [results, setResults] = useState(null);
  const [error, setError] = useState('');
  const fileInputRef = useRef();

  const handleDragEnter = (e) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragging(true);
  };

  const handleDragLeave = (e) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragging(false);
  };

  const handleDragOver = (e) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragging(true);
  };

  const handleDrop = (e) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragging(false);
    
    const files = e.dataTransfer.files;
    if (files && files.length) {
      validateAndSetFile(files[0]);
    }
  };

  const handleFileInputChange = (e) => {
    if (e.target.files && e.target.files.length) {
      validateAndSetFile(e.target.files[0]);
    }
  };

  const validateAndSetFile = (file) => {
    // Reset states
    setError('');
    setResults(null);
    
    // Check if the file is a CSV
    if (file.type !== 'text/csv' && !file.name.endsWith('.csv')) {
      setError('Please upload a CSV file');
      return;
    }
    
    // Check if file size is less than 5MB
    if (file.size > 5 * 1024 * 1024) {
      setError('File size should be less than 5MB');
      return;
    }
    
    setFile(file);
    toast.info(`File "${file.name}" is ready for upload`);
  };

  const handleUpload = async () => {
    if (!file) {
      setError('Please select a file to upload');
      return;
    }

    setIsUploading(true);
    setUploadProgress(0);
    setError('');
    setResults(null);

    const formData = new FormData();
    formData.append('file', file);

    try {
      // Simulate progress for better UX
      const progressInterval = setInterval(() => {
        setUploadProgress(prev => {
          const newProgress = prev + 5;
          return newProgress >= 90 ? 90 : newProgress;
        });
      }, 300);

      const response = await userAPI.uploadBulkUsers(formData);
      
      clearInterval(progressInterval);
      setUploadProgress(100);
      
      setResults({
        total: response.data.total || 0,
        successful: response.data.successful || 0,
        failed: response.data.failed || 0,
        errors: response.data.errors || []
      });
      
      toast.success(`Successfully processed ${response.data.successful} users out of ${response.data.total}`);
    } catch (error) {
      console.error('Error uploading users:', error);
      setError(error.response?.data?.error || 'Failed to upload users. Please try again.');
      toast.error('Failed to upload users');
    } finally {
      setIsUploading(false);
      setFile(null);
      if (fileInputRef.current) {
        fileInputRef.current.value = '';
      }
    }
  };

  const handleDownloadTemplate = async () => {
    try {
      const response = await userAPI.downloadUserTemplate();
      
      // Create a blob from the response
      const blob = new Blob([response.data], { type: 'text/csv' });
      
      // Create a temporary link element
      const url = window.URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.setAttribute('download', 'user_template.csv');
      
      // Append to the document and trigger the download
      document.body.appendChild(link);
      link.click();
      
      // Clean up
      document.body.removeChild(link);
      window.URL.revokeObjectURL(url);
      
      toast.success('Template downloaded successfully');
    } catch (error) {
      console.error('Error downloading template:', error);
      toast.error('Failed to download template');
    }
  };

  return (
    <Container fluid>
      <Row className="mb-4">
        <Col>
          <Link to="/users" className="btn btn-sm btn-outline-secondary">
            <FaArrowLeft /> Back to Users
          </Link>
          <h1 className="mt-2">Bulk Upload Users</h1>
          <p className="text-muted">
            Upload multiple users at once using a CSV file. 
            Download the template to see the required format.
          </p>
        </Col>
      </Row>

      <Row>
        <Col lg={8}>
          <Card className="mb-4">
            <Card.Header>
              <h5 className="mb-0">Upload Users</h5>
            </Card.Header>
            <Card.Body>
              {error && <Alert variant="danger">{error}</Alert>}
              
              <div 
                className={`upload-area p-5 mb-4 text-center ${isDragging ? 'border-primary bg-light' : 'border-dashed'}`}
                onDragEnter={handleDragEnter}
                onDragLeave={handleDragLeave}
                onDragOver={handleDragOver}
                onDrop={handleDrop}
                style={{
                  border: '2px dashed #ccc',
                  borderRadius: '5px',
                  cursor: 'pointer',
                  transition: 'all 0.3s'
                }}
                onClick={() => fileInputRef.current.click()}
              >
                <input
                  type="file"
                  ref={fileInputRef}
                  onChange={handleFileInputChange}
                  accept=".csv"
                  style={{ display: 'none' }}
                />
                
                <FaUpload size={40} className="mb-3 text-muted" />
                <h4>Drag & Drop CSV File Here</h4>
                <p className="mb-0 text-muted">or click to browse</p>
                
                {file && (
                  <div className="mt-3">
                    <span className="badge bg-success">
                      <FaFileAlt /> {file.name} ({(file.size / 1024).toFixed(2)} KB)
                    </span>
                  </div>
                )}
              </div>
              
              {isUploading && (
                <div className="mb-3">
                  <ProgressBar 
                    animated 
                    now={uploadProgress} 
                    label={`${uploadProgress}%`}
                  />
                </div>
              )}
              
              <div className="d-flex gap-2">
                <Button 
                  variant="primary" 
                  onClick={handleUpload} 
                  disabled={!file || isUploading}
                >
                  <FaUpload /> Upload Users
                </Button>
                <Button 
                  variant="outline-secondary" 
                  onClick={handleDownloadTemplate}
                  disabled={isUploading}
                >
                  <FaDownload /> Download Template
                </Button>
              </div>
            </Card.Body>
          </Card>

          {results && (
            <Card className="mb-4">
              <Card.Header>
                <h5 className="mb-0">Upload Results</h5>
              </Card.Header>
              <Card.Body>
                <Row className="mb-4">
                  <Col md={4}>
                    <div className="text-center p-3 border rounded">
                      <h2 className="mb-1">{results.total}</h2>
                      <p className="mb-0 text-muted">Total Processed</p>
                    </div>
                  </Col>
                  <Col md={4}>
                    <div className="text-center p-3 border rounded bg-success bg-opacity-10">
                      <h2 className="mb-1 text-success">{results.successful}</h2>
                      <p className="mb-0 text-muted">Successfully Created</p>
                    </div>
                  </Col>
                  <Col md={4}>
                    <div className="text-center p-3 border rounded bg-danger bg-opacity-10">
                      <h2 className="mb-1 text-danger">{results.failed}</h2>
                      <p className="mb-0 text-muted">Failed</p>
                    </div>
                  </Col>
                </Row>
                
                {results.errors && results.errors.length > 0 && (
                  <>
                    <h6 className="mb-2">Error Details:</h6>
                    <ListGroup variant="flush" className="border rounded">
                      {results.errors.map((error, index) => (
                        <ListGroup.Item key={index}>
                          <strong>Row {error.row}:</strong> {error.message}
                        </ListGroup.Item>
                      ))}
                    </ListGroup>
                  </>
                )}
              </Card.Body>
            </Card>
          )}
        </Col>
        
        <Col lg={4}>
          <Card className="mb-4">
            <Card.Header>
              <h5 className="mb-0">Instructions</h5>
            </Card.Header>
            <Card.Body>
              <h6>Format Requirements:</h6>
              <ol>
                <li>Use the provided CSV template</li>
                <li>Fill in all required fields (marked with *)</li>
                <li>Ensure email addresses are unique</li>
                <li>For multiple roles, separate with comma (e.g., "Admin,User")</li>
                <li>Set isActive to "true" or "false"</li>
              </ol>
              
              <h6>CSV Example:</h6>
              <pre className="bg-light p-2 rounded small">
                firstName,lastName,email,password,roles,isActive<br/>
                John,Doe,john@example.com,StrongPass123!,User,true<br/>
                Jane,Smith,jane@example.com,P@ssw0rd,Admin,true
              </pre>
              
              <h6>Required Fields:</h6>
              <ul>
                <li><strong>firstName*</strong> - First name</li>
                <li><strong>lastName*</strong> - Last name</li>
                <li><strong>email*</strong> - Email address (must be unique)</li>
                <li><strong>password*</strong> - Password (8+ chars with uppercase, lowercase, number, special)</li>
                <li><strong>roles*</strong> - User roles (comma separated for multiple)</li>
                <li><strong>isActive</strong> - User status (true/false, defaults to true)</li>
              </ul>
            </Card.Body>
          </Card>
        </Col>
      </Row>
    </Container>
  );
};

export default UserBulkUpload;
