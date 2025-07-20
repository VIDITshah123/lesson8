import React, { useState, useRef } from 'react';
import { Container, Row, Col, Card, Button, Alert, ProgressBar, Table } from 'react-bootstrap';
import { Link } from 'react-router-dom';
import { FaCloudUploadAlt, FaFileAlt, FaArrowLeft, FaDownload } from 'react-icons/fa';
import { toast } from 'react-toastify';
import { roleAPI } from '../../services/api';
import './RoleBulkUpload.css'; // Will create this next

const RoleBulkUpload = () => {
  const [file, setFile] = useState(null);
  const [fileName, setFileName] = useState('');
  const [dragActive, setDragActive] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [uploadProgress, setUploadProgress] = useState(0);
  const [uploadResult, setUploadResult] = useState(null);
  const [error, setError] = useState('');
  const fileInputRef = useRef(null);

  // Handle file selection
  const handleFileSelect = (e) => {
    const selectedFile = e.target.files[0];
    validateAndSetFile(selectedFile);
  };

  // Handle file drop
  const handleDrop = (e) => {
    e.preventDefault();
    e.stopPropagation();
    setDragActive(false);
    
    if (e.dataTransfer.files && e.dataTransfer.files[0]) {
      validateAndSetFile(e.dataTransfer.files[0]);
    }
  };

  // Validate and set the selected file
  const validateAndSetFile = (selectedFile) => {
    if (!selectedFile) return;
    
    // Check if file is a CSV
    if (!selectedFile.name.endsWith('.csv') && selectedFile.type !== 'text/csv') {
      setError('Please upload a CSV file');
      return;
    }
    
    // Check file size (max 5MB)
    if (selectedFile.size > 5 * 1024 * 1024) {
      setError('File size should be less than 5MB');
      return;
    }
    
    setError('');
    setFile(selectedFile);
    setFileName(selectedFile.name);
  };

  // Handle drag events
  const handleDrag = (e) => {
    e.preventDefault();
    e.stopPropagation();
    
    if (e.type === 'dragenter' || e.type === 'dragover') {
      setDragActive(true);
    } else if (e.type === 'dragleave') {
      setDragActive(false);
    }
  };

  // Handle file upload
  const handleUpload = async () => {
    if (!file) {
      setError('Please select a file to upload');
      return;
    }
    
    try {
      setUploading(true);
      setUploadProgress(0);
      setError('');
      
      const formData = new FormData();
      formData.append('file', file);
      
      // Upload the file with progress tracking
      const response = await roleAPI.uploadBulkRoles(formData, (progressEvent) => {
        const progress = Math.round((progressEvent.loaded * 100) / progressEvent.total);
        setUploadProgress(progress);
      });
      
      setUploadResult(response.data.results);
      toast.success('Bulk role upload completed');
    } catch (error) {
      console.error('Error uploading roles:', error);
      setError(error.response?.data?.error || 'Failed to upload roles');
      toast.error('Failed to upload roles');
    } finally {
      setUploading(false);
      setFile(null);
      setFileName('');
      if (fileInputRef.current) {
        fileInputRef.current.value = '';
      }
    }
  };

  // Handle template download
  const handleTemplateDownload = async () => {
    try {
      await roleAPI.downloadRoleTemplate();
      toast.success('Template downloaded successfully');
    } catch (error) {
      console.error('Error downloading template:', error);
      toast.error('Failed to download template');
    }
  };

  // Reset upload state
  const handleReset = () => {
    setFile(null);
    setFileName('');
    setUploadResult(null);
    setError('');
    if (fileInputRef.current) {
      fileInputRef.current.value = '';
    }
  };

  return (
    <Container fluid className="mt-4 mb-5">
      <Card className="shadow-sm">
        <Card.Header className="bg-light">
          <div className="d-flex justify-content-between align-items-center">
            <h5 className="mb-0">
              <FaCloudUploadAlt className="me-2" />
              Bulk Role Upload
            </h5>
            <Link to="/roles" className="btn btn-outline-secondary btn-sm">
              <FaArrowLeft className="me-1" /> Back to Roles
            </Link>
          </div>
        </Card.Header>
        <Card.Body>
          {uploadResult ? (
            <div className="upload-results">
              <Alert variant={uploadResult.failed > 0 ? "warning" : "success"}>
                <h5>Upload Results</h5>
                <p>
                  Successfully created {uploadResult.success} roles out of {uploadResult.total} total.
                  {uploadResult.failed > 0 && ` Failed: ${uploadResult.failed}`}
                </p>
              </Alert>

              {uploadResult.created.length > 0 && (
                <div className="mt-3">
                  <h6>Successfully Created Roles:</h6>
                  <Table striped bordered hover size="sm">
                    <thead>
                      <tr>
                        <th>#</th>
                        <th>Role ID</th>
                        <th>Role Name</th>
                      </tr>
                    </thead>
                    <tbody>
                      {uploadResult.created.map((role, index) => (
                        <tr key={role.role_id}>
                          <td>{index + 1}</td>
                          <td>{role.role_id}</td>
                          <td>{role.name}</td>
                        </tr>
                      ))}
                    </tbody>
                  </Table>
                </div>
              )}

              {uploadResult.errors.length > 0 && (
                <div className="mt-3">
                  <h6>Errors:</h6>
                  <Table striped bordered hover variant="danger" size="sm">
                    <thead>
                      <tr>
                        <th>#</th>
                        <th>Role Data</th>
                        <th>Error</th>
                      </tr>
                    </thead>
                    <tbody>
                      {uploadResult.errors.map((error, index) => (
                        <tr key={index}>
                          <td>{index + 1}</td>
                          <td>{error.row}</td>
                          <td>{error.error}</td>
                        </tr>
                      ))}
                    </tbody>
                  </Table>
                </div>
              )}

              <div className="mt-4 text-center">
                <Button variant="primary" onClick={handleReset}>Upload Another File</Button>
              </div>
            </div>
          ) : (
            <Row>
              <Col md={8} className="mx-auto">
                <div className="text-center mb-4">
                  <Button 
                    variant="outline-primary" 
                    onClick={handleTemplateDownload} 
                    className="download-template-btn"
                  >
                    <FaDownload className="me-2" />
                    Download Template CSV
                  </Button>
                  <p className="text-muted mt-2 small">
                    Download a template CSV file with proper headers and an example row.
                  </p>
                </div>

                <div 
                  className={`file-upload-area ${dragActive ? 'drag-active' : ''}`}
                  onDragEnter={handleDrag}
                  onDragOver={handleDrag}
                  onDragLeave={handleDrag}
                  onDrop={handleDrop}
                >
                  <input 
                    type="file" 
                    id="fileInput" 
                    ref={fileInputRef}
                    onChange={handleFileSelect} 
                    accept=".csv"
                    className="file-input"
                  />
                  <div className="text-center py-5">
                    <FaFileAlt size={40} className="mb-3 text-primary" />
                    <h5>Drag & Drop Your CSV File Here</h5>
                    <p className="text-muted">or</p>
                    <Button 
                      variant="outline-primary"
                      onClick={() => fileInputRef.current && fileInputRef.current.click()}
                    >
                      Browse Files
                    </Button>
                    <p className="mt-3 text-muted small">
                      File should be a CSV with headers: name, description, permissions
                    </p>
                  </div>
                </div>

                {fileName && (
                  <div className="selected-file mt-3">
                    <p className="mb-1">Selected File: <strong>{fileName}</strong></p>
                    <div className="d-grid gap-2">
                      <Button 
                        variant="primary" 
                        onClick={handleUpload}
                        disabled={uploading}
                      >
                        {uploading ? 'Uploading...' : 'Upload Roles'}
                      </Button>
                    </div>
                  </div>
                )}

                {uploading && (
                  <ProgressBar 
                    animated 
                    now={uploadProgress} 
                    label={`${uploadProgress}%`} 
                    className="mt-3" 
                  />
                )}

                {error && <Alert variant="danger" className="mt-3">{error}</Alert>}
              </Col>
            </Row>
          )}
        </Card.Body>
      </Card>
    </Container>
  );
};

export default RoleBulkUpload;
