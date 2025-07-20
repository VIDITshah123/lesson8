const axios = require('axios');
const fs = require('fs');
const FormData = require('form-data');
const path = require('path');

const API_URL = 'http://localhost:5000/api';
const csvFilePath = path.join(__dirname, 'role-upload-sample.csv');

// Admin credentials
const credentials = {
  username: 'admin',
  password: 'Admin@123'
};

// Check if CSV file exists, if not create it
if (!fs.existsSync(csvFilePath)) {
  console.log('Creating sample CSV file...');
  const csvContent = 'name,description,permissions\nr1,View-only role for users and roles,user_view;role_view\nr2,Basic viewer role,user_view;role_view\nr3,Limited access viewer,user_view;role_view';
  fs.writeFileSync(csvFilePath, csvContent);
  console.log('Sample CSV file created at:', csvFilePath);
}

// Login and get token
async function login() {
  try {
    const response = await axios.post(`${API_URL}/authentication/login`, credentials);
    return response.data.token;
  } catch (error) {
    console.error('Login error:', error.response?.data || error.message);
    process.exit(1);
  }
}

// Test template download
async function downloadTemplate(token) {
  try {
    console.log('Testing template download endpoint...');
    const response = await axios.get(`${API_URL}/role_management/roles/template`, {
      headers: {
        Authorization: `Bearer ${token}`
      },
      responseType: 'text'
    });
    console.log('Template download successful:', response.data);
    return true;
  } catch (error) {
    console.error('Template download error:', error.response?.data || error.message);
    return false;
  }
}

// Upload the CSV file
async function uploadRoles(token) {
  try {
    console.log('Testing bulk upload endpoint...');
    
    // Check if file exists
    if (!fs.existsSync(csvFilePath)) {
      console.error('CSV file not found at:', csvFilePath);
      return false;
    }
    
    // Create form data with file
    const form = new FormData();
    form.append('file', fs.createReadStream(csvFilePath));
    
    const response = await axios.post(`${API_URL}/role_management/roles/bulk`, form, {
      headers: {
        Authorization: `Bearer ${token}`,
        ...form.getHeaders()
      }
    });
    
    console.log('Bulk upload response:', JSON.stringify(response.data, null, 2));
    return true;
  } catch (error) {
    console.error('Bulk upload error:', error.response?.data || error.message);
    return false;
  }
}

// Run the tests
async function runTests() {
  console.log('Starting API tests...');
  
  // Login and get token
  const token = await login();
  if (!token) {
    console.error('Failed to get authentication token');
    return;
  }
  console.log('Login successful, got token');
  
  // Test template download
  const templateResult = await downloadTemplate(token);
  
  // Test bulk upload
  if (templateResult) {
    await uploadRoles(token);
  }
  
  console.log('Tests completed');
}

runTests();
