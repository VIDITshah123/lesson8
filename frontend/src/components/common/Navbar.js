import React from 'react';
import { Container, Navbar as BootstrapNavbar, Nav, NavDropdown } from 'react-bootstrap';
import { useNavigate } from 'react-router-dom';
import { FaBars, FaUserCircle, FaSignOutAlt, FaCog, FaUser } from 'react-icons/fa';
import { useAuth } from '../../contexts/AuthContext';

const Navbar = ({ toggleSidebar, collapsed }) => {
  const { currentUser, logout } = useAuth();
  const navigate = useNavigate();

  const handleLogout = () => {
    logout();
    navigate('/login');
  };

  return (
    <BootstrapNavbar expand="lg" className="mb-4 glass-nav">
      <Container fluid>
        <button
          className="btn btn-light border-0 d-flex align-items-center"
          onClick={toggleSidebar}
          aria-label={collapsed ? 'Expand sidebar' : 'Collapse sidebar'}
        >
          <FaBars />
        </button>
        <BootstrapNavbar.Brand className="ms-2">
          <strong>EmployDEX</strong> Base Platform
        </BootstrapNavbar.Brand>
        <BootstrapNavbar.Toggle aria-controls="basic-navbar-nav" />
        <BootstrapNavbar.Collapse id="basic-navbar-nav">
          <Nav className="ms-auto">
            <NavDropdown 
              title={
                <div className="d-inline">
                  <FaUserCircle className="me-1" />
                  {currentUser?.firstName} {currentUser?.lastName}
                </div>
              } 
              id="user-dropdown"
              align="end"
            >
              <NavDropdown.Item onClick={() => navigate('/profile')}>
                <FaUser className="me-2" /> Profile
              </NavDropdown.Item>
              <NavDropdown.Item onClick={() => navigate('/settings')}>
                <FaCog className="me-2" /> Settings
              </NavDropdown.Item>
              <NavDropdown.Divider />
              <NavDropdown.Item onClick={handleLogout}>
                <FaSignOutAlt className="me-2" /> Logout
              </NavDropdown.Item>
            </NavDropdown>
          </Nav>
        </BootstrapNavbar.Collapse>
      </Container>
    </BootstrapNavbar>
  );
};

export default Navbar;
