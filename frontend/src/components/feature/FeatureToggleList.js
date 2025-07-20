import React, { useEffect, useState } from 'react';
import { Table, Button, Form, Modal, InputGroup } from 'react-bootstrap';
import { FaPlus, FaSave, FaEdit, FaTrash } from 'react-icons/fa';
import { useAuth } from '../../contexts/AuthContext';
import axios from 'axios';

const FeatureToggleList = () => {
  const { hasRole, authToken } = useAuth();
  const [toggles, setToggles] = useState([]);
  const [showModal, setShowModal] = useState(false);
  const [editToggle, setEditToggle] = useState(null);
  const featureOptions = [
  { value: 'user_management', label: 'User Management' },
  { value: 'role_management', label: 'Role Management' },
  { value: 'permissions_management', label: 'Permissions Management' }
];
const [form, setForm] = useState({ feature_name: '', enabled: false, description: '', feature: featureOptions[0].value });
  const [loading, setLoading] = useState(false);

  const fetchToggles = async () => {
    setLoading(true);
    const res = await axios.get('/api/feature-toggles', { headers: { Authorization: `Bearer ${authToken}` } });
    setToggles(res.data);
    setLoading(false);
  };

  useEffect(() => { if (hasRole(['admin', 'full_access'])) fetchToggles(); }, [authToken]);

  const openModal = (toggle = null) => {
    setEditToggle(toggle);
    setForm(toggle ? { ...toggle } : { feature_name: '', enabled: false, description: '' });
    setShowModal(true);
  };

  const closeModal = () => { setShowModal(false); setEditToggle(null); };

  const handleSave = async () => {
    setLoading(true);
    if (editToggle) {
      await axios.put(`/api/feature-toggles/${editToggle.id}`, form, { headers: { Authorization: `Bearer ${authToken}` } });
    } else {
      await axios.post('/api/feature-toggles', form, { headers: { Authorization: `Bearer ${authToken}` } });
    }
    setShowModal(false);
    fetchToggles();
    setLoading(false);
  };

  const handleDelete = async (id) => {
    if (!window.confirm('Delete this feature toggle?')) return;
    setLoading(true);
    await axios.delete(`/api/feature-toggles/${id}`, { headers: { Authorization: `Bearer ${authToken}` } });
    fetchToggles();
    setLoading(false);
  };

  if (!hasRole(['admin', 'full_access'])) return <div className="text-danger">Access Denied</div>;

  return (
    <div>
      <div className="d-flex justify-content-between align-items-center mb-3">
        <h2>Feature Toggles</h2>
        <Button onClick={() => openModal()}><FaPlus className="me-1" /> Add Toggle</Button>
      </div>
      <Table bordered hover responsive>
        <thead>
          <tr>
            <th>Name</th>
            <th>Description</th>
            <th>Status</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>
          {toggles.map(toggle => (
            <tr key={toggle.id}>
              <td>{toggle.feature_name}</td>
              <td>{toggle.description}</td>
              <td>
                <Form.Check type="switch" checked={!!toggle.enabled} disabled readOnly label={toggle.enabled ? 'Enabled' : 'Disabled'} />
              </td>
              <td>
                <Button size="sm" variant="outline-primary" onClick={() => openModal(toggle)} className="me-2"><FaEdit /></Button>
                <Button size="sm" variant="outline-danger" onClick={() => handleDelete(toggle.id)}><FaTrash /></Button>
              </td>
            </tr>
          ))}
        </tbody>
      </Table>

      <Modal show={showModal} onHide={closeModal}>
        <Modal.Header closeButton>
          <Modal.Title>{editToggle ? 'Edit Feature Toggle' : 'Add Feature Toggle'}</Modal.Title>
        </Modal.Header>
        <Modal.Body>
          <Form>
            <Form.Group className="mb-3">
  <Form.Label>Feature</Form.Label>
  <Form.Select value={form.feature} onChange={e => setForm(f => ({ ...f, feature: e.target.value }))} disabled={!!editToggle} required>
    {featureOptions.map(opt => (
      <option key={opt.value} value={opt.value}>{opt.label}</option>
    ))}
  </Form.Select>
</Form.Group>
<Form.Group className="mb-3">
  <Form.Label>Feature Name</Form.Label>
  <Form.Control type="text" value={form.feature_name} onChange={e => setForm(f => ({ ...f, feature_name: e.target.value }))} disabled={!!editToggle} required />
</Form.Group>
            <Form.Group className="mb-3">
              <Form.Label>Description</Form.Label>
              <Form.Control as="textarea" rows={2} value={form.description} onChange={e => setForm(f => ({ ...f, description: e.target.value }))} />
            </Form.Group>
            <Form.Group className="mb-3">
              <Form.Check type="switch" label="Enabled" checked={!!form.enabled} onChange={e => setForm(f => ({ ...f, enabled: e.target.checked }))} />
            </Form.Group>
          </Form>
        </Modal.Body>
        <Modal.Footer>
          <Button variant="secondary" onClick={closeModal}>Cancel</Button>
          <Button variant="primary" onClick={handleSave}><FaSave className="me-1" /> Save</Button>
        </Modal.Footer>
      </Modal>
    </div>
  );
};

export default FeatureToggleList;
