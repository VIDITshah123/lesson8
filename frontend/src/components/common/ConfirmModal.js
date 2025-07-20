import React from 'react';
import { Modal, Button } from 'react-bootstrap';

/**
 * A reusable confirmation modal component
 * 
 * @param {Object} props Component props
 * @param {boolean} props.show Whether the modal is visible
 * @param {string} props.title Modal title
 * @param {string|React.ReactNode} props.message Modal body message content
 * @param {string} props.confirmText Text for the confirm button
 * @param {string} props.cancelText Text for the cancel button
 * @param {string} props.confirmVariant Bootstrap variant for confirm button (e.g. 'primary', 'danger')
 * @param {Function} props.onConfirm Function to call when the confirm button is clicked
 * @param {Function} props.onCancel Function to call when the cancel button is clicked
 * @param {string} props.size Modal size ('sm', 'lg', 'xl')
 */
const ConfirmModal = ({
  show,
  title = 'Confirm Action',
  message = 'Are you sure you want to perform this action?',
  confirmText = 'Confirm',
  cancelText = 'Cancel',
  confirmVariant = 'primary',
  onConfirm,
  onCancel,
  size = 'md'
}) => {
  return (
    <Modal
      show={show}
      onHide={onCancel}
      backdrop="static"
      keyboard={false}
      centered
      size={size}
    >
      <Modal.Header closeButton>
        <Modal.Title>{title}</Modal.Title>
      </Modal.Header>
      <Modal.Body>
        {typeof message === 'string' ? <p>{message}</p> : message}
      </Modal.Body>
      <Modal.Footer>
        <Button variant="secondary" onClick={onCancel}>
          {cancelText}
        </Button>
        <Button variant={confirmVariant} onClick={onConfirm}>
          {confirmText}
        </Button>
      </Modal.Footer>
    </Modal>
  );
};

export default ConfirmModal;
