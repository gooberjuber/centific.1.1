// AddTaskModal.js
import '../node_modules/bootstrap/dist/css/bootstrap-grid.min.css';
import React, { useState } from 'react';
import { Modal, Button, Form } from 'react-bootstrap';

const AddTaskModal = ({ show, handleClose, handleSave }) => {
  const [newTask, setNewTask] = useState({ taskname: '', task1: '', task2: '' });

  const handleInputChange = (event) => {
    const { name, value } = event.target;
    setNewTask(prevState => ({ ...prevState, [name]: value }));
  };

  const handleSaveClick = () => {
    handleSave(newTask);
    setNewTask({ taskname: '', task1: '', task2: '' }); // Clear form
  };

  return (
    <Modal show={show} onHide={handleClose}>
      <Modal.Header closeButton>
        <Modal.Title>Add New Task</Modal.Title>
      </Modal.Header>
      <Modal.Body>
        <Form>
          <Form.Group controlId="formTaskName">
            <Form.Label>Task Name</Form.Label>
            <Form.Control
              type="text"
              placeholder="Enter task name"
              name="taskname"
              value={newTask.taskname}
              onChange={handleInputChange}
            />
          </Form.Group>
          <Form.Group controlId="formTask1">
            <Form.Label>Task 1</Form.Label>
            <Form.Control
              type="text"
              placeholder="Enter value for task 1"
              name="task1"
              value={newTask.task1}
              onChange={handleInputChange}
            />
          </Form.Group>
          <Form.Group controlId="formTask2">
            <Form.Label>Task 2</Form.Label>
            <Form.Control
              type="text"
              placeholder="Enter value for task 2"
              name="task2"
              value={newTask.task2}
              onChange={handleInputChange}
            />
          </Form.Group>
        </Form>
      </Modal.Body>
      <Modal.Footer>
        <Button variant="secondary" onClick={handleClose}>
          Close
        </Button>
        <Button variant="primary" onClick={handleSaveClick}>
          Save Changes
        </Button>
      </Modal.Footer>
    </Modal>
  );
};

export default AddTaskModal;
