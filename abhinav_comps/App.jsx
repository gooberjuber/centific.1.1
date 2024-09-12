import React, { useEffect, useState } from 'react';
import FlushExample from './TaskComponent';
import '../node_modules/bootstrap/dist/css/bootstrap-grid.min.css'; 
import AddTaskModal from './ToAsk';
import { Button } from 'react-bootstrap';

const hardcodedData = [
  { id: 1, taskname: "okati", task1: "Value1", task2: "Value2" },
  { id: 2, taskname: "rendu", task1: "Value3", task2: "Value4" }
];

const CheckboxComponent = () => {
  const [isChecked, setIsChecked] = useState(false);
  const [data, setData] = useState([]);
  const [selectedId, setSelectedId] = useState(null);
  const [selectedObj, setSelectedObj] = useState(null);
  const [showModal, setShowModal] = useState(false);
  const [newTaskData, setNewTaskData] = useState(null);

  const handleCheckboxChange = () => {
    setIsChecked(prev => !prev);
  };

  const handleDropdownChange = (event) => {
    const selectedId = parseInt(event.target.value, 10);
    setSelectedId(selectedId);
    setSelectedObj(data.find(item => item.id === selectedId));
  };

  const handleAddTask = () => {
    setShowModal(true);
  };

  const handleCloseModal = () => {
    setShowModal(false);
  };

  const handleSaveTask = (newTask) => {
    const newId = data.length ? Math.max(data.map(item => item.id)) + 1 : 1;
    const updatedData = [
      ...data,
      { id: newId, ...newTask }
    ];
    setData(updatedData);
    setNewTaskData({ id: newId, ...newTask });
    setShowModal(false);
  };

  useEffect(() => {
    const fetchData = async () => {
      if (isChecked) {
        setData(hardcodedData);
      } else {
        setData([]);
        setSelectedId(null);
        setSelectedObj(null);
      }
    };

    fetchData();
  }, [isChecked]);

  return (
    <div>
      <label>
        <input
          type="checkbox"
          checked={isChecked}
          onChange={handleCheckboxChange}
        />
        Want existing?
      </label>

      {isChecked ? (
        <div>
          <label>
            <select onChange={handleDropdownChange} value={selectedId || ''}>
              <option value="" disabled>Select an option</option>
              {data.map(item => (
                <option key={item.id} value={item.id}>
                  Item {item.taskname}
                </option>
              ))}
            </select>
          </label>

          <div>
            {selectedObj ? (
              <div>
                <p>Selected Object:</p>
                <FlushExample obj={selectedObj} />
              </div>
            ) : (
              <p>Please select an item from the dropdown.</p>
            )}
          </div>
        </div>
      ) : (
        <div>
          <br/>
          <Button variant="primary" onClick={handleAddTask}>
            +
          </Button>

          {/* {hardcodedData.append(newTaskData)} */}
          {newTaskData && <FlushExample obj={newTaskData} />}
        </div>
      )}

      <AddTaskModal
        show={showModal}
        handleClose={handleCloseModal}
        handleSave={handleSaveTask}
      />
    </div>
  );
};

export default CheckboxComponent;
