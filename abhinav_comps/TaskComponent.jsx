import '../node_modules/bootstrap/dist/css/bootstrap-grid.min.css';
import Accordion from 'react-bootstrap/Accordion';

function FlushExample({ obj }) {
  return (
    <div>
    <Accordion defaultActiveKey="0" flush>
      <Accordion.Item eventKey="0">
        {/* <Accordion.Header>Accordion Item</Accordion.Header> */}
        <Accordion.Body>
          {Object.entries(obj).map(([key, value]) => (
            <div key={key}>
              <strong>{key}:</strong> {value}
            </div>
          ))}
        </Accordion.Body>
      </Accordion.Item>
    </Accordion>
    <br/>
    </div>
  );
}

export default FlushExample;
