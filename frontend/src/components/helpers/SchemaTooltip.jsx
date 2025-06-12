import { faInfoCircle, faTimes } from '@fortawesome/free-solid-svg-icons';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { useEffect, useRef, useState } from 'react';

const SchemaTooltip = ({ tableName, schema }) => {
  const [isOpen, setIsOpen] = useState(false);
  const tooltipRef = useRef(null);

  const toggleTooltip = (e) => {
    e.preventDefault();
    e.stopPropagation();
    setIsOpen(!isOpen);
  };

  
  useEffect(() => {
    if (!isOpen) return;
    
    function handleClickOutside(event) {
      if (tooltipRef.current && !tooltipRef.current.contains(event.target)) {
        setIsOpen(false);
      }
    }

    document.addEventListener("mousedown", handleClickOutside);
    return () => {
      document.removeEventListener("mousedown", handleClickOutside);
    };
  }, [isOpen]);

  return (
    <div className="schema-tooltip-container">
      <button 
        className="schema-toggle" 
        onClick={toggleTooltip}
        aria-label="View Schema"
      >
        <FontAwesomeIcon icon={faInfoCircle} />
        <span>Schema</span>
        <span className="column-count">({schema.length} cols)</span>
      </button>
      
      {isOpen && (
        <div className="schema-tooltip active" ref={tooltipRef}>
          <div className="tooltip-header">
            <h4>{tableName} Schema</h4>
            <button 
              className="close-tooltip" 
              onClick={() => setIsOpen(false)}
              aria-label="Close"
            >
              <FontAwesomeIcon icon={faTimes} />
            </button>
          </div>
          <ul className="column-list">
            {schema.map((column, index) => (
              <li key={index} className="column-item">
                <strong>{column[0]}</strong>
                <span className="column-type">{column[1]}</span>
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
};

export default SchemaTooltip;