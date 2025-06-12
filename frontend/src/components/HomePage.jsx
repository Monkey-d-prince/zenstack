import { faSearch, faTable } from '@fortawesome/free-solid-svg-icons';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import axios from 'axios';
import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import '../styles/Homepage.css';
import SchemaTooltip from './helpers/SchemaTooltip';

const HomePage = () => {
    const [tables, setTables] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [schemas, setSchemas] = useState({});
    const [searchTerm, setSearchTerm] = useState('');

    useEffect(() => {
        const fetchData = async () => {
            try {
                setLoading(true);
                const [tablesResponse, schemasResponse] = await Promise.all([
                    axios.get('http://0.0.0.0:3000/tables'),
                    axios.get('http://0.0.0.0:3000/schemas')
                ]);
                
                setTables(tablesResponse.data.tables);
                setSchemas(schemasResponse.data);
                setLoading(false);
            } catch (err) {
                console.error('Error fetching data:', err);
                setError('Failed to fetch database schema. Please try again later.');
                setLoading(false);
            }
        };

        fetchData();
    }, []);

    const filteredTables = tables.filter(tableName => 
        tableName.toLowerCase().includes(searchTerm.toLowerCase())
    );

    if (loading) {
    return (
      <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100vh' }}>
        <Spin size="large" />
      </div>
    );
  }

    if (error) {
        return (
            <div className="error-container">
                <p className="error-message">{error}</p>
                <button onClick={() => window.location.reload()} className="retry-button">
                    Retry
                </button>
            </div>
        );
    }

    return (
        <div className="container">
            <div className="dashboard-header">
                <h2 className="section-title">Available Database Tables</h2>
                <div className="search-container">
                    <FontAwesomeIcon icon={faSearch} className="search-icon" />
                    <input 
                        type="text"
                        placeholder="Search tables..."
                        value={searchTerm}
                        onChange={(e) => setSearchTerm(e.target.value)}
                        className="search-input"
                    />
                </div>
            </div>

            
            
            {filteredTables.length === 0 ? (
                <div className="no-results">
                    <p>No tables match your search criteria</p>
                </div>
            ) : (
                <div className="tables-grid">
                    {filteredTables.map((tableName) => (
                        <div className="table-card" key={tableName}>
                            <div className="table-card-header">
                                <FontAwesomeIcon icon={faTable} className="table-icon" />
                                <h3 className="table-name">
                                    <Link to={`/table/${tableName}`} className="table-link">
                                        {tableName}
                                    </Link>
                                </h3>
                            </div>
                            
                            <div className="table-info">
                                {schemas[tableName] && (
                                    <SchemaTooltip tableName={tableName} schema={schemas[tableName]} />
                                )}
                                
                                <div className="table-meta">
                                    {schemas[tableName] && (
                                        <span className="column-count">
                                            {schemas[tableName].length} column(s)
                                        </span>
                                    )}
                                </div>
                            </div>
                            
                            <Link to={`/table/${tableName}`} className="view-table-button">
                                View Table
                            </Link>
                        </div>
                    ))}
                </div>
            )}
        </div>
    );
};

export default HomePage;