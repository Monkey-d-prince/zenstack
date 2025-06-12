import axios from 'axios';
import React, { useEffect, useState } from 'react';
import { useParams, Link } from 'react-router-dom';
import '../styles/TableData.css';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faSort, faSortUp, faSortDown, faDownload, faHome, faSearch, faFilter } from '@fortawesome/free-solid-svg-icons';

const TableData = () => {
    const { tableName } = useParams();
    const [tableData, setTableData] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [columnNames, setColumnNames] = useState([]);
    
    const [currentPage, setCurrentPage] = useState(1);
    const [rowsPerPage, setRowsPerPage] = useState(20);
    const [totalPages, setTotalPages] = useState(0);
    
    const [sortColumn, setSortColumn] = useState(null);
    const [sortDirection, setSortDirection] = useState('asc');
    const [searchTerm, setSearchTerm] = useState('');
    const [filters, setFilters] = useState({});
    const [showFilters, setShowFilters] = useState(false);
    const [highlightedRow, setHighlightedRow] = useState(null);

    useEffect(() => {
        const fetchData = async () => {
            try {
                setLoading(true);
                
                const [dataResponse, schemaResponse] = await Promise.all([
                    axios.get(`http://0.0.0.0:3000/table/${tableName}`),
                    axios.get(`http://0.0.0.0:3000/schema/${tableName}`)
                ]);
                
                setTableData(dataResponse.data.data);
                
                if (schemaResponse.data && schemaResponse.data.schema) {
                    setColumnNames(schemaResponse.data.schema.map(col => col[0]));
                } else {
                    setColumnNames(dataResponse.data.data.length > 0 
                        ? Object.keys(dataResponse.data.data[0]).map((key) => key) 
                        : []);
                }
                
                setTotalPages(Math.ceil(dataResponse.data.data.length / rowsPerPage));
                setLoading(false);
            } catch (err) {
                console.error('Error fetching data:', err);
                setError(`Failed to fetch data for ${tableName}. Please try again later.`);
                setLoading(false);
            }
        };

        fetchData();
    }, [tableName, rowsPerPage]);

    useEffect(() => {
        setTotalPages(Math.ceil(filteredData.length / rowsPerPage));
        setCurrentPage(1);
    }, [rowsPerPage, tableData.length, searchTerm, filters]);

    const handleSort = (colIndex) => {
        if (sortColumn === colIndex) {
            setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc');
        } else {
            setSortColumn(colIndex);
            setSortDirection('asc');
        }
    };

    const filteredData = tableData.filter(row => {
        if (searchTerm) {
            const searchMatch = Object.values(row).some(value => 
                value !== null && String(value).toLowerCase().includes(searchTerm.toLowerCase())
            );
            if (!searchMatch) return false;
        }
        
        for (const [colIndex, filterValue] of Object.entries(filters)) {
            if (filterValue && row[colIndex] !== null) {
                if (!String(row[colIndex]).toLowerCase().includes(filterValue.toLowerCase())) {
                    return false;
                }
            }
        }
        
        return true;
    });

    const sortedData = [...filteredData].sort((a, b) => {
        if (sortColumn === null) return 0;
        
        const valueA = a[sortColumn];
        const valueB = b[sortColumn];
        
        if (valueA === null) return sortDirection === 'asc' ? -1 : 1;
        if (valueB === null) return sortDirection === 'asc' ? 1 : -1;
        
        if (typeof valueA === 'number' && typeof valueB === 'number') {
            return sortDirection === 'asc' ? valueA - valueB : valueB - valueA;
        }
        
        return sortDirection === 'asc'
            ? String(valueA).localeCompare(String(valueB))
            : String(valueB).localeCompare(String(valueA));
    });

    const indexOfLastRow = currentPage * rowsPerPage;
    const indexOfFirstRow = indexOfLastRow - rowsPerPage;
    const currentRows = sortedData.slice(indexOfFirstRow, indexOfLastRow);
    
    const visibleColumns = tableData.length > 0 
        ? Object.keys(tableData[0]).map((_, i) => i)
        : [];

    const paginate = (pageNumber) => setCurrentPage(pageNumber);

    const handleRowsPerPageChange = (e) => {
        setRowsPerPage(parseInt(e.target.value));
    };
    
    const exportToCSV = () => {
        const headers = columnNames.join(',');
        const csvRows = filteredData.map(row => 
            Object.values(row).map(value => 
                value !== null ? `"${String(value).replace(/"/g, '""')}"` : '""'
            ).join(',')
        );
        
        const csvContent = [headers, ...csvRows].join('\n');
        const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
        const url = URL.createObjectURL(blob);
        
        const link = document.createElement('a');
        link.setAttribute('href', url);
        link.setAttribute('download', `${tableName}_data.csv`);
        link.style.visibility = 'hidden';
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    };
    
    const handleFilterChange = (colIndex, value) => {
        if (value) {
            setFilters(prev => ({...prev, [colIndex]: value}));
        } else {
            setFilters(prev => {
                const newFilters = {...prev};
                delete newFilters[colIndex];
                return newFilters;
            });
        }
    };

    if (loading) {
        return (
            <div className="loading-container">
                <div className="loading-spinner"></div>
                <p className="loading-text">Loading {tableName} data...</p>
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
        <div className="table-container">
            <div className="table-header">
                <div className="breadcrumb">
                    <Link to="/" className="home-link">
                        <FontAwesomeIcon icon={faHome} /> Home
                    </Link>
                    <span className="separator">›</span>
                    <span className="current-page">{tableName}</span>
                </div>
                
                <h1 className="table-title">{tableName}</h1>
                
                <div className="table-actions">
                    <div className="search-container">
                        <FontAwesomeIcon icon={faSearch} className="search-icon" />
                        <input 
                            type="text"
                            placeholder="Search across all columns..."
                            value={searchTerm}
                            onChange={(e) => setSearchTerm(e.target.value)}
                            className="search-input"
                        />
                    </div>
                    
                    <button 
                        className="filter-button"
                        onClick={() => setShowFilters(!showFilters)}
                    >
                        <FontAwesomeIcon icon={faFilter} />
                        {Object.keys(filters).length > 0 && (
                            <span className="filter-count">{Object.keys(filters).length}</span>
                        )}
                    </button>
                    
                    <button className="export-button" onClick={exportToCSV}>
                        <FontAwesomeIcon icon={faDownload} /> Export CSV
                    </button>
                </div>
            </div>
            
            {showFilters && (
                <div className="filters-row">
                    {visibleColumns.map((colIndex) => (
                        <div key={`filter-${colIndex}`} className="filter-item">
                            <label>{columnNames[colIndex] || `Column ${colIndex}`}</label>
                            <input 
                                type="text" 
                                placeholder="Filter..."
                                value={filters[colIndex] || ''}
                                onChange={(e) => handleFilterChange(colIndex, e.target.value)}
                            />
                        </div>
                    ))}
                </div>
            )}
            
            <div className="table-controls">
                <div className="pagination-controls">
                    <button 
                        onClick={() => paginate(1)} 
                        disabled={currentPage === 1}
                        className="pagination-button first"
                        title="First Page"
                    >
                        &laquo;
                    </button>
                    <button 
                        onClick={() => paginate(currentPage - 1)} 
                        disabled={currentPage === 1}
                        className="pagination-button prev"
                        title="Previous Page"
                    >
                        &lsaquo;
                    </button>
                    <span className="pagination-info">
                        Page {currentPage} of {totalPages || 1}
                    </span>
                    <button 
                        onClick={() => paginate(currentPage + 1)} 
                        disabled={currentPage === totalPages || totalPages === 0}
                        className="pagination-button next"
                        title="Next Page"
                    >
                        &rsaquo;
                    </button>
                    <button 
                        onClick={() => paginate(totalPages)} 
                        disabled={currentPage === totalPages || totalPages === 0}
                        className="pagination-button last"
                        title="Last Page"
                    >
                        &raquo;
                    </button>
                </div>
                
                <div className="table-stats">
                    <span>{filteredData.length} records found</span>
                </div>
                
                <div className="rows-per-page">
                    <label htmlFor="rowsPerPage">Rows per page:</label>
                    <select 
                        id="rowsPerPage" 
                        value={rowsPerPage} 
                        onChange={handleRowsPerPageChange}
                        className="rows-select"
                    >
                        <option value={10}>10</option>
                        <option value={20}>20</option>
                        <option value={50}>50</option>
                        <option value={100}>100</option>
                        <option value={500}>500</option>
                    </select>
                </div>
            </div>
            
            <div className="table-wrapper">
                <table className="data-table">
                    <thead>
                        <tr>
                            {visibleColumns.map((colIndex) => (
                                <th 
                                    key={colIndex}
                                    onClick={() => handleSort(colIndex)}
                                    className={sortColumn === colIndex ? 'sorting-active' : 'sortable'}
                                >
                                    <div className="th-content">
                                        <span>{columnNames[colIndex] || `Column ${colIndex}`}</span>
                                        <span className="sort-icon">
                                            {sortColumn === colIndex ? (
                                                sortDirection === 'asc' ? 
                                                    <FontAwesomeIcon icon={faSortUp} /> : 
                                                    <FontAwesomeIcon icon={faSortDown} />
                                            ) : (
                                                <FontAwesomeIcon icon={faSort} />
                                            )}
                                        </span>
                                    </div>
                                </th>
                            ))}
                        </tr>
                    </thead>
                    <tbody>
                        {currentRows.length > 0 ? (
                            currentRows.map((row, rowIndex) => (
                                <tr 
                                    key={rowIndex}
                                    className={highlightedRow === rowIndex ? 'highlighted-row' : ''}
                                    onMouseEnter={() => setHighlightedRow(rowIndex)}
                                    onMouseLeave={() => setHighlightedRow(null)}
                                >
                                    {visibleColumns.map((colIndex) => (
                                        <td key={colIndex} title={row[colIndex] !== null ? String(row[colIndex]) : 'null'}>
                                            {row[colIndex] !== null ? String(row[colIndex]) : 
                                                <span className="null-value">null</span>}
                                        </td>
                                    ))}
                                </tr>
                            ))
                        ) : (
                            <tr>
                                <td colSpan={visibleColumns.length} className="no-data-message">
                                    No data matches your criteria
                                </td>
                            </tr>
                        )}
                    </tbody>
                </table>
            </div>
            
            {totalPages > 1 && (
                <div className="pagination">
                    {Array.from({ length: totalPages }, (_, i) => i + 1)
                        .filter(num => {
                            return num === 1 || 
                                  num === totalPages || 
                                  (num >= currentPage - 2 && num <= currentPage + 2);
                        })
                        .map((number, index, array) => {
                            if (index > 0 && number - array[index - 1] > 1) {
                                return (
                                    <React.Fragment key={`ellipsis-${number}`}>
                                        <span className="pagination-ellipsis">...</span>
                                        <button 
                                            onClick={() => paginate(number)}
                                            className={`pagination-item ${currentPage === number ? 'active' : ''}`}
                                        >
                                            {number}
                                        </button>
                                    </React.Fragment>
                                );
                            }
                            return (
                                <button 
                                    key={number} 
                                    onClick={() => paginate(number)}
                                    className={`pagination-item ${currentPage === number ? 'active' : ''}`}
                                >
                                    {number}
                                </button>
                            );
                        })}
                </div>
            )}
        </div>
    );
};

export default TableData;