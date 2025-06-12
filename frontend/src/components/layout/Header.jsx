import { Link, useLocation } from 'react-router-dom';
import '../../styles/Header.css';

const Header = () => {
  const location = useLocation();

  const isActive = (path) => {
    if (path === '/' && location.pathname === '/') return true;
    if (path !== '/' && location.pathname.startsWith(path)) return true;
    return false;
  };

  return (
    <header className="app-header">
      <div className="header-container">
        <div className="header-logo">
          <Link to="/" className="logo-link">
            <span className="logo-icon">📊</span>
            <span className="logo-text">Employee Analytics</span>
          </Link>
        </div>
        
        <nav className="header-nav">
          <ul className="nav-links">
            {/* <li className="nav-item">
              <Link 
                to="/" 
                className={`nav-link ${isActive('/') ? 'active' : ''}`}
              >
                Home
              </Link>
            </li> */}
            <li className="nav-item">
              <Link 
                to="/" 
                className={`nav-link ${isActive('/') ? 'active' : ''}`}
              >
                Home
              </Link>
            </li>
            {/* <li className="nav-item">
              <Link 
                to="/dashboard" 
                className={`nav-link ${isActive('/dashboard') ? 'active' : ''}`}
              >
                Dashboard
              </Link>
            </li> */}
            <li className="nav-item">
              <Link 
                to="/weekly-trends" 
                className={`nav-link ${isActive('/weekly-trends') ? 'active' : ''}`}
              >
                Dashboard
              </Link>
            </li>
            <li className="nav-item">
              <Link 
                to="/team-performance" 
                className={`nav-link ${isActive('/team-performance') ? 'active' : ''}`}
              >
                Team Analysis
              </Link>
            </li>
            <li className="nav-item">
              <Link 
                to="/chatbot" 
                className={`nav-link ${isActive('/chatbot') ? 'active' : ''}`}
              >
                Zen.Ai
              </Link>
            </li>
          </ul>
        </nav>
      </div>
    </header>
  );
};

export default Header;