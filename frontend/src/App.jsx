import { Route, BrowserRouter as Router, Routes } from 'react-router-dom';
import HomePage from './components/HomePage';
import Dashboard from './components/dashboard';
import OverView from './components/OverView';
import TableData from './components/TableData';
import TeamPerformance from './components/TeamPerformance';
import Chatbot from './components/ChatBot';
import Header from './components/layout/Header';
import './styles/App.css';
import WeeklyRiskDashboard from './components/WeeklyRiskDashboard';

function App() {

  return (
    <Router>
      <div className="app">
        <Header />
        <main className="main-content">
          <Routes>
            {/* <Route path="/" element={<HomePage />} /> */}
            <Route path="/table/:tableName" element={<TableData />} />
            <Route path="/" element={<OverView/>} />
            <Route path="/dashboard" element={<Dashboard/>} />
            <Route path="/team-performance" element={<TeamPerformance/>} />
            <Route path="/chatbot" element={<Chatbot />} />
            <Route path='/weekly-trends' element={<WeeklyRiskDashboard/>}/>
          </Routes>
        </main>
      </div>
    </Router>
  );
}

export default App;