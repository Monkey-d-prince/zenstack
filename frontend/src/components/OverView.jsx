import {
  faArrowDown, faArrowUp,
  faBuildingUser,
  faChartLine,
  faChartPie,
  faChevronLeft, faChevronRight,
  faCircle,
  faClock,
  faComments,
  faEnvelope,
  faEquals,
  faExclamationCircle, faExclamationTriangle,
  faFileAlt,
  faLaptop,
  faLaptopCode,
  faSpinner,
  faUsers
} from '@fortawesome/free-solid-svg-icons';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import axios from 'axios';
import { useEffect, useState } from 'react';
import '../styles/Overview.css';

const OverView = () => {
  const [metrics, setMetrics] = useState(null);
  const [teamData, setTeamData] = useState({
    teamsWithDrops: [],
    teamsWithRise: [],
    teamsWithConsistent: []
  });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  
  const [dropPage, setDropPage] = useState(1);
  const [risePage, setRisePage] = useState(1);
  const [consistentPage, setConsistentPage] = useState(1);
  const itemsPerPage = 5;

  const [teamsData, setTeamsData] = useState([]);
  
  const [selectedTeam, setSelectedTeam] = useState("all");
  const [appUsage, setAppUsage] = useState({
    productiveApps: [],
    unproductiveApps: [],
    neutralApps: []
  });

  const [aiRecommendations, setAiRecommendations] = useState({});
  const [loadingRecommendations, setLoadingRecommendations] = useState({});

  const fetchAppUsage = async (teamName) => {
    try {
      const endpoint = teamName === "all" 
        ? 'http://0.0.0.0:3000/overview/app-usage'
        : `http://0.0.0.0:3000/overview/app-usage/${teamName}`;
      
      const appUsageRes = await axios.get(endpoint);
      
      setAppUsage({
        productiveApps: appUsageRes.data.productive_apps || [],
        unproductiveApps: appUsageRes.data.unproductive_apps || [],
        neutralApps: appUsageRes.data.neutral_apps || []
      });
      
      return appUsageRes;
    } catch (err) {
      console.error('Error fetching app usage data:', err);
      setAppUsage({
        productiveApps: [],
        unproductiveApps: [],
        neutralApps: [],
        error: `Failed to load app usage data for ${teamName === "all" ? "all teams" : teamName}`
      });
      throw err;
    }
  };

const getAIRecommendation = async (team) => {
  try {
    setLoadingRecommendations(prev => ({ ...prev, [team.team_name]: true }));
    
    // Fix the URL format to match the backend endpoint definition
    // Change from /api/team-recommendations to /api/team/recommendations
    const response = await axios.post('http://0.0.0.0:3000/api/team/recommendations', {
      team_data: team
    });
    
    if (response.data && response.data.recommendation) {
      return response.data.recommendation;
    } else {
      throw new Error("Invalid response format from API");
    }
  } catch (error) {
    console.error("Error fetching AI recommendation:", error);
    return "Failed to generate recommendation. Please try again.";
  } finally {
    setLoadingRecommendations(prev => ({ ...prev, [team.team_name]: false }));
  }
};

  const [isRetrying, setIsRetrying] = useState(false);
  
  useEffect(() => {
    if (appUsage.error && !isRetrying) {
      setIsRetrying(true);
      
      const retryTimer = setTimeout(() => {
        console.log("Automatically retrying to fetch app usage data...");
        fetchAppUsage(selectedTeam)
          .finally(() => setIsRetrying(false));
      }, 3000);
      
      return () => clearTimeout(retryTimer);
    }
  }, [appUsage.error, selectedTeam]);

  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true);
        
        try {
          const [metricsRes, changesRes, teamsRes] = await Promise.all([
            axios.get('http://0.0.0.0:3000/overview/metrics'),
            axios.get('http://0.0.0.0:3000/overview/productivity-changes'),
            axios.get('http://0.0.0.0:3000/overview/teams-performance')
          ]);
          
          setMetrics(metricsRes.data);
          setTeamData({
            teamsWithDrops: changesRes.data.teams_with_drops || [],
            teamsWithRise: changesRes.data.teams_with_rise || [],
            teamsWithConsistent: changesRes.data.teams_with_consistent || []
          });
          setTeamsData(teamsRes.data.teams || []);
          
          try {
            const response = await fetchAppUsage("all");
            console.log('App usage data:', response);
          } catch (appErr) {
            console.error('Error fetching app usage data:', appErr);
          }
          
          setLoading(false);
        } catch (mainErr) {
          console.error('Error fetching main dashboard data:', mainErr);
          setError('Failed to load dashboard data. Please try again later.');
          setLoading(false);
        }
      } catch (err) {
        console.error('Unexpected error:', err);
        setError('An unexpected error occurred. Please try again later.');
        setLoading(false);
      }
    };
    
    fetchData();
  }, []);
  
  useEffect(() => {
    setDropPage(1);
    setRisePage(1);
    setConsistentPage(1);
  }, [teamData]);
  
  const [teamsEngagement, setTeamsEngagement] = useState({});
  const [engagementLoading, setEngagementLoading] = useState({});

  const fetchTeamEngagement = async (teamName) => {
    if (!teamName) return;
    
    try {
      setEngagementLoading(prev => ({ ...prev, [teamName]: true }));
      
      const endpoint = `http://0.0.0.0:3000/overview/app-usage/${teamName}`;
      const response = await axios.get(endpoint);
      
      const allApps = [
        ...(response.data.productive_apps || []),
        ...(response.data.neutral_apps || []),
        ...(response.data.unproductive_apps || [])
      ].sort((a, b) => b.hours - a.hours).slice(0, 3);
      
      setTeamsEngagement(prev => ({
        ...prev,
        [teamName]: allApps
      }));
    } catch (error) {
      console.error(`Error fetching engagement for ${teamName}:`, error);
    } finally {
      setEngagementLoading(prev => ({ ...prev, [teamName]: false }));
    }
  };

  useEffect(() => {
    if (teamsData.length > 0) {
      teamsData.forEach(team => {
        fetchTeamEngagement(team.team_name);
      });
    }
  }, [teamsData]);

  const [teamPage, setTeamPage] = useState(1);
  const [teamsPerPage, setTeamsPerPage] = useState(5);
  const [teamNameFilter, setTeamNameFilter] = useState('');
  const [workloadFilter, setWorkloadFilter] = useState('all');
  const [filteredTeams, setFilteredTeams] = useState([]);

  useEffect(() => {
    let filtered = [...teamsData];
    
    if (teamNameFilter.trim()) {
      filtered = filtered.filter(team => 
        team.team_name.toLowerCase().includes(teamNameFilter.toLowerCase())
      );
    }
    
    if (workloadFilter !== 'all') {
      filtered = filtered.filter(team => 
        team.workload_index.toLowerCase() === workloadFilter.toLowerCase()
      );
    }
    
    setFilteredTeams(filtered);
    setTeamPage(1);
  }, [teamsData, teamNameFilter, workloadFilter]);

  useEffect(() => {
    const generateRecommendationsForAllTeams = async () => {
      if (filteredTeams.length > 0) {
        const promises = filteredTeams.map(async (team) => {
          try {
            setLoadingRecommendations(prev => ({ ...prev, [team.team_name]: true }));
            const recommendation = await getAIRecommendation(team);
            setAiRecommendations(prev => ({
              ...prev,
              [team.team_name]: recommendation
            }));
          } catch (error) {
            console.error(`Error getting recommendation for ${team.team_name}:`, error);
          } finally {
            setLoadingRecommendations(prev => ({ ...prev, [team.team_name]: false }));
          }
        });
        
        await Promise.all(promises);
      }
    };

    generateRecommendationsForAllTeams();
  }, [filteredTeams]);

  const paginateData = (data, page, itemsPerPageCount = itemsPerPage) => {
    const startIndex = (page - 1) * itemsPerPageCount;
    return data.slice(startIndex, startIndex + itemsPerPageCount);
  };

  const totalTeamPages = Math.ceil(filteredTeams.length / teamsPerPage);

  const currentTeams = paginateData(filteredTeams, teamPage, teamsPerPage);

  const handleTeamsPerPageChange = (e) => {
    setTeamsPerPage(Number(e.target.value));
    setTeamPage(1);
  };

  const handleTeamNameFilterChange = (e) => {
    setTeamNameFilter(e.target.value);
  };

  const handleWorkloadFilterChange = (e) => {
    setWorkloadFilter(e.target.value);
  };

  if (loading) {
    return (
      <div className="loading-container">
        <div className="loading-spinner"></div>
        <p className="loading-text">Loading dashboard data...</p>
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
  
  const { teamsWithDrops, teamsWithRise, teamsWithConsistent } = teamData;
  
  const totalDropPages = Math.ceil(teamsWithDrops.length / itemsPerPage);
  const totalRisePages = Math.ceil(teamsWithRise.length / itemsPerPage);
  const totalConsistentPages = Math.ceil(teamsWithConsistent.length / itemsPerPage);
  
  const currentDrops = paginateData(teamsWithDrops, dropPage);
  const currentRise = paginateData(teamsWithRise, risePage);
  const currentConsistent = paginateData(teamsWithConsistent, consistentPage);
  
  const handleTeamFilterChange = (e) => {
    const teamName = e.target.value;
    setSelectedTeam(teamName);
    fetchAppUsage(teamName);
  };

  return (
    <div className="overview-container">
      <div className="overview-header">
        <h1>Employee Analytics Dashboard</h1>
        <div className="period-indicator">
          Analysis period: <span>{metrics.analysis_period}</span>
        </div>
      </div>
      
      <div className="metrics-cards">
        <div className="metric-card">
          <div className="metric-title">
            <FontAwesomeIcon icon={faUsers} className="metric-icon" />
            <span>Total Employees</span>
          </div>
          <div className="metric-value">{metrics.total_employees}</div>
        </div>
        
        <div className="metric-card">
          <div className="metric-title">
            <FontAwesomeIcon icon={faBuildingUser} className="metric-icon" />
            <span>Total Departments</span>
          </div>
          <div className="metric-value">{metrics.total_departments}</div>
        </div>
        
        <div className="metric-card">
          <div className="metric-title">
            <FontAwesomeIcon icon={faClock} className="metric-icon" />
            <span>Daily Online Hours</span>
          </div>
          <div className="metric-value">{metrics.daily_online_hours}</div>
          
          <div className="metric-subtitle">
            Daily Avg: {metrics.avg_daily_online_hours.toFixed(2)} hours
          </div>

          <div className="metric-subtitle">
            Overall 30-days: {metrics.total_online_hours} hours
          </div>
        </div>
        
        <div className="metric-card">
          <div className="metric-title">
            <FontAwesomeIcon icon={faChartLine} className="metric-icon" />
            <span>Daily Productive Hours</span>
          </div>
          <div className="metric-value">{metrics.daily_productive_hours}</div>
          
          <div className="metric-subtitle">
            Daily Avg: {metrics.avg_daily_productive_hours.toFixed(2)} hours
          </div>

          <div className="metric-subtitle">
            Overall 30-days: {metrics.total_productive_hours} hours
          </div>
        </div>
        
        <div className="metric-card">
          <div className="metric-title">
            <FontAwesomeIcon icon={faLaptop} className="metric-icon" />
            <span>Daily Active Hours</span>
          </div>
          <div className="metric-value">{metrics.daily_active_hours}</div>

          <div className="metric-subtitle">
            Daily Avg: {metrics.avg_daily_active_hours.toFixed(2)} hours
          </div>

          <div className="metric-subtitle">
            Overall 30-days: {metrics.total_active_hours} hours
          </div>
        </div>
      </div>
      
      <div className="section-header">
        <h2>Team Productivity Changes</h2>
      </div>
      
      <div className="productivity-changes-card drop-card">
        <div className="changes-header">
          <div className="changes-title">
            <h3>Teams with Productivity Drop</h3>
            <span className="counter">({teamsWithDrops.length})</span>
          </div>
        </div>
        
        <div className="changes-table-container">
          <table className="changes-table">
            <thead>
              <tr>
                <th className="text-left">TEAM/DEPARTMENT</th>
                <th>CURRENT %</th>
                <th>PREVIOUS %</th>
                <th>CHANGE</th>
                <th>KEY FACTORS</th>
                <th>ACTION NEEDED</th>
              </tr>
            </thead>
            <tbody>
              {currentDrops.map((team, index) => (
                <tr key={index}>
                  <td className="text-left">{team.team}</td>
                  <td>{team.current_percent}%</td>
                  <td>{team.previous_percent}%</td>
                  <td className="change-cell">
                    <span className="negative-change">
                      <FontAwesomeIcon icon={faArrowDown} />
                      {Math.abs(team.change)}%
                    </span>
                  </td>
                  <td>
                    <ul className="factors-list">
                      {team.key_factors.map((factor, factorIndex) => (
                        <li key={factorIndex}>• {factor}</li>
                      ))}
                    </ul>
                  </td>
                  <td className="action-cell">
                    {team.action_needed}
                  </td>
                </tr>
              ))}
              {teamsWithDrops.length === 0 && (
                <tr>
                  <td colSpan={6} className="no-data">No teams with productivity drops found</td>
                </tr>
              )}
            </tbody>
          </table>
          
          {teamsWithDrops.length > itemsPerPage && (
            <div className="pagination">
              <button 
                className="pagination-button"
                onClick={() => setDropPage(prev => Math.max(prev - 1, 1))}
                disabled={dropPage === 1}
              >
                <FontAwesomeIcon icon={faChevronLeft} />
              </button>
              
              <span className="pagination-info">
                Page {dropPage} of {totalDropPages}
              </span>
              
              <button 
                className="pagination-button"
                onClick={() => setDropPage(prev => Math.min(prev + 1, totalDropPages))}
                disabled={dropPage === totalDropPages}
              >
                <FontAwesomeIcon icon={faChevronRight} />
              </button>
            </div>
          )}
        </div>
      </div>
      
      <div className="productivity-changes-card rise-card">
        <div className="changes-header">
          <div className="changes-title">
            <h3>Teams with Productivity Rise</h3>
            <span className="counter rise-counter">({teamsWithRise.length})</span>
          </div>
        </div>
        
        <div className="changes-table-container">
          <table className="changes-table">
            <thead>
              <tr>
                <th className="text-left">TEAM/DEPARTMENT</th>
                <th>CURRENT %</th>
                <th>PREVIOUS %</th>
                <th>CHANGE</th>
                <th>SUCCESS FACTORS</th>
                <th>RECOMMENDATIONS</th>
              </tr>
            </thead>
            <tbody>
              {currentRise.map((team, index) => (
                <tr key={index}>
                  <td className="text-left">{team.team}</td>
                  <td>{team.current_percent}%</td>
                  <td>{team.previous_percent}%</td>
                  <td className="change-cell">
                    <span className="positive-change">
                      <FontAwesomeIcon icon={faArrowUp} />
                      {team.change}%
                    </span>
                  </td>
                  <td>
                    <ul className="factors-list">
                      {team.success_factors.map((factor, factorIndex) => (
                        <li key={factorIndex}>• {factor}</li>
                      ))}
                    </ul>
                  </td>
                  <td className="recommendations-cell">
                    {team.recommendations}
                  </td>
                </tr>
              ))}
              {teamsWithRise.length === 0 && (
                <tr>
                  <td colSpan={6} className="no-data">No teams with productivity rise found</td>
                </tr>
              )}
            </tbody>
          </table>
          
          {teamsWithRise.length > itemsPerPage && (
            <div className="pagination">
              <button 
                className="pagination-button"
                onClick={() => setRisePage(prev => Math.max(prev - 1, 1))}
                disabled={risePage === 1}
              >
                <FontAwesomeIcon icon={faChevronLeft} />
              </button>
              
              <span className="pagination-info">
                Page {risePage} of {totalRisePages}
              </span>
              
              <button 
                className="pagination-button"
                onClick={() => setRisePage(prev => Math.min(prev + 1, totalRisePages))}
                disabled={risePage === totalRisePages}
              >
                <FontAwesomeIcon icon={faChevronRight} />
              </button>
            </div>
          )}
        </div>
      </div>
      
      <div className="productivity-changes-card consistent-card">
        <div className="changes-header">
          <div className="changes-title">
            <h3>Teams with Consistent Performance</h3>
            <span className="counter consistent-counter">({teamsWithConsistent.length})</span>
          </div>
        </div>
        
        <div className="changes-table-container">
          <table className="changes-table">
            <thead>
              <tr>
                <th className="text-left">TEAM/DEPARTMENT</th>
                <th>CURRENT %</th>
                <th>PREVIOUS %</th>
                <th>VARIANCE</th>
                <th>STABILITY FACTORS</th>
                <th>MAINTAIN STRATEGY</th>
              </tr>
            </thead>
            <tbody>
              {currentConsistent.map((team, index) => (
                <tr key={index}>
                  <td className="text-left">{team.team}</td>
                  <td>{team.current_percent}%</td>
                  <td>{team.previous_percent}%</td>
                  <td className="change-cell">
                    <span className="neutral-change">
                      <FontAwesomeIcon icon={faEquals} />
                      {team.variance.toFixed(1)}%
                    </span>
                  </td>
                  <td>
                    <ul className="factors-list">
                      {team.stability_factors.map((factor, factorIndex) => (
                        <li key={factorIndex}>• {factor}</li>
                      ))}
                    </ul>
                  </td>
                  <td className="strategy-cell">
                    {team.maintain_strategy}
                  </td>
                </tr>
              ))}
              {teamsWithConsistent.length === 0 && (
                <tr>
                  <td colSpan={6} className="no-data">No teams with consistent performance found</td>
                </tr>
              )}
            </tbody>
          </table>
          
          {teamsWithConsistent.length > itemsPerPage && (
            <div className="pagination">
              <button 
                className="pagination-button"
                onClick={() => setConsistentPage(prev => Math.max(prev - 1, 1))}
                disabled={consistentPage === 1}
              >
                <FontAwesomeIcon icon={faChevronLeft} />
              </button>
              
              <span className="pagination-info">
                Page {consistentPage} of {totalConsistentPages}
              </span>
              
              <button 
                className="pagination-button"
                onClick={() => setConsistentPage(prev => Math.min(prev + 1, totalConsistentPages))}
                disabled={consistentPage === totalConsistentPages}
              >
                <FontAwesomeIcon icon={faChevronRight} />
              </button>
            </div>
          )}
        </div>
      </div>

      <div className="section-header" style={{ marginTop: '40px' }}>
        <h2>
          {selectedTeam === "all" 
            ? "Organization-wide App Usage" 
            : `App Usage - ${selectedTeam} Team`}
        </h2>
        <div className="team-filter-container">
          <label htmlFor="team-filter">Filter by team: </label>
          <select 
            id="team-filter" 
            value={selectedTeam} 
            onChange={handleTeamFilterChange}
            className="team-filter-select"
          >
            <option value="all">All Teams</option>
            {teamsData.map((team, index) => (
              <option key={index} value={team.team_name}>
                {team.team_name}
              </option>
            ))}
          </select>
        </div>
      </div>

      <div className="app-usage-container">
        {appUsage.error ? (
          <div className="app-usage-error">
            <FontAwesomeIcon icon={faExclamationCircle} className="error-icon" />
            <p>{appUsage.error}</p>
            {isRetrying ? (
              <div className="retry-loading">
                <FontAwesomeIcon icon={faSpinner} className="loading-icon" spin />
                <span>Automatically retrying...</span>
              </div>
            ) : (
              <button 
                onClick={() => fetchAppUsage(selectedTeam)}
                className="retry-button"
              >
                Retry Loading App Data
              </button>
            )}
          </div>
        ) : (
          <>
            <div className="app-usage-column">
              <h3>Top Productive Apps</h3>
              <div className="app-list">
                {appUsage.productiveApps.length === 0 ? (
                  <div className="no-data-message">No productive app data available</div>
                ) : (
                  appUsage.productiveApps.map((app, index) => (
                    <div key={index} className="app-item">
                      <div className="app-name">{app.name}</div>
                      <div className="app-duration-bar">
                        <div 
                          className="duration-fill productive" 
                          style={{width: `${Math.min(100, (app.hours / (appUsage.productiveApps[0]?.hours || 1)) * 100)}%`}}
                        ></div>
                        <span className="duration-text">{app.formatted_duration}</span>
                      </div>
                      <div className="app-users">{app.user_count} users</div>
                    </div>
                  ))
                )}
              </div>
            </div>
            
            <div className="app-usage-column">
              <h3>Top Unproductive Apps</h3>
              <div className="app-list">
                {appUsage.unproductiveApps.map((app, index) => (
                  <div key={index} className="app-item">
                    <div className="app-name">{app.name}</div>
                    <div className="app-duration-bar">
                      <div 
                        className="duration-fill unproductive" 
                        style={{width: `${Math.min(100, (app.hours / (appUsage.unproductiveApps[0]?.hours || 1)) * 100)}%`}}
                      ></div>
                      <span className="duration-text">{app.formatted_duration}</span>
                    </div>
                    <div className="app-users">{app.user_count} users</div>
                  </div>
                ))}
              </div>
            </div>
            
            <div className="app-usage-column">
              <h3>Top Neutral Apps</h3>
              <div className="app-list">
                {appUsage.neutralApps.map((app, index) => (
                  <div key={index} className="app-item">
                    <div className="app-name">{app.name}</div>
                    <div className="app-duration-bar">
                      <div 
                        className="duration-fill neutral" 
                        style={{width: `${Math.min(100, (app.hours / (appUsage.neutralApps[0]?.hours || 1)) * 100)}%`}}
                      ></div>
                      <span className="duration-text">{app.formatted_duration}</span>
                    </div>
                    <div className="app-users">{app.user_count} users</div>
                  </div>
                ))}
              </div>
            </div>
          </>
        )}
      </div>

      <div className="section-header" style={{ marginTop: '40px' }}>
        <h2>Teams Performance</h2>
        
        <div className="teams-filters">
          <div className="filter-group">
            <label htmlFor="team-name-filter">Search team: </label>
            <input 
              type="text"
              id="team-name-filter"
              value={teamNameFilter}
              onChange={handleTeamNameFilterChange}
              placeholder="Filter by team name"
              className="team-filter-input"
            />
          </div>
          
          <div className="filter-group">
            <label htmlFor="workload-filter">Workload: </label>
            <select 
              id="workload-filter"
              value={workloadFilter}
              onChange={handleWorkloadFilterChange}
              className="team-filter-select"
            >
              <option value="all">All Workloads</option>
              <option value="high">High</option>
              <option value="medium">Medium</option>
              <option value="low">Low</option>
            </select>
          </div>
          
          <div className="filter-group">
            <label htmlFor="teams-per-page">Teams per page: </label>
            <select 
              id="teams-per-page"
              value={teamsPerPage}
              onChange={handleTeamsPerPageChange}
              className="team-filter-select"
            >
              <option value={5}>5</option>
              <option value={10}>10</option>
            </select>
          </div>
          
          <div className="teams-count">
            Showing {Math.min(filteredTeams.length, teamsPerPage)} of {filteredTeams.length} teams
          </div>
        </div>
      </div>

      <div className="teams-table-container">
        <table className="teams-table">
          <thead>
            <tr>
              <th>Team Name</th>
              <th>Performance</th>
              <th>Engagement</th>
              <th>Workload Index</th>
              <th>Alerts</th>
              <th>Action</th>
            </tr>
          </thead>
          <tbody>
            {currentTeams.map((team, index) => (
              <tr key={index}>
                <td className="team-name">
                  <div>{team.team_name}</div>
                  <div className="team-stats">
                    <span className="team-count">{team.employee_count} employees</span>
                    <span className="team-break">Break avg: {team.break_time} min</span>
                  </div>
                </td>
                
                <td className="performance-cell">
                  <div className="performance-metrics">
                    <div className="hours-metric">
                      <FontAwesomeIcon icon={faClock} className="hours-icon" />
                      <span>{team.performance.online} hrs</span>
                    </div>
                    
                    <div className="hours-breakdown">
                      <div className="hour-metric-item">
                        <span className="hour-label">P</span>
                        <span className="hour-value">{team.performance.productive} hrs</span>
                      </div>
                      <div className="hour-metric-item">
                        <span className="hour-label">A</span>
                        <span className="hour-value">{team.performance.active} hrs</span>
                      </div>
                      <div className="hour-metric-item">
                        <span className="hour-label">I</span>
                        <span className="hour-value">{team.performance.idle} hrs</span>
                      </div>
                      <div className="hour-metric-item">
                        <span className="hour-label">N</span>
                        <span className="hour-value">{team.performance.neutral} hrs</span>
                      </div>
                      <div className="hour-metric-item">
                        <span className="hour-label">U</span>
                        <span className="hour-value">{team.performance.unproductive} hrs</span>
                      </div>
                    </div>
                    
                    <div className="progress-bar">
                      <div 
                        className="progress-fill" 
                        style={{width: `${Math.min(100, team.performance.productive * 100 / (team.performance.online || 1))}%`}}
                      ></div>
                    </div>
                  </div>
                </td>
                
                <td className="engagement-cell">
                  {engagementLoading[team.team_name] ? (
                    <div className="loading-engagement">
                      <FontAwesomeIcon icon={faSpinner} className="loading-icon" spin />
                      <span>Loading apps...</span>
                    </div>
                  ) : teamsEngagement[team.team_name] ? (
                    <>
                      {teamsEngagement[team.team_name].map((app, idx) => (
                        <div key={idx} className="engagement-item">
                          <FontAwesomeIcon 
                            icon={
                              app.name.includes('Code') ? faLaptopCode :
                              app.name.includes('GitHub') ? faLaptopCode :
                              app.name.includes('Outlook') || app.name.includes('Mail') ? faEnvelope :
                              app.name.includes('Excel') ? faFileAlt :
                              app.name.includes('Word') ? faFileAlt :
                              app.name.includes('Analytics') ? faChartPie :
                              app.name.includes('Edge') || app.name.includes('Chrome') ? faCircle :
                              app.name.includes('HubSpot') ? faUsers :
                              app.name.includes('Teams') || app.name.includes('Slack') ? faComments :
                              faCircle
                            } 
                            className="engagement-icon"
                          />
                          <span>{app.name}: {app.formatted_duration || `${app.hours} hrs`}</span>
                        </div>
                      ))}
                      <div className="unproductive-day">
                        <FontAwesomeIcon icon={faExclamationCircle} className="warning-icon" />
                        <span>Least productive: <strong>{team.least_productive_day}</strong></span>
                      </div>
                    </>
                  ) : (
                    <>
                      {team.top_apps.map((app, idx) => (
                        <div key={idx} className="engagement-item">
                          <FontAwesomeIcon 
                            icon={
                              app.name.includes('Code') ? faLaptopCode :
                              app.name.includes('GitHub') ? faLaptopCode :
                              app.name.includes('Outlook') ? faEnvelope :
                              app.name.includes('Excel') ? faFileAlt :
                              app.name.includes('Analytics') ? faChartPie :
                              app.name.includes('HubSpot') ? faUsers :
                              app.name.includes('Teams') || app.name.includes('Slack') ? faComments :
                              faCircle
                            } 
                            className="engagement-icon"
                          />
                          <span>
                            {app.name.includes(':') 
                              ? app.name 
                              : `${app.name}: ${app.hours} hrs`}
                          </span>
                        </div>
                      ))}
                      <div className="unproductive-day">
                        <FontAwesomeIcon icon={faExclamationCircle} className="warning-icon" />
                        <span>Least productive: <strong>{team.least_productive_day}</strong></span>
                      </div>
                    </>
                  )}
                </td>
                
                <td className={`workload-cell ${team.workload_index.toLowerCase()}`}>
                  {team.workload_index}
                </td>
                
                <td className="alerts-cell">
                  {team.alert === "No alerts" ? (
                    <span className="no-alert">No alerts</span>
                  ) : (
                    <div className="alert-item">
                      <FontAwesomeIcon icon={faExclamationTriangle} className="alert-icon" />
                      <span>{team.alert}</span>
                    </div>
                  )}
                </td>
                
                <td className="action-cell">
                  {loadingRecommendations[team.team_name] ? (
                    <div className="loading-recommendation">
                      <FontAwesomeIcon icon={faSpinner} spin className="loading-icon" /> 
                      <span>Generating recommendation...</span>
                    </div>
                  ) : (
                    <div className="ai-recommendation">
                      <p>{aiRecommendations[team.team_name] || "Recommendation loading..."}</p>
                    </div>
                  )}
                </td>
              </tr>
            ))}
            
            {filteredTeams.length === 0 && (
              <tr>
                <td colSpan={6} className="no-data">No teams match the selected filters</td>
              </tr>
            )}
          </tbody>
        </table>
        
        {filteredTeams.length > teamsPerPage && (
          <div className="pagination">
            <button 
              className="pagination-button"
              onClick={() => setTeamPage(prev => Math.max(prev - 1, 1))}
              disabled={teamPage === 1}
            >
              <FontAwesomeIcon icon={faChevronLeft} />
            </button>
            
            <span className="pagination-info">
              Page {teamPage} of {totalTeamPages}
            </span>
            
            <button 
              className="pagination-button"
              onClick={() => setTeamPage(prev => Math.min(prev + 1, totalTeamPages))}
              disabled={teamPage === totalTeamPages}
            >
              <FontAwesomeIcon icon={faChevronRight} />
            </button>
          </div>
        )}
      </div>
    </div>
  );
};

export default OverView;