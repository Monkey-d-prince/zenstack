import { ArrowDownOutlined, ArrowUpOutlined, InfoCircleOutlined, WarningOutlined } from '@ant-design/icons';
import { Card, Col, Modal, Progress, Row, Select, Spin, Statistic, Table, Tag } from 'antd';
import { useEffect, useState } from 'react';
import '../styles/WeeklyRiskDashboard.css';

const WeeklyRiskDashboard = () => {
  const [riskData, setRiskData] = useState({
    company_risk: {},
    team_risks: [],
    individual_risks: []
  });
  const [attendanceData, setAttendanceData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [isMetricsModalVisible, setIsMetricsModalVisible] = useState(true);

  const handleMetricsModalClose = () => {
    setIsMetricsModalVisible(false);
  };

  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true);
    
        // Fetch employee weekly trends data
        const weeklyTrendsResponse = await fetch('http://127.0.0.1:3000/overview/employee-weekly-trends');
        const weeklyTrendsData = await weeklyTrendsResponse.json();
        
        // Fetch risk assessment data
        const riskResponse = await fetch('http://127.0.0.1:3000/overview/risk-assessment');
        const riskAssessmentData = await riskResponse.json();
        
        // Fetch attendance data from the new endpoint
        const attendanceResponse = await fetch('http://127.0.0.1:3000/get-attendance');
        const attendanceResult = await attendanceResponse.json();
        
        setAttendanceData(attendanceResult.attendance || []);
        
        // Process the data for weekly comparisons
        processWeeklyData(weeklyTrendsData, riskAssessmentData, attendanceResult.attendance || []);
      } catch (error) {
        console.error("Failed to fetch risk data:", error);
        // Use sample data for demonstration
        generateSampleData();
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, []);

  const processWeeklyData = (weeklyTrendsData, riskAssessmentData, attendanceData) => {
    // Create a Set to track processed employees and avoid duplicates
    const processedEmployeeNames = new Set();
    
    // Process weekly trends data to calculate changes
    console.log('Initial employees count:', weeklyTrendsData.employees?.length);
    
    const processedEmployees = weeklyTrendsData.employees?.map(employee => {
      // Skip if employee already processed (using employee name instead of ID)
      if (processedEmployeeNames.has(employee.employee_name)) {
        console.log('Duplicate employee found:', employee.employee_name);
        return null;
      }
      processedEmployeeNames.add(employee.employee_name);

      const trends = employee.trends || [];
      
      // Get first and last week data
      const firstWeek = trends[0];
      const lastWeek = trends[trends.length - 1];
      
      // IMPORTANT: Don't filter out employees without first/last week data
      // Instead, use default values
      if (!firstWeek || !lastWeek) {
        console.log('Employee missing week data:', employee.employee_name, 'trends length:', trends.length);
        // Use default values instead of returning null
        const defaultWeek = { productivity: 50, hours: 6 };
        const firstWeekData = firstWeek || defaultWeek;
        const lastWeekData = lastWeek || defaultWeek;
        
        // Continue processing with default values...
        const productivityChange = (lastWeekData.productivity - firstWeekData.productivity).toFixed(1);
        const activityChange = (lastWeekData.hours - firstWeekData.hours).toFixed(1);
        
        // Find matching risk assessment data
        const riskData = riskAssessmentData.employees?.find(
          emp => emp.employee_id === employee.employee_id
        );
        
        // Find matching attendance data by employee name
        const employeeAttendance = attendanceData.find(
          att => att.employee === employee.employee_name || 
                 att.employee.toLowerCase().includes(employee.employee_name.toLowerCase()) ||
                 employee.employee_name.toLowerCase().includes(att.employee.toLowerCase())
        );
        
        // Set default risk score for employees with missing data
        let riskScore = 5; // Medium risk for missing data
        let attendanceRate = 0;
        let attendanceMetrics = null;
        
        if (employeeAttendance) {
          attendanceRate = employeeAttendance.attendance_rate;
          const daysPresent = Math.round((attendanceRate / 100) * 30);
          const absences = 30 - daysPresent;
          
          attendanceMetrics = {
            days_present: daysPresent,
            absences: absences,
            total_working_days: 30,
            attendance_rate: attendanceRate,
            punctuality: {
              percentage: attendanceRate > 0 ? Math.min(100, attendanceRate + 10) : 0,
              late_arrivals: attendanceRate === 0 ? 30 : Math.max(0, Math.floor((100 - attendanceRate) / 10)),
              early_departures: attendanceRate === 0 ? 15 : Math.floor((100 - attendanceRate) / 20)
            }
          };
          
          // If attendance is zero, set risk score to 10
          if (attendanceRate === 0) {
            riskScore = 10;
          }
        }
        
        return {
          employee_id: employee.employee_id || `TEMP_${employee.employee_name}`,
          employee_name: employee.employee_name,
          team: employee.team || 'Unknown',
          risk_score: riskScore.toFixed(1),
          metrics: {
            productivity_week1: firstWeekData.productivity.toFixed(1),
            productivity_week4: lastWeekData.productivity.toFixed(1),
            productivity_change: productivityChange,
            activity_week1: firstWeekData.hours.toFixed(1),
            activity_week4: lastWeekData.hours.toFixed(1),
            activity_change: activityChange,
            working_hours: riskData?.metrics?.working_hours || 8.0,
            attendance: attendanceMetrics || {
              days_present: 25,
              absences: 5,
              total_working_days: 30,
              attendance_rate: 83.33,
              punctuality: {
                percentage: 85,
                late_arrivals: 2,
                early_departures: 1
              }
            },
            job_search_visits: riskData?.metrics?.job_search_visits || 0
          }
        };
      }
      
      // Find matching risk assessment data
      const riskData = riskAssessmentData.employees?.find(
        emp => emp.employee_id === employee.employee_id
      );
      
      // Find matching attendance data by employee name
      const employeeAttendance = attendanceData.find(
        att => att.employee === employee.employee_name || 
               att.employee.toLowerCase().includes(employee.employee_name.toLowerCase()) ||
               employee.employee_name.toLowerCase().includes(att.employee.toLowerCase())
      );
      
      // Calculate changes
      const productivityChange = (lastWeek.productivity - firstWeek.productivity).toFixed(1);
      const activityChange = (lastWeek.hours - firstWeek.hours).toFixed(1);
      
      // Initialize risk score
      let riskScore = 0;
      
      // Enhanced attendance risk using actual API data
      let attendanceMetrics = riskData?.metrics?.attendance;
      let attendanceRate = 0;
      
      if (employeeAttendance) {
        attendanceRate = employeeAttendance.attendance_rate;
        // Convert attendance rate to days present (assuming 30 working days)
        const daysPresent = Math.round((attendanceRate / 100) * 30);
        const absences = 30 - daysPresent;
        
        attendanceMetrics = {
          days_present: daysPresent,
          absences: absences,
          total_working_days: 30,
          attendance_rate: attendanceRate,
          punctuality: {
            percentage: attendanceRate > 0 ? Math.min(100, attendanceRate + 10) : 0,
            late_arrivals: attendanceRate === 0 ? 30 : Math.max(0, Math.floor((100 - attendanceRate) / 10)),
            early_departures: attendanceRate === 0 ? 15 : Math.floor((100 - attendanceRate) / 20)
          }
        };
      } else {
        // Fallback to existing logic if no attendance data found
        if (attendanceMetrics) {
          attendanceRate = (attendanceMetrics.days_present / attendanceMetrics.total_working_days) * 100;
        }
      }
      
      // CRITICAL: If attendance is zero, set risk score to 10
      if (attendanceRate === 0) {
        riskScore = 10;
      } else {
        // Calculate risk score based on changes and metrics for non-zero attendance
        
        // Productivity decline risk
        if (parseFloat(productivityChange) <= -10) riskScore += 3;
        else if (parseFloat(productivityChange) <= -5) riskScore += 2;
        else if (parseFloat(productivityChange) < 0) riskScore += 1;
        
        // Activity decline risk
        if (parseFloat(activityChange) <= -2) riskScore += 2;
        else if (parseFloat(activityChange) < 0) riskScore += 1;
        
        // Job search activity risk
        const jobSearchVisits = riskData?.metrics?.job_search_visits || 0;
        if (jobSearchVisits >= 5) riskScore += 3;
        else if (jobSearchVisits >= 2) riskScore += 2;
        else if (jobSearchVisits > 0) riskScore += 1;
        
        // Attendance risk scoring for non-zero attendance
        if (attendanceRate < 35) riskScore += 3; // Very high risk
        else if (attendanceRate < 70) riskScore += 2; // High risk
        else if (attendanceRate < 85) riskScore += 1; // Medium risk
        
        // Ensure risk score doesn't exceed 10
        riskScore = Math.min(10, riskScore);
      }
      
      return {
        employee_id: employee.employee_id || `TEMP_${employee.employee_name}`,
        employee_name: employee.employee_name,
        team: employee.team || 'Unknown',
        risk_score: riskScore.toFixed(1),
        metrics: {
          productivity_week1: firstWeek.productivity.toFixed(1),
          productivity_week4: lastWeek.productivity.toFixed(1),
          productivity_change: productivityChange,
          activity_week1: firstWeek.hours.toFixed(1),
          activity_week4: lastWeek.hours.toFixed(1),
          activity_change: activityChange,
          working_hours: riskData?.metrics?.working_hours || 8.0,
          attendance: attendanceMetrics || {
            days_present: 25,
            absences: 5,
            total_working_days: 30,
            attendance_rate: 83.33,
            punctuality: {
              percentage: 85,
              late_arrivals: 2,
              early_departures: 1
            }
          },
          job_search_visits: riskData?.metrics?.job_search_visits || 0
        }
      };
    }).filter(Boolean);
    
    console.log('After processing:', processedEmployees.length);
    
    // Remove any remaining duplicates based on employee_name
    const uniqueEmployees = processedEmployees.reduce((acc, employee) => {
      const existing = acc.find(emp => emp.employee_name === employee.employee_name);
      if (!existing) {
        acc.push(employee);
      } else {
        console.log('Duplicate found in final step:', employee.employee_name);
      }
      return acc;
    }, []);
    
    console.log('Final unique employees:', uniqueEmployees.length);
    
    // Group by teams
    const teams = {};
    uniqueEmployees?.forEach(emp => {
      if (!teams[emp.team]) {
        teams[emp.team] = {
          employees: [],
          productivity_week1_sum: 0,
          productivity_week4_sum: 0,
          activity_week1_sum: 0,
          activity_week4_sum: 0,
          count: 0
        };
      }
      
      teams[emp.team].employees.push(emp);
      teams[emp.team].productivity_week1_sum += parseFloat(emp.metrics.productivity_week1);
      teams[emp.team].productivity_week4_sum += parseFloat(emp.metrics.productivity_week4);
      teams[emp.team].activity_week1_sum += parseFloat(emp.metrics.activity_week1);
      teams[emp.team].activity_week4_sum += parseFloat(emp.metrics.activity_week4);
      teams[emp.team].count++;
    });
    
    // Calculate team averages and risks
    const teamRisks = Object.keys(teams).map(teamName => {
      const team = teams[teamName];
      const avgProdWeek1 = (team.productivity_week1_sum / team.count).toFixed(1);
      const avgProdWeek4 = (team.productivity_week4_sum / team.count).toFixed(1);
      const avgActWeek1 = (team.activity_week1_sum / team.count).toFixed(1);
      const avgActWeek4 = (team.activity_week4_sum / team.count).toFixed(1);
      
      const prodChange = (avgProdWeek4 - avgProdWeek1).toFixed(1);
      const actChange = (avgActWeek4 - avgActWeek1).toFixed(1);
      
      // Calculate team risk score
      const avgRiskScore = team.employees.reduce((sum, emp) => sum + parseFloat(emp.risk_score), 0) / team.count;
      
      return {
        team_name: teamName,
        risk_score: avgRiskScore.toFixed(1),
        team_size: team.count,
        productivity_week1: avgProdWeek1,
        productivity_week4: avgProdWeek4,
        productivity_change: prodChange,
        activity_week1: avgActWeek1,
        activity_week4: avgActWeek4,
        activity_change: actChange,
        is_high_risk: avgRiskScore >= 6.5
      };
    });
    
    // Calculate company-wide metrics
    const totalEmployees = uniqueEmployees?.length || 0;
    const highRiskEmployees = uniqueEmployees?.filter(emp => parseFloat(emp.risk_score) >= 6.5).length || 0;
    const highRiskTeams = teamRisks.filter(team => team.is_high_risk).length;
    
    const companyProdChange = teamRisks.length > 0 ? 
      teamRisks.reduce((sum, team) => sum + parseFloat(team.productivity_change), 0) / teamRisks.length : 0;
    const companyActChange = teamRisks.length > 0 ? 
      teamRisks.reduce((sum, team) => sum + parseFloat(team.activity_change), 0) / teamRisks.length : 0;
    
    const companyRiskScore = teamRisks.length > 0 ? 
      teamRisks.reduce((sum, team) => sum + parseFloat(team.risk_score), 0) / teamRisks.length : 0;
    
    setRiskData({
      company_risk: {
        risk_score: companyRiskScore.toFixed(1),
        total_teams: teamRisks.length,
        total_employees: totalEmployees, // This should be 274
        risk_breakdown: {
          retention_risk: highRiskTeams,
          retention_risk_teams: teamRisks.filter(team => team.is_high_risk).map(team => team.team_name)
        },
        weekly_comparison: {
          productivity_change: companyProdChange.toFixed(1),
          activity_change: companyActChange.toFixed(1),
          high_risk_employees_change: highRiskEmployees // This should be 75
        }
      },
      team_risks: teamRisks,
      individual_risks: uniqueEmployees || []
    });
  };
  
  const generateSampleData = () => {
    // Sample data simulating first week to last week changes
    const sampleData = {
      company_risk: {
        risk_score: 6.2,
        total_teams: 8,
        total_employees: 274,
        risk_breakdown: {
          retention_risk: 3,
          retention_risk_teams: ["Engineering", "Sales", "Customer Support"]
        },
        weekly_comparison: {
          productivity_change: -7.3,
          activity_change: -8.5,
          high_risk_employees_change: 12
        }
      },
      team_risks: Array(8).fill().map((_, i) => {
        const teamNames = ["Engineering", "Marketing", "Sales", "Finance", "HR", "Customer Support", "Product", "Operations"];
        const weeklyChange = Math.random() < 0.6 ? -(Math.random() * 12 + 3).toFixed(1) : (Math.random() * 5).toFixed(1);
        return {
          team_name: teamNames[i],
          risk_score: (Math.random() * 5 + 3).toFixed(1),
          team_size: Math.floor(Math.random() * 30) + 10,
          productivity_week1: (Math.random() * 30 + 50).toFixed(1),
          productivity_week4: (Math.random() * 30 + 40).toFixed(1),
          productivity_change: weeklyChange,
          activity_week1: (Math.random() * 20 + 60).toFixed(1),
          activity_week4: (Math.random() * 20 + 50).toFixed(1),
          activity_change: (weeklyChange - Math.random() * 2).toFixed(1),
          is_high_risk: Math.random() > 0.6
        };
      }),
      individual_risks: Array(30).fill().map((_, i) => {
        const teams = ["Engineering", "Marketing", "Sales", "Finance", "HR", "Customer Support", "Product", "Operations"];
        const firstWeekProd = (Math.random() * 30 + 50).toFixed(1);
        const lastWeekProd = (firstWeekProd - (Math.random() * 15)).toFixed(1);
        const firstWeekActivity = (Math.random() * 30 + 50).toFixed(1);
        const lastWeekActivity = (firstWeekActivity - (Math.random() * 18)).toFixed(1);
        
        // Generate some employees with zero attendance for testing
        const attendanceRate = i < 5 ? 0 : Math.random() * 100;
        // If attendance is zero, set risk score to 10
        const riskScore = attendanceRate === 0 ? 10 : (Math.random() * 5 + 3);
        
        return {
          employee_id: `EMP${1000 + i}`,
          employee_name: `Employee ${i + 1}`,
          team: teams[Math.floor(Math.random() * teams.length)],
          risk_score: riskScore.toFixed(1),
          metrics: {
            productivity_week1: firstWeekProd,
            productivity_week4: lastWeekProd,
            productivity_change: (lastWeekProd - firstWeekProd).toFixed(1),
            activity_week1: firstWeekActivity,
            activity_week4: lastWeekActivity,
            activity_change: (lastWeekActivity - firstWeekActivity).toFixed(1),
            working_hours: (Math.random() * 3 + 5).toFixed(1),
            attendance: {
              days_present: Math.round((attendanceRate / 100) * 30),
              absences: Math.round((1 - attendanceRate / 100) * 30),
              total_working_days: 30,
              attendance_rate: attendanceRate.toFixed(1),
              punctuality: {
                percentage: Math.min(100, attendanceRate + 10).toFixed(1),
                late_arrivals: Math.floor((100 - attendanceRate) / 10),
                early_departures: Math.floor((100 - attendanceRate) / 20)
              }
            },
            job_search_visits: Math.random() > 0.7 ? Math.floor(Math.random() * 8) : 0
          }
        };
      })
    };
    
    setRiskData(sampleData);
  };

  const teamColumns = [
    {
      title: 'Team',
      dataIndex: 'team_name',
      key: 'team_name',
    },
    {
      title: 'Risk Score',
      dataIndex: 'risk_score',
      key: 'risk_score',
      sorter: (a, b) => parseFloat(b.risk_score) - parseFloat(a.risk_score),
      defaultSortOrder: 'ascend',
      render: (score) => (
        <Progress 
          percent={score * 10} 
          size="small" 
          format={() => `${score}/10`}
          status={score >= 6.5 ? 'exception' : 'active'}
          strokeColor={score >= 6.5 ? '#f5222d' : score >= 4 ? '#faad14' : '#52c41a'}
        />
      ),
    },
    {
      title: 'Team Size',
      dataIndex: 'team_size',
      key: 'team_size',
    },
    {
      title: 'Productivity (First Week)',
      dataIndex: 'productivity_week1',
      key: 'productivity_week1',
      render: (value) => `${value}%`
    },
    {
      title: 'Productivity (Last Week)',
      dataIndex: 'productivity_week4',
      key: 'productivity_week4',
      render: (value) => `${value}%`
    },
    {
      title: 'Productivity Change',
      dataIndex: 'productivity_change',
      key: 'productivity_change',
      render: (value) => (
        <span style={{ color: value <= 0 ? '#f5222d' : '#52c41a' }}>
          {value > 0 ? '+' : ''}{value}%
          {value <= 0 ? <ArrowDownOutlined /> : <ArrowUpOutlined />}
        </span>
      )
    },
    {
      title: 'Activity (First Week)',
      dataIndex: 'activity_week1',
      key: 'activity_week1',
      render: (value) => `${value}h`
    },
    {
      title: 'Activity (Last Week)',
      dataIndex: 'activity_week4',
      key: 'activity_week4',
      render: (value) => `${value}h`
    },
    {
      title: 'Activity Change',
      dataIndex: 'activity_change',
      key: 'activity_change',
      render: (value) => (
        <span style={{ color: value <= 0 ? '#f5222d' : '#52c41a' }}>
          {value > 0 ? '+' : ''}{value}h
          {value <= 0 ? <ArrowDownOutlined /> : <ArrowUpOutlined />}
        </span>
      )
    }
  ];
  
  const DepartmentWeeklyAnalysis = ({ riskData }) => {
    const [selectedDept, setSelectedDept] = useState('all');
    const [currentPage, setCurrentPage] = useState(1);
    const pageSize = 10;

    // Enhanced Gemini AI Recommendation Component with highly specific recommendations
    const RecommendationCell = ({ metrics, riskScore, employeeName }) => {
      const [recs, setRecs] = useState([]);
      const [loading, setLoading] = useState(true);

      useEffect(() => {
        const fetchRecommendations = async () => {
          try {
            // Use our new backend API instead of direct Gemini calls
            const response = await fetch('http://127.0.0.1:3000/api/employee/insight-recommendations', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                employee_data: metrics,
                employee_name: employeeName,
                risk_score: riskScore
              })
            });
            
            const data = await response.json();
            if (data.recommendations && Array.isArray(data.recommendations)) {
              setRecs(data.recommendations);
            } else {
              // Fallback if API returns unexpected data
              setRecs([
                `Schedule immediate performance review for ${employeeName} based on current metrics.`,
                `Create personalized improvement plan for ${employeeName} addressing key risk factors.`
              ]);
            }
          } catch (error) {
            console.error('Error fetching recommendations:', error);
            // Fallback recommendations if API call fails
            setRecs([
              `Schedule performance review for ${employeeName} to address identified issues.`,
              `Develop action plan with ${employeeName} to improve metrics and reduce risk score.`
            ]);
          } finally {
            setLoading(false);
          }
        };

        fetchRecommendations();
      }, [metrics, riskScore, employeeName]);

      if (loading) {
        return <Spin size="small" />;
      }

      return (
        <ul style={{ 
          margin: 0, 
          paddingLeft: 20,
          listStyleType: 'none'
        }}>
          {recs.map((rec, idx) => (
            <li 
              key={idx} 
              style={{ 
                color: riskScore >= 8 ? '#ff4d4f' : 
                      riskScore >= 6.5 ? '#ff7a45' : 
                      riskScore >= 4 ? '#1890ff' : '#52c41a',
                marginBottom: '8px',
                fontSize: '13px',
                position: 'relative',
                paddingLeft: '20px',
                fontWeight: riskScore >= 7 ? 'bold' : 'normal',
                lineHeight: '1.4'
              }}
            >
              <span style={{
                position: 'absolute',
                left: 0,
                content: '"•"',
                color: 'inherit'
              }}>•</span>
              {rec}
            </li>
          ))}
        </ul>
      );
    };

    const departments = ['all', ...new Set(
      riskData.individual_risks
        ?.filter(emp => emp.team)
        .map(emp => emp.team)
        || []
    )].filter(Boolean);

    const filteredEmployees = riskData.individual_risks
      ?.filter(emp => {
        if (!emp.team) return false;
        return selectedDept === 'all' || emp.team === selectedDept;
      })
      .sort((a, b) => b.risk_score - a.risk_score) || [];

    const paginatedEmployees = filteredEmployees.slice(
      (currentPage - 1) * pageSize,
      currentPage * pageSize
    );

    const getRiskReasons = (metrics) => {
      if (!metrics) return [];

      const riskFactors = [];

      // Attendance Risk (Top Priority with actual data)
      const attendance = metrics.attendance || {};
      const attendanceRate = parseFloat(attendance.attendance_rate || 0);
      const daysPresent = parseInt(attendance.days_present || 0);
      const totalDays = parseInt(attendance.total_working_days || 30);
      
      // CRITICAL: If attendance is 0, return ONLY attendance metric
      if (attendanceRate === 0 || daysPresent === 0) {
        return [{
          text: `CRITICAL: Zero Attendance (0/${totalDays} days) - Employee not coming to work`,
          color: '#ff4d4f',
          severity: 'CRITICAL - Zero Attendance'
        }];
      }

      // For non-zero attendance, show all metrics
      riskFactors.push({
        text: `Attendance: ${attendanceRate.toFixed(1)}% (${daysPresent}/${totalDays} days)`,
        color: attendanceRate < 35 ? '#ff4d4f' :
               attendanceRate < 70 ? '#ff7a45' :
               attendanceRate < 85 ? '#faad14' : '#52c41a',
        severity: attendanceRate < 35 ? 'Critical' :
                 attendanceRate < 70 ? 'High' :
                 attendanceRate < 85 ? 'Medium' : 'Good'
      });

      // Productivity Change
      const prodChange = parseFloat(metrics.productivity_change || 0);
      riskFactors.push({
        text: `Productivity Change: ${metrics.productivity_week1}% → ${metrics.productivity_week4}% (${prodChange > 0 ? '+' : ''}${prodChange}%)`,
        color: prodChange <= -10 ? '#ff4d4f' : 
               prodChange <= -5 ? '#ff7a45' : 
               prodChange <= -2 ? '#faad14' : '#52c41a',
        severity: prodChange <= -10 ? 'Critical' : 
                 prodChange <= -5 ? 'High' : 
                 prodChange <= -2 ? 'Medium' : 'Good'
      });

      // Activity Change
      const actChange = parseFloat(metrics.activity_change || 0);
      riskFactors.push({
        text: `Activity Change: ${metrics.activity_week1}h → ${metrics.activity_week4}h (${actChange > 0 ? '+' : ''}${actChange}h)`,
        color: actChange <= -2 ? '#ff4d4f' : 
               actChange <= -1 ? '#ff7a45' : 
               actChange < 0 ? '#faad14' : '#52c41a',
        severity: actChange <= -2 ? 'Critical' : 
                 actChange <= -1 ? 'High' : 
                 actChange < 0 ? 'Medium' : 'Good'
      });
      
      // Job Search Activity
      const jobSearchVisits = parseInt(metrics.job_search_visits || 0);
      if (jobSearchVisits > 0) {
        riskFactors.push({
          text: `Job Search Activity: ${jobSearchVisits} visits detected`,
          color: jobSearchVisits >= 5 ? '#ff4d4f' : 
                jobSearchVisits >= 3 ? '#ff7a45' : '#faad14',
          severity: jobSearchVisits >= 5 ? 'Critical' : 
                  jobSearchVisits >= 3 ? 'High' : 'Medium'
        });
      }

      // Working Hours
      const workingHours = parseFloat(metrics.working_hours || 0).toFixed(1);
      riskFactors.push({
          text: `Working Hours: ${workingHours}hrs/day`,
          color: workingHours < 4 ? '#ff4d4f' :
                 workingHours < 5 ? '#ff7a45' :
                 workingHours < 6 ? '#faad14' : '#52c41a',
          severity: workingHours < 4 ? 'Critical' :
                   workingHours < 5 ? 'High' :
                   workingHours < 6 ? 'Medium' : 'Good'
      });

      return riskFactors.map(factor => ({
          ...factor,
          text: `${factor.text} (${factor.severity})`
      }));
    };

    const columns = [
      {
        title: 'Employee Name',
        dataIndex: 'employee_name',
        key: 'name',
        render: (text) => text || 'N/A'
      },
      {
        title: 'Department',
        dataIndex: 'team',
        key: 'department',
        render: (text) => text || 'N/A'
      },
      {
        title: 'Risk Score',
        key: 'risk',
        render: (record) => (
          <Progress
            percent={record.risk_score * 10}
            size="small"
            format={() => `${record.risk_score}/10`}
            status={record.risk_score >= 6.5 ? 'exception' : 'normal'}
            strokeColor={
              record.risk_score >= 6.5 ? '#f5222d' : 
              record.risk_score >= 4 ? '#faad14' : '#52c41a'
            }
          />
        )
      },
      {
        title: 'Weekly Changes',
        key: 'weekly_changes',
        render: (record) => {
          const attendanceRate = record.metrics.attendance?.attendance_rate || 0;
          
          // If attendance is 0, only show attendance metric
          if (attendanceRate === 0) {
            return (
              <div className="weekly-changes">
                <div className="change-item">
                  <span className="change-label">Attendance:</span>
                  <span className="change-value critical">
                    0%
                  </span>
                </div>
              </div>
            );
          }

          // Otherwise show all metrics
          return (
            <div className="weekly-changes">
              <div className="change-item">
                <span className="change-label">Productivity:</span>
                <span className={`change-value ${parseFloat(record.metrics.productivity_change) < 0 ? 'negative' : 'positive'}`}>
                  {record.metrics.productivity_change > 0 ? '+' : ''}
                  {record.metrics.productivity_change}%
                </span>
              </div>
              <div className="change-item">
                <span className="change-label">Activity:</span>
                <span className={`change-value ${parseFloat(record.metrics.activity_change) < 0 ? 'negative' : 'positive'}`}>
                  {record.metrics.activity_change > 0 ? '+' : ''}
                  {record.metrics.activity_change}h
                </span>
              </div>
              <div className="change-item">
                <span className="change-label">Attendance:</span>
                <span className={`change-value ${
                  attendanceRate < 35 ? 'critical' :
                  attendanceRate < 70 ? 'negative' : 'normal'
                }`}>
                  {attendanceRate.toFixed(1)}%
                </span>
              </div>
            </div>
          );
        }
      },
      {
        title: 'Risk Factors',
        key: 'factors',
        width: 400,
        render: (record) => {
          const riskFactors = getRiskReasons(record.metrics);
          return (
            <ul className="risk-factors-list">
              {riskFactors.map((factor, idx) => (
                <li 
                  key={idx} 
                  className="risk-factor-item"
                  style={{ color: factor.color }}
                >
                  {factor.text}
                </li>
              ))}
            </ul>
          );
        }
      },
      {
        title: 'AI Recommendations',
        key: 'recommendations',
        width: 300,
        render: (record) => (
          <RecommendationCell 
            metrics={record.metrics} 
            riskScore={record.risk_score} 
            employeeName={record.employee_name}
          />
        )
      }
    ];

    return (
      <Card title="Weekly Department Risk Analysis" className="weekly-department-card">
        <Select
          value={selectedDept}
          onChange={(value) => {
            setSelectedDept(value);
            setCurrentPage(1);
          }}
          className="department-select"
        >
          {departments.map(dept => (
            <Select.Option key={dept} value={dept}>
              {dept === 'all' ? 'All Departments' : dept}
            </Select.Option>
          ))}
        </Select>

        <Table
          dataSource={paginatedEmployees}
          columns={columns}
          rowKey="employee_id"
          pagination={{
            current: currentPage,
            pageSize,
            total: filteredEmployees.length,
            onChange: setCurrentPage,
            showSizeChanger: false
          }}
        />
      </Card>
    );
  };

  return (
    <div className="weekly-risk-dashboard">
      <Modal
        title={
          <span>
            <InfoCircleOutlined style={{ marginRight: 8 }} />
            Understanding Risk Score Metrics
          </span>
        }
        open={isMetricsModalVisible}
        onOk={handleMetricsModalClose}
        onCancel={handleMetricsModalClose}
        width={700}
      >
        <div style={{ fontSize: '14px' }}>
          <h3>Welcome to the Risk Dashboard!</h3>
          <p>Our risk assessment system uses several key metrics to calculate risk scores on a scale of 0-10:</p>
          
          <h4 style={{ color: '#cf1322', marginTop: 16 }}>Critical Indicators</h4>
          <ul>
            <li><strong>Attendance (Highest Priority):</strong>
              <ul>
                <li>0% attendance automatically sets risk score to 10</li>
                <li>Below 35%: High Risk (+3 points)</li>
                <li>35-70%: Medium Risk (+2 points)</li>
                <li>70-85%: Low Risk (+1 point)</li>
              </ul>
            </li>
          </ul>

          <h4 style={{ color: '#faad14', marginTop: 16 }}>Performance Metrics</h4>
          <ul>
            <li><strong>Productivity Decline:</strong>
              <ul>
                <li>≤ -10%: +3 points</li>
                <li>≤ -5%: +2 points</li>
                <li>&lt; 0%: +1 point</li>
              </ul>
            </li>
            <li><strong>Activity Hours Change:</strong>
              <ul>
                <li>≤ -2 hours: +2 points</li>
                <li>&lt; 0 hours: +1 point</li>
              </ul>
            </li>
          </ul>

          <h4 style={{ color: '#1890ff', marginTop: 16 }}>Risk Levels</h4>
          <ul>
            <li><strong>High Risk:</strong> Score ≥ 6.5</li>
            <li><strong>Medium Risk:</strong> Score 4.0 - 6.4</li>
            <li><strong>Low Risk:</strong> Score &lt; 4.0</li>
          </ul>

          <div style={{ marginTop: 16, padding: 12, backgroundColor: '#f6f6f6', borderRadius: 4 }}>
            <strong>Note:</strong> Risk scores are cumulative but capped at 10. Additional factors like job search activity can contribute to the risk score.
          </div>
        </div>
      </Modal>

      <Card title="Weekly Risk Comparison (April 2025)" className="main-card">
        <Row gutter={16} className="stats-row">
          <Col span={6}>
            <Statistic
              title="Overall Risk Score"
              value={riskData.company_risk?.risk_score || 0}
              suffix="/10"
              valueStyle={{ 
                color: (riskData.company_risk?.risk_score || 0) >= 6.5 ? '#cf1322' : 
                       (riskData.company_risk?.risk_score || 0) >= 4 ? '#faad14' : '#3f8600' 
              }}
            />
          </Col>
          <Col span={6}>
            <Statistic
              title="Productivity Decline"
              value={Math.abs(riskData.company_risk?.weekly_comparison?.productivity_change || 0)}
              suffix="%"
              valueStyle={{ color: '#cf1322' }}
              prefix={<ArrowDownOutlined />}
            />
            <div className="stat-subtitle">First to Last Week</div>
          </Col>
          <Col span={6}>
            <Statistic
              title="Activity Decline"
              value={Math.abs(riskData.company_risk?.weekly_comparison?.activity_change || 0)}
              suffix="h"
              valueStyle={{ color: '#cf1322' }}
              prefix={<ArrowDownOutlined />}
            />
            <div className="stat-subtitle">First to Last Week</div>
          </Col>
          <Col span={6}>
            <Statistic
              title="High Risk Employees"
              value={riskData.company_risk?.weekly_comparison?.high_risk_employees_change || 0}
              suffix={`/${riskData.company_risk?.total_employees || 0}`}
              valueStyle={{ color: '#cf1322' }}
              prefix={<WarningOutlined />}
            />
            <div className="stat-subtitle">Risk Score ≥ 6.5</div>
          </Col>
         <Col span={6}>            <Statistic
              title="High Risk Teams"
              value={
                (riskData.company_risk?.risk_breakdown?.retention_risk_teams?.length) || 0
              }
              suffix={`/ ${riskData.company_risk?.total_teams || 0}`}
              valueStyle={{ color: '#cf1322' }}
              prefix={<WarningOutlined />}
            />
            <div className="stat-subtitle">Risk Score ≥ 6.5</div>
</Col>

        </Row>
      </Card>

      <Card title="Team Weekly Trend Analysis" className="team-card">
        <Table 
          dataSource={riskData.team_risks} 
          columns={teamColumns} 
          rowKey="team_name"
          loading={loading}
        />
      </Card>

      <Card 
        title="Retention Risk Details" 
        className="retention-card"
        loading={loading}
      >
        <Row gutter={16}>
          <Col span={24}>
            <h4>Teams at Risk ({riskData.company_risk?.risk_breakdown?.retention_risk || 0})</h4>
            <div className="retention-risk-teams">
              {riskData.company_risk?.risk_breakdown?.retention_risk_teams?.map(team => (
                <Tag color="red" key={team}>
                  {team}
                </Tag>
              ))}
            </div>
          </Col>
        </Row>
      </Card>

      <DepartmentWeeklyAnalysis riskData={riskData} />
    </div>
  );
};

export default WeeklyRiskDashboard;
