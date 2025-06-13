import React, { useState, useEffect } from 'react';
import { Card, Table, Tag, Progress, Statistic, Row, Col, Select, Spin } from 'antd';
import { WarningOutlined } from '@ant-design/icons';
import axios from 'axios';
import '../styles/dashboard.css';

const RiskAssessment = () => {
  const [riskData, setRiskData] = useState({ 
    company_risk: {},
    team_risks: [], 
    individual_risks: [] 
  });
  const [loading, setLoading] = useState(true);

  const calculateRiskScore = (metrics) => {
    if (!metrics) return 10.0;

    // Critical conditions - automatic 10.0 score
    if ( 
        (metrics.activity === 0 && metrics.working_hours === 0) || 
        metrics.attendance?.days_present === 0 ||
        metrics.attendance?.absences === 30
    ) {
        return 10.0;
    }

    // Initialize weights for risk factors
    const weights = {
        job_search: 0.30,     // 30% - Critical retention risk
        attendance: 0.20,     // 20% - Physical presence
        productivity: 0.20,   // 20% - Work output
        punctuality: 0.10,    // 10% - Time management
        activity: 0.10,       // 10% - Engagement
        working_hours: 0.10   // 10% - Time commitment
    };

    // Individual risk calculations (0-10 scale)
    const attendance = metrics.attendance || {};
    const punctuality = attendance.punctuality || {};
    
    const risks = {
        // Job Search Risk (Highest priority)
        job_search: (() => {
            const visits = parseInt(metrics.job_search_visits || 0);
            return visits >= 8 ? 10 :
                   visits >= 6 ? 9 :
                   visits >= 4 ? 8 :
                   visits >= 2 ? 6 :
                   visits >= 1 ? 4 : 0;
        })(),

        // Attendance Risk
        attendance: (() => {
            const absences = parseInt(attendance.absences || 0);
            return absences >= 15 ? 10 :
                   absences >= 10 ? 8 :
                   absences >= 5 ? 6 :
                   absences >= 3 ? 4 : 2;
        })(),

        // Punctuality Risk
        punctuality: (() => {
            const percentage = parseFloat(punctuality.percentage || 0);
            return percentage <= 30 ? 10 :
                   percentage <= 50 ? 8 :
                   percentage <= 70 ? 6 :
                   percentage <= 85 ? 4 : 2;
        })(),

        // Productivity Risk
        productivity: (() => {
            const prod = parseFloat(metrics.productivity || 0);
            return prod === 0 ? 10 :
                   prod < 35 ? 9 :
                   prod < 50 ? 7 :
                   prod < 65 ? 5 :
                   prod < 80 ? 3 : 1;
        })(),

        // Activity Risk
        activity: (() => {
            const act = parseFloat(metrics.activity || 0);
            return act === 0 ? 10 :
                   act < 35 ? 9 :
                   act < 50 ? 7 :
                   act < 65 ? 5 :
                   act < 80 ? 3 : 1;
        })(),

        // Working Hours Risk
        working_hours: (() => {
            const hours = parseFloat(metrics.working_hours || 0);
            return hours === 0 ? 10 :
                   hours < 4 ? 9 :
                   hours < 5 ? 7 :
                   hours < 6 ? 5 :
                   hours < 7 ? 3 : 1;
        })()
    };

    // Calculate base risk score
    let riskScore = Object.entries(weights).reduce((score, [key, weight]) => {
        return score + (risks[key] * weight);
    }, 0);

    // Apply risk multipliers for severe conditions
    const multipliers = [
        // Job search + Low productivity/activity
        {
            condition: risks.job_search >= 8 && (risks.productivity >= 8 || risks.activity >= 8),
            factor: 1.3  // 30% increase
        },
        // Zero productivity + Low activity
        {
            condition: risks.productivity === 10 && risks.activity >= 8,
            factor: 1.25 // 25% increase
        },
        // Poor attendance + Poor punctuality
        {
            condition: risks.attendance >= 8 && risks.punctuality >= 8,
            factor: 1.2  // 20% increase
        },
        // Multiple high risks (3 or more metrics at critical level)
        {
            condition: Object.values(risks).filter(r => r >= 8).length >= 3,
            factor: 1.15 // 15% increase
        }
    ];

    // Apply multipliers
    multipliers.forEach(({ condition, factor }) => {
        if (condition) {
            riskScore *= factor;
        }
    });

    // Ensure final score is between 0 and 10
    return Math.round(Math.min(10.0, Math.max(0.0, riskScore)) * 10) / 10;
};

  useEffect(() => {
    const fetchData = async () => {
        try {
            const response = await axios.get('http://127.0.0.1:3000/overview/risk-assessment');
            const employees = response.data.employees;

            // Calculate individual risks
            const individualRisks = employees.map(emp => ({
                ...emp,
                risk_score: calculateRiskScore(emp.metrics)
            }));

            // Calculate team risks with productivity consideration
            const teamRisks = Object.values(
                individualRisks.reduce((acc, emp) => {
                    if (!acc[emp.team]) {
                        acc[emp.team] = {
                            team_name: emp.team,
                            team_size: 0,
                            total_risk: 0,
                            high_risk_count: 0,
                            zero_productivity_count: 0,
                            total_productivity: 0
                        };
                    }

                    acc[emp.team].team_size++;
                    acc[emp.team].total_risk += emp.risk_score;

                    // Track productivity metrics
                    const productivity = parseFloat(emp.metrics?.productivity || 0);
                    if (productivity === 0) {
                        acc[emp.team].zero_productivity_count++;
                    }
                    acc[emp.team].total_productivity += productivity;

                    // Count high risk members
                    if (
                        emp.risk_score >= 6.5 || 
                        productivity === 0 || 
                        emp.metrics?.activity === 0 || 
                        emp.metrics?.attendance?.days_present === 0
                    ) {
                        acc[emp.team].high_risk_count++;
                    }

                    return acc;
                }, {})
            ).map(team => {
                // Team is high risk if:
                // 1. Has any members with zero productivity
                // 2. More than 40% members are high risk
                // 3. Average team productivity is critical
                const hasZeroProductivity = team.zero_productivity_count > 0;
                const avgProductivity = team.total_productivity / team.team_size;
                const highRiskPercentage = (team.high_risk_count / team.team_size) >= 0.4;
                
                const isHighRisk = hasZeroProductivity || 
                                  highRiskPercentage || 
                                  avgProductivity < 35;

                return {
                    ...team,
                    risk_score: hasZeroProductivity ? 10.0 : 
                               Math.round((team.total_risk / team.team_size) * 10) / 10,
                    is_high_risk: isHighRisk,
                    avg_productivity: avgProductivity
                };
            });

            // Update company risk calculation
            const totalEmployees = individualRisks.length;
            const highRiskEmployees = individualRisks.filter(emp => emp.risk_score >= 6.5).length;
            const retentionRiskTeams = teamRisks
                .filter(team => team.risk_score >= 7.5)
                .map(team => team.team_name);

            setRiskData({
                company_risk: {
                    total_employees: totalEmployees,
                    total_teams: teamRisks.length,
                    high_risk_teams: teamRisks.filter(team => 
                        team.is_high_risk || 
                        team.zero_productivity_count > 0
                    ).length,
                    high_risk_employees: highRiskEmployees,
                    risk_score: Math.round(
                        individualRisks.reduce((sum, emp) => sum + emp.risk_score, 0) / 
                        totalEmployees * 10
                    ) / 10,
                    risk_breakdown: {
                        productivity_risk: teamRisks.filter(t => 
                            t.zero_productivity_count > 0 || 
                            t.avg_productivity < 35
                        ).length,
                        attendance_risk: teamRisks.filter(t => {
                            const teamMembers = individualRisks.filter(e => e.team === t.team_name);
                            return teamMembers.filter(m => m.metrics?.attendance?.days_present < 15).length / 
                                   teamMembers.length >= 0.4;
                        }).length,
                        retention_risk: retentionRiskTeams.length,
                        retention_risk_teams: retentionRiskTeams
                    }
                },
                team_risks: teamRisks,
                individual_risks: individualRisks
            });

        } catch (error) {
            console.error('Error fetching data:', error);
        } finally {
            setLoading(false);
        }
    };

    fetchData();
  }, []);

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
      title: 'High Risk Members',
      key: 'risk_members',
      render: (text, record) => {
        const highRiskCount = riskData.individual_risks.filter(
          member => member.team === record.team_name && member.risk_score >= 6.5
        ).length;
        return `${highRiskCount}/${record.team_size}`;
      },
    }
  ];

  const DepartmentRiskAnalysis = ({ riskData }) => {
    const [selectedDept, setSelectedDept] = useState('all');
    const [currentPage, setCurrentPage] = useState(1);
    const pageSize = 10;

    const RecommendationCell = ({ metrics, riskScore }) => {
      const [recs, setRecs] = useState([]);
      const [loading, setLoading] = useState(true);

      useEffect(() => {
        const API_KEY = "AIzaSyAcbqan-PqKDHO0WNlrcfa2O3JN8lbEqlk";
        const endpoint = "https://generativelanguage.googleapis.com/v1/models/gemini-2.0-flash:generateContent";
        
        const fetchRecommendations = async () => {
          try {
            // If risk score is low, return single positive feedback
            if (riskScore < 4) {
              setRecs(['Continue current performance with periodic check-ins and recognition.']);
              setLoading(false);
              return;
            }

            // If risk score is 10, return single critical alert
            if (riskScore === 10) {
              setRecs(['Schedule urgent HR meeting to address zero activity and engagement.']);
              setLoading(false);
              return;
            }

            const promptText = `As an HR Analytics expert, provide targeted intervention recommendations based on this risk assessment:

Risk Profile:
- Risk Score: ${riskScore}/10 ${riskScore >= 7 ? '(CRITICAL)' : riskScore >= 5 ? '(HIGH RISK)' : riskScore >= 4 ? '(MEDIUM RISK)' : '(LOW RISK)'}
- Productivity: ${metrics.productivity}% ${metrics.productivity < 35 ? '(CRITICAL)' : metrics.productivity < 50 ? '(CONCERNING)' : ''}
- Activity: ${metrics.activity}% ${metrics.activity < 35 ? '(CRITICAL)' : metrics.activity < 50 ? '(CONCERNING)' : ''}
- Working Hours: ${metrics.working_hours}hrs/day ${metrics.working_hours < 6 ? '(CRITICAL)' : metrics.working_hours < 7 ? '(CONCERNING)' : ''}
- Job Site Activity: ${metrics.job_hunting_activity?.site_visits || 0} visits
${metrics.attendance?.punctuality ? `- Punctuality: ${metrics.attendance.punctuality}% ${metrics.attendance.punctuality < 70 ? '(CONCERNING)' : ''}` : ''}

Instructions:
1. Return EXACTLY 2 recommendations 
2. Each recommendation must:
   - Start with a specific action verb
   - Be immediately actionable by HR/manager
   - Address the most critical metrics first
   - Be 10-15 words maximum
   - Include timeframe or frequency where relevant
3. For HIGH/CRITICAL risks (>5):
   - First recommendation must be urgent (within 24-48 hours)
   - Include specific intervention steps
4. For medium risks (4-5):
   - Focus on preventive measures
   - Include regular check-in schedule
5. Avoid generic phrases like:
   - "improve performance"
   - "monitor progress"
   - "consider discussing"
6. Use clear, direct language in short 
Return format:
[actionable recommendation 1]
[actionable recommendation 2]
`;

            const response = await fetch(`${endpoint}?key=${API_KEY}`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                contents: [{
                  parts: [{
                    text: promptText
                  }]
                }]
              })
            });
            const data = await response.json();
            if (data.candidates && data.candidates[0]?.content?.parts[0]?.text) {
              const recommendations = data.candidates[0].content.parts[0].text
                .split('\n')
                .filter(line => line.trim())
                .map(line => line.replace(/^[•-]\s*/, ''))
                .map(line => line.charAt(0).toUpperCase() + line.slice(1));
              setRecs(recommendations);
            } else {
              setRecs(['Schedule a performance review to discuss improvement areas.']);
            }
          } catch (error) {
            setRecs(['Schedule a performance review to discuss improvement areas.']);
          }
          setLoading(false);
        };

        fetchRecommendations();
      }, [metrics, riskScore]);

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
                color: riskScore === 10 ? '#ff4d4f' : 
                       riskScore >= 6.5 ? '#ff7a45' : 
                       riskScore >= 4 ? '#1890ff' : '#52c41a',
                marginBottom: '8px',
                fontSize: '13px',
                position: 'relative',
                paddingLeft: '20px'
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
            format={() => `${record.risk_score.toFixed(1)}/10`}
            status={record.risk_score >= 6.5 ? 'exception' : 'normal'}
            strokeColor={
              record.risk_score >= 6.5 ? '#f5222d' : 
              record.risk_score >= 4 ? '#faad14' : '#52c41a'
            }
          />
        )
      },
      {
        title: 'Risk Factors',
        key: 'factors',
        width: 300,
        render: (record) => {
          const riskFactors = getRiskReasons(record.metrics, record.employee_id);
          return (
            <ul style={{ 
                margin: 0, 
                paddingLeft: 20,
                listStyle: 'none',
                maxHeight: '200px',
                overflowY: 'auto'
            }}>
                {riskFactors.map((factor, idx) => (
                    <li key={idx} 
                        style={{ 
                            color: factor.color,
                            marginBottom: '4px',
                            fontSize: '12px',
                            display: 'flex',
                            alignItems: 'center',
                            gap: '8px'
                        }}
                    >
                        <div style={{
                            width: '8px',
                            height: '8px',
                            borderRadius: '50%',
                            backgroundColor: factor.color
                        }} />
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
        render: (record) => <RecommendationCell metrics={record.metrics} riskScore={record.risk_score} />
      }
    ];

    return (
      <Card title="Department Risk Analysis" style={{ marginTop: 16 }}>
        <Select
          value={selectedDept}
          onChange={(value) => {
            setSelectedDept(value);
            setCurrentPage(1);
          }}
          style={{ width: 200, marginBottom: 16 }}
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

  const getRiskReasons = (metrics) => {
    if (!metrics) return [{
        text: "No metrics data available",
        color: "#ff4d4f"
    }];

    const riskFactors = [];

    // Job Search Activity (First Priority)
    const jobSearchVisits = parseInt(metrics.job_search_visits || 0);
    if (jobSearchVisits > 0) {
        riskFactors.push({
            text: `⚠️ Job Search Activity: ${jobSearchVisits} visits`,
            color: '#ff4d4f',
            severity: jobSearchVisits >= 8 ? 'Critical' :
                     jobSearchVisits >= 6 ? 'Very High' :
                     jobSearchVisits >= 4 ? 'High' :
                     jobSearchVisits >= 2 ? 'Medium' : 'Low'
        });
    }

    // Attendance & Punctuality
    const attendance = metrics.attendance || {};
    const daysPresent = parseInt(attendance.days_present || 0);
    const totalDays = parseInt(attendance.total_working_days || 30);
    const absences = parseInt(attendance.absences || 0);
    const punctuality = attendance.punctuality || {};
    const lateArrivals = parseInt(punctuality.late_arrivals || 0);
    const earlyDepartures = parseInt(punctuality.early_departures || 0);
    
    // Calculate actual attendance based on punch-in data
    const actualDaysPresent = daysPresent;
    const actualAbsences = absences;
    
    riskFactors.push({
        text: `Attendance: ${actualDaysPresent}/${totalDays} days (${actualAbsences} absences)`,
        color: actualDaysPresent <= 15 ? '#ff4d4f' : 
               actualDaysPresent <= 20 ? '#ff7a45' : 
               actualDaysPresent <= 25 ? '#faad14' : '#52c41a',
        severity: actualDaysPresent <= 15 ? 'Critical' : 
                 actualDaysPresent <= 20 ? 'High' : 
                 actualDaysPresent <= 25 ? 'Medium' : 'Good'
    });

    // Calculate punctuality only for days actually present
    if (actualDaysPresent > 0) {
        const punctualityPercentage = Math.max(0, Math.min(100, 
            100 - ((lateArrivals / actualDaysPresent) * 100)
        )).toFixed(1);

        riskFactors.push({
            text: `Punctuality: ${punctualityPercentage}% (${lateArrivals} late, ${earlyDepartures} early)`,
            color: punctualityPercentage <= 50 ? '#ff4d4f' :
                   punctualityPercentage <= 65 ? '#ff7a45' :
                   punctualityPercentage <= 80 ? '#faad14' : '#52c41a',
            severity: punctualityPercentage <= 50 ? 'Critical' :
                     punctualityPercentage <= 65 ? 'High' :
                     punctualityPercentage <= 80 ? 'Medium' : 'Good'
        });
    } else {
        riskFactors.push({
            text: 'Punctuality: N/A (No attendance)',
            color: '#ff4d4f',
            severity: 'Critical'
        });
    }

    // Rest of the metrics with Critical status for zero values
    const productivity = parseFloat(metrics.productivity || 0).toFixed(1);
    riskFactors.push({
        text: `Productivity: ${productivity}%`,
        color: productivity == 0 ? '#ff4d4f' :
               productivity < 35 ? '#ff4d4f' :
               productivity < 50 ? '#ff7a45' :
               productivity < 65 ? '#faad14' : '#52c41a',
        severity: productivity == 0 ? 'Critical' :
                 productivity < 35 ? 'Critical' :
                 productivity < 50 ? 'High' :
                 productivity < 65 ? 'Medium' : 'Good'
    });

    // Calculate activity risk (0-10)
    const activity = parseFloat(metrics.activity || 0).toFixed(1);
    riskFactors.push({
        text: `Activity: ${activity}%`,
        color: activity < 35 ? '#ff4d4f' :
               activity < 50 ? '#ff7a45' :
               activity < 65 ? '#faad14' : '#52c41a',
        severity: activity < 35 ? 'Critical' :
                 activity < 50 ? 'High' :
                 activity < 65 ? 'Medium' : 'Good'
    });

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

  return (
    <div className="risk-assessment-container">
      <Card title="Company Risk Overview" loading={loading}>
        <Row gutter={16}>
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
              title="High Risk Teams"
              value={riskData.team_risks?.filter(team => team.is_high_risk).length || 0}
              suffix={`/${riskData.team_risks?.length || 0}`}
              valueStyle={{ color: '#cf1322' }}
              prefix={<WarningOutlined />}
            />
          </Col>
          <Col span={6}>
            <Statistic
              title="High Risk Employees"
              value={riskData.individual_risks?.filter(emp => emp.risk_score >= 6.5).length || 0}
              suffix={`/${riskData.company_risk?.total_employees || 0}`}
              valueStyle={{ color: '#cf1322' }}
            />
          </Col>
          <Col span={6}>
            <Statistic
              title="Retention Risk Teams"
              value={riskData.company_risk?.risk_breakdown?.retention_risk || 0}
              suffix={`/${riskData.company_risk?.total_teams || 0}`}
              valueStyle={{ color: '#cf1322' }}
              prefix={<WarningOutlined />}
            />
          </Col>
        </Row>
      </Card>

      <Card title="Team Risk Assessment" style={{ marginTop: 16 }} loading={loading}>
        <Table 
          dataSource={riskData.team_risks} 
          columns={teamColumns} 
          rowKey="team_name"
        />
      </Card>

      <Card 
        title="Retention Risk Details" 
        style={{ marginTop: 16 }} 
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

      <DepartmentRiskAnalysis riskData={riskData} />
    </div>
  );
};

export default RiskAssessment;
