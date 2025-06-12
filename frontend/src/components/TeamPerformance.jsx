import React, { useState, useEffect } from 'react';
import { Table, Spin, Badge, Progress, Tag, Card, Select } from 'antd';
import { 
    LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, 
    ResponsiveContainer, BarChart, Bar, Cell, ScatterChart, Scatter, ComposedChart 
} from 'recharts';
import '../styles/TeamPerformance.css';
import axios from 'axios';

const { Option } = Select;

const TeamPerformance = () => {
    const [loading, setLoading] = useState(true);
    const [teamData, setTeamData] = useState([]);
    const [trendsData, setTrendsData] = useState([]);
    const [selectedTeam, setSelectedTeam] = useState(null);
    const [employeeData, setEmployeeData] = useState([]);
    const [selectedEmployee, setSelectedEmployee] = useState(null);

    useEffect(() => {
        Promise.all([
            fetchTeamData(),
            fetchTrendsData(),
            fetchEmployeeTrendsData()
        ]).then(() => setLoading(false));
    }, []);

    const fetchTeamData = async () => {
        try {
            const response = await axios.get('http://localhost:3000/overview/teams-performance');
            setTeamData(response.data.teams || []);
        } catch (error) {
            console.error('Error fetching team data:', error);
        }
    };

    const fetchTrendsData = async () => {
        try {
            const response = await axios.get('http://localhost:3000/overview/weekly-trends');
            setTrendsData(response.data.teams || []);
            if (response.data.teams?.length > 0) {
                setSelectedTeam(response.data.teams[0].team);
            }
        } catch (error) {
            console.error('Error fetching trends:', error);
        }
    };

    const fetchEmployeeTrendsData = async () => {
        try {
            const response = await axios.get('http://localhost:3000/overview/employee-weekly-trends');
            console.log('Raw Employee Data Response:', response.data);
            
            if (response.data.employees && Array.isArray(response.data.employees)) {
                // Remove duplicates based on employee_id
                const uniqueEmployees = Array.from(new Map(
                    response.data.employees.map(item => [item.employee_id, item])
                ).values());
                
                console.log('Unique employees count:', uniqueEmployees.length);
                setEmployeeData(uniqueEmployees);
                if (uniqueEmployees.length > 0) {
                    setSelectedEmployee(uniqueEmployees[0].employee_id);
                }
            } else {
                console.warn('Invalid or empty employee data received:', response.data);
                setEmployeeData([]);
                setSelectedEmployee(null);
            }
        } catch (error) {
            console.error('Error fetching employee trends:', error);
            setEmployeeData([]);
            setSelectedEmployee(null);
        }
    };

    const getTeamTrendData = () => {
        const team = trendsData.find(t => t.team === selectedTeam);
        return team ? team.trends : [];
    };

    const getEmployeeTrendData = () => {
        if (!selectedEmployee || !employeeData) return [];
        
        const employee = employeeData.find(e => e.employee_id === selectedEmployee);
        if (!employee) {
            console.warn('Selected employee not found in data');
            return [];
        }
        
        return employee.trends || [];
    };

    const formatYAxis = (value, type) => {
        if (type === 'hours') {
            return `${value}h`;
        }
        return value;
    };

    const getProductivityComparisonData = () => {
        return trendsData.map(team => {
            const weeklyData = team.trends || [];
            let totalProductivity = 0;
            let validDaysCount = 0;

            // Calculate total productivity and count valid days
            weeklyData.forEach(week => {
                if (week.productivity !== null && !isNaN(week.productivity)) {
                    // Each week represents 7 days except last week (2 days)
                    const daysInWeek = week.week === 'Week 5' ? 2 : 7;
                    totalProductivity += (parseFloat(week.productivity) * daysInWeek);
                    validDaysCount += daysInWeek;
                }
            });

            // Calculate monthly average (over 30 days)
            const monthlyAvg = validDaysCount > 0 ? (totalProductivity / 30) : 0;

            return {
                name: team.team,
                productivity: parseFloat(monthlyAvg).toFixed(2),
                productiveHours: parseFloat(weeklyData[0]?.hours || 0).toFixed(1),
                teamSize: weeklyData[0]?.employees || 0,
                onlineHours: parseFloat(weeklyData[0]?.hours || 0).toFixed(1)
            };
        }).sort((a, b) => {
            const prodA = parseFloat(a.productivity);
            const prodB = parseFloat(b.productivity);
            // Handle zero values
            if (prodA === 0 && prodB === 0) return 0;
            if (prodA === 0) return 1;
            if (prodB === 0) return -1;
            // Sort by productivity
            return prodB - prodA;
        });
    };

    const getOnlineVsBreakData = () => {
        return teamData.map(team => ({
            name: team.team_name,
            onlineHours: team.performance.online,
            breakHours: team.break_time / 60,
        }));
    };

    const getWeeklyProductivityData = () => {
        const team = trendsData.find(t => t.team === selectedTeam);
        if (!team) return [];
        
        return team.trends.map(trend => ({
            week: trend.week,
            productivity: trend.productivity,
            productiveHours: trend.hours,
            teamSize: trend.employees,
            level: trend.productivity >= 75 ? 'High' :
                   trend.productivity >= 50 ? 'Medium' :
                   trend.productivity >= 25 ? 'Low' : 'Poor'
        }));
    };

    const getTopAndBottomTeams = () => {
        if (!trendsData.length) return { topTeams: [], bottomTeams: [] };

        const teamsWithAvgProductivity = trendsData.map(team => {
            const weeklyData = team.trends || [];
            let totalProductivity = 0;
            let validDaysCount = 0;

            // Calculate total productivity with proper weighting
            weeklyData.forEach(week => {
                if (week.productivity !== null && !isNaN(week.productivity)) {
                    // Each week represents 7 days except last week (2 days)
                    const daysInWeek = week.week === 'Week 5' ? 2 : 7;
                    totalProductivity += (parseFloat(week.productivity) * daysInWeek);
                    validDaysCount += daysInWeek;
                }
            });

            // Calculate monthly average (over 30 days)
            const monthlyAvg = validDaysCount > 0 ? (totalProductivity / 30) : 0;

            return {
                name: team.team,
                productivity: monthlyAvg,
                productiveHours: parseFloat(weeklyData[0]?.hours || 0).toFixed(1),
                employeeCount: weeklyData[0]?.employees || 0
            };
        });

        // Sort teams by productivity
        const sortedTeams = [...teamsWithAvgProductivity]
            .sort((a, b) => parseFloat(b.productivity) - parseFloat(a.productivity));

        // Get top 10 teams
        const topTeams = sortedTeams
            .filter(team => parseFloat(team.productivity) > 0)
            .slice(0, 10)
            .map((team, index) => ({
                name: team.name,
                productivity: parseFloat(team.productivity).toFixed(2),
                productiveHours: team.productiveHours,
                employeeCount: team.employeeCount,
                rank: index + 1
            }));

        // Get bottom 10 teams
        const bottomTeams = [...sortedTeams]
            .reverse()
            .slice(0, 10)
            .map((team, index) => ({
                name: team.name,
                productivity: parseFloat(team.productivity).toFixed(2),
                productiveHours: team.productiveHours,
                employeeCount: team.employeeCount,
                rank: sortedTeams.length - index
            }))
            .sort((a, b) => parseFloat(a.productivity) - parseFloat(b.productivity));

        return { topTeams, bottomTeams };
    };

    const getProductivityChanges = () => {
        return trendsData.map(team => {
            const weeklyData = team.trends || [];
            if (weeklyData.length < 2) return null;

            // Get the last week number
            const lastWeek = Math.max(...weeklyData.map(w => 
                parseInt(w.week.split(' ')[1])
            ));

            // Find last week and previous week data
            const lastWeekData = weeklyData.find(w => 
                parseInt(w.week.split(' ')[1]) === lastWeek
            );
            const previousWeekData = weeklyData.find(w => 
                parseInt(w.week.split(' ')[1]) === lastWeek - 1
            );

            // Only calculate if we have both weeks' data
            if (!lastWeekData || !previousWeekData) return null;

            // Calculate the change
            const productivityChange = 
                parseFloat(lastWeekData.productivity) - 
                parseFloat(previousWeekData.productivity);

            return {
                name: team.team,
                change: parseFloat(productivityChange.toFixed(2)),
                currentProductivity: parseFloat(lastWeekData.productivity).toFixed(2),
                previousProductivity: parseFloat(previousWeekData.productivity).toFixed(2),
                employeeCount: lastWeekData.employees,
                currentHours: lastWeekData.hours,
                previousHours: previousWeekData.hours,
                weekNumber: lastWeek
            };
        })
        .filter(item => 
            item !== null && 
            !isNaN(item.change) && 
            (parseFloat(item.currentProductivity) > 0 || 
             parseFloat(item.previousProductivity) > 0)
        )
        .sort((a, b) => Math.abs(b.change) - Math.abs(a.change));
    };

    return (
        <div className="team-performance-container">
            {loading ? (
                <Spin size="large" />
            ) : (
                <>
                    {/* <h2>Team Performance Analysis</h2>
                    <div className="team-analysis-container">
                        <Table 
                            dataSource={teamData} 
                            columns={columns} 
                            rowKey="team_name"
                            pagination={{ pageSize: 5 }}
                        />
                    </div> */}

                    <div className="team-trends-section">
                        <h3>Weekly Trends</h3>
                        <Select
                            style={{ width: 200, marginBottom: 20 }}
                            value={selectedTeam}
                            onChange={setSelectedTeam}
                        >
                            {trendsData.map(team => (
                                <Option key={team.team} value={team.team}>{team.team}</Option>
                            ))}
                        </Select>

                        <div className="trends-charts">
                            <Card title="Productivity Trend">
                                <ResponsiveContainer width="100%" height={300}>
                                    <LineChart data={getTeamTrendData()}>
                                        <CartesianGrid strokeDasharray="3 3" />
                                        <XAxis dataKey="week" />
                                        <YAxis />
                                        <Tooltip />
                                        <Legend />
                                        <Line type="monotone" dataKey="productivity" stroke="#8884d8" name="Productivity %" />
                                    </LineChart>
                                </ResponsiveContainer>
                            </Card>

                            <Card 
                                title="Working Hours & Team Size" 
                                style={{ marginTop: 20 }}
                                extra={
                                    <div style={{ color: '#666', fontSize: '12px' }}>
                                        <span style={{ marginRight: '15px' }}>
                                            <span style={{ color: '#82ca9d' }}>●</span> Hours
                                        </span>
                                        <span>
                                            <span style={{ color: '#ffc658' }}>●</span> Team Size
                                        </span>
                                    </div>
                                }
                            >
                                <ResponsiveContainer width="100%" height={300}>
                                    <LineChart 
                                        data={getTeamTrendData()}
                                        margin={{ top: 5, right: 30, left: 20, bottom: 5 }}
                                    >
                                        <CartesianGrid strokeDasharray="3 3" />
                                        <XAxis 
                                            dataKey="week"
                                            tick={{ fill: '#666' }}
                                            tickSize={8}
                                        />
                                        <YAxis 
                                            yAxisId="left"
                                            tick={{ fill: '#666' }}
                                            tickFormatter={(value) => formatYAxis(value, 'hours')}
                                            label={{ 
                                                value: 'Working Hours', 
                                                angle: -90, 
                                                position: 'insideLeft',
                                                fill: '#666',
                                                fontSize: 12 
                                            }}
                                        />
                                        <YAxis 
                                            yAxisId="right" 
                                            orientation="right"
                                            tick={{ fill: '#666' }}
                                            label={{ 
                                                value: 'Team Size', 
                                                angle: 90, 
                                                position: 'insideRight',
                                                fill: '#666',
                                                fontSize: 12 
                                            }}
                                        />
                                        <Tooltip 
                                            content={({ active, payload, label }) => {
                                                if (active && payload && payload.length) {
                                                    return (
                                                        <div className="chart-tooltip">
                                                            <div className="chart-tooltip-title">{label}</div>
                                                            <div className="chart-tooltip-value">
                                                                <span style={{ color: '#82ca9d' }}>Working Hours: </span>
                                                                <strong>{payload[0].value}h</strong>
                                                            </div>
                                                            <div className="chart-tooltip-value">
                                                                <span style={{ color: '#ffc658' }}>Team Size: </span>
                                                                <strong>{payload[1].value}</strong>
                                                            </div>
                                                        </div>
                                                    );
                                                }
                                                return null;
                                            }}
                                        />
                                        <Legend />
                                        <Line
                                            yAxisId="left"
                                            type="monotone"
                                            dataKey="hours"
                                            stroke="#82ca9d"
                                            name="Working Hours"
                                            strokeWidth={2}
                                            dot={{ r: 4 }}
                                            activeDot={{ r: 6 }}
                                        />
                                        <Line
                                            yAxisId="right"
                                            type="monotone"
                                            dataKey="employees"
                                            stroke="#ffc658"
                                            name="Team Size"
                                            strokeWidth={2}
                                            dot={{ r: 4 }}
                                            activeDot={{ r: 6 }}
                                        />
                                    </LineChart>
                                </ResponsiveContainer>
                            </Card>

                            <Card 
                                title="Monthly Team Productivity (April 2025)" 
                                style={{ marginTop: 20 }}
                                extra={
                                    <div className="productivity-indicators">
                                        <span className="productivity-indicator productivity-high">High (≥75%)</span>
                                        <span className="productivity-indicator productivity-medium">Medium (50-74%)</span>
                                        <span className="productivity-indicator productivity-low">Low (25-49%)</span>
                                        <span className="productivity-indicator productivity-poor">Poor (&lt;25%)</span>
                                    </div>
                                }
                            >
                                <ResponsiveContainer width="100%" height={300}>
                                    <BarChart 
                                        data={getProductivityComparisonData()}
                                        margin={{ top: 5, right: 30, left: 20, bottom: 70 }}
                                    >
                                        <CartesianGrid strokeDasharray="3 3" />
                                        <XAxis 
                                            dataKey="name" 
                                            angle={-45}
                                            textAnchor="end"
                                            height={60}
                                            interval={0}
                                            tick={{ fill: '#666', fontSize: 12 }}
                                        />
                                        <YAxis 
                                            label={{ 
                                                value: 'Monthly Productivity %', 
                                                angle: -90, 
                                                position: 'insideLeft',
                                                fill: '#666',
                                                fontSize: 12 
                                            }}
                                            domain={[0, 100]}
                                            tick={{ fill: '#666' }}
                                        />
                                        <Tooltip 
                                            cursor={{ fill: 'rgba(0, 0, 0, 0.04)' }}
                                            content={({ active, payload, label }) => {
                                                if (active && payload && payload.length) {
                                                    const data = payload[0].payload;
                                                    const level = data.productivity >= 75 ? 'High' :
                                                                 data.productivity >= 50 ? 'Medium' :
                                                                 data.productivity >= 25 ? 'Low' : 'Poor';
                                                    const levelClass = `level-${level.toLowerCase()}`;
                                                    
                                                    return (
                                                        <div className="chart-tooltip">
                                                            <div className="chart-tooltip-title">
                                                                {label}
                                                                <span className={`productivity-level ${levelClass}`}>{level}</span>
                                                            </div>
                                                            <div className="chart-tooltip-value">
                                                                Productivity: <strong>{data.productivity}%</strong>
                                                            </div>
                                                            <div className="chart-tooltip-value">
                                                                Productive Hours: <strong>{data.productiveHours}h</strong>
                                                            </div>
                                                            <div className="chart-tooltip-value">
                                                                Online Hours: <strong>{data.onlineHours}h</strong>
                                                            </div>
                                                            <div className="chart-tooltip-value">
                                                                Team Size: <strong>{data.teamSize}</strong>
                                                            </div>
                                                        </div>
                                                    );
                                                }
                                                return null;
                                            }}
                                        />
                                        <Bar 
                                            dataKey="productivity" 
                                            name="Monthly Productivity %"
                                        >
                                            {getProductivityComparisonData().map((entry, index) => (
                                                <Cell 
                                                    key={`cell-${index}`} 
                                                    fill={
                                                        entry.productivity >= 75 ? '#52c41a' :
                                                        entry.productivity >= 50 ? '#1890ff' :
                                                        entry.productivity >= 25 ? '#faad14' :
                                                        '#f5222d'
                                                    }
                                                />
                                            ))}
                                        </Bar>
                                    </BarChart>
                                </ResponsiveContainer>
                            </Card>

                            <Card 
                                title={
                                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                                        <span>Team Productivity Rankings</span>
                                        <div className="productivity-legend">
                                            <span><Badge color="#52c41a" /> High Productivity</span>
                                            <span><Badge color="#f5222d" /> Needs Improvement</span>
                                        </div>
                                    </div>
                                }
                                className="productivity-rankings-card"
                                style={{ marginTop: 20 }}
                            >
                                <div className="rankings-container" style={{ flexDirection: 'row', gap: '20px' }}>
                                    <div className="chart-section" style={{ flex: 1, minWidth: '45%' }}>
                                        <h4 className="chart-title">
                                            <span className="trend-icon trend-up">▲</span>
                                            Top 10 Most Productive Teams
                                        </h4>
                                        <ResponsiveContainer width="100%" height={400}>
                                            <BarChart
                                                layout="vertical"
                                                data={getTopAndBottomTeams().topTeams}
                                                margin={{ top: 5, right: 20, left: 120, bottom: 25 }}
                                            >
                                                <CartesianGrid strokeDasharray="3 3" horizontal={false} />
                                                <XAxis type="number" domain={[0, 100]} />
                                                <YAxis
                                                    type="category"
                                                    dataKey="name"
                                                    width={110}
                                                    tickFormatter={(value) => value.length > 20 ? `${value.substring(0, 20)}...` : value}
                                                />
                                                <Tooltip 
                                                    cursor={{ fill: 'rgba(82, 196, 26, 0.1)' }}
                                                    content={({ active, payload }) => {
                                                        if (active && payload?.[0]) {
                                                            const data = payload[0].payload;
                                                            return (
                                                                <div className="chart-tooltip">
                                                                    <div className="chart-tooltip-title">{data.name}</div>
                                                                    <div className="chart-tooltip-value">
                                                                        Productivity: <strong>{data.productivity}%</strong>
                                                                    </div>
                                                                    <div className="chart-tooltip-value">
                                                                        Rank: <strong>#{data.rank}</strong>
                                                                    </div>
                                                                </div>
                                                            );
                                                        }
                                                        return null;
                                                    }}
                                                />
                                                <Bar dataKey="productivity" name="Productivity">
                                                    {getTopAndBottomTeams().topTeams.map((entry, index) => (
                                                        <Cell 
                                                            key={`cell-${index}`} 
                                                            fill="#52c41a"
                                                            fillOpacity={0.9 - (index * 0.06)}
                                                        />
                                                    ))}
                                                </Bar>
                                            </BarChart>
                                        </ResponsiveContainer>
                                    </div>
                                    
                                    <div className="chart-section" style={{ flex: 1, minWidth: '45%' }}>
                                        <h4 className="chart-title">
                                            <span className="trend-icon trend-down">▼</span>
                                            Bottom 10 Teams by Productivity
                                        </h4>
                                        <ResponsiveContainer width="100%" height={400}>
                                            <BarChart
                                                layout="vertical"
                                                data={getTopAndBottomTeams().bottomTeams}
                                                margin={{ top: 5, right: 20, left: 120, bottom: 25 }}
                                            >
                                                <CartesianGrid strokeDasharray="3 3" horizontal={false} />
                                                <XAxis type="number" domain={[0, 100]} />
                                                <YAxis
                                                    type="category"
                                                    dataKey="name"
                                                    width={110}
                                                    tickFormatter={(value) => value.length > 20 ? `${value.substring(0, 20)}...` : value}
                                                />
                                                <Tooltip 
                                                    cursor={{ fill: 'rgba(245, 34, 45, 0.1)' }}
                                                    content={({ active, payload }) => {
                                                        if (active && payload?.[0]) {
                                                            const data = payload[0].payload;
                                                            return (
                                                                <div className="chart-tooltip">
                                                                    <div className="chart-tooltip-title">{data.name}</div>
                                                                    <div className="chart-tooltip-value">
                                                                        Productivity: <strong>{data.productivity}%</strong>
                                                                    </div>
                                                                    <div className="chart-tooltip-value">
                                                                        Rank: <strong>#{data.rank}</strong>
                                                                    </div>
                                                                </div>
                                                            );
                                                        }
                                                        return null;
                                                    }}
                                                />
                                                <Bar dataKey="productivity" name="Productivity">
                                                    {getTopAndBottomTeams().bottomTeams.map((entry, index) => (
                                                        <Cell 
                                                            key={`cell-${index}`}
                                                            fill={parseFloat(entry.productivity) === 0 ? '#d9d9d9' : '#f5222d'}
                                                            fillOpacity={parseFloat(entry.productivity) === 0 ? 0.5 : 0.4 + (index * 0.06)}
                                                        />
                                                    ))}
                                                </Bar>
                                            </BarChart>
                                        </ResponsiveContainer>
                                    </div>
                                </div>
                            </Card>

                            <Card 
                                title="Break Time vs Online Time Analysis"
                                style={{ marginTop: 20 }}
                                extra={
                                    <div style={{ color: '#666', fontSize: '12px' }}>
                                        <span style={{ marginRight: '15px' }}>
                                            <span style={{ color: '#8884d8' }}>●</span> Team Data Points
                                        </span>
                                        <span>
                                            <span style={{ color: '#ffa07a' }}>―</span> Trend Line
                                        </span>
                                    </div>
                                }
                            >
                                <ResponsiveContainer width="100%" height={400}>
                                    <ScatterChart
                                        margin={{ top: 20, right: 30, left: 20, bottom: 60 }}
                                    >
                                        <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                                        <XAxis 
                                            type="number" 
                                            dataKey="onlineHours" 
                                            name="Online Time"
                                            label={{ 
                                                value: 'Average Online Time (Hours per Day)', 
                                                position: 'bottom',
                                                offset: 20,
                                                style: { fill: '#666', fontSize: 12 }
                                            }}
                                            domain={[0, 10]}
                                            tick={{ fill: '#666' }}
                                        />
                                        <YAxis 
                                            type="number" 
                                            dataKey="breakHours" 
                                            name="Break Time"
                                            label={{ 
                                                value: 'Average Break Time (Hours per Day)', 
                                                angle: -90, 
                                                position: 'insideLeft',
                                                offset: 10,
                                                style: { fill: '#666', fontSize: 12 }
                                            }}
                                            domain={[0, 5]}
                                            tick={{ fill: '#666' }}
                                        />
                                        <Tooltip 
                                            cursor={{ strokeDasharray: '3 3' }}
                                            content={({ active, payload }) => {
                                                if (active && payload && payload.length) {
                                                    const data = payload[0].payload;
                                                    const ratio = ((data.breakHours / data.onlineHours) * 100).toFixed(1);
                                                    
                                                    return (
                                                        <div className="chart-tooltip">
                                                            <div className="chart-tooltip-title">{data.name}</div>
                                                            <div className="chart-tooltip-value">
                                                                Online Time: <strong>{data.onlineHours.toFixed(1)}h</strong>
                                                            </div>
                                                            <div className="chart-tooltip-value">
                                                                Break Time: <strong>{data.breakHours.toFixed(1)}h</strong>
                                                            </div>
                                                            <div className="chart-tooltip-value">
                                                                Break/Online Ratio: <strong>{ratio}%</strong>
                                                            </div>
                                                        </div>
                                                    );
                                                }
                                                return null;
                                            }}
                                        />
                                        <Legend 
                                            verticalAlign="top" 
                                            height={36}
                                            iconType="circle"
                                        />
                                        <Scatter 
                                            name="Teams" 
                                            data={getOnlineVsBreakData()} 
                                            fill="#8884d8"
                                            shape="circle"
                                            legendType="circle"
                                        >
                                            {
                                                getOnlineVsBreakData().map((entry, index) => (
                                                    <Cell 
                                                        key={`cell-${index}`}
                                                        fill="#8884d8"
                                                        fillOpacity={0.8}
                                                        stroke="#8884d8"
                                                        strokeWidth={1}
                                                    />
                                                ))
                                            }
                                        </Scatter>
                                        <Line
                                            name="Trend Line"
                                            type="monotone"
                                            dataKey="breakHours"
                                            data={getOnlineVsBreakData()}
                                            stroke="#ffa07a"
                                            strokeWidth={2}
                                            dot={false}
                                            activeDot={false}
                                            legendType="line"
                                        />
                                    </ScatterChart>
                                </ResponsiveContainer>
                            </Card>

                            <Card 
                                title="Weekly Productivity Comparison"
                                style={{ marginTop: 20 }}
                                extra={
                                    <div className="productivity-indicators">
                                        <span className="productivity-indicator productivity-high">High (≥75%)</span>
                                        <span className="productivity-indicator productivity-medium">Medium (50-74%)</span>
                                        <span className="productivity-indicator productivity-low">Low (25-49%)</span>
                                        <span className="productivity-indicator productivity-poor">Poor (&lt;25%)</span>
                                    </div>
                                }
                            >
                                <ResponsiveContainer width="100%" height={300}>
                                    <BarChart
                                        data={getWeeklyProductivityData()}
                                        margin={{ top: 5, right: 30, left: 20, bottom: 5 }}
                                    >
                                        <CartesianGrid strokeDasharray="3 3" />
                                        <XAxis 
                                            dataKey="week"
                                            tick={{ fill: '#666', fontSize: 12 }}
                                        />
                                        <YAxis
                                            label={{ 
                                                value: 'Productivity %', 
                                                angle: -90, 
                                                position: 'insideLeft',
                                                fill: '#666',
                                                fontSize: 12 
                                            }}
                                            domain={[0, 100]}
                                            tick={{ fill: '#666' }}
                                        />
                                        <Tooltip
                                            cursor={{ fill: 'rgba(0, 0, 0, 0.04)' }}
                                            content={({ active, payload, label }) => {
                                                if (active && payload && payload.length) {
                                                    const data = payload[0].payload;
                                                    return (
                                                        <div className="chart-tooltip">
                                                            <div className="chart-tooltip-title">
                                                                {label}
                                                                <span className={`productivity-level level-${data.level.toLowerCase()}`}>
                                                                    {data.level}
                                                                </span>
                                                            </div>
                                                            <div className="chart-tooltip-value">
                                                                Productivity: <strong>{data.productivity}%</strong>
                                                            </div>
                                                            <div className="chart-tooltip-value">
                                                                Productive Hours: <strong>{data.productiveHours}h</strong>
                                                            </div>
                                                            <div className="chart-tooltip-value">
                                                                Team Size: <strong>{data.teamSize}</strong>
                                                            </div>
                                                        </div>
                                                    );
                                                }
                                                return null;
                                            }}
                                        />
                                        <Bar 
                                            dataKey="productivity" 
                                            name="Weekly Productivity %"
                                        >
                                            {
                                                getWeeklyProductivityData().map((entry, index) => (
                                                    <Cell 
                                                        key={`cell-${index}`}

                                                        fill={
                                                            entry.productivity >= 75 ? '#52c41a' :
                                                            entry.productivity >= 50 ? '#1890ff' :
                                                            entry.productivity >= 25 ? '#faad14' :
                                                            '#f5222d'
                                                        }
                                                    />
                                                ))
                                            }
                                        </Bar>
                                    </BarChart>
                                </ResponsiveContainer>
                            </Card>

                            <Card style={{ marginTop: 20 }}>
                                <div style={{ display: 'flex', gap: '20px' }}>
                                    {/* Rising Teams Chart */}
                                    <div style={{ flex: 1 }}>
                                        <h4>
                                            <span style={{ color: '#52c41a', marginRight: '8px' }}>▲</span>
                                            Rising Teams
                                        </h4>
                                        <ResponsiveContainer width="100%" height={400}>
                                            <BarChart
                                                layout="vertical"
                                                data={getProductivityChanges()
                                                    .filter(team => team.change > 0)
                                                    .slice(0, 5)}
                                                margin={{ top: 5, right: 20, left: 120, bottom: 25 }}
                                            >
                                                <CartesianGrid strokeDasharray="3 3" horizontal={false} />
                                                <XAxis type="number" domain={[0, 'auto']} />
                                                <YAxis type="category" dataKey="name" width={110} />
                                                <Tooltip
                                                    content={({ active, payload }) => {
                                                        if (active && payload?.[0]) {
                                                            const data = payload[0].payload;
                                                            return (
                                                                <div className="chart-tooltip">
                                                                    <div className="chart-tooltip-title">{data.name}</div>
                                                                    <div className="chart-tooltip-value">
                                                                        Week {data.weekNumber} Improvement: <strong>+{data.change}%</strong>
                                                                    </div>
                                                                    <div className="chart-tooltip-value">
                                                                        Current Week: <strong>{data.currentProductivity}%</strong>
                                                                        <span style={{ color: '#666' }}> ({data.currentHours}h)</span>
                                                                    </div>
                                                                    <div className="chart-tooltip-value">
                                                                        Previous Week: <strong>{data.previousProductivity}%</strong>
                                                                        <span style={{ color: '#666' }}> ({data.previousHours}h)</span>
                                                                    </div>
                                                                    <div className="chart-tooltip-value">
                                                                        Team Size: <strong>{data.employeeCount}</strong>
                                                                    </div>
                                                                </div>
                                                            );
                                                        }
                                                        return null;
                                                    }}
                                                />
                                                <Bar dataKey="change" fill="#52c41a">
                                                    {getProductivityChanges()
                                                        .filter(team => team.change > 0)
                                                        .slice(0, 5)
                                                        .map((entry, index) => (
                                                            <Cell 
                                                                key={`cell-${index}`} 
                                                                fillOpacity={0.9 - (index * 0.15)}
                                                            />
                                                        ))}
                                                </Bar>
                                            </BarChart>
                                        </ResponsiveContainer>
                                    </div>

                                    {/* Declining Teams Chart */}
                                    <div style={{ flex: 1 }}>
                                        <h4>
                                            <span style={{ color: '#f5222d', marginRight: '8px' }}>▼</span>
                                            Declining Teams
                                        </h4>
                                        <ResponsiveContainer width="100%" height={400}>
                                            <BarChart
                                                layout="vertical"
                                                data={getProductivityChanges()
                                                    .filter(team => team.change < 0)
                                                    .slice(0, 5)
                                                    .map(team => ({ ...team, change: Math.abs(team.change) }))}
                                                margin={{ top: 5, right: 20, left: 120, bottom: 25 }}
                                            >
                                                <CartesianGrid strokeDasharray="3 3" horizontal={false} />
                                                <XAxis type="number" domain={[0, 'auto']} />
                                                <YAxis type="category" dataKey="name" width={110} />
                                                <Tooltip
                                                    content={({ active, payload }) => {
                                                        if (active && payload?.[0]) {
                                                            const data = payload[0].payload;
                                                            return (
                                                                <div className="chart-tooltip">
                                                                    <div className="chart-tooltip-title">{data.name}</div>
                                                                    <div className="chart-tooltip-value">
                                                                        Week {data.weekNumber} Decline: <strong>-{data.change}%</strong>
                                                                    </div>
                                                                    <div className="chart-tooltip-value">
                                                                        Current Week: <strong>{data.currentProductivity}%</strong>
                                                                        <span style={{ color: '#666' }}> ({data.currentHours}h)</span>
                                                                    </div>
                                                                    <div className="chart-tooltip-value">
                                                                        Previous Week: <strong>{data.previousProductivity}%</strong>
                                                                        <span style={{ color: '#666' }}> ({data.previousHours}h)</span>
                                                                    </div>
                                                                    <div className="chart-tooltip-value">
                                                                        Team Size: <strong>{data.employeeCount}</strong>
                                                                    </div>
                                                                </div>
                                                            );
                                                        }
                                                        return null;
                                                    }}
                                                />
                                                <Bar dataKey="change" fill="#f5222d">
                                                    {getProductivityChanges()
                                                        .filter(team => team.change < 0)
                                                        .slice(0, 5)
                                                        .map((entry, index) => (
                                                            <Cell 
                                                                key={`cell-${index}`} 
                                                                fillOpacity={0.9 - (index * 0.15)}
                                                            />
                                                        ))}
                                                </Bar>
                                            </BarChart>
                                        </ResponsiveContainer>
                                    </div>
                                </div>
                            </Card>
                        </div>
                    </div>

                    <div className="employee-trends-section">
                        <h3>Employee Weekly Trends</h3>
                        <Select
                            style={{ width: 300, marginBottom: 20 }}
                            value={selectedEmployee}
                            onChange={(value) => {
                                console.log('Selected employee value:', value);
                                setSelectedEmployee(value);
                            }}
                            showSearch
                            optionFilterProp="children"
                            placeholder="Select an employee"
                            loading={loading}
                            filterOption={(input, option) =>
                                option.children.toLowerCase().includes(input.toLowerCase())
                            }
                        >
                            {employeeData && employeeData.length > 0 ? (
                                employeeData.map(employee => (
                                    <Option 
                                        key={employee.employee_id} 
                                        value={employee.employee_id}
                                    >
                                        {`${employee.employee_name} (${employee.team})`}
                                    </Option>
                                ))
                            ) : (
                                <Option disabled>No employees available</Option>
                            )}
                        </Select>

                        {/* Ensure you have proper handling for when no employee is selected */}
                        {selectedEmployee ? (
                            <div className="trends-charts">
                                <Card title="Employee Productivity Trend">
                                    <ResponsiveContainer width="100%" height={300}>
                                        <LineChart data={getEmployeeTrendData()}>
                                            <CartesianGrid strokeDasharray="3 3" />
                                            <XAxis dataKey="week" />
                                            <YAxis domain={[0, 100]} />
                                            <Tooltip 
                                                content={({ active, payload, label }) => {
                                                    if (active && payload && payload.length) {
                                                        return (
                                                            <div className="chart-tooltip">
                                                                <div className="chart-tooltip-title">{label}</div>
                                                                <div className="chart-tooltip-value">
                                                                    Productivity: <strong>{payload[0].value}%</strong>
                                                                </div>
                                                            </div>
                                                        );
                                                    }
                                                    return null;
                                                }}
                                            />
                                            <Legend />
                                            <Line 
                                                type="monotone" 
                                                dataKey="productivity" 
                                                stroke="#8884d8" 
                                                name="Productivity %" 
                                                strokeWidth={2}
                                                dot={{ r: 4 }}
                                                activeDot={{ r: 6 }}
                                            />
                                        </LineChart>
                                    </ResponsiveContainer>
                                </Card>

                                <Card title="Employee Working Hours" style={{ marginTop: 20 }}>
                                    <ResponsiveContainer width="100%" height={300}>
                                        <LineChart data={getEmployeeTrendData()}>
                                            <CartesianGrid strokeDasharray="3 3" />
                                            <XAxis 
                                                dataKey="week"
                                                tick={{ fill: '#666' }}
                                            />
                                            <YAxis 
                                                domain={[0, 12]}
                                                label={{ 
                                                    value: 'Working Hours', 
                                                    angle: -90, 
                                                    position: 'insideLeft',
                                                    fill: '#666',
                                                    fontSize: 12
                                                }}
                                                tick={{ fill: '#666' }}
                                                tickFormatter={(value) => `${value}h`}
                                            />
                                            <Tooltip 
                                                content={({ active, payload, label }) => {
                                                    if (active && payload && payload.length) {
                                                        return (
                                                            <div className="chart-tooltip">
                                                                <div className="chart-tooltip-title">{label}</div>
                                                                <div className="chart-tooltip-value">
                                                                    Working Hours: <strong>{payload[0].value}h</strong>
                                                                </div>
                                                            </div>
                                                        );
                                                    }
                                                    return null;
                                                }}
                                            />
                                            <Legend />
                                            <Line 
                                                type="monotone" 
                                                dataKey="hours" 
                                                stroke="#8884d8" 
                                                name="Working Hours"
                                                strokeWidth={2}
                                                dot={{ r: 4 }}
                                                activeDot={{ r: 6 }}
                                            />
                                        </LineChart>
                                    </ResponsiveContainer>
                                </Card>
                            </div>
                        ) : (
                            <div style={{ textAlign: 'center', padding: '20px' }}>
                                Please select an employee to view their trends
                            </div>
                        )}
                    </div>
                </>
            )}
        </div>
    );
};

export default TeamPerformance;
