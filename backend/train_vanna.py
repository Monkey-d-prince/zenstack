import vanna
import clickhouse_connect
import pandas as pd
from vanna.chromadb import ChromaDB_VectorStore
from vanna.google import GoogleGeminiChat
import chromadb

class MyVanna(ChromaDB_VectorStore, GoogleGeminiChat):
    def __init__(self, config=None):
        # Default config with persistent ChromaDB client
        config = config or {}
        client = chromadb.PersistentClient(path="./chroma_data")
        config.setdefault('client', client)
        config.setdefault('collection_name', 'vanna')

        # Store both collection name and client for debugging
        self.collection_name = config['collection_name']
        self.chroma_client = client  # Store client reference

        # Initialize ChromaDB_VectorStore
        try:
            ChromaDB_VectorStore.__init__(self, config=config)
        except Exception as e:
            print(f"Error initializing ChromaDB_VectorStore: {e}")
            raise
        gemini_config = {
            'api_key': 'AIzaSyAcbqan-PqKDHO0WNlrcfa2O3JN8lbEqlk',
            'model': 'gemini-2.0-flash'  # Updated to a likely valid model
        }
        try:
            GoogleGeminiChat.__init__(self, config=gemini_config)
        except Exception as e:
            print(f"Error initializing GoogleGeminiChat: {e}")
            raise

        # Debug: Print collection name
        print(f"Using ChromaDB collection: {self.collection_name}")
        
    def reset(self):
        """Reset the ChromaDB collection, clearing all training history"""
        try:
            # Use the stored client reference
            client = self.chroma_client
            
            # Delete the existing collection if it exists
            try:
                client.delete_collection(self.collection_name)
                print(f"Deleted existing collection: {self.collection_name}")
            except Exception as e:
                print(f"No existing collection to delete or error: {e}")
                
            # Create a new collection
            self.collection = client.create_collection(name=self.collection_name)
            print(f"Created fresh collection: {self.collection_name}")
            return True
        except Exception as e:
            print(f"Error resetting collection: {e}")
            return False



# Connect to ClickHouse
def connect_to_clickhouse():
    """Establish connection to ClickHouse database"""
    client = clickhouse_connect.get_client(
        host='20.244.1.191',
        port=8123,
    )
    return client

# Create SQL execution function for Vanna
def run_sql(sql: str) -> pd.DataFrame:
    client = connect_to_clickhouse()
    result = client.query(sql)
    df = pd.DataFrame(result.result_rows, columns=result.column_names)
    return df

# Initialize Vanna
vn = MyVanna()
vn.run_sql = run_sql
vn.run_sql_is_set = True

# Reset Vanna to clear all training history
print("Resetting Vanna to clear all training history...")
vn.reset()
print("Vanna reset complete. Ready for fresh training.")

vn.train(documentation="""
Employee Productivity Analytics - Clickhouse SQL Guide
Data Context
Our employee productivity tracking system contains metrics about employee application usage and activity. Key metrics include:

productive_percent: Percentage of time spent on productive applications
active_percent: Percentage of time with keyboard/mouse activity
department: Employee's department
employee_id: Unique identifier for each employee
date: Date of the activity records

Business Rules

Employees with productivity below 50% need attention
Department average productivity should ideally be above 75%
Weekly and monthly trends are important for performance reviews

Technical Requirements
ALWAYS USE CLICKHOUSE-SPECIFIC FUNCTIONS AND SYNTAX
All generated queries MUST exclusively use Clickhouse-compatible functions and syntax. Do NOT use standard SQL functions that aren't supported in Clickhouse.
Clickhouse-Compatible Functions to Use:

Date functions: toDate(), toDateTime(), toYYYYMM(), toMonday(), toStartOfMonth(), dateDiff('unit', date1, date2)
Aggregation: avg(), sum(), count(), min(), max(), groupArray(), arrayJoin()
String functions: concat(), substringUTF8(), position(), lowerUTF8(), upperUTF8()
Conditional logic: if(), multiIf(), case when ... then ... end
Window functions: rowNumberInAllBlocks(), rank(), dense_rank()

Clickhouse-Specific Syntax to Remember:
dont use today's date using date between 01-04-2025 and 30-04-2025 also use this for week wise
date range of data - 01-04-2025 to 30-04-2025 in attendance_date table
Use toDate() instead of DATE()
Use substringUTF8() instead of SUBSTRING()
Use formatDateTime() instead of DATE_FORMAT()
Use now() for current timestamp
For date intervals use date + INTERVAL X day/week/month syntax

Tables schemas are as follows:

CREATE TABLE default.app_category_mapping
(
    `type` String,
    `app_url_name` String,
    `category` String
)
ENGINE = SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY (type, app_url_name)
SETTINGS index_granularity = 8192


CREATE TABLE default.category_mapping
(
    `Category` String,
    `Mapping` String
)
ENGINE = SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY Category
SETTINGS index_granularity = 8192

CREATE TABLE default.daily_attendance
(
    `employee` String,
    `email` String,
    `employee_id` String,
    `gender` String,
    `team` String,
    `team_role` String,
    `designation` String,
    `shift` String,
    `arrival` String,
    `in_time` String,
    `in_by` String,
    `platform` String,
    `out_time` String,
    `out_by` String,
    `out_platform` String,
    `departure` String,
    `working_time` String,
    `online_time` String,
    `remark` String,
    `date` Date
)
ENGINE = SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY (employee, date)
SETTINGS index_granularity = 8192

CREATE TABLE default.employee_breaks
(
    `Employee` String,
    `Email` String,
    `Employee_ID` String,
    `Break_Type` String,
    `Break_Start` DateTime,
    `Break_End` DateTime,
    `Break_Duration` String,
    `Attendance_Date` Date
)
ENGINE = SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY (Employee, Break_Start)
SETTINGS index_granularity = 8192

CREATE TABLE default.employee_metrics
(
    `Attendance_Date` Date,
    `Employee_ID` String,
    `First_Name` String,
    `Last_Name` String,
    `Email` String,
    `Punch_In` String,
    `Punch_Out` String,
    `Punch_Duration` String,
    `Shift_Name` String,
    `Shift_Start` String,
    `Shift_End` String,
    `Manager` String,
    `Group_Name` String,
    `Online_Duration` String,
    `Active_Duration` String,
    `Active_Percent` Float64,
    `Idle_Duration` String,
    `Break_Duration` String,
    `Productive_Duration` String,
    `Productive_Percent` Float64,
    `Unproductive_Duration` String,
    `Neutral_Duration` String
)
ENGINE = SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY (Attendance_Date, Employee_ID)
SETTINGS index_granularity = 8192

CREATE TABLE default.monthly_attendance
(
    `Employee` String,
    `Email` String,
    `Employee_ID` String,
    `Team` String,
    `Date` Date,
    `Working_Time` String,
    `Status` String
)
ENGINE = SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY (Employee, Date)
SETTINGS index_granularity = 8192

CREATE TABLE default.monthly_breaks
(
    `Employee` String,
    `Email` String,
    `Employee_ID` String,
    `Break_Type` String,
    `Attendance_Date` Date,
    `Break_Duration` String
)
ENGINE = SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY (Employee, Attendance_Date)
SETTINGS index_granularity = 8192

CREATE TABLE default.monthly_inout
(
    `Employee` String,
    `Email` String,
    `Employee_ID` String,
    `Team` String,
    `Date` Date,
    `In_Time` String,
    `Out_Time` String
)
ENGINE = SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY (Employee, Date)
SETTINGS index_granularity = 8192

CREATE TABLE default.productivity_mapping
(
    `category` String,
    `mapping` String
)
ENGINE = SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY category
SETTINGS index_granularity = 8192

CREATE TABLE default.team_hierarchy
(
    `team` String,
    `team_hierarchy` String
)
ENGINE = SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY team
SETTINGS index_granularity = 8192


CREATE TABLE default.employee_activity
(
    `Employee` String,
    `Email` String,
    `Secondary_Email` String,
    `Employee_ID` String,
    `Gender` String,
    `Team` String,
    `Team_Role` String,
    `Designation` String,
    `Shift` String,
    `Application` String,
    `URL` String,
    `Title` String,
    `Date` Date,
    `Duration` String,
    `Mapping_Status` String,
    `Active_Time` String,
    `Idle_Time` String,
    `Key_Presses` Int32,
    `Mouse_Clicks` Int32,
    `System_Status` String
)
ENGINE = SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY (Employee, Date)
SETTINGS index_granularity = 8192


Example Queries:
Low Productivity Employees
sqlSELECT 
    employee_id,
    avg(productive_percent) AS avg_productivity,
    avg(active_percent) AS avg_activity
FROM employee_productivity
WHERE toDate(date) BETWEEN (today() - INTERVAL 30 DAY) AND today()
GROUP BY employee_id
HAVING avg_productivity < 50
ORDER BY avg_productivity ASC
Department Productivity Assessment
sqlSELECT 
    department,
    avg(productive_percent) AS dept_avg_productivity,
    if(avg(productive_percent) < 75, 'Needs Improvement', 'Good') AS status
FROM employee_productivity
WHERE toDate(date) BETWEEN toStartOfMonth(today() - INTERVAL 1 MONTH) AND toEndOfMonth(today() - INTERVAL 1 MONTH)
GROUP BY department
ORDER BY dept_avg_productivity DESC
Weekly Productivity Trend
sqlSELECT 
    toMonday(date) AS week_start,
    department,
    avg(productive_percent) AS avg_productivity,
    avg(active_percent) AS avg_activity
FROM employee_productivity
WHERE toDate(date) >= (today() - INTERVAL 3 MONTH)
GROUP BY week_start, department
ORDER BY department, week_start
Daily Productivity with Moving Average
sqlSELECT 
    date,
    employee_id,
    productive_percent,
    avgMoving(productive_percent, 7) OVER (PARTITION BY employee_id ORDER BY date) AS moving_avg_7day
FROM employee_productivity
WHERE toDate(date) >= (today() - INTERVAL 30 DAY)
ORDER BY employee_id, date
""")

vn.train(documentation="""
Null Value Handling in ClickHouse SQL:

1. Always use NULLIF() when division is involved to prevent division by zero:
   - Example: AVG(productive_duration) / NULLIF(AVG(unproductive_duration), 0)

2. Use COALESCE() or IFNULL() to provide default values for NULL fields:
   - Example: AVG(COALESCE(productive_percent, 0)) AS avg_productivity

3. Explicitly check for NULL values in WHERE conditions:
   - Use "IS NULL" or "IS NOT NULL" rather than = NULL or != NULL
   - Example: WHERE punch_in IS NOT NULL AND shift_name IS NOT NULL

4. Use COUNTIf() to count records where a condition involving NULL is true:
   - Example: COUNTIf(productive_percent IS NOT NULL) AS records_with_data

5. For time-series or trending data, handle NULL values consistently:
   - Either filter them out: WHERE productive_percent IS NOT NULL
   - Or replace with defaults: COALESCE(productive_percent, 0)
   
6. Always Use if(isFinite(<col_name>)),<col_name>, 0) while running avg or sum function in query
    - Example: AVG(if(isFinite(Productive_Percent), Productive_Percent, 0)) AS avg_productivity-
    - This ensures that any non-finite values (like NaN or Inf) are treated as 0 in the calculation.
""")

vn.train(documentation="""
IMPORTANT: ClickHouse is CASE-SENSITIVE with column names!
Use the exact case from the schema:
- Use Date between 2025-04-01 to 2025-04-30
- Attendance_Date (not attendance_date)
- Group_Name (not group_name)  
- Productive_Percent (not productive_percent)
- Employee_ID (not employee_id)
- First_Name, Last_Name (not first_name, last_name)
- All other column names must match case exactly as shown in the schema
""")

# Add training examples based on your existing data
print("Training on example queries...")

training_examples = [
    {
      "question": "Show me employees with productivity below 50%",
      "clickhouse_sql": "SELECT employee_id, first_name, last_name, department, avg_productive_percent FROM employee_duration_summary WHERE avg_productive_percent < 50 ORDER BY avg_productive_percent ASC LIMIT 10"
    },
    {
      "question": "What is the average productivity of each department?",
      "clickhouse_sql": "SELECT department, AVG(avg_productive_percent) AS avg_productivity FROM employee_duration_summary GROUP BY department ORDER BY avg_productivity DESC"
    },
    {
      "question": "List employees with high active time but low productivity",
      "clickhouse_sql": "SELECT employee_id, first_name, last_name, department, avg_active_percent FROM employee_duration_summary WHERE avg_active_percent > 75 AND avg_productive_percent < 50 ORDER BY avg_active_percent DESC"
    },
    {
      "question": "all data tables?",
      "clickhouse_sql": "SHOW TABLES"
    },
    {
      "question": "What is the average productivity of each department?",
      "clickhouse_sql": "SELECT department, AVG(avg_productive_percent) AS avg_productivity FROM employee_duration_summary GROUP BY department ORDER BY avg_productivity DESC"
    },
    {
      "question": "List employees with high active time but low productivity",
      "clickhouse_sql": "SELECT employee_id, first_name, last_name, department, avg_active_percent FROM employee_duration_summary WHERE avg_active_percent > 75 AND avg_productive_percent < 50 ORDER BY avg_active_percent DESC"
    },
    {
      "question": "Which team has the highest average productive hours?",
      "clickhouse_sql": "SELECT Group_Name, AVG(if(isFinite(Productive_Percent), Productive_Percent, 0)) AS avg_productivity FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY Group_Name ORDER BY avg_productivity DESC LIMIT 1;"
    },
    {
      "question": "give names of top 10 most productive teams",
      "clickhouse_sql": "SELECT Group_Name, AVG(if(isFinite(Productive_Percent), Productive_Percent, 0)) AS avg_productivity FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY Group_Name ORDER BY avg_productivity DESC LIMIT 10;"
    },
    {
      "question": "Show me employees with low productivity",
      "clickhouse_sql": "SELECT employee_id, first_name, last_name, department, avg_productive_percent FROM employee_duration_summary WHERE avg_productive_percent < 50 ORDER BY avg_productive_percent ASC LIMIT 10"
    },
    {
      "question": "get me all teams with productivity less than 29",
        "clickhouse_sql": "SELECT Group_Name FROM employee_metrics GROUP BY Group_Name HAVING AVG(Productive_Percent) < 29"
    },
    {
    "question": "Find employees with high active time but low productivity",
    "clickhouse_sql": "SELECT employee_id, first_name, last_name, department, avg_active_percent FROM employee_duration_summary WHERE avg_active_percent > 75 AND avg_productive_percent < 50 ORDER BY avg_active_percent DESC"
    },
    {
    "question": "Which day had the highest attendance?",
    "clickhouse_sql": "SELECT attendance_date, COUNT(*) AS total_attendance FROM attendance_data GROUP BY attendance_date ORDER BY total_attendance DESC LIMIT 1"
    },
    {
      "question": "Which department has the highest average productive hours?",
      "clickhouse_sql": "SELECT department, avg_productive_duration FROM department_duration_summary ORDER BY avg_productive_duration DESC LIMIT 1"
    },
    {
      "question": "Show attendance data for IT department",
      "clickhouse_sql": "SELECT * FROM attendance_data WHERE shift_name = 'IT' ORDER BY attendance_date DESC LIMIT 20"
    },
    {
      "question": "List employees with over 80% productivity",
      "clickhouse_sql": "SELECT employee_id, first_name, last_name, department, avg_productive_percent FROM employee_duration_summary WHERE avg_productive_percent > 80 ORDER BY avg_productive_percent DESC"
    },
    {
      "question": "Show me departments with highest attendance rates",
      "clickhouse_sql": "SELECT department, AVG(attendance_rate) AS avg_attendance FROM employee_attendance_summary GROUP BY department ORDER BY avg_attendance DESC LIMIT 5"
    },
    {
      "question": "Find employees with high productivity but low active time",
      "clickhouse_sql": "SELECT employee_id, first_name, last_name, department, avg_productive_percent, avg_active_percent FROM employee_duration_summary WHERE avg_productive_percent > 75 AND avg_active_percent < 50 ORDER BY avg_productive_percent DESC, avg_active_percent ASC"
    },
    {
      "question": "Which employees show decreasing productivity over the last month?",
      "clickhouse_sql": "WITH daily_productivity AS (SELECT employee_id, attendance_date, productive_percent FROM attendance_data WHERE attendance_date >= date_sub(DAY, 30, today())) SELECT a.employee_id, a.first_name, a.last_name, a.department, corr(toDayOfMonth(d.attendance_date), d.productive_percent) AS productivity_trend FROM employee_duration_summary a JOIN daily_productivity d ON a.employee_id = d.employee_id GROUP BY a.employee_id, a.first_name, a.last_name, a.department HAVING productivity_trend < -0.5 ORDER BY productivity_trend ASC"
    },
    {"question": "Compare productivity between specified departments",
    "clickhouse_sql": "  SELECT Group_Name, AVG(if(isFinite(Productive_Percent), Productive_Percent, 0)) AS avg_productivity, AVG(if(isFinite(Active_Percent), Active_Percent, 0)) AS avg_activity FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND Group_Name IN ({departments:String})  GROUP BY Group_Name ORDER BY avg_productivity DESC"
    },
    {
      "question": "Which employees have the highest keyboard activity?",
      "clickhouse_sql": "SELECT Employee, Employee_ID, Team, SUM(Key_Presses) AS total_key_presses, SUM(Mouse_Clicks) AS total_mouse_clicks FROM employee_usage_data GROUP BY Employee, Employee_ID, Team ORDER BY total_key_presses DESC LIMIT 15"
    },
    {
      "question": "Find departments with productivity below company average",
      "clickhouse_sql": "WITH company_avg AS (SELECT AVG(avg_productive_percent) AS avg_productive_percent FROM overall_summary) SELECT d.department, d.avg_productive_percent, d.avg_active_percent FROM department_duration_summary d, company_avg c WHERE d.avg_productive_percent < c.avg_productive_percent ORDER BY d.avg_productive_percent ASC"
    },
    {
      "question": "Who has the most absences in the last quarter?",
      "clickhouse_sql": "SELECT e.employee_id, e.first_name, e.last_name, e.department, e.total_days - e.days_present AS absent_days FROM employee_attendance_summary e WHERE e.total_days > 0 ORDER BY absent_days DESC LIMIT 10"
    },
    {
      "question": "Show me daily attendance trends over the past month",
      "clickhouse_sql": "SELECT attendance_date, total_employees, employees_present, (employees_present / total_employees) * 100 AS attendance_percentage FROM daily_attendance_summary WHERE attendance_date >= date_sub(DAY, 30, today()) ORDER BY attendance_date"
    },
    {
      "question": "Which day of the week has the lowest attendance rate?",
      "clickhouse_sql": "SELECT formatDateTime(attendance_date, '%A') AS day_of_week, AVG(employees_present / total_employees) * 100 AS avg_attendance_rate FROM daily_attendance_summary GROUP BY day_of_week ORDER BY avg_attendance_rate ASC LIMIT 1"
    },
    {
      "question": "Find employees who are consistently late to their shifts",
      "clickhouse_sql": "SELECT Employee_ID, First_Name, Last_Name, Group_Name, COUNT(*) AS late_day FROM default.employee_metric WHERE toDate(Attendance_Date) BETWEEN toDate('2025-04-01') A toDate('2025-04-30' AND Punch_In IS NOT NUL AND Shift_Start IS NOT NUL AND parseDateTimeBestEffort(concat(toString(Attendance_Date), ' ', Punch_In)) > parseDateTimeBestEffort(concat(toString(Attendance_Date), ' ', Shift_Start)) + INTERVAL 15 MINUTE GROUP BY Employee_ID, First_Name, Last_Name, Group_Name HAVING late_days >= 5 ORDER BY late_days DESC LIMIT 10 "

    },
    {
      "question": "Identify teams with perfect attendance last week",
      "clickhouse_sql": "SELECT shift_name, COUNT(DISTINCT employee_id) AS team_size FROM attendance_data WHERE attendance_date >= date_sub(DAY, 7, today()) GROUP BY shift_name HAVING COUNT(DISTINCT employee_id) = countIf(punch_in IS NOT NULL, employee_id)"
    },
    {
      "question": "Which applications do employees spend most time on?",
      "clickhouse_sql": "SELECT Application, SUM(toUInt32(splitByChar(':', Duration)[1]) * 3600 + toUInt32(splitByChar(':', Duration)[2]) * 60 + toUInt32(splitByChar(':', Duration)[3])) AS total_seconds FROM employee_usage_data GROUP BY Application ORDER BY total_seconds DESC LIMIT 10"
    },
    {
      "question": "Show me time spent on unproductive websites by department",
      "clickhouse_sql": "SELECT e.department, SUM(toUInt32(splitByChar(':', u.Duration)[1]) * 3600 + toUInt32(splitByChar(':', u.Duration)[2]) * 60 + toUInt32(splitByChar(':', u.Duration)[3])) / 3600 AS hours_spent FROM employee_usage_data u JOIN employee_duration_summary e ON u.Employee_ID = toString(e.employee_id) WHERE u.Mapping_Status = 'Unproductive' AND u.URL IS NOT NULL GROUP BY e.department ORDER BY hours_spent DESC"
    },
    {
      "question": "Compare idle time between departments",
      "clickhouse_sql": "SELECT department, avg_idle_duration, ROUND((avg_idle_duration / (avg_online_duration + 0.001)) * 100, 2) AS idle_percentage FROM department_duration_summary ORDER BY idle_percentage DESC"
    },
    {
      "question": "Find employees who spend more than 2 hours daily on social media",
      "clickhouse_sql": "SELECT u.Employee_ID, e.first_name, e.last_name, e.department, AVG(toUInt32(splitByChar(':', u.Duration)[1]) * 3600 + toUInt32(splitByChar(':', u.Duration)[2]) * 60 + toUInt32(splitByChar(':', u.Duration)[3])) / 3600 AS avg_daily_hours FROM employee_usage_data u JOIN employee_duration_summary e ON u.Employee_ID = toString(e.employee_id) WHERE u.URL LIKE '%facebook%' OR u.URL LIKE '%instagram%' OR u.URL LIKE '%twitter%' OR u.URL LIKE '%linkedin%' GROUP BY u.Employee_ID, e.first_name, e.last_name, e.department HAVING avg_daily_hours > 2 ORDER BY avg_daily_hours DESC"
    },
    {
      "question": "Which team has the highest break duration?",
      "clickhouse_sql": "SELECT shift_name AS team, AVG(break_duration) / 3600 AS avg_break_hours FROM attendance_data WHERE shift_name IS NOT NULL GROUP BY shift_name ORDER BY avg_break_hours DESC LIMIT 1"
    },
    {
      "question": "List employees who consistently miss their goal hours",
      "clickhouse_sql": "SELECT employee_id, first_name, last_name, department, Deficit FROM employee_duration_summary WHERE Deficit < 0 ORDER BY Deficit ASC LIMIT 20"
    },
    {
      "question": "Show departments exceeding their goal hours on average",
      "clickhouse_sql": "SELECT department, avg_online_goal_hours, avg_online_duration, (avg_online_duration - avg_online_goal_hours) AS avg_surplus FROM department_duration_summary WHERE avg_online_duration > avg_online_goal_hours ORDER BY avg_surplus DESC"
    },
    {
      "question": "Find employees who meet goals with minimal activity",
      "clickhouse_sql": "SELECT employee_id, first_name, last_name, department, avg_online_duration, avg_online_goal_hours, avg_active_percent FROM employee_duration_summary WHERE avg_online_duration >= avg_online_goal_hours AND avg_active_percent < 60 ORDER BY avg_active_percent ASC"
    },
    {
      "question": "Calculate what percentage of employees are meeting their goals",
      "clickhouse_sql": "SELECT (sum(Deficit >= 0) * 100.0 / count(*)) AS percentage_meeting_goals FROM employee_duration_summary"
    },
    {
      "question": "Which department has the largest deficit in goal hours?",
      "clickhouse_sql": "SELECT department, Deficit FROM department_duration_summary ORDER BY Deficit ASC LIMIT 1"
    },
    {
      "question": "Compare top and bottom 10% of employees by productivity",
      "clickhouse_sql": "WITH ranked_employees AS (SELECT employee_id, first_name, last_name, department, avg_productive_percent, NTILE(10) OVER (ORDER BY avg_productive_percent) AS productivity_decile FROM employee_duration_summary) SELECT productivity_decile, AVG(avg_productive_percent) AS avg_productivity FROM ranked_employees WHERE productivity_decile IN (1, 10) GROUP BY productivity_decile ORDER BY productivity_decile"
    },
    {
      "question": "Show employees with productivity significantly above their department average",
      "clickhouse_sql": "WITH dept_avg AS (SELECT department, AVG(avg_productive_percent) AS dept_avg_productivity FROM employee_duration_summary GROUP BY department) SELECT e.employee_id, e.first_name, e.last_name, e.department, e.avg_productive_percent, d.dept_avg_productivity, (e.avg_productive_percent - d.dept_avg_productivity) AS productivity_difference FROM employee_duration_summary e JOIN dept_avg d ON e.department = d.department WHERE (e.avg_productive_percent - d.dept_avg_productivity) > 20 ORDER BY productivity_difference DESC"
    },
    {
      "question": "Identify employees with consistently high keyboard activity but low productivity",
      "clickhouse_sql": "WITH employee_keyboard AS (SELECT Employee_ID, AVG(Key_Presses) AS avg_keys FROM employee_usage_data GROUP BY Employee_ID) SELECT e.employee_id, e.first_name, e.last_name, e.department, e.avg_productive_percent, k.avg_keys FROM employee_duration_summary e JOIN employee_keyboard k ON toString(e.employee_id) = k.Employee_ID WHERE k.avg_keys > (SELECT AVG(Key_Presses) * 1.5 FROM employee_usage_data) AND e.avg_productive_percent < 50 ORDER BY k.avg_keys DESC, e.avg_productive_percent ASC"
    },
    {
      "question": "Compare performance of new vs veteran employees",
      "clickhouse_sql": "WITH employee_tenure AS (SELECT employee_id, IF(MIN(attendance_date) > date_sub(DAY, 90, today()), 'New', 'Veteran') AS tenure FROM attendance_data GROUP BY employee_id) SELECT t.tenure, COUNT(*) AS employee_count, AVG(e.avg_productive_percent) AS avg_productivity, AVG(e.avg_active_percent) AS avg_activity FROM employee_duration_summary e JOIN employee_tenure t ON e.employee_id = t.employee_id GROUP BY t.tenure ORDER BY t.tenure"
    },
    {
      "question": "Find most improved employees in the last month",
      "clickhouse_sql": "WITH monthly_productivity AS (SELECT employee_id, toYYYYMM(attendance_date) AS month, AVG(productive_percent) AS monthly_productivity FROM attendance_data WHERE attendance_date >= date_sub(DAY, 60, today()) GROUP BY employee_id, month), improvement AS (SELECT m1.employee_id, m1.monthly_productivity - m2.monthly_productivity AS productivity_increase FROM monthly_productivity m1 JOIN monthly_productivity m2 ON m1.employee_id = m2.employee_id AND m1.month > m2.month) SELECT e.employee_id, e.first_name, e.last_name, e.department, i.productivity_increase FROM employee_duration_summary e JOIN improvement i ON e.employee_id = i.employee_id ORDER BY i.productivity_increase DESC LIMIT 10"
    },
    {
      "question": "Which websites are most visited during working hours?",
      "clickhouse_sql": "SELECT URL, COUNT(*) AS visit_count, SUM(toUInt32(splitByChar(':', Duration)[1]) * 3600 + toUInt32(splitByChar(':', Duration)[2]) * 60 + toUInt32(splitByChar(':', Duration)[3])) / 3600 AS total_hours FROM employee_usage_data WHERE URL IS NOT NULL AND URL != '' GROUP BY URL ORDER BY visit_count DESC LIMIT 20"
    },
    {
      "question": "Show me which departments use Microsoft Excel the most",
      "clickhouse_sql": "SELECT e.department, COUNT(*) AS usage_count, SUM(toUInt32(splitByChar(':', COALESCE(u.Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(u.Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(u.Duration, '0:0:0'))[3])) / 3600 AS total_hours FROM employee_usage_data u JOIN employee_duration_summary e ON u.Employee_ID = toString(e.employee_id) WHERE u.Application LIKE '%Excel%' GROUP BY e.department ORDER BY total_hours DESC"
    },
    {
      "question": "Find employees who spend excessive time on email",
      "clickhouse_sql": "SELECT u.Employee_ID, e.first_name, e.last_name, e.department, SUM(toUInt32(splitByChar(':', u.Duration)[1]) * 3600 + toUInt32(splitByChar(':', u.Duration)[2]) * 60 + toUInt32(splitByChar(':', u.Duration)[3])) / 3600 AS email_hours FROM employee_usage_data u JOIN employee_duration_summary e ON u.Employee_ID = toString(e.employee_id) WHERE u.Application LIKE '%Outlook%' OR u.Application LIKE '%Gmail%' OR u.URL LIKE '%mail.%' GROUP BY u.Employee_ID, e.first_name, e.last_name, e.department HAVING email_hours > 4 ORDER BY email_hours DESC"
    },
    {
      "question": "Compare coding tool usage across development teams",
      "clickhouse_sql": "SELECT e.department, u.Application, COUNT(*) AS usage_count FROM employee_usage_data u JOIN employee_duration_summary e ON u.Employee_ID = toString(e.employee_id) WHERE u.Application IN ('Visual Studio Code', 'IntelliJ IDEA', 'PyCharm', 'Eclipse', 'Visual Studio') AND e.department LIKE '%Development%' GROUP BY e.department, u.Application ORDER BY e.department, usage_count DESC"
    },
    {
      "question": "Which teams spend more time in meetings?",
      "clickhouse_sql": "SELECT e.department, SUM(toUInt32(splitByChar(':', u.Duration)[1]) * 3600 + toUInt32(splitByChar(':', u.Duration)[2]) * 60 + toUInt32(splitByChar(':', u.Duration)[3])) / 3600 AS meeting_hours FROM employee_usage_data u JOIN employee_duration_summary e ON u.Employee_ID = toString(e.employee_id) WHERE u.Application LIKE '%Teams%' OR u.Application LIKE '%Zoom%' OR u.Application LIKE '%Meet%' OR u.Title LIKE '%meeting%' GROUP BY e.department ORDER BY meeting_hours DESC"
    },
    {
      "question": "Show attendance trends by day of week",
      "clickhouse_sql": "SELECT formatDateTime(attendance_date, '%A') AS day_of_week, COUNT(*) AS record_count, AVG(productive_percent) AS avg_productivity FROM attendance_data WHERE attendance_date >= date_sub(DAY, 90, today()) GROUP BY day_of_week ORDER BY toDayOfWeek(MIN(attendance_date))"
    },
    {
      "question": "When do employees typically start and end their workday?",
      "clickhouse_sql": "SELECT toHour(punch_in) AS start_hour, COUNT(*) AS frequency FROM attendance_data WHERE punch_in IS NOT NULL GROUP BY start_hour ORDER BY start_hour"
    },
    {
      "question": "Find productivity patterns throughout the day",
      "clickhouse_sql": "WITH hourly_data AS (SELECT employee_id, attendance_date, toHour(punch_in) AS hour_of_day, productive_percent FROM attendance_data WHERE punch_in IS NOT NULL) SELECT hour_of_day, AVG(productive_percent) AS avg_productivity FROM hourly_data GROUP BY hour_of_day ORDER BY hour_of_day"
    },
    {
      "question": "Show me productivity trends over the past 6 months",
      "clickhouse_sql": "SELECT toYYYYMM(attendance_date) AS month, AVG(productive_percent) AS avg_productivity, AVG(active_percent) AS avg_activity FROM attendance_data WHERE attendance_date >= date_sub(DAY, 180, today()) GROUP BY month ORDER BY month"
    },
    {
      "question": "Identify days with unusual productivity patterns",
      "clickhouse_sql": "WITH daily_stats AS (SELECT attendance_date, AVG(productive_percent) AS day_productivity FROM attendance_data GROUP BY attendance_date), overall_stats AS (SELECT AVG(productive_percent) AS avg_productivity, stddevPop(productive_percent) AS std_productivity FROM attendance_data) SELECT d.attendance_date, d.day_productivity FROM daily_stats d, overall_stats o WHERE ABS(d.day_productivity - o.avg_productivity) > 2 * o.std_productivity ORDER BY ABS(d.day_productivity - o.avg_productivity) DESC"
    },
    {
      "question": "Calculate efficiency score for each department",
      "clickhouse_sql": "SELECT department, AVG(avg_productive_percent) * 0.6 + AVG(avg_active_percent) * 0.3 + (AVG(avg_online_duration) / AVG(avg_online_goal_hours)) * 0.1 AS efficiency_score FROM employee_duration_summary GROUP BY department ORDER BY efficiency_score DESC"
    },
    {
      "question": "Create a comprehensive employee performance index",
      "clickhouse_sql": "WITH attendance_metrics AS (SELECT employee_id, attendance_rate FROM employee_attendance_summary), productivity_metrics AS (SELECT employee_id, avg_productive_percent, avg_active_percent, (avg_online_duration / avg_online_goal_hours) AS goal_achievement FROM employee_duration_summary) SELECT p.employee_id, e.first_name, e.last_name, e.department, (0.4 * p.avg_productive_percent + 0.2 * p.avg_active_percent + 0.2 * a.attendance_rate * 100 + 0.2 * LEAST(p.goal_achievement * 100, 100)) AS performance_index FROM productivity_metrics p JOIN attendance_metrics a ON p.employee_id = a.employee_id JOIN employee_duration_summary e ON p.employee_id = e.employee_id ORDER BY performance_index DESC"
    },
    {
      "question": "Identify teams with balanced work distribution",
      "clickhouse_sql": "WITH team_stats AS (SELECT shift_name, employee_id, AVG(productive_duration) AS avg_productive_time FROM attendance_data WHERE shift_name IS NOT NULL GROUP BY shift_name, employee_id), team_variance AS (SELECT shift_name, stddevPop(avg_productive_time) AS time_stddev, AVG(avg_productive_time) AS time_avg FROM team_stats GROUP BY shift_name) SELECT shift_name, time_avg / 3600 AS avg_productive_hours, time_stddev / 3600 AS stddev_hours, (time_stddev / time_avg) AS coefficient_of_variation FROM team_variance ORDER BY coefficient_of_variation ASC"
    },
    {
      "question": "Find departments with high collaboration metrics",
      "clickhouse_sql": "WITH collaboration_apps AS (SELECT Employee_ID, SUM(toUInt32(splitByChar(':', Duration)[1]) * 3600 + toUInt32(splitByChar(':', Duration)[2]) * 60 + toUInt32(splitByChar(':', Duration)[3])) AS collab_seconds FROM employee_usage_data WHERE Application IN ('Microsoft Teams', 'Slack', 'Zoom', 'Google Meet', 'Discord') OR Application LIKE '%chat%' GROUP BY Employee_ID), department_stats AS (SELECT e.department, AVG(c.collab_seconds) / 3600 AS avg_collab_hours FROM collaboration_apps c JOIN employee_duration_summary e ON c.Employee_ID = toString(e.employee_id) GROUP BY e.department) SELECT department, avg_collab_hours, RANK() OVER (ORDER BY avg_collab_hours DESC) AS collaboration_rank FROM department_stats ORDER BY avg_collab_hours DESC"
    },
    {
      "question": "Compare cross-functional team productivity with specialized teams",
      "clickhouse_sql": "WITH team_members AS (SELECT shift_name, COUNT(DISTINCT employee_id) AS member_count FROM attendance_data WHERE shift_name IS NOT NULL GROUP BY shift_name), team_departments AS (SELECT shift_name, COUNT(DISTINCT e.department) AS dept_count FROM attendance_data a JOIN employee_duration_summary e ON a.employee_id = e.employee_id WHERE shift_name IS NOT NULL GROUP BY shift_name), team_productivity AS (SELECT shift_name, AVG(productive_percent) AS avg_productivity FROM attendance_data WHERE shift_name IS NOT NULL GROUP BY shift_name) SELECT t.shift_name, m.member_count, d.dept_count, p.avg_productivity, CASE WHEN d.dept_count > 1 THEN 'Cross-functional' ELSE 'Specialized' END AS team_type FROM team_members m JOIN team_departments d ON m.shift_name = d.shift_name JOIN team_productivity p ON m.shift_name = p.shift_name ORDER BY p.avg_productivity DESC"
    },
    {
      "question": "Identify employees with inconsistent work patterns",
      "clickhouse_sql": "WITH daily_patterns AS (SELECT employee_id, attendance_date, productive_percent, stddevPop(productive_percent) OVER (PARTITION BY employee_id ORDER BY attendance_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_stddev FROM attendance_data), employee_consistency AS (SELECT employee_id, AVG(rolling_stddev) AS avg_consistency FROM daily_patterns GROUP BY employee_id) SELECT e.employee_id, e.first_name, e.last_name, e.department, c.avg_consistency FROM employee_duration_summary e JOIN employee_consistency c ON e.employee_id = c.employee_id ORDER BY c.avg_consistency DESC LIMIT 20"
    },
    {
      "question": "Find correlations between application usage and productivity",
      "clickhouse_sql": "WITH app_usage AS (SELECT Employee_ID, Application, SUM(toUInt32(splitByChar(':', Duration)[1]) * 3600 + toUInt32(splitByChar(':', Duration)[2]) * 60 + toUInt32(splitByChar(':', Duration)[3])) / 3600 AS hours_used FROM employee_usage_data GROUP BY Employee_ID, Application), top_apps AS (SELECT Application FROM app_usage GROUP BY Application ORDER BY COUNT(*) DESC LIMIT 10), productivity_data AS (SELECT employee_id, avg_productive_percent FROM employee_duration_summary) SELECT a.Application, AVG(p.avg_productive_percent) AS avg_productivity, COUNT(DISTINCT a.Employee_ID) AS user_count, corr(a.hours_used, p.avg_productive_percent) AS correlation FROM app_usage a JOIN productivity_data p ON a.Employee_ID = toString(p.employee_id) WHERE a.Application IN (SELECT Application FROM top_apps) GROUP BY a.Application ORDER BY ABS(correlation) DESC"
    },
    {
      "question": "Calculate optimal team size based on productivity data",
      "clickhouse_sql": "WITH team_size_productivity AS (SELECT shift_name, COUNT(DISTINCT employee_id) AS team_size, AVG(productive_percent) AS team_productivity FROM attendance_data WHERE shift_name IS NOT NULL GROUP BY shift_name) SELECT team_size, AVG(team_productivity) AS avg_productivity, COUNT(*) AS number_of_teams FROM team_size_productivity GROUP BY team_size ORDER BY avg_productivity DESC"
    },
    {
      "question": "Analyze impact of breaks on productivity",
      "clickhouse_sql": "WITH break_analysis AS (SELECT employee_id, attendance_date, break_duration / 3600 AS break_hours, productive_percent FROM attendance_data WHERE break_duration > 0) SELECT CASE WHEN break_hours < 0.5 THEN 'Less than 30min' WHEN break_hours < 1 THEN '30-60min' WHEN break_hours < 1.5 THEN '1-1.5 hours' ELSE 'Over 1.5 hours' END AS break_duration_category, AVG(productive_percent) AS avg_productivity, COUNT(*) AS sample_count FROM break_analysis GROUP BY break_duration_category ORDER BY MIN(break_hours)"
    },
    {
      "question": "Find optimal work duration for highest productivity",
      "clickhouse_sql": "WITH duration_productivity AS (SELECT employee_id, attendance_date, online_duration / 3600 AS work_hours, productive_percent FROM attendance_data) SELECT FLOOR(work_hours) AS hour_bracket, concat(toString(FLOOR(work_hours)), '-', toString(FLOOR(work_hours) + 1), ' hours') AS work_duration, AVG(productive_percent) AS avg_productivity, COUNT(*) AS sample_count FROM duration_productivity WHERE work_hours > 0 AND work_hours < 12 GROUP BY hour_bracket ORDER BY avg_productivity DESC"
    },
    {
      "question": "Show employees who maintain productivity despite long hours",
      "clickhouse_sql": "WITH long_days AS (SELECT employee_id, COUNT(*) AS long_day_count FROM attendance_data WHERE online_duration > 9 * 3600 GROUP BY employee_id HAVING long_day_count >= 5) SELECT e.employee_id, e.first_name, e.last_name, e.department, e.avg_productive_percent, l.long_day_count FROM employee_duration_summary e JOIN long_days l ON e.employee_id = l.employee_id WHERE e.avg_productive_percent > 70 ORDER BY e.avg_productive_percent DESC, l.long_day_count DESC"
    },
    {
      "question": "Analyze impact of meeting frequency on department productivity",
      "clickhouse_sql": "WITH meeting_counts AS (SELECT e.department, COUNT(*) AS meeting_count FROM employee_usage_data u JOIN employee_duration_summary e ON u.Employee_ID = toString(e.employee_id) WHERE u.Application LIKE '%Teams%' OR u.Application LIKE '%Zoom%' OR u.Title LIKE '%meeting%' GROUP BY e.department), dept_productivity AS (SELECT department, avg_productive_percent FROM department_duration_summary) SELECT m.department, m.meeting_count, p.avg_productive_percent FROM meeting_counts m JOIN dept_productivity p ON m.department = p.department ORDER BY m.meeting_count DESC"
    },
    {
      "question": "Find best performing employees who mentor others",
      "clickhouse_sql": "WITH mentors AS (SELECT DISTINCT Manager AS mentor_id FROM attendance_data WHERE Manager IS NOT NULL), mentor_performance AS (SELECT e.employee_id, e.first_name, e.last_name, e.department, e.avg_productive_percent FROM employee_duration_summary e JOIN mentors m ON toString(e.employee_id) = m.mentor_id) SELECT employee_id, first_name, last_name, department, avg_productive_percent FROM mentor_performance WHERE avg_productive_percent > 75 ORDER BY avg_productive_percent DESC"
    },
    {
      "question": "Compare productive vs unproductive application usage patterns",
      "clickhouse_sql": "SELECT u.Mapping_Status, COUNT(DISTINCT u.Application) AS app_count, AVG(toUInt32(splitByChar(':', u.Duration)[1]) * 3600 + toUInt32(splitByChar(':', u.Duration)[2]) * 60 + toUInt32(splitByChar(':', u.Duration)[3])) / 3600 AS avg_app_hours, AVG(e.avg_productive_percent) AS avg_employee_productivity FROM employee_usage_data u JOIN employee_duration_summary e ON u.Employee_ID = toString(e.employee_id) WHERE u.Mapping_Status IN ('Productive', 'Unproductive') GROUP BY u.Mapping_Status ORDER BY u.Mapping_Status"
    },
    {
      "question": "Find employees who work most weekends",
      "clickhouse_sql": "SELECT e.employee_id, e.first_name, e.last_name, e.department, COUNT(DISTINCT a.attendance_date) AS weekend_days FROM attendance_data a JOIN employee_duration_summary e ON a.employee_id = e.employee_id WHERE toDayOfWeek(a.attendance_date) IN (1, 7) AND a.punch_in IS NOT NULL GROUP BY e.employee_id, e.first_name, e.last_name, e.department ORDER BY weekend_days DESC LIMIT 15"
    },
    {
      "question": "Find most common applications used by high performers",
      "clickhouse_sql": "WITH high_performers AS (SELECT employee_id FROM employee_duration_summary WHERE avg_productive_percent > 80), top_apps AS (SELECT u.Application, COUNT(*) AS usage_count FROM employee_usage_data u WHERE u.Employee_ID IN (SELECT toString(employee_id) FROM high_performers) GROUP BY u.Application ORDER BY usage_count DESC LIMIT 15) SELECT Application, usage_count FROM top_apps ORDER BY usage_count DESC"
    },
    {
      "question": "Analyze keyboard activity vs. mouse usage by department",
      "clickhouse_sql": "SELECT e.department, AVG(u.Key_Presses) AS avg_key_presses, AVG(u.Mouse_Clicks) AS avg_mouse_clicks, AVG(u.Key_Presses) / NULLIF(AVG(u.Mouse_Clicks), 0) AS key_to_mouse_ratio FROM employee_usage_data u JOIN employee_duration_summary e ON u.Employee_ID = toString(e.employee_id) GROUP BY e.department ORDER BY key_to_mouse_ratio DESC"
    },
    {
      "question": "Which teams have the highest ratio of productive to unproductive time?",
      "clickhouse_sql": "SELECT shift_name, AVG(productive_duration) AS avg_productive_time, AVG(unproductive_duration) AS avg_unproductive_time, AVG(productive_duration) / NULLIF(AVG(unproductive_duration), 0) AS productivity_ratio FROM attendance_data WHERE shift_name IS NOT NULL GROUP BY shift_name ORDER BY productivity_ratio DESC"
    },
    {
      "question": "Identify employees who might be experiencing burnout",
      "clickhouse_sql": "WITH overtime_data AS (SELECT employee_id, COUNT(*) AS overtime_days FROM attendance_data WHERE online_duration > 10 * 3600 GROUP BY employee_id), productivity_trend AS (SELECT employee_id, corr(toDayOfMonth(attendance_date), productive_percent) AS productivity_slope FROM attendance_data WHERE attendance_date >= date_sub(DAY, 30, today()) GROUP BY employee_id) SELECT e.employee_id, e.first_name, e.last_name, e.department, o.overtime_days, p.productivity_slope FROM employee_duration_summary e JOIN overtime_data o ON e.employee_id = o.employee_id JOIN productivity_trend p ON e.employee_id = p.employee_id WHERE o.overtime_days > 10 AND p.productivity_slope < -0.3 ORDER BY o.overtime_days DESC, p.productivity_slope ASC"
    },
    {
      "question": "Show teams with the most balanced work distribution",
      "clickhouse_sql": "WITH employee_workloads AS (SELECT employee_id, shift_name, SUM(online_duration) AS total_work_time FROM attendance_data WHERE shift_name IS NOT NULL GROUP BY employee_id, shift_name), team_workload_stats AS (SELECT shift_name, stddevPop(total_work_time) / AVG(total_work_time) AS workload_variation_coefficient FROM employee_workloads GROUP BY shift_name HAVING COUNT(*) >= 3) SELECT shift_name, ROUND(workload_variation_coefficient, 4) AS coeff_variation FROM team_workload_stats ORDER BY coeff_variation ASC LIMIT 10"
    },
    {
      "question": "Find managers with the most productive teams",
      "clickhouse_sql": "WITH manager_teams AS (SELECT DISTINCT Manager, employee_id FROM attendance_data WHERE Manager IS NOT NULL) SELECT m.Manager AS manager_name, COUNT(DISTINCT m.employee_id) AS team_size, AVG(e.avg_productive_percent) AS team_avg_productivity FROM manager_teams m JOIN employee_duration_summary e ON m.employee_id = e.employee_id GROUP BY m.Manager HAVING team_size >= 3 ORDER BY team_avg_productivity DESC LIMIT 10"
    },
    {
      "question": "Show departments with significant productivity differences between morning and afternoon",
      "clickhouse_sql": "WITH time_productivity AS (SELECT e.department, a.employee_id, CASE WHEN toHour(a.punch_in) < 12 THEN 'Morning' ELSE 'Afternoon' END AS day_period, AVG(a.productive_percent) AS avg_productivity FROM attendance_data a JOIN employee_duration_summary e ON a.employee_id = e.employee_id WHERE a.punch_in IS NOT NULL GROUP BY e.department, a.employee_id, day_period), department_time_diff AS (SELECT department, AVG(CASE WHEN day_period = 'Morning' THEN avg_productivity END) AS morning_productivity, AVG(CASE WHEN day_period = 'Afternoon' THEN avg_productivity END) AS afternoon_productivity FROM time_productivity GROUP BY department) SELECT department, morning_productivity, afternoon_productivity, ABS(morning_productivity - afternoon_productivity) AS productivity_difference FROM department_time_diff ORDER BY productivity_difference DESC"
    },
    {
      "question": "Identify employees with high productivity but low break duration",
      "clickhouse_sql": "SELECT e.employee_id, e.first_name, e.last_name, e.department, e.avg_productive_percent, AVG(toUInt32(splitByChar(':', b.Break_Duration)[1]) * 3600 + toUInt32(splitByChar(':', b.Break_Duration)[2]) * 60 + toUInt32(splitByChar(':', b.Break_Duration)[3])) / 3600 AS avg_break_hours FROM employee_duration_summary e JOIN employee_breaks b ON toString(e.employee_id) = b.Employee_ID WHERE toDate(b.Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY e.employee_id, e.first_name, e.last_name, e.department HAVING e.avg_productive_percent > 75 AND avg_break_hours < 0.5 ORDER BY e.avg_productive_percent DESC"
    },
    {
      "question": "Which teams have the most diverse application usage?",
      "clickhouse_sql": "SELECT a.shift_name, COUNT(DISTINCT u.Application) AS unique_apps FROM employee_usage_data u JOIN attendance_data a ON u.Employee_ID = toString(a.employee_id) AND toDate(u.Date) = toDate(a.attendance_date) WHERE toDate(a.attendance_date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY a.shift_name ORDER BY unique_apps DESC LIMIT 5"
    },
    {
      "question": "Find employees who frequently use unproductive URLs",
      "clickhouse_sql": "SELECT u.Employee_ID, e.first_name, e.last_name, e.department, COUNT(*) AS unproductive_url_count FROM employee_activity u JOIN employee_duration_summary e ON u.Employee_ID = toString(e.employee_id) WHERE u.Mapping_Status = 'Unproductive' AND u.URL IS NOT NULL AND toDate(u.Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY u.Employee_ID, e.first_name, e.last_name, e.department ORDER BY unproductive_url_count DESC LIMIT 10"
    },
    {
      "question": "Show productivity trends for employees who changed teams",
      "clickhouse_sql": "WITH team_changes AS (SELECT employee_id, COUNT(DISTINCT shift_name) AS team_count, groupArray(shift_name) AS teams FROM attendance_data WHERE toDate(attendance_date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY employee_id HAVING team_count > 1) SELECT t.employee_id, e.first_name, e.last_name, e.department, t.teams, AVG(a.productive_percent) AS avg_productivity FROM team_changes t JOIN employee_duration_summary e ON t.employee_id = e.employee_id JOIN attendance_data a ON t.employee_id = a.employee_id WHERE toDate(a.attendance_date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY t.employee_id, e.first_name, e.last_name, e.department, t.teams ORDER BY avg_productivity DESC"
    },
    {
      "question": "Which departments have the highest correlation between active time and productivity?",
      "clickhouse_sql": "SELECT department, corr(COALESCE(active_duration, 0), COALESCE(productive_percent, 0)) AS active_productivity_corr FROM attendance_data WHERE toDate(attendance_date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY department ORDER BY active_productivity_corr DESC LIMIT 5"
    },
    {
      "question": "Show employees with high productivity but low mouse activity",
      "clickhouse_sql": "SELECT e.employee_id, e.first_name, e.last_name, e.department, e.avg_productive_percent, AVG(u.Mouse_Clicks) AS avg_mouse_clicks FROM employee_activity u JOIN employee_duration_summary e ON u.Employee_ID = toString(e.employee_id) WHERE toDate(u.Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY e.employee_id, e.first_name, e.last_name, e.department HAVING e.avg_productive_percent > 75 AND avg_mouse_clicks < (SELECT AVG(Mouse_Clicks) FROM employee_activity WHERE toDate(Date) BETWEEN '2025-04-01' AND '2025-04-30') ORDER BY e.avg_productive_percent DESC"
    },
    {
      "question": "Which employees have the highest productivity improvement week-over-week?",
      "clickhouse_sql": "WITH weekly_productivity AS (SELECT employee_id, formatDateTime(toStartOfWeek(attendance_date), '%Y-%m-%d') AS week_start, AVG(COALESCE(Productive_Percent, 0)) AS avg_productivity FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY employee_id, week_start) SELECT w1.employee_id, e.first_name, e.last_name, e.department, w1.week_start AS current_week, (w1.avg_productivity - w2.avg_productivity) AS productivity_increase FROM weekly_productivity w1 JOIN weekly_productivity w2 ON w1.employee_id = w2.employee_id AND w1.week_start > w2.week_start JOIN employee_duration_summary e ON w1.employee_id = e.employee_id WHERE w1.avg_productivity > w2.avg_productivity ORDER BY productivity_increase DESC LIMIT 10"
    },
    {
      "question": "Find departments with high attendance but low productivity",
      "clickhouse_sql": "SELECT d.department, AVG(COALESCE(a.employees_present / NULLIF(a.total_employees, 0), 0)) * 100 AS avg_attendance_rate, AVG(COALESCE(e.avg_productive_percent, 0)) AS avg_productivity FROM daily_attendance_summary a JOIN department_duration_summary e ON a.department = e.department WHERE toDate(a.attendance_date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY d.department HAVING avg_attendance_rate > 90 AND avg_productivity < 50 ORDER BY avg_productivity ASC"
    },
    {
      "question": "Show employees with the most consistent break durations",
      "clickhouse_sql": "SELECT b.Employee_ID, e.first_name, e.last_name, e.department, stddevPop(toUInt32(splitByChar(':', b.Break_Duration)[1]) * 3600 + toUInt32(splitByChar(':', b.Break_Duration)[2]) * 60 + toUInt32(splitByChar(':', b.Break_Duration)[3])) / 3600 AS break_duration_stddev FROM employee_breaks b JOIN employee_duration_summary e ON b.Employee_ID = toString(e.employee_id) WHERE toDate(b.Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY b.Employee_ID, e.first_name, e.last_name, e.department ORDER BY break_duration_stddev ASC LIMIT 10"
    },
    {
      "question": "Which teams had the highest productivity during the first week of April 2025?",
      "clickhouse_sql": "SELECT department AS team, round(avgProductivityPrevWeek - avgProductivityLastWeek, 2) AS productivity_decline FROM ( SELECT department, avgIf(productive_percent, toDate(attendance_date) BETWEEN '2025-04-01' AND '2025-04-07') AS avgProductivityPrevWeek, avgIf(productive_percent, toDate(attendance_date) BETWEEN '2025-04-08' AND '2025-04-30') AS avgProductivityLastWeek FROM attendance_data WHERE toDate(attendance_date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY department ) ORDER BY productivity_decline DESC LIMIT 3"
    },
    {
      "question": "Get login and logout times for a specific date",
      "clickhouse_sql": "SELECT Employee_ID, In_Time, Out_Time FROM monthly_inout WHERE Date = toDate('2025-04-15') ORDER BY In_Time"
    },
    {
      "question": "Average punch duration by shift",
      "clickhouse_sql": "SELECT Shift_Name, avg(toIntervalSecond(Punch_Duration)) AS avg_punch_seconds FROM employee_metrics WHERE Attendance_Date BETWEEN toDate('2025-04-01') AND toDate('2025-04-30') GROUP BY Shift_Name ORDER BY avg_punch_seconds DESC"
    },
    {
      "question": "Which employees had more than 3 unproductive days?",
      "clickhouse_sql": "SELECT Employee_ID, count() AS unproductive_days FROM employee_metrics WHERE Productive_Percent < 30 AND Attendance_Date BETWEEN toDate('2025-04-01') AND toDate('2025-04-30') GROUP BY Employee_ID HAVING unproductive_days > 3 ORDER BY unproductive_days DESC"
    },
    {
      "question": "Employees with login time after 11 AM on more than 2 days",
      "clickhouse_sql": "SELECT Employee_ID, count() AS late_days FROM employee_metrics WHERE toHour(Punch_In) > 11 AND Attendance_Date BETWEEN toDate('2025-04-01') AND toDate('2025-04-30') GROUP BY Employee_ID HAVING late_days > 2 ORDER BY late_days DESC"
    },
    {
      "question": "Which employees consistently rank in the bottom 10% productivity across the last 3 months?",
      "clickhouse_sql": "SELECT employee_id, avg(avg_productive_percent) AS avg_productivity FROM employee_duration_summary WHERE toStartOfMonth(date) >= toStartOfMonth(now() - INTERVAL 3 MONTH) GROUP BY employee_id HAVING avg_productivity <= (SELECT quantile(0.1)(avg_productive_percent) FROM employee_duration_summary WHERE toStartOfMonth(date) >= toStartOfMonth(now() - INTERVAL 3 MONTH)) ORDER BY avg_productivity ASC"
    },
    {
      "question": "Get the 3-day moving average of productivity per employee",
      "clickhouse_sql": "SELECT Employee_ID, Attendance_Date, avg(Productive_Percent) OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg_productivity FROM employee_metrics WHERE Attendance_Date BETWEEN toDate('2025-04-01') AND toDate('2025-04-30')"
    },
    {
      "question": "Identify productivity drops over 20% within a week for employees",
      "clickhouse_sql": "SELECT Employee_ID, Attendance_Date, Productive_Percent, lag(Productive_Percent, 1) OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS previous_day_percent FROM employee_metrics WHERE Attendance_Date BETWEEN toDate('2025-04-01') AND toDate('2025-04-30') HAVING (previous_day_percent - Productive_Percent) > 20"
    },
    {
      "question": "Find employees whose break time exceeded 25% of their total duration on any day",
      "clickhouse_sql": "SELECT Employee_ID, Attendance_Date, Break_Duration, Total_Duration FROM employee_metrics WHERE toFloat64(toIntervalSecond(Break_Duration)) / toFloat64(toIntervalSecond(Total_Duration)) > 0.25 ORDER BY Employee_ID, Attendance_Date"
    },
    {
      "question": "Compare department-wise productivity between the current and previous month",
      "clickhouse_sql": "SELECT department, avgIf(Productive_Percent, toStartOfMonth(Attendance_Date) = toStartOfMonth(now())) AS current_month_avg, avgIf(Productive_Percent, toStartOfMonth(Attendance_Date) = toStartOfMonth(now() - INTERVAL 1 MONTH)) AS prev_month_avg FROM employee_metrics WHERE Attendance_Date >= toDate(now() - INTERVAL 2 MONTH) GROUP BY department"
    },
    {
      "question": "List employees with a sudden spike in idle time compared to the previous week average",
      "clickhouse_sql": "SELECT Employee_ID, Attendance_Date, Idle_Duration, avg(Idle_Duration) OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) AS last_week_avg FROM employee_metrics WHERE Attendance_Date BETWEEN toDate('2025-04-01') AND toDate('2025-04-30') HAVING toIntervalSecond(Idle_Duration) > 1.5 * toIntervalSecond(last_week_avg)"
    },
    {
      "question": "Get productivity percentiles per department",
      "clickhouse_sql": "SELECT department, quantile(0.25)(Productive_Percent) AS p25, quantile(0.5)(Productive_Percent) AS median, quantile(0.75)(Productive_Percent) AS p75 FROM employee_metrics WHERE Attendance_Date BETWEEN toDate('2025-04-01') AND toDate('2025-04-30') GROUP BY department"
    },
    {
      "question": "Detect outliers in productivity using IQR method",
      "clickhouse_sql": "SELECT * FROM (SELECT Employee_ID, Productive_Percent, quantile(0.25)(Productive_Percent) AS Q1, quantile(0.75)(Productive_Percent) AS Q3 FROM employee_metrics WHERE Attendance_Date BETWEEN toDate('2025-04-01') AND toDate('2025-04-30') GROUP BY Employee_ID) WHERE Productive_Percent < (Q1 - 1.5 * (Q3 - Q1)) OR Productive_Percent > (Q3 + 1.5 * (Q3 - Q1))"
    },
    {
      "question": "Which teams improved their productivity over the last month?",
      "clickhouse_sql": "SELECT Team, avgIf(Productive_Percent, toStartOfMonth(Attendance_Date) = toStartOfMonth(now())) AS current_avg, avgIf(Productive_Percent, toStartOfMonth(Attendance_Date) = toStartOfMonth(now() - INTERVAL 1 MONTH)) AS prev_avg FROM employee_metrics WHERE Attendance_Date >= toDate(now() - INTERVAL 2 MONTH) GROUP BY Team HAVING current_avg > prev_avg"
    },
    {
      "question": "Get the longest continuous streak of 100% productivity per employee",
      "clickhouse_sql": "SELECT Employee_ID, max(streak) AS max_streak FROM (SELECT Employee_ID, count() AS streak FROM (SELECT Employee_ID, Attendance_Date, Productive_Percent, rowNumberInAllBlocks() - rowNumberInAllBlocks() OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS grp FROM employee_metrics WHERE Productive_Percent = 100) GROUP BY Employee_ID, grp) GROUP BY Employee_ID"
    },
    {
      "question": "Which employees show a continuous decline in productivity over the last 3 weeks?",
      "clickhouse_sql": "SELECT Employee_ID FROM (SELECT Employee_ID, toMonday(Attendance_Date) AS week, avg(Productive_Percent) AS avg_prod FROM employee_metrics WHERE Attendance_Date >= subtractWeeks(today(), 3) GROUP BY Employee_ID, week ORDER BY Employee_ID, week) WHERE avg_prod < lag(avg_prod, 1) OVER (PARTITION BY Employee_ID ORDER BY week) AND avg_prod < lag(avg_prod, 2) OVER (PARTITION BY Employee_ID ORDER BY week)"
    },
    {
      "question": "Find average break time by department and shift for each week",
      "clickhouse_sql": "SELECT toMonday(Attendance_Date) AS week, department, Shift_Name, avg(toIntervalSecond(Break_Duration)) AS avg_break FROM employee_metrics WHERE Attendance_Date >= toDate('2025-04-01') GROUP BY week, department, Shift_Name ORDER BY week, avg_break DESC"
    },
    {
      "question": "Which applications are associated with low productivity employees?",
      "clickhouse_sql": "SELECT Application, count(DISTINCT ea.Employee_ID) AS low_perf_users FROM employee_activity ea JOIN employee_metrics em ON ea.Employee_ID = em.Employee_ID AND ea.Date = em.Attendance_Date WHERE em.Productive_Percent < 40 AND ea.Date BETWEEN toDate('2025-04-01') AND toDate('2025-04-30') GROUP BY Application ORDER BY low_perf_users DESC"
    },
    {
      "question": "Detect anomalies in productivity using standard deviation",
      "clickhouse_sql": "SELECT Employee_ID, Attendance_Date, Productive_Percent FROM (SELECT Employee_ID, Attendance_Date, Productive_Percent, avg(Productive_Percent) OVER (PARTITION BY Employee_ID) AS avg_prod, stddevPop(Productive_Percent) OVER (PARTITION BY Employee_ID) AS std_dev FROM employee_metrics WHERE Attendance_Date >= toDate('2025-04-01')) WHERE abs(Productive_Percent - avg_prod) > 2 * std_dev ORDER BY Employee_ID, Attendance_Date"
    },
    {
      "question": "Identify productivity improvement from last month to this month",
      "clickhouse_sql": "SELECT Employee_ID, (curr_month - last_month) AS improvement FROM (SELECT Employee_ID, avgIf(Productive_Percent, toMonth(Attendance_Date) = toStartOfMonth(now())) AS curr_month, avgIf(Productive_Percent, toMonth(Attendance_Date) = toStartOfMonth(now() - INTERVAL 1 MONTH)) AS last_month FROM employee_metrics GROUP BY Employee_ID) WHERE improvement > 0 ORDER BY improvement DESC"
    },
    {
        "question": "fetch me teams with productivity below 40 and their avg working hours(online duration)",
        "clickhouse_sql": "SELECT Group_Name AS team, ROUND(AVG(COALESCE(Productive_Percent, 0)), 2) AS avg_productivity, ROUND(AVG(toUInt32(splitByChar(':', COALESCE(Online_Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(Online_Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(Online_Duration, '0:0:0'))[3])) / 3600, 2) AS avg_working_hours FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY Group_Name HAVING avg_productivity < 40 ORDER BY avg_productivity ASC"
    },
    {
        "question": "Which teams have an average active duration below 5 hours and productivity above 60% in April 2025?",
        "clickhouse_sql": "SELECT Group_Name AS team, ROUND(AVG(COALESCE(Productive_Percent, 0)), 2) AS avg_productivity, ROUND(AVG(toUInt32(splitByChar(':', COALESCE(Active_Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(Active_Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(Active_Duration, '0:0:0'))[3])) / 3600, 2) AS avg_active_hours FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY Group_Name HAVING avg_active_hours < 5 AND avg_productivity > 60 ORDER BY avg_productivity DESC"
    },
    {
        "question": "Show employees with high keyboard activity but low productivity in the last two weeks of April 2025",
        "clickhouse_sql": "WITH keyboard_activity AS (SELECT Employee_ID, AVG(Key_Presses) AS avg_key_presses FROM employee_activity WHERE toDate(Date) BETWEEN '2025-04-16' AND '2025-04-30' GROUP BY Employee_ID) SELECT e.Employee_ID, e.First_Name, e.Last_Name, e.department, ROUND(AVG(COALESCE(e.Productive_Percent, 0)), 2) AS avg_productivity, k.avg_key_presses FROM employee_metrics e JOIN keyboard_activity k ON e.Employee_ID = k.Employee_ID WHERE toDate(e.Attendance_Date) BETWEEN '2025-04-16' AND '2025-04-30' GROUP BY e.Employee_ID, e.First_Name, e.Last_Name, e.department, k.avg_key_presses HAVING avg_productivity < 40 AND k.avg_key_presses > (SELECT AVG(Key_Presses) * 1.5 FROM employee_activity WHERE toDate(Date) BETWEEN '2025-04-16' AND '2025-04-30') ORDER BY k.avg_key_presses DESC LIMIT 10"
    },
    {
        "question": "Which departments had the highest average break duration in April 2025, and what is their average productivity?",
        "clickhouse_sql": "SELECT department, ROUND(AVG(toUInt32(splitByChar(':', COALESCE(Break_Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(Break_Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(Break_Duration, '0:0:0'))[3])) / 3600, 2) AS avg_break_hours, ROUND(AVG(COALESCE(Productive_Percent, 0)), 2) AS avg_productivity FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY department ORDER BY avg_break_hours DESC LIMIT 5"
    },
    {
        "question": "Identify teams with more than 10% of days having zero productivity in April 2025",
        "clickhouse_sql": "SELECT Group_Name AS team, COUNTIf(COALESCE(Productive_Percent, 0) = 0) AS zero_prod_days, COUNT(*) AS total_days, ROUND((COUNTIf(COALESCE(Productive_Percent, 0) = 0) / COUNT(*) * 100), 2) AS zero_prod_percentage FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY Group_Name HAVING zero_prod_percentage > 10 ORDER BY zero_prod_percentage DESC"
    },
    {
        "question": "Which employees have an average online duration exceeding their shift duration by more than 1 hour in April 2025?",
        "clickhouse_sql": "SELECT e.Employee_ID, e.First_Name, e.Last_Name, e.department, ROUND(AVG(toUInt32(splitByChar(':', COALESCE(Online_Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(Online_Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(Online_Duration, '0:0:0'))[3])) / 3600, 2) AS avg_online_hours, ROUND(AVG(toUInt32(splitByChar(':', COALESCE(Punch_Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(Punch_Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(Punch_Duration, '0:0:0'))[3])) / 3600, 2) AS avg_shift_hours FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY e.Employee_ID, e.First_Name, e.Last_Name, e.department HAVING avg_online_hours > (avg_shift_hours + 1) ORDER BY (avg_online_hours - avg_shift_hours) DESC LIMIT 10"
    },
    {
        "question": "Show the top 5 applications used by teams with productivity below 40% in April 2025",
        "clickhouse_sql": "WITH low_prod_teams AS (SELECT Group_Name FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY Group_Name HAVING AVG(COALESCE(Productive_Percent, 0)) < 40) SELECT a.Application, COUNT(*) AS usage_count, ROUND(SUM(toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[3])) / 3600, 2) AS total_hours FROM employee_activity a JOIN employee_metrics m ON a.Employee_ID = m.Employee_ID AND a.Date = m.Attendance_Date WHERE a.Date BETWEEN '2025-04-01' AND '2025-04-30' AND m.Group_Name IN (SELECT Group_Name FROM low_prod_teams) GROUP BY a.Application ORDER BY total_hours DESC LIMIT 5"
    },
    {
      "question": "Which employees had the highest average productivity during the last week of April 2025?",
      "clickhouse_sql": "SELECT Employee_ID, First_Name, Last_Name, department, ROUND(AVG(COALESCE(Productive_Percent, 0)), 2) AS avg_productivity FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-24' AND '2025-04-30' GROUP BY Employee_ID, First_Name, Last_Name, department ORDER BY avg_productivity DESC LIMIT 10"
    },
    {
      "question": "Show teams with the highest average time spent on cloud-based apps in April 2025",
      "clickhouse_sql": "SELECT m.Group_Name AS team, ROUND(AVG(toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(a.Duration,' '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(a.Duration,' '0:0:0'))[3])) / 3600, 2) AS avg_cloud_app_hours FROM employee_activity a JOIN employee_metrics m ON a.Employee_ID = m.Employee_ID AND a.Date = m.Attendance_Date WHERE toDate(a.Date) BETWEEN '2025-04-01' AND '2025-04-30' AND a.Application IN ('Google Drive', 'Dropbox', 'AWS') GROUP BY m.Group_Name ORDER BY avg_cloud_app_hours DESC LIMIT 5"
    },
    {
      "question": "Which employees had more than 5 days with productivity below 30% and online duration > 8 hours in April 2025?",
      "clickhouse_sql": "SELECT Employee_ID, First_Name, Last_Name, department, COUNTIf(COALESCE(Productive_Percent, 0) < 30 AND toUInt32(splitByChar(':', COALESCE(Online_Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(Online_Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(Online_Duration, '0:0:0'))[3]) / 3600 > 8) AS low_prod_long_hours_days FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY Employee_ID, First_Name, Last_Name, department HAVING low_prod_long_hours_days > 5 ORDER BY low_prod_long_hours_days DESC"
    },
    {
      "question": "Find departments with the highest average number of applications used per employee in April 2025",
      "clickhouse_sql": "SELECT m.department, ROUND(AVG(COALESCE(a.App_Count, 0)), 2) AS avg_app_count FROM employee_metrics m JOIN employee_activity a ON m.Employee_ID = a.Employee_ID AND m.Attendance_Date = a.Date WHERE toDate(m.Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY m.department ORDER BY avg_app_count DESC LIMIT 5"
    },
    {
      "question": "Which teams had the highest average productivity during the middle of the workday (10 AM - 2 PM) in April 2025?",
      "clickhouse_sql": "SELECT Group_Name AS team, ROUND(AVG(COALESCE(Productive_Percent, 0)), 2) AS avg_midday_productivity FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toHour(Punch_In) <= 10 AND (Punch_Out IS NULL OR toHour(Punch_Out) >= 14) GROUP BY Group_Name ORDER BY avg_midday_productivity DESC LIMIT 5"
    },
    {
      "question": "Show employees with the highest average time spent on gaming apps in April 2025",
      "clickhouse_sql": "SELECT a.Employee_ID, m.First_Name, m.Last_Name, m.department, ROUND(AVG(toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[3])) / 3600, 2) AS avg_gaming_hours FROM employee_activity a JOIN employee_metrics m ON a.Employee_ID = m.Employee_ID AND a.Date = m.Attendance_Date WHERE toDate(a.Date) BETWEEN '2025-04-01' AND '2025-04-30' AND a.Application LIKE '%Game%' GROUP BY a.Employee_ID, m.First_Name, m.Last_Name, m.department ORDER BY avg_gaming_hours DESC LIMIT 5"
    },
    {
    "question": "Which departments had the highest variance in employee attendance duration in April 2025?",
    "clickhouse_sql": "SELECT department, ROUND(STDDEV_POP(toUInt32(splitByChar(':', COALESCE(Punch_Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(Punch_Duration,' '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(Punch_Duration,' '0:0'))[3])) / 3600, 2) AS variance_shift_hours FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY department ORDER BY variance_shift_hours DESC LIMIT 5"
    },
    {
        "question": "Which employees worked overtime (Punch_Duration > Shift_End) for more than 10 hours total in April 2025?",
        "clickhouse_sql": "SELECT m.Employee_ID, m.First_Name, m.Last_Name, m.department, ROUND(SUM(CASE WHEN toDateTime(COALESCE(m.Punch_Out, '2000-01-01 00:00:00')) > toDateTime(COALESCE(m.Shift_End, '2000-01-01 00:00:00')) THEN (toUnixTimestamp(toDateTime(COALESCE(m.Punch_Duration, '0:0:0')) - toUnixTimestamp(toDateTime(COALESCE(m.Shift_End, '2000-01-01 00:00:00'))) / 3600) ELSE 0 END) ELSE 2) AS total_overtime_hours FROM employee_metrics m WHERE toDate(m.Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND m.Punch_Out IS NOT NULL AND m.Shift_End IS NOT NULL GROUP BY m.Employee_ID, m.First_Name, m.Last_Name, m.department HAVING total_overtime_hours > 10 ORDER BY total_overtime_hours DESC LIMIT 10"
    },
    {
        "question": "Which teams had the highest 95th percentile of productivity in April 2025?",
        "clickhouse_sql": "SELECT Group_Name AS team, ROUND(quantile(0.95)(COALESCE(Productive_Percent, 0)), 2) AS productivity_p95 FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY Group_Name ORDER BY productivity_p95 DESC LIMIT 5"
    },
    {
        "question": "Which employees switched between apps more than 100 times per day on average in April 2025?",
        "clickhouse_sql": "SELECT a.Employee_ID, m.First_Name, m.Last_Name, m.department, ROUND(AVG(COALESCE(a.App_Switches, 0)), 0) AS avg_app_switches FROM employee_activity a JOIN employee_metrics m ON a.Employee_ID = m.Employee_ID AND a.Date = m.Attendance_Date WHERE toDate(a.Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY a.Employee_ID, m.First_Name, m.Last_Name, m.department HAVING avg_app_switches > 100 ORDER BY avg_app_switches DESC LIMIT 10"
    },
    {
        "question": "Which departments had the highest average meeting time based on calendar app activity in April 2025?",
        "clickhouse_sql": "SELECT m.department, ROUND(AVG(toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[3])) / 3600/3600, 2) AS avg_meeting_hours FROM employee_activity a JOIN employee_metrics m ON a.Employee_ID = m.Employee_ID AND a.Date = m.Attendance_Date WHERE toDate(a.Date) BETWEEN '2025-04-01' AND '2025-04-30' AND a.Application IN ('Google Calendar,' 'Outlook Calendar') GROUP BY m.department ORDER BY avg_meeting_hours DESC LIMIT 5"
    },
    {
        "question": "Find employees with the highest average number of distinct applications used per day in April 2025",
        "clickhouse_sql": "SELECT a.Employee_ID, m.First_Name, m.Last_Name, m.department, ROUND(AVG(COALESCE(a.App_Distinct_count, 0)), 2) AS avg_distinct_apps FROM employee_activity a JOIN employee_metrics m ON a.Employee_ID = m.Employee_ID AND a.Date = m.Attendance_Date WHERE toDate(a.Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY a.Employee_ID, m.First_Name, m.Last_Name, m.department ORDER BY avg_distinct_apps DESC LIMIT 10"
    },
    {
        "question": "Which teams had the highest correlation between key presses and productivity in April 2025",
        "clickhouse_sql": "SELECT m.Group_Name AS team, corr(COALESCE(a.Key_Presses, 0), COALESCE(m.Productive_Percent, 0)) AS key_press_productivity_corr FROM employee_metrics m JOIN employee_activity a ON m.Employee_ID = a.Employee_ID AND m.Attendance_Date = a.Date WHERE toDate(m.Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY m.Group_Name HAVING ABS(key_press_productivity_corr) > 0.5 ORDER BY key_press_productivity_corr DESC LIMIT 5"
    },
    {
        "question": "Which employees were continuously absent for 5 or more workdays in April 2025?",
        "clickhouse_sql": "WITH absences AS (SELECT Employee_ID, First_Name, Last_Name, department, Attendance_Date, Punch_In, row_number() OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS rn, row_number() OVER (PARTITION BY Employee_ID, CASE WHEN Punch_In IS NULL THEN 1 ELSE 0 END ORDER BY Attendance_Date) AS absent_rn FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(Attendance_Date)) BETWEEN 1 AND 5), absence_groups AS (SELECT Employee_ID, First_Name, Last_Name, department, COUNT(*) AS consecutive_absences FROM absences WHERE Punch_In IS NULL GROUP BY Employee_ID, First_Name, Last_Name, department, (rn - absent_rn) HAVING consecutive_absences >= 5) SELECT Employee_ID, First_Name, Last_Name, department, consecutive_absences FROM absence_groups ORDER BY consecutive_absences DESC"
    },
    {
        "question": "Which employees had 3 or more consecutive absences with zero productivity in April 2025?",
        "clickhouse_sql": "WITH absences AS (SELECT Employee_ID, First_Name, Last_Name, department, Attendance_Date, Punch_In, COALESCE(Productive_Percent, 0) AS prod_percent, row_number() OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS rn, row_number() OVER (PARTITION BY Employee_ID, CASE WHEN Punch_In IS NULL AND prod_percent = 0 THEN 1 ELSE 0 END ORDER BY Attendance_Date) AS absent_rn FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(Attendance_Date)) BETWEEN 1 AND 5), absence_groups AS (SELECT Employee_ID, First_Name, Last_Name, department, COUNT(*) AS consecutive_absences FROM absences WHERE Punch_In IS NULL AND prod_percent = 0 GROUP BY Employee_ID, First_Name, Last_Name, department, (rn - absent_rn) HAVING consecutive_absences >= 3) SELECT Employee_ID, First_Name, Last_Name, department, consecutive_absences FROM absence_groups ORDER BY consecutive_absences DESC"
    },
    {
        "question": "Which employees were absent for 4 or more consecutive workdays and had no online activity in April 2025?",
        "clickhouse_sql": "WITH absences AS (SELECT m.Employee_ID, m.First_Name, m.Last_Name, m.department, m.Attendance_Date, m.Punch_In, COALESCE(m.Online_Duration, '0:0:0') AS online_dur, row_number() OVER (PARTITION BY m.Employee_ID ORDER BY m.Attendance_Date) AS rn, row_number() OVER (PARTITION BY m.Employee_ID, CASE WHEN m.Punch_In IS NULL AND online_dur = '0:0:0' THEN 1 ELSE 0 END ORDER BY m.Attendance_Date) AS absent_rn FROM employee_metrics m WHERE toDate(m.Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(m.Attendance_Date)) BETWEEN 1 AND 5), absence_groups AS (SELECT Employee_ID, First_Name, Last_Name, department, COUNT(*) AS consecutive_absences FROM absences WHERE Punch_In IS NULL AND online_dur = '0:0:0' GROUP BY Employee_ID, First_Name, Last_Name, department, (rn - absent_rn) HAVING consecutive_absences >= 4) SELECT Employee_ID, First_Name, Last_Name, department, consecutive_absences FROM absence_groups ORDER BY consecutive_absences DESC"
    },
    {
        "question": "Which teams had employees with 5 or more consecutive absences in April 2025?",
        "clickhouse_sql": "WITH absences AS (SELECT Employee_ID, First_Name, Last_Name, Group_Name, Attendance_Date, Punch_In, row_number() OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS rn, row_number() OVER (PARTITION BY Employee_ID, CASE WHEN Punch_In IS NULL THEN 1 ELSE 0 END ORDER BY Attendance_Date) AS absent_rn FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(Attendance_Date)) BETWEEN 1 AND 5), absence_groups AS (SELECT Employee_ID, First_Name, Last_Name, Group_Name, COUNT(*) AS consecutive_absences FROM absences WHERE Punch_In IS NULL GROUP BY Employee_ID, First_Name, Last_Name, Group_Name, (rn - absent_rn) HAVING consecutive_absences >= 5) SELECT Group_Name AS team, Employee_ID, First_Name, Last_Name, consecutive_absences FROM absence_groups ORDER BY Group_Name, consecutive_absences DESC"
    },
    {
        "question": "Which employees had a productivity increase of more than 15% in the second half of April to the 2025 compared to the first half?",
        "clickhouse_sql": "WITH first_half AS (SELECT Employee_ID, AVG(COALESCE(Productive_Percent, 0)) AS first_half_avg FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-15' GROUP BY Employee_ID), second_half AS (SELECT Employee_ID, AVG(COALESCE(Productive_Percent, 0)) AS second_half_avg FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-16' AND '2025-04-30' GROUP BY Employee_ID) SELECT s.Employee_ID, m.First_Name, m.Last_Name, m.department, ROUND(first_half_avg, 2) AS first_half, ROUND(second_half_avg, 2) AS second_half FROM second_half s JOIN first_half f ON s.Employee_ID = f.Employee_ID JOIN employee_metrics m ON s.Employee_ID = m.Employee_ID WHERE toDate(m.Attendance_Date) BETWEEN '2025-04-16' AND '2025-04-30' AND (second_half_avg - first_half_avg) > 15 GROUP BY s.Employee_ID, m.First_Name, m.Last_Name, m.department, first_half_avg, second_half_avg ORDER BY (second_half_avg - first_half_avg) DESC LIMIT 10"
    },
    {
        "question": "Which departments had the lowest average time spent on collaboration tools in April 2025?",
        "clickhouse_sql": "SELECT m.department, ROUND(AVG(toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[3])) / 3600, 2) AS avg_collaboration_hours FROM employee_activity a JOIN employee_metrics m ON a.Employee_ID = m.Employee_ID AND a.Date = m.Attendance_Date WHERE toDate(a.Date) BETWEEN '2025-04-01' AND '2025-04-30' AND a.Application IN ('Trello', 'Jira', 'Asana') GROUP BY m.department ORDER BY avg_collaboration_hours ASC LIMIT 5"
    },
    {
        "question": "Find employees with the highest average number of distinct applications used per day in April 2025",
        "clickhouse_sql": "SELECT a.Employee_ID, m.First_Name, m.Last_Name, m.department, ROUND(AVG(COALESCE(a.App_Distinct_count, 0)), 2) AS avg_distinct_apps FROM employee_activity a JOIN employee_metrics m ON a.Employee_ID = m.Employee_ID AND a.Date = m.Attendance_Date WHERE toDate(a.Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY a.Employee_ID, m.First_Name, m.Last_Name, m.department ORDER BY avg_distinct_apps DESC LIMIT 10"
    },
    {
        "question": "Which teams had the highest correlation between key presses and productivity in April 2025",
        "clickhouse_sql": "SELECT m.Group_Name AS team, corr(COALESCE(a.Key_Presses, 0), COALESCE(m.Productive_Percent, 0)) AS key_press_productivity_corr FROM employee_metrics m JOIN employee_activity a ON m.Employee_ID = a.Employee_ID AND m.Attendance_Date = a.Date WHERE toDate(m.Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY m.Group_Name HAVING ABS(key_press_productivity_corr) > 0.5 ORDER BY key_press_productivity_corr DESC LIMIT 5"
    },
    {
        "question": "Which employees were continuously absent for 5 or more workdays in April 2025?",
        "clickhouse_sql": "WITH absences AS (SELECT Employee_ID, First_Name, Last_Name, department, Attendance_Date, Punch_In, row_number() OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS rn, row_number() OVER (PARTITION BY Employee_ID, CASE WHEN Punch_In IS NULL THEN 1 ELSE 0 END ORDER BY Attendance_Date) AS absent_rn FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(Attendance_Date)) BETWEEN 1 AND 5), absence_groups AS (SELECT Employee_ID, First_Name, Last_Name, department, COUNT(*) AS consecutive_absences FROM absences WHERE Punch_In IS NULL GROUP BY Employee_ID, First_Name, Last_Name, department, (rn - absent_rn) HAVING consecutive_absences >= 5) SELECT Employee_ID, First_Name, Last_Name, department, consecutive_absences FROM absence_groups ORDER BY consecutive_absences DESC"
    },
    {
        "question": "Which employees had 3 or more consecutive absences with zero productivity in April 2025?",
        "clickhouse_sql": "WITH absences AS (SELECT Employee_ID, First_Name, Last_Name, department, Attendance_Date, Punch_In, COALESCE(Productive_Percent, 0) AS prod_percent, row_number() OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS rn, row_number() OVER (PARTITION BY Employee_ID, CASE WHEN Punch_In IS NULL AND prod_percent = 0 THEN 1 ELSE 0 END ORDER BY Attendance_Date) AS absent_rn FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(Attendance_Date)) BETWEEN 1 AND 5), absence_groups AS (SELECT Employee_ID, First_Name, Last_Name, department, COUNT(*) AS consecutive_absences FROM absences WHERE Punch_In IS NULL AND prod_percent = 0 GROUP BY Employee_ID, First_Name, Last_Name, department, (rn - absent_rn) HAVING consecutive_absences >= 3) SELECT Employee_ID, First_Name, Last_Name, department, consecutive_absences FROM absence_groups ORDER BY consecutive_absences DESC"
    },
    {
        "question": "Which employees were absent for 4 or more consecutive workdays and had no online activity in April 2025?",
        "clickhouse_sql": "WITH absences AS (SELECT Employee_ID, First_Name, Last_Name, department, Attendance_Date, Punch_In, COALESCE(m.Online_Duration, '0:0:0') AS online_dur, row_number() OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS rn, row_number() OVER (PARTITION BY Employee_ID, CASE WHEN Punch_In IS NULL AND online_dur = '0:0:0' THEN 1 ELSE 0 END ORDER BY Attendance_Date) AS absent_rn FROM employee_metrics m WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(Attendance_Date)) BETWEEN 1 AND 5), absence_groups AS (SELECT Employee_ID, First_Name, Last_Name, department, COUNT(*) AS consecutive_absences FROM absences WHERE Punch_In IS NULL AND online_dur = '0:0:0' GROUP BY Employee_ID, First_Name, Last_Name, department, (rn - absent_rn) HAVING consecutive_absences >= 4) SELECT Employee_ID, First_Name, Last_Name, department, consecutive_absences FROM absence_groups ORDER BY consecutive_absences DESC"
    },
    {
        "question": "Which teams had employees with 5 or more consecutive absences in April 2025?",
        "clickhouse_sql": "WITH absences AS (SELECT Employee_ID, First_Name, Last_Name, Group_Name, Attendance_Date, Punch_In, row_number() OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS rn, row_number() OVER (PARTITION BY Employee_ID, CASE WHEN Punch_In IS NULL THEN 1 ELSE 0 END ORDER BY Attendance_Date) AS absent_rn FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(Attendance_Date)) BETWEEN 1 AND 5), absence_groups AS (SELECT Employee_ID, First_Name, Last_Name, Group_Name, COUNT(*) AS consecutive_absences FROM absences WHERE Punch_In IS NULL GROUP BY Employee_ID, First_Name, Last_Name, Group_Name, (rn - absent_rn) HAVING consecutive_absences >= 5) SELECT Group_Name AS team, Employee_ID, First_Name, Last_Name, consecutive_absences FROM absence_groups ORDER BY Group_Name, consecutive_absences DESC"
    },
    {
        "question": "Which employees had a productivity increase of more than 15% in the second half of April to the 2025 compared to the first half?",
        "clickhouse_sql": "WITH first_half AS (SELECT Employee_ID, AVG(COALESCE(Productive_Percent, 0)) AS first_half_avg FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-15' GROUP BY Employee_ID), second_half AS (SELECT Employee_ID, AVG(COALESCE(Productive_Percent, 0)) AS second_half_avg FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-16' AND '2025-04-30' GROUP BY Employee_ID) SELECT s.Employee_ID, m.First_Name, m.Last_Name, m.department, ROUND(first_half_avg, 2) AS first_half, ROUND(second_half_avg, 2) AS second_half FROM second_half s JOIN first_half f ON s.Employee_ID = f.Employee_ID JOIN employee_metrics m ON s.Employee_ID = m.Employee_ID WHERE toDate(m.Attendance_Date) BETWEEN '2025-04-16' AND '2025-04-30' AND (second_half_avg - first_half_avg) > 15 GROUP BY s.Employee_ID, m.First_Name, m.Last_Name, m.department, first_half_avg, second_half_avg ORDER BY (second_half_avg - first_half_avg) DESC LIMIT 10"
    },
    {
        "question": "Which departments had the lowest average time spent on collaboration tools in April 2025?",
        "clickhouse_sql": "SELECT m.department, ROUND(AVG(toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[3])) / 3600, 2) AS avg_collaboration_hours FROM employee_activity a JOIN employee_metrics m ON a.Employee_ID = m.Employee_ID AND a.Date = m.Attendance_Date WHERE toDate(a.Date) BETWEEN '2025-04-01' AND '2025-04-30' AND a.Application IN ('Trello', 'Jira', 'Asana') GROUP BY m.department ORDER BY avg_collaboration_hours ASC LIMIT 5"
    },
    {
        "question": "Find employees with the highest average number of distinct applications used per day in April 2025",
        "clickhouse_sql": "SELECT a.Employee_ID, m.First_Name, m.Last_Name, m.department, ROUND(AVG(COALESCE(a.App_Distinct_count, 0)), 2) AS avg_distinct_apps FROM employee_activity a JOIN employee_metrics m ON a.Employee_ID = m.Employee_ID AND a.Date = m.Attendance_Date WHERE toDate(a.Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY a.Employee_ID, m.First_Name, m.Last_Name, m.department ORDER BY avg_distinct_apps DESC LIMIT 10"
    },
    {
        "question": "Which teams had the highest correlation between key presses and productivity in April 2025",
        "clickhouse_sql": "SELECT m.Group_Name AS team, corr(COALESCE(a.Key_Presses, 0), COALESCE(m.Productive_Percent, 0)) AS key_press_productivity_corr FROM employee_metrics m JOIN employee_activity a ON m.Employee_ID = a.Employee_ID AND m.Attendance_Date = a.Date WHERE toDate(m.Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY m.Group_Name HAVING ABS(key_press_productivity_corr) > 0.5 ORDER BY key_press_productivity_corr DESC LIMIT 5"
    },
    {
        "question": "Which employees were continuously absent for 5 or more workdays in April 2025?",
        "clickhouse_sql": "WITH absences AS (SELECT Employee_ID, First_Name, Last_Name, department, Attendance_Date, Punch_In, row_number() OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS rn, row_number() OVER (PARTITION BY Employee_ID, CASE WHEN Punch_In IS NULL THEN 1 ELSE 0 END ORDER BY Attendance_Date) AS absent_rn FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(Attendance_Date)) BETWEEN 1 AND 5), absence_groups AS (SELECT Employee_ID, First_Name, Last_Name, department, COUNT(*) AS consecutive_absences FROM absences WHERE Punch_In IS NULL GROUP BY Employee_ID, First_Name, Last_Name, department, (rn - absent_rn) HAVING consecutive_absences >= 5) SELECT Employee_ID, First_Name, Last_Name, department, consecutive_absences FROM absence_groups ORDER BY consecutive_absences DESC"
    },
    {
        "question": "Which employees had 3 or more consecutive absences with zero productivity in April 2025?",
        "clickhouse_sql": "WITH absences AS (SELECT Employee_ID, First_Name, Last_Name, department, Attendance_Date, Punch_In, COALESCE(Productive_Percent, 0) AS prod_percent, row_number() OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS rn, row_number() OVER (PARTITION BY Employee_ID, CASE WHEN Punch_In IS NULL AND prod_percent = 0 THEN 1 ELSE 0 END ORDER BY Attendance_Date) AS absent_rn FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(Attendance_Date)) BETWEEN 1 AND 5), absence_groups AS (SELECT Employee_ID, First_Name, Last_Name, department, COUNT(*) AS consecutive_absences FROM absences WHERE Punch_In IS NULL AND prod_percent = 0 GROUP BY Employee_ID, First_Name, Last_Name, department, (rn - absent_rn) HAVING consecutive_absences >= 3) SELECT Employee_ID, First_Name, Last_Name, department, consecutive_absences FROM absence_groups ORDER BY consecutive_absences DESC"
    },
    {
        "question": "Which employees were absent for 4 or more consecutive workdays and had no online activity in April 2025?",
        "clickhouse_sql": "WITH absences AS (SELECT Employee_ID, First_Name, Last_Name, department, Attendance_Date, Punch_In, COALESCE(m.Online_Duration, '0:0:0') AS online_dur, row_number() OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS rn, row_number() OVER (PARTITION BY Employee_ID, CASE WHEN Punch_In IS NULL AND online_dur = '0:0:0' THEN 1 ELSE 0 END ORDER BY Attendance_Date) AS absent_rn FROM employee_metrics m WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(Attendance_Date)) BETWEEN 1 AND 5), absence_groups AS (SELECT Employee_ID, First_Name, Last_Name, department, COUNT(*) AS consecutive_absences FROM absences WHERE Punch_In IS NULL AND online_dur = '0:0:0' GROUP BY Employee_ID, First_Name, Last_Name, department, (rn - absent_rn) HAVING consecutive_absences >= 4) SELECT Employee_ID, First_Name, Last_Name, department, consecutive_absences FROM absence_groups ORDER BY consecutive_absences DESC"
    },
    {
        "question": "Which teams had employees with 5 or more consecutive absences in April 2025?",
        "clickhouse_sql": "WITH absences AS (SELECT Employee_ID, First_Name, Last_Name, Group_Name, Attendance_Date, Punch_In, row_number() OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS rn, row_number() OVER (PARTITION BY Employee_ID, CASE WHEN Punch_In IS NULL THEN 1 ELSE 0 END ORDER BY Attendance_Date) AS absent_rn FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(Attendance_Date)) BETWEEN 1 AND 5), absence_groups AS (SELECT Employee_ID, First_Name, Last_Name, Group_Name, COUNT(*) AS consecutive_absences FROM absences WHERE Punch_In IS NULL GROUP BY Employee_ID, First_Name, Last_Name, Group_Name, (rn - absent_rn) HAVING consecutive_absences >= 5) SELECT Group_Name AS team, Employee_ID, First_Name, Last_Name, consecutive_absences FROM absence_groups ORDER BY Group_Name, consecutive_absences DESC"
    },
    {
        "question": "Which employees had a productivity increase of more than 15% in the second half of April to the 2025 compared to the first half?",
        "clickhouse_sql": "WITH first_half AS (SELECT Employee_ID, AVG(COALESCE(Productive_Percent, 0)) AS first_half_avg FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-15' GROUP BY Employee_ID), second_half AS (SELECT Employee_ID, AVG(COALESCE(Productive_Percent, 0)) AS second_half_avg FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-16' AND '2025-04-30' GROUP BY Employee_ID) SELECT s.Employee_ID, m.First_Name, m.Last_Name, m.department, ROUND(first_half_avg, 2) AS first_half, ROUND(second_half_avg, 2) AS second_half FROM second_half s JOIN first_half f ON s.Employee_ID = f.Employee_ID JOIN employee_metrics m ON s.Employee_ID = m.Employee_ID WHERE toDate(m.Attendance_Date) BETWEEN '2025-04-16' AND '2025-04-30' AND (second_half_avg - first_half_avg) > 15 GROUP BY s.Employee_ID, m.First_Name, m.Last_Name, m.department, first_half_avg, second_half_avg ORDER BY (second_half_avg - first_half_avg) DESC LIMIT 10"
    },
    {
        "question": "Which departments had the lowest average time spent on collaboration tools in April 2025?",
        "clickhouse_sql": "SELECT m.department, ROUND(AVG(toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[3])) / 3600, 2) AS avg_collaboration_hours FROM employee_activity a JOIN employee_metrics m ON a.Employee_ID = m.Employee_ID AND a.Date = m.Attendance_Date WHERE toDate(a.Date) BETWEEN '2025-04-01' AND '2025-04-30' AND a.Application IN ('Trello', 'Jira', 'Asana') GROUP BY m.department ORDER BY avg_collaboration_hours ASC LIMIT 5"
    },
    {
        "question": "Find employees with the highest average number of distinct applications used per day in April 2025",
        "clickhouse_sql": "SELECT a.Employee_ID, m.First_Name, m.Last_Name, m.department, ROUND(AVG(COALESCE(a.App_Distinct_count, 0)), 2) AS avg_distinct_apps FROM employee_activity a JOIN employee_metrics m ON a.Employee_ID = m.Employee_ID AND a.Date = m.Attendance_Date WHERE toDate(a.Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY a.Employee_ID, m.First_Name, m.Last_Name, m.department ORDER BY avg_distinct_apps DESC LIMIT 10"
    },
    {
        "question": "Which teams had the highest correlation between key presses and productivity in April 2025",
        "clickhouse_sql": "SELECT m.Group_Name AS team, corr(COALESCE(a.Key_Presses, 0), COALESCE(m.Productive_Percent, 0)) AS key_press_productivity_corr FROM employee_metrics m JOIN employee_activity a ON m.Employee_ID = a.Employee_ID AND m.Attendance_Date = a.Date WHERE toDate(m.Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY m.Group_Name HAVING ABS(key_press_productivity_corr) > 0.5 ORDER BY key_press_productivity_corr DESC LIMIT 5"
    },
    {
        "question": "Which employees were continuously absent for 5 or more workdays in April 2025?",
        "clickhouse_sql": "WITH absences AS (SELECT Employee_ID, First_Name, Last_Name, department, Attendance_Date, Punch_In, row_number() OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS rn, row_number() OVER (PARTITION BY Employee_ID, CASE WHEN Punch_In IS NULL THEN 1 ELSE 0 END ORDER BY Attendance_Date) AS absent_rn FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(Attendance_Date)) BETWEEN 1 AND 5), absence_groups AS (SELECT Employee_ID, First_Name, Last_Name, department, COUNT(*) AS consecutive_absences FROM absences WHERE Punch_In IS NULL GROUP BY Employee_ID, First_Name, Last_Name, department, (rn - absent_rn) HAVING consecutive_absences >= 5) SELECT Employee_ID, First_Name, Last_Name, department, consecutive_absences FROM absence_groups ORDER BY consecutive_absences DESC"
    },
    {
        "question": "Which employees had 3 or more consecutive absences with zero productivity in April 2025?",
        "clickhouse_sql": "WITH absences AS (SELECT Employee_ID, First_Name, Last_Name, department, Attendance_Date, Punch_In, COALESCE(Productive_Percent, 0) AS prod_percent, row_number() OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS rn, row_number() OVER (PARTITION BY Employee_ID, CASE WHEN Punch_In IS NULL AND prod_percent = 0 THEN 1 ELSE 0 END ORDER BY Attendance_Date) AS absent_rn FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(Attendance_Date)) BETWEEN 1 AND 5), absence_groups AS (SELECT Employee_ID, First_Name, Last_Name, department, COUNT(*) AS consecutive_absences FROM absences WHERE Punch_In IS NULL AND prod_percent = 0 GROUP BY Employee_ID, First_Name, Last_Name, department, (rn - absent_rn) HAVING consecutive_absences >= 3) SELECT Employee_ID, First_Name, Last_Name, department, consecutive_absences FROM absence_groups ORDER BY consecutive_absences DESC"
    },
    {
        "question": "Which employees were absent for 4 or more consecutive workdays and had no online activity in April 2025?",
        "clickhouse_sql": "WITH absences AS (SELECT Employee_ID, First_Name, Last_Name, department, Attendance_Date, Punch_In, COALESCE(m.Online_Duration, '0:0:0') AS online_dur, row_number() OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS rn, row_number() OVER (PARTITION BY Employee_ID, CASE WHEN Punch_In IS NULL AND online_dur = '0:0:0' THEN 1 ELSE 0 END ORDER BY Attendance_Date) AS absent_rn FROM employee_metrics m WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(Attendance_Date)) BETWEEN 1 AND 5), absence_groups AS (SELECT Employee_ID, First_Name, Last_Name, department, COUNT(*) AS consecutive_absences FROM absences WHERE Punch_In IS NULL AND online_dur = '0:0:0' GROUP BY Employee_ID, First_Name, Last_Name, department, (rn - absent_rn) HAVING consecutive_absences >= 4) SELECT Employee_ID, First_Name, Last_Name, department, consecutive_absences FROM absence_groups ORDER BY consecutive_absences DESC"
    },
    {
        "question": "Which teams had employees with 5 or more consecutive absences in April 2025?",
        "clickhouse_sql": "WITH absences AS (SELECT Employee_ID, First_Name, Last_Name, Group_Name, Attendance_Date, Punch_In, row_number() OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS rn, row_number() OVER (PARTITION BY Employee_ID, CASE WHEN Punch_In IS NULL THEN 1 ELSE 0 END ORDER BY Attendance_Date) AS absent_rn FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(Attendance_Date)) BETWEEN 1 AND 5), absence_groups AS (SELECT Employee_ID, First_Name, Last_Name, Group_Name, COUNT(*) AS consecutive_absences FROM absences WHERE Punch_In IS NULL GROUP BY Employee_ID, First_Name, Last_Name, Group_Name, (rn - absent_rn) HAVING consecutive_absences >= 5) SELECT Group_Name AS team, Employee_ID, First_Name, Last_Name, consecutive_absences FROM absence_groups ORDER BY Group_Name, consecutive_absences DESC"
    },
    {
        "question": "Which employees had a productivity increase of more than 15% in the second half of April to the 2025 compared to the first half?",
        "clickhouse_sql": "WITH first_half AS (SELECT Employee_ID, AVG(COALESCE(Productive_Percent, 0)) AS first_half_avg FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-15' GROUP BY Employee_ID), second_half AS (SELECT Employee_ID, AVG(COALESCE(Productive_Percent, 0)) AS second_half_avg FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-16' AND '2025-04-30' GROUP BY Employee_ID) SELECT s.Employee_ID, m.First_Name, m.Last_Name, m.department, ROUND(first_half_avg, 2) AS first_half, ROUND(second_half_avg, 2) AS second_half FROM second_half s JOIN first_half f ON s.Employee_ID = f.Employee_ID JOIN employee_metrics m ON s.Employee_ID = m.Employee_ID WHERE toDate(m.Attendance_Date) BETWEEN '2025-04-16' AND '2025-04-30' AND (second_half_avg - first_half_avg) > 15 GROUP BY s.Employee_ID, m.First_Name, m.Last_Name, m.department, first_half_avg, second_half_avg ORDER BY (second_half_avg - first_half_avg) DESC LIMIT 10"
    },
    {
        "question": "Which departments had the lowest average time spent on collaboration tools in April 2025?",
        "clickhouse_sql": "SELECT m.department, ROUND(AVG(toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[3])) / 3600, 2) AS avg_collaboration_hours FROM employee_activity a JOIN employee_metrics m ON a.Employee_ID = m.Employee_ID AND a.Date = m.Attendance_Date WHERE toDate(a.Date) BETWEEN '2025-04-01' AND '2025-04-30' AND a.Application IN ('Trello', 'Jira', 'Asana') GROUP BY m.department ORDER BY avg_collaboration_hours ASC LIMIT 5"
    },
    {
        "question": "Find employees with the highest average number of distinct applications used per day in April 2025",
        "clickhouse_sql": "SELECT a.Employee_ID, m.First_Name, m.Last_Name, m.department, ROUND(AVG(COALESCE(a.App_Distinct_count, 0)), 2) AS avg_distinct_apps FROM employee_activity a JOIN employee_metrics m ON a.Employee_ID = m.Employee_ID AND a.Date = m.Attendance_Date WHERE toDate(a.Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY a.Employee_ID, m.First_Name, m.Last_Name, m.department ORDER BY avg_distinct_apps DESC LIMIT 10"
    },
    {
        "question": "Which teams had the highest correlation between key presses and productivity in April 2025",
        "clickhouse_sql": "SELECT m.Group_Name AS team, corr(COALESCE(a.Key_Presses, 0), COALESCE(m.Productive_Percent, 0)) AS key_press_productivity_corr FROM employee_metrics m JOIN employee_activity a ON m.Employee_ID = a.Employee_ID AND m.Attendance_Date = a.Date WHERE toDate(m.Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY m.Group_Name HAVING ABS(key_press_productivity_corr) > 0.5 ORDER BY key_press_productivity_corr DESC LIMIT 5"
    },
    {
        "question": "Which employees were continuously absent for 5 or more workdays in April 2025?",
        "clickhouse_sql": "WITH absences AS (SELECT Employee_ID, First_Name, Last_Name, department, Attendance_Date, Punch_In, row_number() OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS rn, row_number() OVER (PARTITION BY Employee_ID, CASE WHEN Punch_In IS NULL THEN 1 ELSE 0 END ORDER BY Attendance_Date) AS absent_rn FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(Attendance_Date)) BETWEEN 1 AND 5), absence_groups AS (SELECT Employee_ID, First_Name, Last_Name, department, COUNT(*) AS consecutive_absences FROM absences WHERE Punch_In IS NULL GROUP BY Employee_ID, First_Name, Last_Name, department, (rn - absent_rn) HAVING consecutive_absences >= 5) SELECT Employee_ID, First_Name, Last_Name, department, consecutive_absences FROM absence_groups ORDER BY consecutive_absences DESC"
    },
    {
        "question": "Which employees had 3 or more consecutive absences with zero productivity in April 2025?",
        "clickhouse_sql": "WITH absences AS (SELECT Employee_ID, First_Name, Last_Name, department, Attendance_Date, Punch_In, COALESCE(Productive_Percent, 0) AS prod_percent, row_number() OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS rn, row_number() OVER (PARTITION BY Employee_ID, CASE WHEN Punch_In IS NULL AND prod_percent = 0 THEN 1 ELSE 0 END ORDER BY Attendance_Date) AS absent_rn FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(Attendance_Date)) BETWEEN 1 AND 5), absence_groups AS (SELECT Employee_ID, First_Name, Last_Name, department, COUNT(*) AS consecutive_absences FROM absences WHERE Punch_In IS NULL AND prod_percent = 0 GROUP BY Employee_ID, First_Name, Last_Name, department, (rn - absent_rn) HAVING consecutive_absences >= 3) SELECT Employee_ID, First_Name, Last_Name, department, consecutive_absences FROM absence_groups ORDER BY consecutive_absences DESC"
    },
    {
        "question": "Which employees were absent for 4 or more consecutive workdays and had no online activity in April 2025?",
        "clickhouse_sql": "WITH absences AS (SELECT Employee_ID, First_Name, Last_Name, department, Attendance_Date, Punch_In, COALESCE(m.Online_Duration, '0:0:0') AS online_dur, row_number() OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS rn, row_number() OVER (PARTITION BY Employee_ID, CASE WHEN Punch_In IS NULL AND online_dur = '0:0:0' THEN 1 ELSE 0 END ORDER BY Attendance_Date) AS absent_rn FROM employee_metrics m WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(Attendance_Date)) BETWEEN 1 AND 5), absence_groups AS (SELECT Employee_ID, First_Name, Last_Name, department, COUNT(*) AS consecutive_absences FROM absences WHERE Punch_In IS NULL AND online_dur = '0:0:0' GROUP BY Employee_ID, First_Name, Last_Name, department, (rn - absent_rn) HAVING consecutive_absences >= 4) SELECT Employee_ID, First_Name, Last_Name, department, consecutive_absences FROM absence_groups ORDER BY consecutive_absences DESC"
    },
    {
        "question": "Which teams had employees with 5 or more consecutive absences in April 2025?",
        "clickhouse_sql": "WITH absences AS (SELECT Employee_ID, First_Name, Last_Name, Group_Name, Attendance_Date, Punch_In, row_number() OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS rn, row_number() OVER (PARTITION BY Employee_ID, CASE WHEN Punch_In IS NULL THEN 1 ELSE 0 END ORDER BY Attendance_Date) AS absent_rn FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(Attendance_Date)) BETWEEN 1 AND 5), absence_groups AS (SELECT Employee_ID, First_Name, Last_Name, Group_Name, COUNT(*) AS consecutive_absences FROM absences WHERE Punch_In IS NULL GROUP BY Employee_ID, First_Name, Last_Name, Group_Name, (rn - absent_rn) HAVING consecutive_absences >= 5) SELECT Group_Name AS team, Employee_ID, First_Name, Last_Name, consecutive_absences FROM absence_groups ORDER BY Group_Name, consecutive_absences DESC"
    },
    {
        "question": "Which employees had a productivity increase of more than 15% in the second half of April to the 2025 compared to the first half?",
        "clickhouse_sql": "WITH first_half AS (SELECT Employee_ID, AVG(COALESCE(Productive_Percent, 0)) AS first_half_avg FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-15' GROUP BY Employee_ID), second_half AS (SELECT Employee_ID, AVG(COALESCE(Productive_Percent, 0)) AS second_half_avg FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-16' AND '2025-04-30' GROUP BY Employee_ID) SELECT s.Employee_ID, m.First_Name, m.Last_Name, m.department, ROUND(first_half_avg, 2) AS first_half, ROUND(second_half_avg, 2) AS second_half FROM second_half s JOIN first_half f ON s.Employee_ID = f.Employee_ID JOIN employee_metrics m ON s.Employee_ID = m.Employee_ID WHERE toDate(m.Attendance_Date) BETWEEN '2025-04-16' AND '2025-04-30' AND (second_half_avg - first_half_avg) > 15 GROUP BY s.Employee_ID, m.First_Name, m.Last_Name, m.department, first_half_avg, second_half_avg ORDER BY (second_half_avg - first_half_avg) DESC LIMIT 10"
    },
    {
        "question": "Which departments had the lowest average time spent on collaboration tools in April 2025?",
        "clickhouse_sql": "SELECT m.department, ROUND(AVG(toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[3])) / 3600, 2) AS avg_collaboration_hours FROM employee_activity a JOIN employee_metrics m ON a.Employee_ID = m.Employee_ID AND a.Date = m.Attendance_Date WHERE toDate(a.Date) BETWEEN '2025-04-01' AND '2025-04-30' AND a.Application IN ('Trello', 'Jira', 'Asana') GROUP BY m.department ORDER BY avg_collaboration_hours ASC LIMIT 5"
    },
    {
        "question": "Find employees with the highest average number of distinct applications used per day in April 2025",
        "clickhouse_sql": "SELECT a.Employee_ID, m.First_Name, m.Last_Name, m.department, ROUND(AVG(COALESCE(a.App_Distinct_count, 0)), 2) AS avg_distinct_apps FROM employee_activity a JOIN employee_metrics m ON a.Employee_ID = m.Employee_ID AND a.Date = m.Attendance_Date WHERE toDate(a.Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY a.Employee_ID, m.First_Name, m.Last_Name, m.department ORDER BY avg_distinct_apps DESC LIMIT 10"
    },
    {
        "question": "Which teams had the highest correlation between key presses and productivity in April 2025",
        "clickhouse_sql": "SELECT m.Group_Name AS team, corr(COALESCE(a.Key_Presses, 0), COALESCE(m.Productive_Percent, 0)) AS key_press_productivity_corr FROM employee_metrics m JOIN employee_activity a ON m.Employee_ID = a.Employee_ID AND m.Attendance_Date = a.Date WHERE toDate(m.Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY m.Group_Name HAVING ABS(key_press_productivity_corr) > 0.5 ORDER BY key_press_productivity_corr DESC LIMIT 5"
    },
    {
        "question": "Which employees were continuously absent for 5 or more workdays in April 2025?",
        "clickhouse_sql": "WITH absences AS (SELECT Employee_ID, First_Name, Last_Name, department, Attendance_Date, Punch_In, row_number() OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS rn, row_number() OVER (PARTITION BY Employee_ID, CASE WHEN Punch_In IS NULL THEN 1 ELSE 0 END ORDER BY Attendance_Date) AS absent_rn FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(Attendance_Date)) BETWEEN 1 AND 5), absence_groups AS (SELECT Employee_ID, First_Name, Last_Name, department, COUNT(*) AS consecutive_absences FROM absences WHERE Punch_In IS NULL GROUP BY Employee_ID, First_Name, Last_Name, department, (rn - absent_rn) HAVING consecutive_absences >= 5) SELECT Employee_ID, First_Name, Last_Name, department, consecutive_absences FROM absence_groups ORDER BY consecutive_absences DESC"
    },
    {
        "question": "Which employees had 3 or more consecutive absences with zero productivity in April 2025?",
        "clickhouse_sql": "WITH absences AS (SELECT Employee_ID, First_Name, Last_Name, department, Attendance_Date, Punch_In, COALESCE(Productive_Percent, 0) AS prod_percent, row_number() OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS rn, row_number() OVER (PARTITION BY Employee_ID, CASE WHEN Punch_In IS NULL AND prod_percent = 0 THEN 1 ELSE 0 END ORDER BY Attendance_Date) AS absent_rn FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(Attendance_Date)) BETWEEN 1 AND 5), absence_groups AS (SELECT Employee_ID, First_Name, Last_Name, department, COUNT(*) AS consecutive_absences FROM absences WHERE Punch_In IS NULL AND prod_percent = 0 GROUP BY Employee_ID, First_Name, Last_Name, department, (rn - absent_rn) HAVING consecutive_absences >= 3) SELECT Employee_ID, First_Name, Last_Name, department, consecutive_absences FROM absence_groups ORDER BY consecutive_absences DESC"
    },
    {
        "question": "Which employees were absent for 4 or more consecutive workdays and had no online activity in April 2025?",
        "clickhouse_sql": "WITH absences AS (SELECT Employee_ID, First_Name, Last_Name, department, Attendance_Date, Punch_In, COALESCE(m.Online_Duration, '0:0:0') AS online_dur, row_number() OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS rn, row_number() OVER (PARTITION BY Employee_ID, CASE WHEN Punch_In IS NULL AND online_dur = '0:0:0' THEN 1 ELSE 0 END ORDER BY Attendance_Date) AS absent_rn FROM employee_metrics m WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(Attendance_Date)) BETWEEN 1 AND 5), absence_groups AS (SELECT Employee_ID, First_Name, Last_Name, department, COUNT(*) AS consecutive_absences FROM absences WHERE Punch_In IS NULL AND online_dur = '0:0:0' GROUP BY Employee_ID, First_Name, Last_Name, department, (rn - absent_rn) HAVING consecutive_absences >= 4) SELECT Employee_ID, First_Name, Last_Name, department, consecutive_absences FROM absence_groups ORDER BY consecutive_absences DESC"
    },
    {
        "question": "Which teams had employees with 5 or more consecutive absences in April 2025?",
        "clickhouse_sql": "WITH absences AS (SELECT Employee_ID, First_Name, Last_Name, Group_Name, Attendance_Date, Punch_In, row_number() OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS rn, row_number() OVER (PARTITION BY Employee_ID, CASE WHEN Punch_In IS NULL THEN 1 ELSE 0 END ORDER BY Attendance_Date) AS absent_rn FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(Attendance_Date)) BETWEEN 1 AND 5), absence_groups AS (SELECT Employee_ID, First_Name, Last_Name, Group_Name, COUNT(*) AS consecutive_absences FROM absences WHERE Punch_In IS NULL GROUP BY Employee_ID, First_Name, Last_Name, Group_Name, (rn - absent_rn) HAVING consecutive_absences >= 5) SELECT Group_Name AS team, Employee_ID, First_Name, Last_Name, consecutive_absences FROM absence_groups ORDER BY Group_Name, consecutive_absences DESC"
    },
    {
        "question": "Which employees had a productivity increase of more than 15% in the second half of April to the 2025 compared to the first half?",
        "clickhouse_sql": "WITH first_half AS (SELECT Employee_ID, AVG(COALESCE(Productive_Percent, 0)) AS first_half_avg FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-15' GROUP BY Employee_ID), second_half AS (SELECT Employee_ID, AVG(COALESCE(Productive_Percent, 0)) AS second_half_avg FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-16' AND '2025-04-30' GROUP BY Employee_ID) SELECT s.Employee_ID, m.First_Name, m.Last_Name, m.department, ROUND(first_half_avg, 2) AS first_half, ROUND(second_half_avg, 2) AS second_half FROM second_half s JOIN first_half f ON s.Employee_ID = f.Employee_ID JOIN employee_metrics m ON s.Employee_ID = m.Employee_ID WHERE toDate(m.Attendance_Date) BETWEEN '2025-04-16' AND '2025-04-30' AND (second_half_avg - first_half_avg) > 15 GROUP BY s.Employee_ID, m.First_Name, m.Last_Name, m.department, first_half_avg, second_half_avg ORDER BY (second_half_avg - first_half_avg) DESC LIMIT 10"
    },
    {
        "question": "Which departments had the lowest average time spent on collaboration tools in April 2025?",
        "clickhouse_sql": "SELECT m.department, ROUND(AVG(toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[3])) / 3600, 2) AS avg_collaboration_hours FROM employee_activity a JOIN employee_metrics m ON a.Employee_ID = m.Employee_ID AND a.Date = m.Attendance_Date WHERE toDate(a.Date) BETWEEN '2025-04-01' AND '2025-04-30' AND a.Application IN ('Trello', 'Jira', 'Asana') GROUP BY m.department ORDER BY avg_collaboration_hours ASC LIMIT 5"
    },
    {
        "question": "Find employees with the highest average number of distinct applications used per day in April 2025",
        "clickhouse_sql": "SELECT a.Employee_ID, m.First_Name, m.Last_Name, m.department, ROUND(AVG(COALESCE(a.App_Distinct_count, 0)), 2) AS avg_distinct_apps FROM employee_activity a JOIN employee_metrics m ON a.Employee_ID = m.Employee_ID AND a.Date = m.Attendance_Date WHERE toDate(a.Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY a.Employee_ID, m.First_Name, m.Last_Name, m.department ORDER BY avg_distinct_apps DESC LIMIT 10"
    },
    {
        "question": "Which teams had the highest correlation between key presses and productivity in April 2025",
        "clickhouse_sql": "SELECT m.Group_Name AS team, corr(COALESCE(a.Key_Presses, 0), COALESCE(m.Productive_Percent, 0)) AS key_press_productivity_corr FROM employee_metrics m JOIN employee_activity a ON m.Employee_ID = a.Employee_ID AND m.Attendance_Date = a.Date WHERE toDate(m.Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY m.Group_Name HAVING ABS(key_press_productivity_corr) > 0.5 ORDER BY key_press_productivity_corr DESC LIMIT 5"
    },
    {
        "question": "Which employees were continuously absent for 5 or more workdays in April 2025?",
        "clickhouse_sql": "WITH absences AS (SELECT Employee_ID, First_Name, Last_Name, department, Attendance_Date, Punch_In, row_number() OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS rn, row_number() OVER (PARTITION BY Employee_ID, CASE WHEN Punch_In IS NULL THEN 1 ELSE 0 END ORDER BY Attendance_Date) AS absent_rn FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(Attendance_Date)) BETWEEN 1 AND 5), absence_groups AS (SELECT Employee_ID, First_Name, Last_Name, department, COUNT(*) AS consecutive_absences FROM absences WHERE Punch_In IS NULL GROUP BY Employee_ID, First_Name, Last_Name, department, (rn - absent_rn) HAVING consecutive_absences >= 5) SELECT Employee_ID, First_Name, Last_Name, department, consecutive_absences FROM absence_groups ORDER BY consecutive_absences DESC"
    },
    {
        "question": "Which employees had 3 or more consecutive absences with zero productivity in April 2025?",
        "clickhouse_sql": "WITH absences AS (SELECT Employee_ID, First_Name, Last_Name, department, Attendance_Date, Punch_In, COALESCE(Productive_Percent, 0) AS prod_percent, row_number() OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS rn, row_number() OVER (PARTITION BY Employee_ID, CASE WHEN Punch_In IS NULL AND prod_percent = 0 THEN 1 ELSE 0 END ORDER BY Attendance_Date) AS absent_rn FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(Attendance_Date)) BETWEEN 1 AND 5), absence_groups AS (SELECT Employee_ID, First_Name, Last_Name, department, COUNT(*) AS consecutive_absences FROM absences WHERE Punch_In IS NULL AND prod_percent = 0 GROUP BY Employee_ID, First_Name, Last_Name, department, (rn - absent_rn) HAVING consecutive_absences >= 3) SELECT Employee_ID, First_Name, Last_Name, department, consecutive_absences FROM absence_groups ORDER BY consecutive_absences DESC"
    },
    {
        "question": "Which employees were absent for 4 or more consecutive workdays and had no online activity in April 2025?",
        "clickhouse_sql": "WITH absences AS (SELECT Employee_ID, First_Name, Last_Name, department, Attendance_Date, Punch_In, COALESCE(m.Online_Duration, '0:0:0') AS online_dur, row_number() OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS rn, row_number() OVER (PARTITION BY Employee_ID, CASE WHEN Punch_In IS NULL AND online_dur = '0:0:0' THEN 1 ELSE 0 END ORDER BY Attendance_Date) AS absent_rn FROM employee_metrics m WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(Attendance_Date)) BETWEEN 1 AND 5), absence_groups AS (SELECT Employee_ID, First_Name, Last_Name, department, COUNT(*) AS consecutive_absences FROM absences WHERE Punch_In IS NULL AND online_dur = '0:0:0' GROUP BY Employee_ID, First_Name, Last_Name, department, (rn - absent_rn) HAVING consecutive_absences >= 4) SELECT Employee_ID, First_Name, Last_Name, department, consecutive_absences FROM absence_groups ORDER BY consecutive_absences DESC"
    },
    {
        "question": "Which teams had employees with 5 or more consecutive absences in April 2025?",
        "clickhouse_sql": "WITH absences AS (SELECT Employee_ID, First_Name, Last_Name, Group_Name, Attendance_Date, Punch_In, row_number() OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS rn, row_number() OVER (PARTITION BY Employee_ID, CASE WHEN Punch_In IS NULL THEN 1 ELSE 0 END ORDER BY Attendance_Date) AS absent_rn FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(Attendance_Date)) BETWEEN 1 AND 5), absence_groups AS (SELECT Employee_ID, First_Name, Last_Name, Group_Name, COUNT(*) AS consecutive_absences FROM absences WHERE Punch_In IS NULL GROUP BY Employee_ID, First_Name, Last_Name, Group_Name, (rn - absent_rn) HAVING consecutive_absences >= 5) SELECT Group_Name AS team, Employee_ID, First_Name, Last_Name, consecutive_absences FROM absence_groups ORDER BY Group_Name, consecutive_absences DESC"
    },
    {
        "question": "Which employees had a productivity increase of more than 15% in the second half of April to the 2025 compared to the first half?",
        "clickhouse_sql": "WITH first_half AS (SELECT Employee_ID, AVG(COALESCE(Productive_Percent, 0)) AS first_half_avg FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-15' GROUP BY Employee_ID), second_half AS (SELECT Employee_ID, AVG(COALESCE(Productive_Percent, 0)) AS second_half_avg FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-16' AND '2025-04-30' GROUP BY Employee_ID) SELECT s.Employee_ID, m.First_Name, m.Last_Name, m.department, ROUND(first_half_avg, 2) AS first_half, ROUND(second_half_avg, 2) AS second_half FROM second_half s JOIN first_half f ON s.Employee_ID = f.Employee_ID JOIN employee_metrics m ON s.Employee_ID = m.Employee_ID WHERE toDate(m.Attendance_Date) BETWEEN '2025-04-16' AND '2025-04-30' AND (second_half_avg - first_half_avg) > 15 GROUP BY s.Employee_ID, m.First_Name, m.Last_Name, m.department, first_half_avg, second_half_avg ORDER BY (second_half_avg - first_half_avg) DESC LIMIT 10"
    },
    {
        "question": "Which departments had the lowest average time spent on collaboration tools in April 2025?",
        "clickhouse_sql": "SELECT m.department, ROUND(AVG(toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[3])) / 3600, 2) AS avg_collaboration_hours FROM employee_activity a JOIN employee_metrics m ON a.Employee_ID = m.Employee_ID AND a.Date = m.Attendance_Date WHERE toDate(a.Date) BETWEEN '2025-04-01' AND '2025-04-30' AND a.Application IN ('Trello', 'Jira', 'Asana') GROUP BY m.department ORDER BY avg_collaboration_hours ASC LIMIT 5"
    },
    {
        "question": "Find employees with the highest average number of distinct applications used per day in April 2025",
        "clickhouse_sql": "SELECT a.Employee_ID, m.First_Name, m.Last_Name, m.department, ROUND(AVG(COALESCE(a.App_Distinct_count, 0)), 2) AS avg_distinct_apps FROM employee_activity a JOIN employee_metrics m ON a.Employee_ID = m.Employee_ID AND a.Date = m.Attendance_Date WHERE toDate(a.Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY a.Employee_ID, m.First_Name, m.Last_Name, m.department ORDER BY avg_distinct_apps DESC LIMIT 10"
    },
    {
        "question": "Which teams had the highest correlation between key presses and productivity in April 2025",
        "clickhouse_sql": "SELECT m.Group_Name AS team, corr(COALESCE(a.Key_Presses, 0), COALESCE(m.Productive_Percent, 0)) AS key_press_productivity_corr FROM employee_metrics m JOIN employee_activity a ON m.Employee_ID = a.Employee_ID AND m.Attendance_Date = a.Date WHERE toDate(m.Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY m.Group_Name HAVING ABS(key_press_productivity_corr) > 0.5 ORDER BY key_press_productivity_corr DESC LIMIT 5"
    },
    {
        "question": "Which employees were continuously absent for 5 or more workdays in April 2025?",
        "clickhouse_sql": "WITH absences AS (SELECT Employee_ID, First_Name, Last_Name, department, Attendance_Date, Punch_In, row_number() OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS rn, row_number() OVER (PARTITION BY Employee_ID, CASE WHEN Punch_In IS NULL THEN 1 ELSE 0 END ORDER BY Attendance_Date) AS absent_rn FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(Attendance_Date)) BETWEEN 1 AND 5), absence_groups AS (SELECT Employee_ID, First_Name, Last_Name, department, COUNT(*) AS consecutive_absences FROM absences WHERE Punch_In IS NULL GROUP BY Employee_ID, First_Name, Last_Name, department, (rn - absent_rn) HAVING consecutive_absences >= 5) SELECT Employee_ID, First_Name, Last_Name, department, consecutive_absences FROM absence_groups ORDER BY consecutive_absences DESC"
    },
    {
        "question": "Which employees had 3 or more consecutive absences with zero productivity in April 2025?",
        "clickhouse_sql": "WITH absences AS (SELECT Employee_ID, First_Name, Last_Name, department, Attendance_Date, Punch_In, COALESCE(Productive_Percent, 0) AS prod_percent, row_number() OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS rn, row_number() OVER (PARTITION BY Employee_ID, CASE WHEN Punch_In IS NULL AND prod_percent = 0 THEN 1 ELSE 0 END ORDER BY Attendance_Date) AS absent_rn FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(Attendance_Date)) BETWEEN 1 AND 5), absence_groups AS (SELECT Employee_ID, First_Name, Last_Name, department, COUNT(*) AS consecutive_absences FROM absences WHERE Punch_In IS NULL AND prod_percent = 0 GROUP BY Employee_ID, First_Name, Last_Name, department, (rn - absent_rn) HAVING consecutive_absences >= 3) SELECT Employee_ID, First_Name, Last_Name, department, consecutive_absences FROM absence_groups ORDER BY consecutive_absences DESC"
    },
    {
        "question": "Which employees were absent for 4 or more consecutive workdays and had no online activity in April 2025?",
        "clickhouse_sql": "WITH absences AS (SELECT Employee_ID, First_Name, Last_Name, department, Attendance_Date, Punch_In, COALESCE(m.Online_Duration, '0:0:0') AS online_dur, row_number() OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS rn, row_number() OVER (PARTITION BY Employee_ID, CASE WHEN Punch_In IS NULL AND online_dur = '0:0:0' THEN 1 ELSE 0 END ORDER BY Attendance_Date) AS absent_rn FROM employee_metrics m WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(Attendance_Date)) BETWEEN 1 AND 5), absence_groups AS (SELECT Employee_ID, First_Name, Last_Name, department, COUNT(*) AS consecutive_absences FROM absences WHERE Punch_In IS NULL AND online_dur = '0:0:0' GROUP BY Employee_ID, First_Name, Last_Name, department, (rn - absent_rn) HAVING consecutive_absences >= 4) SELECT Employee_ID, First_Name, Last_Name, department, consecutive_absences FROM absence_groups ORDER BY consecutive_absences DESC"
    },
    {
      "question": "Get the longest continuous streak of 100% productivity per employee",
      "clickhouse_sql": "SELECT Employee_ID, max(streak) AS max_streak FROM (SELECT Employee_ID, count() AS streak FROM (SELECT Employee_ID, Attendance_Date, Productive_Percent, rowNumberInAllBlocks() - rowNumberInAllBlocks() OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS grp FROM employee_metrics WHERE Productive_Percent = 100) GROUP BY Employee_ID, grp) GROUP BY Employee_ID"
    },
    {
      "question": "Which employees show a continuous decline in productivity over the last 3 weeks?",
      "clickhouse_sql": "SELECT Employee_ID FROM (SELECT Employee_ID, toMonday(Attendance_Date) AS week, avg(Productive_Percent) AS avg_prod FROM employee_metrics WHERE Attendance_Date >= subtractWeeks(today(), 3) GROUP BY Employee_ID, week ORDER BY Employee_ID, week) WHERE avg_prod < lag(avg_prod, 1) OVER (PARTITION BY Employee_ID ORDER BY week) AND avg_prod < lag(avg_prod, 2) OVER (PARTITION BY Employee_ID ORDER BY week)"
    },
    {
      "question": "Find average break time by department and shift for each week",
      "clickhouse_sql": "SELECT toMonday(Attendance_Date) AS week, department, Shift_Name, avg(toIntervalSecond(Break_Duration)) AS avg_break FROM employee_metrics WHERE Attendance_Date >= toDate('2025-04-01') GROUP BY week, department, Shift_Name ORDER BY week, avg_break DESC"
    },
    {
      "question": "Which applications are associated with low productivity employees?",
      "clickhouse_sql": "SELECT Application, count(DISTINCT ea.Employee_ID) AS low_perf_users FROM employee_activity ea JOIN employee_metrics em ON ea.Employee_ID = em.Employee_ID AND ea.Date = em.Attendance_Date WHERE em.Productive_Percent < 40 AND ea.Date BETWEEN toDate('2025-04-01') AND toDate('2025-04-30') GROUP BY Application ORDER BY low_perf_users DESC"
    },
    {
      "question": "Detect anomalies in productivity using standard deviation",
      "clickhouse_sql": "SELECT Employee_ID, Attendance_Date, Productive_Percent FROM (SELECT Employee_ID, Attendance_Date, Productive_Percent, avg(Productive_Percent) OVER (PARTITION BY Employee_ID) AS avg_prod, stddevPop(Productive_Percent) OVER (PARTITION BY Employee_ID) AS std_dev FROM employee_metrics WHERE Attendance_Date >= toDate('2025-04-01')) WHERE abs(Productive_Percent - avg_prod) > 2 * std_dev ORDER BY Employee_ID, Attendance_Date"
    },
    {
      "question": "Identify productivity improvement from last month to this month",
      "clickhouse_sql": "SELECT Employee_ID, (curr_month - last_month) AS improvement FROM (SELECT Employee_ID, avgIf(Productive_Percent, toMonth(Attendance_Date) = toStartOfMonth(now())) AS curr_month, avgIf(Productive_Percent, toMonth(Attendance_Date) = toStartOfMonth(now() - INTERVAL 1 MONTH)) AS last_month FROM employee_metrics GROUP BY Employee_ID) WHERE improvement > 0 ORDER BY improvement DESC"
    },
    {
        "question": "fetch me teams with productivity below 40 and their avg working hours(online duration)",
        "clickhouse_sql": "SELECT Group_Name AS team, ROUND(AVG(COALESCE(Productive_Percent, 0)), 2) AS avg_productivity, ROUND(AVG(toUInt32(splitByChar(':', COALESCE(Online_Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(Online_Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(Online_Duration, '0:0:0'))[3])) / 3600, 2) AS avg_working_hours FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY Group_Name HAVING avg_productivity < 40 ORDER BY avg_productivity ASC"
    },
    {
        "question": "Which teams have an average active duration below 5 hours and productivity above 60% in April 2025?",
        "clickhouse_sql": "SELECT Group_Name AS team, ROUND(AVG(COALESCE(Productive_Percent, 0)), 2) AS avg_productivity, ROUND(AVG(toUInt32(splitByChar(':', COALESCE(Active_Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(Active_Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(Active_Duration, '0:0:0'))[3])) / 3600, 2) AS avg_active_hours FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY Group_Name HAVING avg_active_hours < 5 AND avg_productivity > 60 ORDER BY avg_productivity DESC"
    },
    {
        "question": "Show employees with high keyboard activity but low productivity in the last two weeks of April 2025",
        "clickhouse_sql": "WITH keyboard_activity AS (SELECT Employee_ID, AVG(Key_Presses) AS avg_key_presses FROM employee_activity WHERE toDate(Date) BETWEEN '2025-04-16' AND '2025-04-30' GROUP BY Employee_ID) SELECT e.Employee_ID, e.First_Name, e.Last_Name, e.department, ROUND(AVG(COALESCE(e.Productive_Percent, 0)), 2) AS avg_productivity, k.avg_key_presses FROM employee_metrics e JOIN keyboard_activity k ON e.Employee_ID = k.Employee_ID WHERE toDate(e.Attendance_Date) BETWEEN '2025-04-16' AND '2025-04-30' GROUP BY e.Employee_ID, e.First_Name, e.Last_Name, e.department, k.avg_key_presses HAVING avg_productivity < 40 AND k.avg_key_presses > (SELECT AVG(Key_Presses) * 1.5 FROM employee_activity WHERE toDate(Date) BETWEEN '2025-04-16' AND '2025-04-30') ORDER BY k.avg_key_presses DESC LIMIT 10"
    },
    {
        "question": "Which departments had the highest average break duration in April 2025, and what is their average productivity?",
        "clickhouse_sql": "SELECT department, ROUND(AVG(toUInt32(splitByChar(':', COALESCE(Break_Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(Break_Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(Break_Duration, '0:0:0'))[3])) / 3600, 2) AS avg_break_hours, ROUND(AVG(COALESCE(Productive_Percent, 0)), 2) AS avg_productivity FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY department ORDER BY avg_break_hours DESC LIMIT 5"
    },
    {
        "question": "Identify teams with more than 10% of days having zero productivity in April 2025",
        "clickhouse_sql": "SELECT Group_Name AS team, COUNTIf(COALESCE(Productive_Percent, 0) = 0) AS zero_prod_days, COUNT(*) AS total_days, ROUND((COUNTIf(COALESCE(Productive_Percent, 0) = 0) / COUNT(*) * 100), 2) AS zero_prod_percentage FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY Group_Name HAVING zero_prod_percentage > 10 ORDER BY zero_prod_percentage DESC"
    },
    {
        "question": "Which employees have an average online duration exceeding their shift duration by more than 1 hour in April 2025?",
        "clickhouse_sql": "SELECT e.Employee_ID, e.First_Name, e.Last_Name, e.department, ROUND(AVG(toUInt32(splitByChar(':', COALESCE(Online_Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(Online_Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(Online_Duration, '0:0:0'))[3])) / 3600, 2) AS avg_online_hours, ROUND(AVG(toUInt32(splitByChar(':', COALESCE(Punch_Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(Punch_Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(Punch_Duration, '0:0:0'))[3])) / 3600, 2) AS avg_shift_hours FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY e.Employee_ID, e.First_Name, e.Last_Name, e.department HAVING avg_online_hours > (avg_shift_hours + 1) ORDER BY (avg_online_hours - avg_shift_hours) DESC LIMIT 10"
    },
    {
        "question": "Show the top 5 applications used by teams with productivity below 40% in April 2025",
        "clickhouse_sql": "WITH low_prod_teams AS (SELECT Group_Name FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY Group_Name HAVING AVG(COALESCE(Productive_Percent, 0)) < 40) SELECT a.Application, COUNT(*) AS usage_count, ROUND(SUM(toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[3])) / 3600, 2) AS total_hours FROM employee_activity a JOIN employee_metrics m ON a.Employee_ID = m.Employee_ID AND a.Date = m.Attendance_Date WHERE a.Date BETWEEN '2025-04-01' AND '2025-04-30' AND m.Group_Name IN (SELECT Group_Name FROM low_prod_teams) GROUP BY a.Application ORDER BY total_hours DESC LIMIT 5"
    },
    {
        "question": "Which employees have a high correlation between break duration and productivity in April 2025?",
        "clickhouse_sql": "SELECT m.Employee_ID, m.First_Name, m.Last_Name, m.department, corr(toUInt32(splitByChar(':', COALESCE(m.Break_Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(m.Break_Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(m.Break_Duration, '0:0:0'))[3]), COALESCE(m.Productive_Percent, 0)) AS break_productivity_corr FROM employee_metrics m WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY m.Employee_ID, m.First_Name, m.Last_Name, m.department HAVING ABS(break_productivity_corr) > 0.5 ORDER BY break_productivity_corr DESC LIMIT 10"
    },
    {
        "question": "Find departments with the highest ratio of unproductive to productive time in April 2025",
        "clickhouse_sql": "SELECT department, ROUND(AVG(toUInt32(splitByChar(':', COALESCE(Unproductive_Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(Unproductive_Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(Unproductive_Duration, '0:0:0'))[3])) / NULLIF(AVG(toUInt32(splitByChar(':', COALESCE(Productive_Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(Productive_Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(Productive_Duration, '0:0:0'))[3])), 0), 2) AS unproductive_to_productive_ratio FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY department ORDER BY unproductive_to_productive_ratio DESC LIMIT 5"
    },
    {
      "question": "Show teams with the highest variance in daily online hours in April 2025",
      "clickhouse_sql": "SELECT Group_Name AS team, ROUND(STDDEV_POP(toUInt32(splitByChar(':', COALESCE(Online_Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(Online_Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(Online_Duration, '0:0:0'))[3])) / 3600, 2) AS online_hours_variance FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY Group_Name ORDER BY online_hours_variance DESC LIMIT 5"
    },
    {
      "question": "Identify employees with high productivity but frequent late arrivals in April 2025",
      "clickhouse_sql": "SELECT m.Employee_ID, m.First_Name, m.Last_Name, m.department, ROUND(AVG(COALESCE(m.Productive_Percent, 0)), 2) AS avg_productivity, COUNTIf(toHour(m.Punch_In) > toHour(m.Shift_Start)) AS late_arrivals FROM employee_metrics m WHERE toDate(m.Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND m.Punch_In IS NOT NULL AND m.Shift_Start IS NOT NULL GROUP BY m.Employee_ID, m.First_Name, m.Last_Name, m.department HAVING avg_productivity > 75 AND late_arrivals > 5 ORDER BY late_arrivals DESC, avg_productivity DESC LIMIT 10"
    },
    {
      "question": "Which employees had more than 5 days with productivity below 20% in April 2025?",
      "clickhouse_sql": "SELECT Employee_ID, First_Name, Last_Name, department, COUNTIf(COALESCE(Productive_Percent, 0) < 20) AS low_prod_days FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY Employee_ID, First_Name, Last_Name, department HAVING low_prod_days > 5 ORDER BY low_prod_days DESC"
    },
    {
      "question": "Show teams with the highest average mouse clicks per employee in April 2025",
      "clickhouse_sql": "SELECT e.department, AVG(u.Mouse_Clicks) AS avg_mouse_clicks FROM employee_usage_data u JOIN employee_duration_summary e ON u.Employee_ID = toString(e.employee_id) GROUP BY e.department ORDER BY avg_mouse_clicks DESC LIMIT 5"
    },
    {
      "question": "Which employees have improved their attendance rate over the last two months?",
      "clickhouse_sql": "WITH monthly_attendance AS (SELECT Employee, toYYYYMM(attendance_date) AS month, AVG(attendance_rate) AS monthly_attendance FROM employee_attendance_summary WHERE toDate(attendance_date) >= date_sub(DAY, 60, today()) GROUP BY Employee, month) SELECT e.Employee, e.first_name, e.last_name, e.department, m1.monthly_attendance - m2.monthly_attendance AS attendance_improvement FROM employee_duration_summary e JOIN monthly_attendance m1 ON e.Employee = m1.Employee JOIN monthly_attendance m2 ON e.Employee = m2.Employee AND m1.month > m2.month WHERE m1.monthly_attendance > m2.monthly_attendance ORDER BY attendance_improvement DESC LIMIT 10"
    },
    {
      "question": "Which applications are used most during peak productivity hours?",
      "clickhouse_sql": "WITH peak_hours AS (SELECT toHour(punch_in) AS hour_of_day, AVG(productive_percent) AS avg_productivity FROM attendance_data WHERE punch_in IS NOT NULL GROUP BY hour_of_day ORDER BY avg_productivity DESC LIMIT 2), app_usage AS (SELECT u.Application, COUNT(*) AS usage_count, SUM(toUInt32(splitByChar(':', Duration)[1]) * 3600 + toUInt32(splitByChar(':', Duration)[2]) * 60 + toUInt32(splitByChar(':', Duration)[3])) / 3600 AS total_hours FROM employee_usage_data u JOIN attendance_data a ON u.Employee = toString(a.Employee) AND toDate(u.Date) = toDate(a.attendance_date) WHERE toHour(a.punch_in) IN (SELECT hour_of_day FROM peak_hours) GROUP BY u.Application) SELECT Application, usage_count, total_hours FROM app_usage ORDER BY total_hours DESC LIMIT 10"
    },
    {
      "question": "Find employees who frequently switch between applications",
      "clickhouse_sql": "SELECT u.Employee, e.first_name, e.last_name, e.department, COUNT(DISTINCT u.Application) AS app_count FROM employee_usage_data u JOIN employee_duration_summary e ON u.Employee_ID = toString(e.employee_id) WHERE toDate(u.Date) >= date_sub(DAY, 30, today()) GROUP BY u.Employee, e.first_name, e.last_name, e.department HAVING app_count > 20 ORDER BY app_count DESC LIMIT 10"
    },
    {
      "question": "Which departments have the highest variance in daily attendance?",
      "clickhouse_sql": "SELECT department, stddevPop(employees_present / total_employees) * 100 AS attendance_variance FROM daily_attendance_summary WHERE toDate(attendance_date) >= date_sub(DAY, 30, today()) GROUP BY department ORDER BY attendance_variance DESC LIMIT 5"
    },
    {
      "question": "Show employees with high productivity on Mondays",
      "clickhouse_sql": "SELECT a.Employee, e.first_name, e.last_name, e.department, AVG(a.productive_percent) AS monday_productivity FROM attendance_data a JOIN employee_duration_summary e ON a.Employee = e.employee_id WHERE toDayOfWeek(a.attendance_date) = 2 GROUP BY a.Employee, e.first_name, e.last_name, e.department HAVING monday_productivity > 80 ORDER BY monday_productivity DESC LIMIT 10"
    },
    {
      "question": "Which employees have the lowest idle time on Fridays?",
      "clickhouse_sql": "SELECT a.Employee, e.first_name, e.last_name, e.department, AVG(a.idle_duration) / 3600 AS avg_friday_idle_hours FROM attendance_data a JOIN employee_duration_summary e ON a.Employee = e.employee_id WHERE toDayOfWeek(a.attendance_date) = 6 GROUP BY a.Employee, e.first_name, e.last_name, e.department ORDER BY avg_friday_idle_hours ASC LIMIT 10"
    },
    {
      "question": "Find teams with the most consistent application usage patterns",
      "clickhouse_sql": "WITH app_usage_stats AS (SELECT a.shift_name, u.Application, stddevPop(toUInt32(splitByChar(':', u.Duration)[1]) * 3600 + toUInt32(splitByChar(':', u.Duration)[2]) * 60 + toUInt32(splitByChar(':', u.Duration)[3])) AS usage_stddev FROM employee_usage_data u JOIN attendance_data a ON u.Employee_ID = toString(a.employee_id) AND toDate(u.Date) = toDate(a.attendance_date) WHERE a.shift_name IS NOT NULL GROUP BY a.shift_name, u.Application) SELECT shift_name, AVG(usage_stddev) AS avg_usage_stddev FROM app_usage_stats GROUP BY shift_name ORDER BY avg_usage_stddev ASC LIMIT 5"
    },
    {
      "question": "Which employees have the highest engagement in team collaboration tools?",
      "clickhouse_sql": "SELECT u.Employee, e.first_name, e.last_name, e.department, SUM(toUInt32(splitByChar(':', u.Duration)[1]) * 3600 + toUInt32(splitByChar(':', u.Duration)[2]) * 60 + toUInt32(splitByChar(':', u.Duration)[3])) / 3600 AS collab_hours FROM employee_usage_data u JOIN employee_duration_summary e ON u.Employee_ID = toString(e.employee_id) WHERE u.Application IN ('Microsoft Teams', 'Slack', 'Zoom', 'Google Meet') GROUP BY u.Employee, e.first_name, e.last_name, e.department ORDER BY collab_hours DESC LIMIT 10"
    },
    {
      "question": "Show employees with the most consistent daily productivity",
      "clickhouse_sql": "SELECT employee_id, first_name, last_name, stddevPop(productive_percent) AS productivity_stddev FROM attendance_data GROUP BY employee_id, first_name, last_name ORDER BY productivity_stddev ASC LIMIT 15"
    },
    {
      "question": "Which departments show increasing productivity trends over the last quarter?",
      "clickhouse_sql": "WITH dept_trends AS (SELECT department, corr(toDayOfYear(attendance_date), productive_percent) AS trend_coefficient FROM attendance_data WHERE attendance_date >= date_sub(DAY, 90, today()) GROUP BY department) SELECT department, trend_coefficient FROM dept_trends WHERE trend_coefficient > 0.3 ORDER BY trend_coefficient DESC"
    },
    {
      "question": "Find employees with high productivity but excessive meeting time",
      "clickhouse_sql": "WITH meeting_time AS (SELECT Employee_ID, SUM(toUInt32(splitByChar(':', Duration)[1]) * 3600 + toUInt32(splitByChar(':', Duration)[2]) * 60 + toUInt32(splitByChar(':', Duration)[3])) / 3600 AS meeting_hours FROM employee_usage_data WHERE Application IN ('Zoom','Teams') GROUP BY Employee_ID) SELECT e.employee_id, e.first_name, e.last_name, e.department, e.avg_productive_percent, m.meeting_hours FROM employee_duration_summary e JOIN meeting_time m ON e.employee_id = m.Employee_ID WHERE e.avg_productive_percent > 75 AND m.meeting_hours > 15 ORDER BY m.meeting_hours DESC"
    },
    {
      "question": "Show departments with highest weekend productivity",
      "clickhouse_sql": "SELECT department, AVG(productive_percent) AS weekend_productivity FROM attendance_data WHERE toDayOfWeek(attendance_date) IN (6,7) GROUP BY department ORDER BY weekend_productivity DESC LIMIT 5"
    },
    {
      "question": "Identify employees who work late but maintain high productivity",
      "clickhouse_sql": "SELECT employee_id, first_name, last_name, e.department, ROUND(AVG(toHour(punch_out)), 2) AS avg_end_time, ROUND(AVG(productive_percent), 2) AS avg_productivity FROM attendance_data a JOIN employee_duration_summary e ON a.employee_id = e.employee_id WHERE punch_out IS NOT NULL GROUP BY employee_id, first_name, last_name, e.department HAVING avg_end_time > 19 AND avg_productivity > 70 ORDER BY avg_end_time DESC"
    },
    {
      "question": "Compare morning vs afternoon productivity for each employee",
      "clickhouse_sql": "SELECT employee_id, first_name, last_name, AVG(if(toHour(punch_in) < 12, productive_percent, null)) AS morning_productivity, AVG(if(toHour(punch_in) >= 12, productive_percent, null)) AS afternoon_productivity FROM attendance_data GROUP BY employee_id, first_name, last_name HAVING morning_productivity IS NOT NULL AND afternoon_productivity IS NOT NULL ORDER BY (afternoon_productivity - morning_productivity) DESC"
    },
    {
      "question": "Find employees with matching keyboard and mouse activity patterns",
      "clickhouse_sql": "WITH input_patterns AS (SELECT Employee_ID, corr(Key_Presses, Mouse_Clicks) AS input_correlation FROM employee_usage_data GROUP BY Employee_ID) SELECT e.employee_id, e.first_name, e.last_name, e.department, i.input_correlation FROM employee_duration_summary e JOIN input_patterns i ON toString(e.employee_id) = i.Employee_ID WHERE abs(input_correlation) > 0.7 ORDER BY abs(input_correlation) DESC"
    },
    {
      "question": "Show application usage patterns during high vs low productivity days",
      "clickhouse_sql": "WITH productivity_days AS (SELECT attendance_date, AVG(productive_percent) > 70 AS high_productivity_day FROM attendance_data GROUP BY attendance_date) SELECT u.Application, COUNTIf(p.high_productivity_day) AS high_prod_usage, COUNTIf(NOT p.high_productivity_day) AS low_prod_usage FROM employee_usage_data u JOIN productivity_days p ON u.Date = p.attendance_date GROUP BY u.Application ORDER BY (high_prod_usage - low_prod_usage) DESC"
    },
    {
      "question": "Identify employees who work through lunch breaks regularly",
      "clickhouse_sql": "SELECT employee_id, first_name, last_name, COUNTIf(break_duration < 1800) AS short_break_days FROM attendance_data WHERE break_duration > 0 GROUP BY employee_id, first_name, last_name HAVING short_break_days > 10 ORDER BY short_break_days DESC"
    },
    {
      "question": "Find correlation between meeting frequency and productivity",
      "clickhouse_sql": "WITH meeting_counts AS (SELECT Employee_ID, COUNT(*) AS meeting_count FROM employee_usage_data WHERE Application IN ('Zoom','Teams') GROUP BY Employee_ID) SELECT corr(m.meeting_count, e.avg_productive_percent) AS meeting_productivity_corr FROM meeting_counts m JOIN employee_duration_summary e ON m.Employee_ID = toString(e.employee_id)"
    },
    {
      "question": "Show departments with unusual login patterns",
      "clickhouse_sql": "WITH login_stats AS (SELECT department, toHour(punch_in) AS login_hour, COUNT(*) AS logins FROM attendance_data GROUP BY department, login_hour) SELECT department, entropy(logins) AS pattern_entropy FROM login_stats GROUP BY department ORDER BY pattern_entropy DESC LIMIT 5"
    },
    {
      "question": "Identify employees who switch departments frequently",
      "clickhouse_sql": "SELECT employee_id, COUNT(DISTINCT department) AS department_changes, groupArray(department) AS department_history FROM attendance_data GROUP BY employee_id HAVING department_changes > 1 ORDER BY department_changes DESC"
    },
    {
      "question": "Find applications used more by productive employees",
      "clickhouse_sql": "WITH prod_employees AS (SELECT employee_id FROM employee_duration_summary WHERE avg_productive_percent > 75) SELECT u.Application, COUNT(DISTINCT u.Employee_ID) AS user_count FROM employee_usage_data u JOIN prod_employees p ON u.Employee_ID = toString(p.employee_id) GROUP BY u.Application ORDER BY user_count DESC LIMIT 10"
    },
    {
      "question": "Show productivity distribution by employee age group",
      "clickhouse_sql": "WITH age_groups AS (SELECT employee_id, CASE WHEN age < 25 THEN 'Under 25' WHEN age < 35 THEN '25-34' WHEN age < 45 THEN '35-44' ELSE '45+' END AS age_group FROM employee_metadata) SELECT a.age_group, AVG(e.avg_productive_percent) AS avg_productivity FROM employee_duration_summary e JOIN age_groups a ON e.employee_id = a.employee_id GROUP BY a.age_group ORDER BY avg_productivity DESC"
    },
    {
      "question": "Identify teams with complementary productivity patterns",
      "clickhouse_sql": "WITH team_hourly AS (SELECT shift_name, toHour(punch_in) AS work_hour, AVG(productive_percent) AS hourly_productivity FROM attendance_data GROUP BY shift_name, work_hour) SELECT shift_name, arrayMap(x -> x.2, arraySort(x -> x.1, groupArray((work_hour, hourly_productivity)))) AS productivity_pattern FROM team_hourly GROUP BY shift_name ORDER BY entropy(productivity_pattern) DESC"
    },
    {
      "question": "Find employees who work multiple shifts per day",
      "clickhouse_sql": "SELECT employee_id, first_name, last_name, COUNTIf(punch_out IS NOT NULL) AS shift_count FROM attendance_data GROUP BY employee_id, first_name, last_name, attendance_date HAVING shift_count > 1 ORDER BY shift_count DESC LIMIT 15"
    },
    {
      "question": "Show productivity impact of software updates",
      "clickhouse_sql": "WITH update_dates AS (SELECT DISTINCT software_update_date FROM system_updates) SELECT u.software_update_date, AVG(a.productive_percent) AS post_update_productivity FROM attendance_data a JOIN update_dates u ON a.attendance_date > u.software_update_date GROUP BY u.software_update_date ORDER BY u.software_update_date"
    },
    {
      "question": "Identify employees with abnormal active/idle ratios",
      "clickhouse_sql": "SELECT employee_id, first_name, last_name, (avg_active_duration / avg_idle_duration) AS activity_ratio FROM employee_duration_summary WHERE activity_ratio < 0.5 OR activity_ratio > 2.0 ORDER BY abs(activity_ratio - 1) DESC"
    },
    {
      "question": "Compare productivity before and after holidays",
      "clickhouse_sql": "WITH holidays AS (SELECT holiday_date FROM company_holidays) SELECT AVGIf(productive_percent, a.attendance_date IN (SELECT holiday_date + 1 FROM holidays)) AS post_holiday_prod, AVGIf(productive_percent, a.attendance_date IN (SELECT holiday_date - 1 FROM holidays)) AS pre_holiday_prod FROM attendance_data a"
    },
    {
      "question": "Find employees with matching work patterns",
      "clickhouse_sql": "WITH pattern_matrix AS (SELECT employee_id, arrayFlatten(groupArray([toHour(punch_in), avg_productive_percent, online_duration])) AS pattern FROM attendance_data GROUP BY employee_id) SELECT a.employee_id AS emp1, b.employee_id AS emp2, cosineDistance(a.pattern, b.pattern) AS similarity FROM pattern_matrix a CROSS JOIN pattern_matrix b WHERE emp1 < emp2 ORDER BY similarity ASC LIMIT 10"
    },
    {
      "question": "Show departments with sustainable work hours",
      "clickhouse_sql": "SELECT department, AVG(online_duration) AS avg_hours, stddevPop(online_duration) AS hours_variation FROM attendance_data GROUP BY department ORDER BY (8 - abs(avg_hours - 8)) + (1 / hours_variation) DESC"
    },
    {
      "question": "Identify employees at risk of burnout",
      "clickhouse_sql": "SELECT employee_id, first_name, last_name, SUM(online_duration) / 3600 AS total_hours, AVG(productive_percent) AS avg_productivity, corr(online_duration, productive_percent) AS productivity_trend FROM attendance_data GROUP BY employee_id, first_name, last_name HAVING total_hours > 200 AND productivity_trend < -0.3 ORDER BY total_hours DESC"
    },
    {
      "question": "Find optimal break frequency for productivity",
      "clickhouse_sql": "WITH break_stats AS (SELECT employee_id, attendance_date, COUNT(break_duration) AS break_count, AVG(productive_percent) AS daily_productivity FROM attendance_data GROUP BY employee_id, attendance_date) SELECT break_count, AVG(daily_productivity) AS avg_productivity FROM break_stats GROUP BY break_count ORDER BY avg_productivity DESC LIMIT 5"
    },
    {
      "question": "Show departments with balanced work-life metrics",
      "clickhouse_sql": "SELECT department, AVG(online_duration) AS work_hours, AVG(break_duration) AS break_hours, AVG(productive_percent) AS productivity FROM attendance_data GROUP BY department ORDER BY (productivity * 0.6) + ((break_hours / work_hours) * 0.4) DESC"
    },
    {
      "question": "Identify employees who excel in multiple metrics",
      "clickhouse_sql": "SELECT employee_id, first_name, last_name, NTILE(4) OVER (ORDER BY avg_productive_percent) AS productivity_quartile, NTILE(4) OVER (ORDER BY attendance_rate) AS attendance_quartile FROM employee_duration_summary WHERE productivity_quartile = 4 AND attendance_quartile = 4 ORDER BY (avg_productive_percent + attendance_rate) DESC"
    },
    {
      "question": "Which employees have the highest productivity on weekends compared to weekdays?",
      "clickhouse_sql": "SELECT e.employee_id, e.first_name, e.last_name, e.department, AVG(CASE WHEN toDayOfWeek(a.attendance_date) IN (6, 7) THEN a.productive_percent ELSE NULL END) AS weekend_productivity, AVG(CASE WHEN toDayOfWeek(a.attendance_date) NOT IN (6, 7) THEN a.productive_percent ELSE NULL END) AS weekday_productivity FROM attendance_data a JOIN employee_duration_summary e ON a.employee_id = e.employee_id WHERE toDate(a.attendance_date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY e.employee_id, e.first_name, e.last_name, e.department HAVING weekend_productivity IS NOT NULL AND weekday_productivity IS NOT NULL AND weekend_productivity > weekday_productivity ORDER BY (weekend_productivity - weekday_productivity) DESC LIMIT 10"
    },
    {
      "question": "Show departments with the highest variance in keyboard activity",
      "clickhouse_sql": "SELECT e.department, varPop(u.Key_Presses) AS key_press_variance FROM employee_activity u JOIN employee_duration_summary e ON u.Employee_ID = toString(e.employee_id) WHERE toDate(u.Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY e.department ORDER BY key_press_variance DESC LIMIT 5"
    },
    {
      "question": "Identify employees who use collaboration tools during low productivity periods",
      "clickhouse_sql": "WITH low_prod_days AS (SELECT employee_id, attendance_date FROM attendance_data WHERE productive_percent < 50 AND toDate(attendance_date) BETWEEN '2025-04-01' AND '2025-04-30') SELECT u.Employee_ID, e.first_name, e.last_name, e.department, COUNT(*) AS collab_usage_count FROM employee_activity u JOIN employee_duration_summary e ON u.Employee_ID = toString(e.employee_id) JOIN low_prod_days l ON u.Employee_ID = toString(l.employee_id) AND u.Date = l.attendance_date WHERE u.Application IN ('Microsoft Teams', 'Slack', 'Zoom', 'Google Meet') GROUP BY u.Employee_ID, e.first_name, e.last_name, e.department ORDER BY collab_usage_count DESC LIMIT 10"
    },
    {
      "question": "Which employees have the most consistent login times?",
      "clickhouse_sql": "SELECT employee_id, first_name, last_name, stddevPop(toHour(punch_in)) AS login_time_stddev FROM attendance_data WHERE toDate(attendance_date) BETWEEN '2025-04-01' AND '2025-04-30' AND punch_in IS NOT NULL GROUP BY employee_id, first_name, last_name ORDER BY login_time_stddev ASC LIMIT 10"
    },
    {
      "question": "Show teams with the highest average break frequency per employee",
      "clickhouse_sql": "SELECT a.shift_name, COUNT(*) / COUNT(DISTINCT a.employee_id) AS avg_breaks_per_employee FROM employee_breaks b JOIN attendance_data a ON b.Employee_ID = a.employee_id AND b.Attendance_Date = a.attendance_date WHERE toDate(a.attendance_date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY a.shift_name ORDER BY avg_breaks_per_employee DESC LIMIT 5"
    },
    {
      "question": "Find employees with high productivity but frequent short breaks",
      "clickhouse_sql": "WITH break_counts AS (SELECT Employee_ID, COUNTIf(toUInt32(splitByChar(':', Break_Duration)[1]) * 3600 + toUInt32(splitByChar(':', Break_Duration)[2]) * 60 + toUInt32(splitByChar(':', Break_Duration)[3]) < 1800) AS short_break_count FROM employee_breaks WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY Employee_ID) SELECT e.employee_id, e.first_name, e.last_name, e.department, e.avg_productive_percent, b.short_break_count FROM employee_duration_summary e JOIN break_counts b ON toString(e.employee_id) = b.Employee_ID WHERE e.avg_productive_percent > 75 AND b.short_break_count > 10 ORDER BY b.short_break_count DESC LIMIT 10"
    },
    {
      "question": "Which applications are used most by employees with perfect attendance?",
      "clickhouse_sql": "WITH perfect_attendance AS (SELECT employee_id FROM attendance_data WHERE toDate(attendance_date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY employee_id HAVING COUNT(DISTINCT attendance_date) = 30) SELECT u.Application, COUNT(*) AS usage_count, SUM(toUInt32(splitByChar(':', COALESCE(u.Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(u.Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(u.Duration, '0:0:0'))[3])) / 3600 AS total_hours FROM employee_activity u JOIN perfect_attendance p ON u.Employee_ID = toString(p.employee_id) WHERE toDate(u.Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY u.Application ORDER BY total_hours DESC LIMIT 10"
    },
    {
      "question": "Show departments with the highest ratio of active to idle time",
      "clickhouse_sql": "SELECT department, AVG(COALESCE(avg_active_duration, 0)) / NULLIF(AVG(COALESCE(avg_idle_duration, 0)), 0) AS active_to_idle_ratio FROM department_duration_summary WHERE avg_active_duration IS NOT NULL AND avg_idle_duration IS NOT NULL GROUP BY department ORDER BY active_to_idle_ratio DESC LIMIT 5"
    },
    {
      "question": "Identify employees with high productivity but low break duration",
      "clickhouse_sql": "SELECT e.employee_id, e.first_name, e.last_name, e.department, e.avg_productive_percent, AVG(toUInt32(splitByChar(':', b.Break_Duration)[1]) * 3600 + toUInt32(splitByChar(':', b.Break_Duration)[2]) * 60 + toUInt32(splitByChar(':', b.Break_Duration)[3])) / 3600 AS avg_break_hours FROM employee_duration_summary e JOIN employee_breaks b ON toString(e.employee_id) = b.Employee_ID WHERE toDate(b.Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY e.employee_id, e.first_name, e.last_name, e.department HAVING e.avg_productive_percent > 75 AND avg_break_hours < 0.5 ORDER BY e.avg_productive_percent DESC"
    },
    {
      "question": "Which teams have the most diverse application usage?",
      "clickhouse_sql": "SELECT a.shift_name, COUNT(DISTINCT u.Application) AS unique_apps FROM employee_usage_data u JOIN attendance_data a ON u.Employee_ID = toString(a.employee_id) AND toDate(u.Date) = toDate(a.attendance_date) WHERE toDate(a.attendance_date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY a.shift_name ORDER BY unique_apps DESC LIMIT 5"
    },
    {
      "question": "Find employees who frequently use unproductive URLs",
      "clickhouse_sql": "SELECT u.Employee_ID, e.first_name, e.last_name, e.department, COUNT(*) AS unproductive_url_count FROM employee_activity u JOIN employee_duration_summary e ON u.Employee_ID = toString(e.employee_id) WHERE u.Mapping_Status = 'Unproductive' AND u.URL IS NOT NULL AND toDate(u.Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY u.Employee_ID, e.first_name, e.last_name, e.department ORDER BY unproductive_url_count DESC LIMIT 10"
    },
    {
      "question": "Show productivity trends for employees who changed teams",
      "clickhouse_sql": "WITH team_changes AS (SELECT employee_id, COUNT(DISTINCT shift_name) AS team_count, groupArray(shift_name) AS teams FROM attendance_data WHERE toDate(attendance_date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY employee_id HAVING team_count > 1) SELECT t.employee_id, e.first_name, e.last_name, e.department, t.teams, AVG(a.productive_percent) AS avg_productivity FROM team_changes t JOIN employee_duration_summary e ON t.employee_id = e.employee_id JOIN attendance_data a ON t.employee_id = a.employee_id WHERE toDate(a.attendance_date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY t.employee_id, e.first_name, e.last_name, e.department, t.teams ORDER BY avg_productivity DESC"
    },
    {
      "question": "Which departments have the highest correlation between active time and productivity?",
      "clickhouse_sql": "SELECT department, corr(COALESCE(active_duration, 0), COALESCE(productive_percent, 0)) AS active_productivity_corr FROM attendance_data WHERE toDate(attendance_date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY department ORDER BY active_productivity_corr DESC LIMIT 5"
    },
    {
      "question": "Show employees with high productivity but low mouse activity",
      "clickhouse_sql": "SELECT e.employee_id, e.first_name, e.last_name, e.department, e.avg_productive_percent, AVG(u.Mouse_Clicks) AS avg_mouse_clicks FROM employee_activity u JOIN employee_duration_summary e ON u.Employee_ID = toString(e.employee_id) WHERE toDate(u.Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY e.employee_id, e.first_name, e.last_name, e.department HAVING e.avg_productive_percent > 75 AND avg_mouse_clicks < (SELECT AVG(Mouse_Clicks) FROM employee_activity WHERE toDate(Date) BETWEEN '2025-04-01' AND '2025-04-30') ORDER BY e.avg_productive_percent DESC"
    },
    {
      "question": "Which employees have the highest productivity improvement week-over-week?",
      "clickhouse_sql": "WITH weekly_productivity AS (SELECT employee_id, formatDateTime(toStartOfWeek(attendance_date), '%Y-%m-%d') AS week_start, AVG(COALESCE(Productive_Percent, 0)) AS avg_productivity FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY employee_id, week_start) SELECT w1.employee_id, e.first_name, e.last_name, e.department, w1.week_start AS current_week, (w1.avg_productivity - w2.avg_productivity) AS productivity_increase FROM weekly_productivity w1 JOIN weekly_productivity w2 ON w1.employee_id = w2.employee_id AND w1.week_start > w2.week_start JOIN employee_duration_summary e ON w1.employee_id = e.employee_id WHERE w1.avg_productivity > w2.avg_productivity ORDER BY productivity_increase DESC LIMIT 10"
    },
    {
      "question": "Find departments with high attendance but low productivity",
      "clickhouse_sql": "SELECT d.department, AVG(COALESCE(a.employees_present / NULLIF(a.total_employees, 0), 0)) * 100 AS avg_attendance_rate, AVG(COALESCE(e.avg_productive_percent, 0)) AS avg_productivity FROM daily_attendance_summary a JOIN department_duration_summary e ON a.department = e.department WHERE toDate(a.attendance_date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY d.department HAVING avg_attendance_rate > 90 AND avg_productivity < 50 ORDER BY avg_productivity ASC"
    },
    {
      "question": "Show employees with the most consistent break durations",
      "clickhouse_sql": "SELECT b.Employee_ID, e.first_name, e.last_name, e.department, stddevPop(toUInt32(splitByChar(':', b.Break_Duration)[1]) * 3600 + toUInt32(splitByChar(':', b.Break_Duration)[2]) * 60 + toUInt32(splitByChar(':', b.Break_Duration)[3])) / 3600 AS break_duration_stddev FROM employee_breaks b JOIN employee_duration_summary e ON b.Employee_ID = toString(e.employee_id) WHERE toDate(b.Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY b.Employee_ID, e.first_name, e.last_name, e.department ORDER BY break_duration_stddev ASC LIMIT 10"
    },
    {
      "question": "Which teams had the highest productivity during the first week of April 2025?",
      "clickhouse_sql": "SELECT department AS team, round(avgProductivityPrevWeek - avgProductivityLastWeek, 2) AS productivity_decline FROM ( SELECT department, avgIf(productive_percent, toDate(attendance_date) BETWEEN '2025-04-01' AND '2025-04-07') AS avgProductivityPrevWeek, avgIf(productive_percent, toDate(attendance_date) BETWEEN '2025-04-08' AND '2025-04-30') AS avgProductivityLastWeek FROM attendance_data WHERE toDate(attendance_date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY department ) ORDER BY productivity_decline DESC LIMIT 3"
    },
    {
      "question": "Get login and logout times for a specific date",
      "clickhouse_sql": "SELECT Employee_ID, In_Time, Out_Time FROM monthly_inout WHERE Date = toDate('2025-04-15') ORDER BY In_Time"
    },
    {
      "question": "Average punch duration by shift",
      "clickhouse_sql": "SELECT Shift_Name, avg(toIntervalSecond(Punch_Duration)) AS avg_punch_seconds FROM employee_metrics WHERE Attendance_Date BETWEEN toDate('2025-04-01') AND toDate('2025-04-30') GROUP BY Shift_Name ORDER BY avg_punch_seconds DESC"
    },
    {
      "question": "Which employees had more than 3 unproductive days?",
      "clickhouse_sql": "SELECT Employee_ID, count() AS unproductive_days FROM employee_metrics WHERE Productive_Percent < 30 AND Attendance_Date BETWEEN toDate('2025-04-01') AND toDate('2025-04-30') GROUP BY Employee_ID HAVING unproductive_days > 3 ORDER BY unproductive_days DESC"
    },
    {
      "question": "Employees with login time after 11 AM on more than 2 days",
      "clickhouse_sql": "SELECT Employee_ID, count() AS late_days FROM employee_metrics WHERE toHour(Punch_In) > 11 AND Attendance_Date BETWEEN toDate('2025-04-01') AND toDate('2025-04-30') GROUP BY Employee_ID HAVING late_days > 2 ORDER BY late_days DESC"
    },
    {
      "question": "Which employees consistently rank in the bottom 10% productivity across the last 3 months?",
      "clickhouse_sql": "SELECT employee_id, avg(avg_productive_percent) AS avg_productivity FROM employee_duration_summary WHERE toStartOfMonth(date) >= toStartOfMonth(now() - INTERVAL 3 MONTH) GROUP BY employee_id HAVING avg_productivity <= (SELECT quantile(0.1)(avg_productive_percent) FROM employee_duration_summary WHERE toStartOfMonth(date) >= toStartOfMonth(now() - INTERVAL 3 MONTH)) ORDER BY avg_productivity ASC"
    },
    {
      "question": "Get the 3-day moving average of productivity per employee",
      "clickhouse_sql": "SELECT Employee_ID, Attendance_Date, avg(Productive_Percent) OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg_productivity FROM employee_metrics WHERE Attendance_Date BETWEEN toDate('2025-04-01') AND toDate('2025-04-30')"
    },
    {
      "question": "Identify productivity drops over 20% within a week for employees",
      "clickhouse_sql": "SELECT Employee_ID, Attendance_Date, Productive_Percent, lag(Productive_Percent, 1) OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS previous_day_percent FROM employee_metrics WHERE Attendance_Date BETWEEN toDate('2025-04-01') AND toDate('2025-04-30') HAVING (previous_day_percent - Productive_Percent) > 20"
    },
    {
      "question": "Find employees whose break time exceeded 25% of their total duration on any day",
      "clickhouse_sql": "SELECT Employee_ID, Attendance_Date, Break_Duration, Total_Duration FROM employee_metrics WHERE toFloat64(toIntervalSecond(Break_Duration)) / toFloat64(toIntervalSecond(Total_Duration)) > 0.25 ORDER BY Employee_ID, Attendance_Date"
    },
    {
      "question": "Compare department-wise productivity between the current and previous month",
      "clickhouse_sql": "SELECT department, avgIf(Productive_Percent, toStartOfMonth(Attendance_Date) = toStartOfMonth(now())) AS current_month_avg, avgIf(Productive_Percent, toStartOfMonth(Attendance_Date) = toStartOfMonth(now() - INTERVAL 1 MONTH)) AS prev_month_avg FROM employee_metrics WHERE Attendance_Date >= toDate(now() - INTERVAL 2 MONTH) GROUP BY department"
    },
    {
      "question": "List employees with a sudden spike in idle time compared to the previous week average",
      "clickhouse_sql": "SELECT Employee_ID, Attendance_Date, Idle_Duration, avg(Idle_Duration) OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) AS last_week_avg FROM employee_metrics WHERE Attendance_Date BETWEEN toDate('2025-04-01') AND toDate('2025-04-30') HAVING toIntervalSecond(Idle_Duration) > 1.5 * toIntervalSecond(last_week_avg)"
    },
    {
      "question": "Get productivity percentiles per department",
      "clickhouse_sql": "SELECT department, quantile(0.25)(Productive_Percent) AS p25, quantile(0.5)(Productive_Percent) AS median, quantile(0.75)(Productive_Percent) AS p75 FROM employee_metrics WHERE Attendance_Date BETWEEN toDate('2025-04-01') AND toDate('2025-04-30') GROUP BY department"
    },
    {
      "question": "Detect outliers in productivity using IQR method",
      "clickhouse_sql": "SELECT * FROM (SELECT Employee_ID, Productive_Percent, quantile(0.25)(Productive_Percent) AS Q1, quantile(0.75)(Productive_Percent) AS Q3 FROM employee_metrics WHERE Attendance_Date BETWEEN toDate('2025-04-01') AND toDate('2025-04-30') GROUP BY Employee_ID) WHERE Productive_Percent < (Q1 - 1.5 * (Q3 - Q1)) OR Productive_Percent > (Q3 + 1.5 * (Q3 - Q1))"
    },
    {
      "question": "Which teams improved their productivity over the last month?",
      "clickhouse_sql": "SELECT Team, avgIf(Productive_Percent, toStartOfMonth(Attendance_Date) = toStartOfMonth(now())) AS current_avg, avgIf(Productive_Percent, toStartOfMonth(Attendance_Date) = toStartOfMonth(now() - INTERVAL 1 MONTH)) AS prev_avg FROM employee_metrics WHERE Attendance_Date >= toDate(now() - INTERVAL 2 MONTH) GROUP BY Team HAVING current_avg > prev_avg"
    },
    {
      "question": "Get the longest continuous streak of 100% productivity per employee",
      "clickhouse_sql": "SELECT Employee_ID, max(streak) AS max_streak FROM (SELECT Employee_ID, count() AS streak FROM (SELECT Employee_ID, Attendance_Date, Productive_Percent, rowNumberInAllBlocks() - rowNumberInAllBlocks() OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS grp FROM employee_metrics WHERE Productive_Percent = 100) GROUP BY Employee_ID, grp) GROUP BY Employee_ID"
    },
    {
      "question": "Which employees show a continuous decline in productivity over the last 3 weeks?",
      "clickhouse_sql": "SELECT Employee_ID FROM (SELECT Employee_ID, toMonday(Attendance_Date) AS week, avg(Productive_Percent) AS avg_prod FROM employee_metrics WHERE Attendance_Date >= subtractWeeks(today(), 3) GROUP BY Employee_ID, week ORDER BY Employee_ID, week) WHERE avg_prod < lag(avg_prod, 1) OVER (PARTITION BY Employee_ID ORDER BY week) AND avg_prod < lag(avg_prod, 2) OVER (PARTITION BY Employee_ID ORDER BY week)"
    },
    {
      "question": "Find average break time by department and shift for each week",
      "clickhouse_sql": "SELECT toMonday(Attendance_Date) AS week, department, Shift_Name, avg(toIntervalSecond(Break_Duration)) AS avg_break FROM employee_metrics WHERE Attendance_Date >= toDate('2025-04-01') GROUP BY week, department, Shift_Name ORDER BY week, avg_break DESC"
    },
    {
      "question": "Which applications are associated with low productivity employees?",
      "clickhouse_sql": "SELECT Application, count(DISTINCT ea.Employee_ID) AS low_perf_users FROM employee_activity ea JOIN employee_metrics em ON ea.Employee_ID = em.Employee_ID AND ea.Date = em.Attendance_Date WHERE em.Productive_Percent < 40 AND ea.Date BETWEEN toDate('2025-04-01') AND toDate('2025-04-30') GROUP BY Application ORDER BY low_perf_users DESC"
    },
    {
      "question": "Detect anomalies in productivity using standard deviation",
      "clickhouse_sql": "SELECT Employee_ID, Attendance_Date, Productive_Percent FROM (SELECT Employee_ID, Attendance_Date, Productive_Percent, avg(Productive_Percent) OVER (PARTITION BY Employee_ID) AS avg_prod, stddevPop(Productive_Percent) OVER (PARTITION BY Employee_ID) AS std_dev FROM employee_metrics WHERE Attendance_Date >= toDate('2025-04-01')) WHERE abs(Productive_Percent - avg_prod) > 2 * std_dev ORDER BY Employee_ID, Attendance_Date"
    },
    {
      "question": "Identify productivity improvement from last month to this month",
      "clickhouse_sql": "SELECT Employee_ID, (curr_month - last_month) AS improvement FROM (SELECT Employee_ID, avgIf(Productive_Percent, toMonth(Attendance_Date) = toStartOfMonth(now())) AS curr_month, avgIf(Productive_Percent, toMonth(Attendance_Date) = toStartOfMonth(now() - INTERVAL 1 MONTH)) AS last_month FROM employee_metrics GROUP BY Employee_ID) WHERE improvement > 0 ORDER BY improvement DESC"
    },
    {
        "question": "fetch me teams with productivity below 40 and their avg working hours(online duration)",
        "clickhouse_sql": "SELECT Group_Name AS team, ROUND(AVG(COALESCE(Productive_Percent, 0)), 2) AS avg_productivity, ROUND(AVG(toUInt32(splitByChar(':', COALESCE(Online_Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(Online_Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(Online_Duration, '0:0:0'))[3])) / 3600, 2) AS avg_working_hours FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY Group_Name HAVING avg_productivity < 40 ORDER BY avg_productivity ASC"
    },
    {
        "question": "Which teams have an average active duration below 5 hours and productivity above 60% in April 2025?",
        "clickhouse_sql": "SELECT Group_Name AS team, ROUND(AVG(COALESCE(Productive_Percent, 0)), 2) AS avg_productivity, ROUND(AVG(toUInt32(splitByChar(':', COALESCE(Active_Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(Active_Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(Active_Duration, '0:0:0'))[3])) / 3600, 2) AS avg_active_hours FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY Group_Name HAVING avg_active_hours < 5 AND avg_productivity > 60 ORDER BY avg_productivity DESC"
    },
    {
        "question": "Show employees with high keyboard activity but low productivity in the last two weeks of April 2025",
        "clickhouse_sql": "WITH keyboard_activity AS (SELECT Employee_ID, AVG(Key_Presses) AS avg_key_presses FROM employee_activity WHERE toDate(Date) BETWEEN '2025-04-16' AND '2025-04-30' GROUP BY Employee_ID) SELECT e.Employee_ID, e.First_Name, e.Last_Name, e.department, ROUND(AVG(COALESCE(e.Productive_Percent, 0)), 2) AS avg_productivity, k.avg_key_presses FROM employee_metrics e JOIN keyboard_activity k ON e.Employee_ID = k.Employee_ID WHERE toDate(e.Attendance_Date) BETWEEN '2025-04-16' AND '2025-04-30' GROUP BY e.Employee_ID, e.First_Name, e.Last_Name, e.department, k.avg_key_presses HAVING avg_productivity < 40 AND k.avg_key_presses > (SELECT AVG(Key_Presses) * 1.5 FROM employee_activity WHERE toDate(Date) BETWEEN '2025-04-16' AND '2025-04-30') ORDER BY k.avg_key_presses DESC LIMIT 10"
    },
    {
        "question": "Which departments had the highest average break duration in April 2025, and what is their average productivity?",
        "clickhouse_sql": "SELECT department, ROUND(AVG(toUInt32(splitByChar(':', COALESCE(Break_Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(Break_Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(Break_Duration, '0:0:0'))[3])) / 3600, 2) AS avg_break_hours, ROUND(AVG(COALESCE(Productive_Percent, 0)), 2) AS avg_productivity FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY department ORDER BY avg_break_hours DESC LIMIT 5"
    },
    {
        "question": "Identify teams with more than 10% of days having zero productivity in April 2025",
        "clickhouse_sql": "SELECT Group_Name AS team, COUNTIf(COALESCE(Productive_Percent, 0) = 0) AS zero_prod_days, COUNT(*) AS total_days, ROUND((COUNTIf(COALESCE(Productive_Percent, 0) = 0) / COUNT(*) * 100), 2) AS zero_prod_percentage FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY Group_Name HAVING zero_prod_percentage > 10 ORDER BY zero_prod_percentage DESC"
    },
    {
        "question": "Which employees have an average online duration exceeding their shift duration by more than 1 hour in April 2025?",
        "clickhouse_sql": "SELECT e.Employee_ID, e.First_Name, e.Last_Name, e.department, ROUND(AVG(toUInt32(splitByChar(':', COALESCE(Online_Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(Online_Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(Online_Duration, '0:0:0'))[3])) / 3600, 2) AS avg_online_hours, ROUND(AVG(toUInt32(splitByChar(':', COALESCE(Punch_Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(Punch_Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(Punch_Duration, '0:0:0'))[3])) / 3600, 2) AS avg_shift_hours FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY e.Employee_ID, e.First_Name, e.Last_Name, e.department HAVING avg_online_hours > (avg_shift_hours + 1) ORDER BY (avg_online_hours - avg_shift_hours) DESC LIMIT 10"
    },
    {
        "question": "Show the top 5 applications used by teams with productivity below 40% in April 2025",
        "clickhouse_sql": "WITH low_prod_teams AS (SELECT Group_Name FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY Group_Name HAVING AVG(COALESCE(Productive_Percent, 0)) < 40) SELECT a.Application, COUNT(*) AS usage_count, ROUND(SUM(toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[3])) / 3600, 2) AS total_hours FROM employee_activity a JOIN employee_metrics m ON a.Employee_ID = m.Employee_ID AND a.Date = m.Attendance_Date WHERE a.Date BETWEEN '2025-04-01' AND '2025-04-30' AND m.Group_Name IN (SELECT Group_Name FROM low_prod_teams) GROUP BY a.Application ORDER BY total_hours DESC LIMIT 5"
    },
    {
        "question": "Which employees had the highest average productivity during the last week of April 2025?",
        "clickhouse_sql": "SELECT Employee_ID, First_Name, Last_Name, department, ROUND(AVG(COALESCE(Productive_Percent, 0)), 2) AS avg_productivity FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-24' AND '2025-04-30' GROUP BY Employee_ID, First_Name, Last_Name, department ORDER BY avg_productivity DESC LIMIT 10"
    },
    {
        "question": "Show teams with the highest average time spent on cloud-based apps in April 2025",
        "clickhouse_sql": "SELECT m.Group_Name AS team, ROUND(AVG(toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(a.Duration,' '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(a.Duration,' '0:0:0'))[3])) / 3600, 2) AS avg_cloud_app_hours FROM employee_activity a JOIN employee_metrics m ON a.Employee_ID = m.Employee_ID AND a.Date = m.Attendance_Date WHERE toDate(a.Date) BETWEEN '2025-04-01' AND '2025-04-30' AND a.Application IN ('Google Drive', 'Dropbox', 'AWS') GROUP BY m.Group_Name ORDER BY avg_cloud_app_hours DESC LIMIT 5"
    },
    {
        "question": "Which employees had more than 5 days with productivity below 30% and online duration > 8 hours in April 2025?",
        "clickhouse_sql": "SELECT Employee_ID, First_Name, Last_Name, department, COUNTIf(COALESCE(Productive_Percent, 0) < 30 AND toUInt32(splitByChar(':', COALESCE(Online_Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(Online_Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(Online_Duration, '0:0:0'))[3]) / 3600 > 8) AS low_prod_long_hours_days FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY Employee_ID, First_Name, Last_Name, department HAVING low_prod_long_hours_days > 5 ORDER BY low_prod_long_hours_days DESC"
    },
    {
        "question": "Find departments with the highest average number of applications used per employee in April 2025",
        "clickhouse_sql": "SELECT m.department, ROUND(AVG(COALESCE(a.App_Count, 0)), 2) AS avg_app_count FROM employee_metrics m JOIN employee_activity a ON m.Employee_ID = a.Employee_ID AND m.Attendance_Date = a.Date WHERE toDate(m.Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY m.department ORDER BY avg_app_count DESC LIMIT 5"
    },
    {
        "question": "Which teams had the highest average productivity during the middle of the workday (10 AM - 2 PM) in April 2025?",
        "clickhouse_sql": "SELECT Group_Name AS team, ROUND(AVG(COALESCE(Productive_Percent, 0)), 2) AS avg_midday_productivity FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toHour(Punch_In) <= 10 AND (Punch_Out IS NULL OR toHour(Punch_Out) >= 14) GROUP BY Group_Name ORDER BY avg_midday_productivity DESC LIMIT 5"
    },
    {
        "question": "Show employees with the highest average time spent on gaming apps in April 2025",
        "clickhouse_sql": "SELECT a.Employee_ID, m.First_Name, m.Last_Name, m.department, ROUND(AVG(toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[3])) / 3600, 2) AS avg_gaming_hours FROM employee_activity a JOIN employee_metrics m ON a.Employee_ID = m.Employee_ID AND a.Date = m.Attendance_Date WHERE toDate(a.Date) BETWEEN '2025-04-01' AND '2025-04-30' AND a.Application LIKE '%Game%' GROUP BY a.Employee_ID, m.First_Name, m.Last_Name, m.department ORDER BY avg_gaming_hours DESC LIMIT 5"
    },
    {
    "question": "Which departments had the highest variance in employee attendance duration in April 2025?",
    "clickhouse_sql": "SELECT department, ROUND(STDDEV_POP(toUInt32(splitByChar(':', COALESCE(Punch_Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(Punch_Duration,' '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(Punch_Duration,' '0:0'))[3])) / 3600, 2) AS variance_shift_hours FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY department ORDER BY variance_shift_hours DESC LIMIT 5"
    },
    {
        "question": "Which employees worked overtime (Punch_Duration > Shift_End) for more than 10 hours total in April 2025?",
        "clickhouse_sql": "SELECT m.Employee_ID, m.First_Name, m.Last_Name, m.department, ROUND(SUM(CASE WHEN toDateTime(COALESCE(m.Punch_Out, '2000-01-01 00:00:00')) > toDateTime(COALESCE(m.Shift_End, '2000-01-01 00:00:00')) THEN (toUnixTimestamp(toDateTime(COALESCE(m.Punch_Duration, '0:0:0')) - toUnixTimestamp(toDateTime(COALESCE(m.Shift_End, '2000-01-01 00:00:00'))) / 3600) ELSE 0 END) ELSE 2) AS total_overtime_hours FROM employee_metrics m WHERE toDate(m.Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND m.Punch_Out IS NOT NULL AND m.Shift_End IS NOT NULL GROUP BY m.Employee_ID, m.First_Name, m.Last_Name, m.department HAVING total_overtime_hours > 10 ORDER BY total_overtime_hours DESC LIMIT 10"
    },
    {
        "question": "Which teams had the highest 95th percentile of productivity in April 2025?",
        "clickhouse_sql": "SELECT Group_Name AS team, ROUND(quantile(0.95)(COALESCE(Productive_Percent, 0)), 2) AS productivity_p95 FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY Group_Name ORDER BY productivity_p95 DESC LIMIT 5"
    },
    {
        "question": "Which employees switched between apps more than 100 times per day on average in April 2025?",
        "clickhouse_sql": "SELECT a.Employee_ID, m.First_Name, m.Last_Name, m.department, ROUND(AVG(COALESCE(a.App_Switches, 0)), 0) AS avg_app_switches FROM employee_activity a JOIN employee_metrics m ON a.Employee_ID = m.Employee_ID AND a.Date = m.Attendance_Date WHERE toDate(a.Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY a.Employee_ID, m.First_Name, m.Last_Name, m.department HAVING avg_app_switches > 100 ORDER BY avg_app_switches DESC LIMIT 10"
    },
    {
        "question": "Which departments had the highest average meeting time based on calendar app activity in April 2025?",
        "clickhouse_sql": "SELECT m.department, ROUND(AVG(toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(a.Duration,' '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(a.Duration,' '0:0:0'))[3])) / 3600/3600, 2) AS avg_meeting_hours FROM employee_activity a JOIN employee_metrics m ON a.Employee_ID = m.Employee_ID AND a.Date = m.Attendance_Date WHERE toDate(a.Date) BETWEEN '2025-04-01' AND '2025-04-30' AND a.Application IN ('Google Calendar,' 'Outlook Calendar') GROUP BY m.department ORDER BY avg_meeting_hours DESC LIMIT 5"
    },
    {
        "question": "Find employees with more than 5 days where unproductive duration exceeded productive duration in April 2025",
        "clickhouse_sql": "SELECT Employee_ID, First_Name, Last_Name, department, COUNTIf(toUInt32(splitByChar(':', COALESCE(Unproductive_Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(Unproductive_Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(Unproductive_Duration,' '0:0:0'))[3]) > toUInt32(splitByChar(':', COALESCE(Productive_Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(Productive_Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(Productive_Duration,' '0:0:0'))[3])) AS unprod_dominant_days FROM employee_metrics WHERE toDate(toDate(Attendance_Date)) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY Employee_ID, First_Name, Last_Name, department HAVING unprod_dominant_days > 5 ORDER BY unprod_dominant_days DESC"
    },
    {
        "question": "Which teams had the highest average productivity during the last 5 workdays of April 2025?",
        "clickhouse_sql": "SELECT Group_Name AS team, ROUND(AVG(COALESCE(Productive_Percent, 0)), 2) AS avg_productivity FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-24' AND '2025-04-30' AND toDayOfWeek(toDate(Attendance_Date)) BETWEEN 1 AND 5 GROUP BY Group_Name ORDER BY avg_productivity DESC LIMIT 5"
    },
    {
        "question": "Which employees had a productivity increase of more than 15% in the second half of April to the 2025 compared to the first half?",
        "clickhouse_sql": "WITH first_half AS (SELECT Employee_ID, AVG(COALESCE(Productive_Percent, 0)) AS first_half_avg FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-15' GROUP BY Employee_ID), second_half AS (SELECT Employee_ID, AVG(COALESCE(Productive_Percent, 0)) AS second_half_avg FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-16' AND '2025-04-30' GROUP BY Employee_ID) SELECT s.Employee_ID, m.First_Name, m.Last_Name, m.department, ROUND(first_half_avg, 2) AS first_half, ROUND(second_half_avg, 2) AS second_half FROM second_half s JOIN first_half f ON s.Employee_ID = f.Employee_ID JOIN employee_metrics m ON s.Employee_ID = m.Employee_ID WHERE toDate(m.Attendance_Date) BETWEEN '2025-04-16' AND '2025-04-30' AND (second_half_avg - first_half_avg) > 15 GROUP BY s.Employee_ID, m.First_Name, m.Last_Name, m.department, first_half_avg, second_half_avg ORDER BY (second_half_avg - first_half_avg) DESC LIMIT 10"
    },
    {
        "question": "Which departments had the lowest average time spent on collaboration tools in April 2025?",
        "clickhouse_sql": "SELECT m.department, ROUND(AVG(toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(a.Duration,' '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(a.Duration,' '0:0:0'))[3])) / 3600, 2) AS avg_collaboration_hours FROM employee_activity a JOIN employee_metrics m ON a.Employee_ID = m.Employee_ID AND a.Date = m.Attendance_Date WHERE toDate(a.Date) BETWEEN '2025-04-01' AND '2025-04-30' AND a.Application IN ('Trello', 'Jira', 'Asana') GROUP BY m.department ORDER BY avg_collaboration_hours ASC LIMIT 5"
    },
    {
        "question": "Find employees with the highest average number of distinct applications used per day in April 2025",
        "clickhouse_sql": "SELECT a.Employee_ID, m.First_Name, m.Last_Name, m.department, ROUND(AVG(COALESCE(a.App_Distinct_count, 0)), 2) AS avg_distinct_apps FROM employee_activity a JOIN employee_metrics m ON a.Employee_ID = m.Employee_ID AND a.Date = m.Attendance_Date WHERE toDate(a.Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY a.Employee_ID, m.First_Name, m.Last_Name, m.department ORDER BY avg_distinct_apps DESC LIMIT 10"
    },
    {
        "question": "Which teams had the highest correlation between key presses and productivity in April 2025",
        "clickhouse_sql": "SELECT m.Group_Name AS team, corr(COALESCE(a.Key_Presses, 0), COALESCE(m.Productive_Percent, 0)) AS key_press_productivity_corr FROM employee_metrics m JOIN employee_activity a ON m.Employee_ID = a.Employee_ID AND m.Attendance_Date = a.Date WHERE toDate(m.Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY m.Group_Name HAVING ABS(key_press_productivity_corr) > 0.5 ORDER BY key_press_productivity_corr DESC LIMIT 5"
    },
    {
        "question": "Which employees were continuously absent for 5 or more workdays in April 2025?",
        "clickhouse_sql": "WITH absences AS (SELECT Employee_ID, First_Name, Last_Name, department, Attendance_Date, Punch_In, row_number() OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS rn, row_number() OVER (PARTITION BY Employee_ID, CASE WHEN Punch_In IS NULL THEN 1 ELSE 0 END ORDER BY Attendance_Date) AS absent_rn FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(Attendance_Date)) BETWEEN 1 AND 5), absence_groups AS (SELECT Employee_ID, First_Name, Last_Name, department, COUNT(*) AS consecutive_absences FROM absences WHERE Punch_In IS NULL GROUP BY Employee_ID, First_Name, Last_Name, department, (rn - absent_rn) HAVING consecutive_absences >= 5) SELECT Employee_ID, First_Name, Last_Name, department, consecutive_absences FROM absence_groups ORDER BY consecutive_absences DESC"
    },
    {
        "question": "Which employees had 3 or more consecutive absences with zero productivity in April 2025?",
        "clickhouse_sql": "WITH absences AS (SELECT Employee_ID, First_Name, Last_Name, department, Attendance_Date, Punch_In, COALESCE(Productive_Percent, 0) AS prod_percent, row_number() OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS rn, row_number() OVER (PARTITION BY Employee_ID, CASE WHEN Punch_In IS NULL AND prod_percent = 0 THEN 1 ELSE 0 END ORDER BY Attendance_Date) AS absent_rn FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(Attendance_Date)) BETWEEN 1 AND 5), absence_groups AS (SELECT Employee_ID, First_Name, Last_Name, department, COUNT(*) AS consecutive_absences FROM absences WHERE Punch_In IS NULL AND prod_percent = 0 GROUP BY Employee_ID, First_Name, Last_Name, department, (rn - absent_rn) HAVING consecutive_absences >= 3) SELECT Employee_ID, First_Name, Last_Name, department, consecutive_absences FROM absence_groups ORDER BY consecutive_absences DESC"
    },
    {
        "question": "Which employees were absent for 4 or more consecutive workdays and had no online activity in April 2025?",
        "clickhouse_sql": "WITH absences AS (SELECT m.Employee_ID, m.First_Name, m.Last_Name, m.department, m.Attendance_Date, m.Punch_In, COALESCE(m.Online_Duration, '0:0:0') AS online_dur, row_number() OVER (PARTITION BY m.Employee_ID ORDER BY m.Attendance_Date) AS rn, row_number() OVER (PARTITION BY m.Employee_ID, CASE WHEN m.Punch_In IS NULL AND online_dur = '0:0:0' THEN 1 ELSE 0 END ORDER BY m.Attendance_Date) AS absent_rn FROM employee_metrics m WHERE toDate(m.Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(m.Attendance_Date)) BETWEEN 1 AND 5), absence_groups AS (SELECT Employee_ID, First_Name, Last_Name, department, COUNT(*) AS consecutive_absences FROM absences WHERE Punch_In IS NULL AND online_dur = '0:0:0' GROUP BY Employee_ID, First_Name, Last_Name, department, (rn - absent_rn) HAVING consecutive_absences >= 4) SELECT Employee_ID, First_Name, Last_Name, department, consecutive_absences FROM absence_groups ORDER BY consecutive_absences DESC"
    },
    {
        "question": "Which teams had employees with 5 or more consecutive absent days in April 2025?",
        "clickhouse_sql": "WITH absences AS (SELECT Employee_ID, First_Name, Last_Name, Group_Name, Attendance_Date, Punch_In, row_number() OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS rn, row_number() OVER (PARTITION BY Employee_ID, CASE WHEN Punch_In IS NULL THEN 1 ELSE 0 END ORDER BY Attendance_Date) AS absent_rn FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(Attendance_Date)) BETWEEN 1 AND 5), absence_groups AS (SELECT Employee_ID, First_Name, Last_Name, Group_Name, COUNT(*) AS consecutive_absences FROM absences WHERE Punch_In IS NULL GROUP BY Employee_ID, First_Name, Last_Name, Group_Name, (rn - absent_rn) HAVING consecutive_absences >= 5) SELECT Group_Name AS team, Employee_ID, First_Name, Last_Name, consecutive_absences FROM absence_groups ORDER BY Group_Name, consecutive_absences DESC"
    },
    {
        "question": "Which employees were absent for 3 or more consecutive workdays in the first week of April 2025?",
        "clickhouse_sql": "WITH absences AS (SELECT Employee_ID, First_Name, Last_Name, department, Attendance_Date, Punch_In, row_number() OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS rn, row_number() OVER (PARTITION BY Employee_ID, CASE WHEN Punch_In IS NULL THEN 1 ELSE 0 END ORDER BY Attendance_Date) AS absent_rn FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-07' AND toDayOfWeek(toDate(Attendance_Date)) BETWEEN 1 AND 4), absence_groups AS (SELECT Employee_ID, First_Name, Last_Name, department, COUNT(*) AS consecutive_absences FROM absences WHERE Punch_In IS NULL GROUP BY Employee_ID, First_Name, Last_Name, department, (rn - absent_rn) HAVING consecutive_absences >= 3) SELECT Employee_ID, First_Name, Last_Name, department, consecutive_absences FROM absence_groups ORDER BY consecutive_absences DESC"
    },
    {
        "question": "Which employees had 7 or more consecutive absences with no key presses in April 2025?",
        "clickhouse_sql": "WITH absences AS (SELECT m.Employee_ID, m.First_Name, m.Last_Name, m.department, m.Attendance_Date, m.Punch_In, COALESCE(a.Key_Presses, 0) AS key_presses, row_number() OVER (PARTITION BY m.Employee_ID ORDER BY m.Attendance_Date) AS rn, row_number() OVER (PARTITION BY m.Employee_ID, CASE WHEN m.Punch_In IS NULL AND key_presses = 0 THEN 1 ELSE 0 END ORDER BY m.Attendance_Date) AS absent_rn FROM employee_metrics m LEFT JOIN employee_activity a ON m.Employee_ID = a.Employee_ID AND m.Attendance_Date = a.Date WHERE toDate(m.Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(m.Attendance_Date)) BETWEEN 1 AND 5), absence_groups AS (SELECT Employee_ID, First_Name, Last_Name, department, COUNT(*) AS consecutive_absences FROM absences WHERE Punch_In IS NULL AND key_presses = 0 GROUP BY Employee_ID, First_Name, Last_Name, department, (rn - absent_rn) HAVING consecutive_absences >= 7) SELECT Employee_ID, First_Name, Last_Name, department, consecutive_absences FROM absence_groups ORDER BY consecutive_absences DESC"
    },
    {
        "question": "Which departments had the most employees with 4 or more consecutive absences in April 2025?",
        "clickhouse_sql": "WITH absences AS (SELECT Employee_ID, First_Name, Last_Name, department, Attendance_Date, Punch_In, row_number() OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS rn, row_number() OVER (PARTITION BY Employee_ID, CASE WHEN Punch_In IS NULL THEN 1 ELSE 0 END ORDER BY Attendance_Date) AS absent_rn FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(Attendance_Date)) BETWEEN 1 AND 5), absence_groups AS (SELECT Employee_ID, department, COUNT(*) AS consecutive_absences FROM absences WHERE Punch_In IS NULL GROUP BY Employee_ID, department, (rn - absent_rn) HAVING consecutive_absences >= 4) SELECT department, COUNT(DISTINCT Employee_ID) AS employees_with_absences FROM absence_groups GROUP BY department ORDER BY employees_with_absences DESC"
    },
    {
        "question": "Which employees were continuously absent for the entire last week of April 2025 (April 28–30)?",
        "clickhouse_sql": "WITH absences AS (SELECT Employee_ID, First_Name, Last_Name, department, Attendance_Date, Punch_In FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-28' AND '2025-04-30' AND toDayOfWeek(toDate(Attendance_Date)) BETWEEN 1 AND 3), absence_counts AS (SELECT Employee_ID, First_Name, Last_Name, department, COUNT(*) AS absent_days FROM absences WHERE Punch_In IS NULL GROUP BY Employee_ID, First_Name, Last_Name, department) SELECT Employee_ID, First_Name, Last_Name, department, absent_days FROM absence_counts WHERE absent_days = 3 ORDER BY Employee_ID"
    },
    {
        "question": "Which employees had 5 or more consecutive absences and low productivity (< 30%) before absence in April 2025?",
        "clickhouse_sql": "WITH absences AS (SELECT Employee_ID, First_Name, Last_Name, department, Attendance_Date, Punch_In, COALESCE(Productive_Percent, 0) AS prod_percent, row_number() OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS rn, row_number() OVER (PARTITION BY Employee_ID, CASE WHEN Punch_In IS NULL THEN 1 ELSE 0 END ORDER BY Attendance_Date) AS absent_rn, lag(prod_percent) OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS prev_prod FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(Attendance_Date)) BETWEEN 1 AND 5), absence_groups AS (SELECT Employee_ID, First_Name, Last_Name, department, COUNT(*) AS consecutive_absences, MIN(prev_prod) AS pre_absence_prod FROM absences WHERE Punch_In IS NULL GROUP BY Employee_ID, First_Name, Last_Name, department, (rn - absent_rn) HAVING consecutive_absences >= 5 AND pre_absence_prod < 30) SELECT Employee_ID, First_Name, Last_Name, department, consecutive_absences, pre_absence_prod FROM absence_groups ORDER BY consecutive_absences DESC"
    },
    {
        "question": "Which employees had 3 or more consecutive absences with no application activity in April 2025?",
        "clickhouse_sql": "WITH absences AS (SELECT m.Employee_ID, m.First_Name, m.Last_Name, m.department, m.Attendance_Date, m.Punch_In, COUNT(a.Application) AS app_count, row_number() OVER (PARTITION BY m.Employee_ID ORDER BY m.Attendance_Date) AS rn, row_number() OVER (PARTITION BY m.Employee_ID, CASE WHEN m.Punch_In IS NULL AND app_count = 0 THEN 1 ELSE 0 END ORDER BY m.Attendance_Date) AS absent_rn FROM employee_metrics m LEFT JOIN employee_activity a ON m.Employee_ID = a.Employee_ID AND m.Attendance_Date = a.Date WHERE toDate(m.Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(m.Attendance_Date)) BETWEEN 1 AND 5 GROUP BY m.Employee_ID, m.First_Name, m.Last_Name, m.department, m.Attendance_Date, m.Punch_In), absence_groups AS (SELECT Employee_ID, First_Name, Last_Name, department, COUNT(*) AS consecutive_absences FROM absences WHERE Punch_In IS NULL AND app_count = 0 GROUP BY Employee_ID, First_Name, Last_Name, department, (rn - absent_rn) HAVING consecutive_absences >= 3) SELECT Employee_ID, First_Name, Last_Name, department, consecutive_absences FROM absence_groups ORDER BY consecutive_absences DESC"
    },
    {
        "question": "Which employees had 5 or more consecutive days of declining productivity in April 2025?",
        "clickhouse_sql": "WITH prod_seq AS (SELECT Employee_ID, First_Name, Last_Name, department, Attendance_Date, COALESCE(Productive_Percent, 0) AS prod_percent, lag(COALESCE(Productive_Percent, 0)) OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS prev_prod, row_number() OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS rn, row_number() OVER (PARTITION BY Employee_ID, CASE WHEN COALESCE(Productive_Percent, 0) < lag(COALESCE(Productive_Percent, 0)) OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) THEN 1 ELSE 0 END ORDER BY Attendance_Date) AS decline_rn FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(Attendance_Date)) BETWEEN 1 AND 5 AND Punch_In IS NOT NULL), decline_groups AS (SELECT Employee_ID, First_Name, Last_Name, department, COUNT(*) AS consecutive_decline_days FROM prod_seq WHERE prod_percent < prev_prod GROUP BY Employee_ID, First_Name, Last_Name, department, (rn - decline_rn) HAVING consecutive_decline_days >= 5) SELECT Employee_ID, First_Name, Last_Name, department, consecutive_decline_days FROM decline_groups ORDER BY consecutive_decline_days DESC"
    },
    {
        "question": "Which employees had 3 or more consecutive workdays with declining productivity and increasing break duration in April 2025?",
        "clickhouse_sql": "WITH prod_seq AS (SELECT Employee_ID, First_Name, Last_Name, department, Attendance_Date, COALESCE(Productive_Percent, 0) AS prod_percent, toUInt32(splitByChar(':', COALESCE(Break_Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(Break_Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(Break_Duration, '0:0:0'))[3]) AS break_secs, lag(COALESCE(Productive_Percent, 0)) OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS prev_prod, lag(toUInt32(splitByChar(':', COALESCE(Break_Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(Break_Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(Break_Duration, '0:0:0'))[3])) OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS prev_break_secs, row_number() OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS rn, row_number() OVER (PARTITION BY Employee_ID, CASE WHEN COALESCE(Productive_Percent, 0) < lag(COALESCE(Productive_Percent, 0)) OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AND (toUInt32(splitByChar(':', COALESCE(Break_Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(Break_Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(Break_Duration, '0:0:0'))[3])) > lag(toUInt32(splitByChar(':', COALESCE(Break_Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(Break_Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(Break_Duration, '0:0:0'))[3])) OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) THEN 1 ELSE 0 END ORDER BY Attendance_Date) AS decline_rn FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(Attendance_Date)) BETWEEN 1 AND 5 AND Punch_In IS NOT NULL), decline_groups AS (SELECT Employee_ID, First_Name, Last_Name, department, COUNT(*) AS consecutive_decline_days FROM prod_seq WHERE prod_percent < prev_prod AND break_secs > prev_break_secs GROUP BY Employee_ID, First_Name, Last_Name, department, (rn - decline_rn) HAVING consecutive_decline_days >= 3) SELECT Employee_ID, First_Name, Last_Name, department, consecutive_decline_days FROM decline_groups ORDER BY consecutive_decline_days DESC"
    },
    {
        "question": "Which employees had 4 or more consecutive days of declining productivity with zero key presses in April 2025?",
        "clickhouse_sql": "WITH prod_seq AS (SELECT m.Employee_ID, m.First_Name, m.Last_Name, m.department, m.Attendance_Date, COALESCE(m.Productive_Percent, 0) AS prod_percent, COALESCE(a.Key_Presses, 0) AS key_presses, lag(COALESCE(m.Productive_Percent, 0)) OVER (PARTITION BY m.Employee_ID ORDER BY m.Attendance_Date) AS prev_prod, row_number() OVER (PARTITION BY m.Employee_ID ORDER BY m.Attendance_Date) AS rn, row_number() OVER (PARTITION BY m.Employee_ID, CASE WHEN COALESCE(m.Productive_Percent, 0) < lag(COALESCE(m.Productive_Percent, 0)) OVER (PARTITION BY m.Employee_ID ORDER BY m.Attendance_Date) AND COALESCE(a.Key_Presses, 0) = 0 THEN 1 ELSE 0 END ORDER BY m.Attendance_Date) AS decline_rn FROM employee_metrics m LEFT JOIN employee_activity a ON m.Employee_ID = a.Employee_ID AND m.Attendance_Date = a.Date WHERE toDate(m.Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(m.Attendance_Date)) BETWEEN 1 AND 5 AND m.Punch_In IS NOT NULL), decline_groups AS (SELECT Employee_ID, First_Name, Last_Name, department, COUNT(*) AS consecutive_decline_days FROM prod_seq WHERE prod_percent < prev_prod AND key_presses = 0 GROUP BY Employee_ID, First_Name, Last_Name, department, (rn - decline_rn) HAVING consecutive_decline_days >= 4) SELECT Employee_ID, First_Name, Last_Name, department, consecutive_decline_days FROM decline_groups ORDER BY consecutive_decline_days DESC"
    },
    {
        "question": "Which teams had employees with 5 or more consecutive days of declining productivity in April 2025?",
        "clickhouse_sql": "WITH prod_seq AS (SELECT Employee_ID, First_Name, Last_Name, Group_Name, Attendance_Date, COALESCE(Productive_Percent, 0) AS prod_percent, lag(COALESCE(Productive_Percent, 0)) OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS prev_prod, row_number() OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS rn, row_number() OVER (PARTITION BY Employee_ID, CASE WHEN COALESCE(Productive_Percent, 0) < lag(COALESCE(Productive_Percent, 0)) OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) THEN 1 ELSE 0 END ORDER BY Attendance_Date) AS decline_rn FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(Attendance_Date)) BETWEEN 1 AND 5 AND Punch_In IS NOT NULL), decline_groups AS (SELECT Employee_ID, First_Name, Last_Name, Group_Name, COUNT(*) AS consecutive_decline_days FROM prod_seq WHERE prod_percent < prev_prod GROUP BY Employee_ID, First_Name, Last_Name, Group_Name, (rn - decline_rn) HAVING consecutive_decline_days >= 5) SELECT Group_Name AS team, Employee_ID, First_Name, Last_Name, consecutive_decline_days FROM decline_groups ORDER BY Group_Name, consecutive_decline_days DESC"
    },
    {
        "question": "Which employees had a weekly productivity decline across all weeks in April 2025?",
        "clickhouse_sql": "WITH weekly_prod AS (SELECT Employee_ID, First_Name, Last_Name, department, toStartOfWeek(toDate(Attendance_Date), 1) AS week_start, AVG(COALESCE(Productive_Percent, 0)) AS avg_prod FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(Attendance_Date)) BETWEEN 1 AND 5 AND Punch_In IS NOT NULL GROUP BY Employee_ID, First_Name, Last_Name, department, week_start), prod_diff AS (SELECT Employee_ID, First_Name, Last_Name, department, week_start, avg_prod, lag(avg_prod) OVER (PARTITION BY Employee_ID ORDER BY week_start) AS prev_prod FROM weekly_prod WHERE week_start IN ('2025-04-01', '2025-04-08', '2025-04-15', '2025-04-22', '2025-04-30')) SELECT Employee_ID, First_Name, Last_Name, department FROM prod_diff WHERE prev_prod IS NOT NULL GROUP BY Employee_ID, First_Name, Last_Name, department HAVING COUNT(*) = 4 AND EVERY(avg_prod < prev_prod) ORDER BY Employee_ID"
    },
    {
        "question": "Which employees had 3 or more consecutive days of declining productivity and increasing social media usage in April 2025?",
        "clickhouse_sql": "WITH prod_seq AS (SELECT m.Employee_ID, m.First_Name, m.Last_Name, m.department, m.Attendance_Date, COALESCE(m.Productive_Percent, 0) AS prod_percent, SUM(toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[3])) AS social_secs, lag(COALESCE(m.Productive_Percent, 0)) OVER (PARTITION BY m.Employee_ID ORDER BY m.Attendance_Date) AS prev_prod, lag(SUM(toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[3]))) OVER (PARTITION BY m.Employee_ID ORDER BY m.Attendance_Date) AS prev_social_secs, row_number() OVER (PARTITION BY m.Employee_ID ORDER BY m.Attendance_Date) AS rn, row_number() OVER (PARTITION BY m.Employee_ID, CASE WHEN COALESCE(m.Productive_Percent, 0) < lag(COALESCE(m.Productive_Percent, 0)) OVER (PARTITION BY m.Employee_ID ORDER BY m.Attendance_Date) AND SUM(toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[3])) > lag(SUM(toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[3]))) OVER (PARTITION BY m.Employee_ID ORDER BY m.Attendance_Date) THEN 1 ELSE 0 END ORDER BY m.Attendance_Date) AS decline_rn FROM employee_metrics m LEFT JOIN employee_activity a ON m.Employee_ID = a.Employee_ID AND m.Attendance_Date = a.Date WHERE toDate(m.Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(m.Attendance_Date)) BETWEEN 1 AND 5 AND m.Punch_In IS NOT NULL AND a.Application IN ('Facebook', 'Twitter', 'Instagram', 'LinkedIn') GROUP BY m.Employee_ID, m.First_Name, m.Last_Name, m.department, m.Attendance_Date, m.Productive_Percent), decline_groups AS (SELECT Employee_ID, First_Name, Last_Name, department, COUNT(*) AS consecutive_decline_days FROM prod_seq WHERE prod_percent < prev_prod AND social_secs > prev_social_secs GROUP BY Employee_ID, First_Name, Last_Name, department, (rn - decline_rn) HAVING consecutive_decline_days >= 3) SELECT Employee_ID, First_Name, Last_Name, department, consecutive_decline_days FROM decline_groups ORDER BY consecutive_decline_days DESC"
    },
    {
        "question": "Which employees had 4 or more consecutive days of declining productivity in the last two weeks of April 2025?",
        "clickhouse_sql": "WITH prod_seq AS (SELECT Employee_ID, First_Name, Last_Name, department, Attendance_Date, COALESCE(Productive_Percent, 0) AS prod_percent, lag(COALESCE(Productive_Percent, 0)) OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS prev_prod, row_number() OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS rn, row_number() OVER (PARTITION BY Employee_ID, CASE WHEN COALESCE(Productive_Percent, 0) < lag(COALESCE(Productive_Percent, 0)) OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) THEN 1 ELSE 0 END ORDER BY Attendance_Date) AS decline_rn FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-16' AND '2025-04-30' AND toDayOfWeek(toDate(Attendance_Date)) BETWEEN 1 AND 5 AND Punch_In IS NOT NULL), decline_groups AS (SELECT Employee_ID, First_Name, Last_Name, department, COUNT(*) AS consecutive_decline_days FROM prod_seq WHERE prod_percent < prev_prod GROUP BY Employee_ID, First_Name, Last_Name, department, (rn - decline_rn) HAVING consecutive_decline_days >= 4) SELECT Employee_ID, First_Name, Last_Name, department, consecutive_decline_days FROM decline_groups ORDER BY consecutive_decline_days DESC"
    },
    {
        "question": "Which employees had declining productivity for 3 or more consecutive days with low online duration (< 4 hours) in April 2025?",
        "clickhouse_sql": "WITH prod_seq AS (SELECT Employee_ID, First_Name, Last_Name, department, Attendance_Date, COALESCE(Productive_Percent, 0) AS prod_percent, (toUInt32(splitByChar(':', COALESCE(Online_Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(Online_Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(Online_Duration, '0:0:0'))[3])) / 3600 AS online_hours, lag(COALESCE(Productive_Percent, 0)) OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS prev_prod, row_number() OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS rn, row_number() OVER (PARTITION BY Employee_ID, CASE WHEN COALESCE(Productive_Percent, 0) < lag(COALESCE(Productive_Percent, 0)) OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AND (toUInt32(splitByChar(':', COALESCE(Online_Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(Online_Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(Online_Duration, '0:0:0'))[3])) / 3600 < 4 THEN 1 ELSE 0 END ORDER BY Attendance_Date) AS decline_rn FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(Attendance_Date)) BETWEEN 1 AND 5 AND Punch_In IS NOT NULL), decline_groups AS (SELECT Employee_ID, First_Name, Last_Name, department, COUNT(*) AS consecutive_decline_days FROM prod_seq WHERE prod_percent < prev_prod AND online_hours < 4 GROUP BY Employee_ID, First_Name, Last_Name, department, (rn - decline_rn) HAVING consecutive_decline_days >= 3) SELECT Employee_ID, First_Name, Last_Name, department, consecutive_decline_days FROM decline_groups ORDER BY consecutive_decline_days DESC"
    },
    {
        "question": "Which departments had the most employees with 4 or more consecutive days of declining productivity in April 2025?",
        "clickhouse_sql": "WITH prod_seq AS (SELECT Employee_ID, First_Name, Last_Name, department, Attendance_Date, COALESCE(Productive_Percent, 0) AS prod_percent, lag(COALESCE(Productive_Percent, 0)) OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS prev_prod, row_number() OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS rn, row_number() OVER (PARTITION BY Employee_ID, CASE WHEN COALESCE(Productive_Percent, 0) < lag(COALESCE(Productive_Percent, 0)) OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) THEN 1 ELSE 0 END ORDER BY Attendance_Date) AS decline_rn FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(Attendance_Date)) BETWEEN 1 AND 5 AND Punch_In IS NOT NULL), decline_groups AS (SELECT Employee_ID, department, COUNT(*) AS consecutive_decline_days FROM prod_seq WHERE prod_percent < prev_prod GROUP BY Employee_ID, department, (rn - decline_rn) HAVING consecutive_decline_days >= 4) SELECT department, COUNT(DISTINCT Employee_ID) AS employees_with_decline FROM decline_groups GROUP BY department ORDER BY employees_with_decline DESC"
    },
    {
        "question": "Which employees had both 3 or more consecutive absences and 3 or more consecutive days of declining productivity in April 2025?",
        "clickhouse_sql": "WITH absences AS (SELECT Employee_ID, Attendance_Date, Punch_In, row_number() OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS rn, row_number() OVER (PARTITION BY Employee_ID, CASE WHEN Punch_In IS NULL THEN 1 ELSE 0 END ORDER BY Attendance_Date) AS absent_rn FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(Attendance_Date)) BETWEEN 1 AND 5), absence_groups AS (SELECT Employee_ID, COUNT(*) AS consecutive_absences FROM absences WHERE Punch_In IS NULL GROUP BY Employee_ID, (rn - absent_rn) HAVING consecutive_absences >= 3), prod_seq AS (SELECT Employee_ID, First_Name, Last_Name, department, Attendance_Date, COALESCE(Productive_Percent, 0) AS prod_percent, lag(COALESCE(Productive_Percent, 0)) OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS prev_prod, row_number() OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) AS rn, row_number() OVER (PARTITION BY Employee_ID, CASE WHEN COALESCE(Productive_Percent, 0) < lag(COALESCE(Productive_Percent, 0)) OVER (PARTITION BY Employee_ID ORDER BY Attendance_Date) THEN 1 ELSE 0 END ORDER BY Attendance_Date) AS decline_rn FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' AND toDayOfWeek(toDate(Attendance_Date)) BETWEEN 1 AND 5 AND Punch_In IS NOT NULL), decline_groups AS (SELECT Employee_ID, First_Name, Last_Name, department, COUNT(*) AS consecutive_decline_days FROM prod_seq WHERE prod_percent < prev_prod GROUP BY Employee_ID, First_Name, Last_Name, department, (rn - decline_rn) HAVING consecutive_decline_days >= 3) SELECT d.Employee_ID, d.First_Name, d.Last_Name, d.department, a.consecutive_absences, d.consecutive_decline_days FROM decline_groups d JOIN absence_groups a ON d.Employee_ID = a.Employee_ID ORDER BY consecutive_absences DESC, consecutive_decline_days DESC"
    }
  ]

# Add these examples to your training_examples list
null_handling_examples = [
    {
        "question": "Show departments with productivity data, properly handling NULL values",
        "clickhouse_sql": "SELECT department, AVG(COALESCE(productive_percent, 0)) AS avg_productivity, COUNT(*) AS total_records, COUNTIf(productive_percent IS NOT NULL) AS records_with_data FROM attendance_data GROUP BY department ORDER BY avg_productivity DESC"
    },
    {
        "question": "Calculate productivity ratios safely avoiding division by zero",
        "clickhouse_sql": "SELECT department, AVG(productive_duration) AS avg_productive, AVG(unproductive_duration) AS avg_unproductive, AVG(productive_duration) / NULLIF(AVG(unproductive_duration), 0) AS safe_ratio FROM attendance_data GROUP BY department ORDER BY safe_ratio DESC"
    },
    {
        "question": "Count employees with missing productivity data",
        "clickhouse_sql": "SELECT department, COUNT(*) AS total_employees, COUNTIf(avg_productive_percent IS NULL) AS missing_data FROM employee_duration_summary GROUP BY department ORDER BY missing_data DESC"
    },
    {
        "question": "Show team productivity trends with missing data filled with zeros",
        "clickhouse_sql": "SELECT shift_name, formatDateTime(toStartOfWeek(attendance_date), '%Y-%m-%d') AS week, AVG(COALESCE(productive_percent, 0)) AS avg_productivity FROM attendance_data WHERE toDate(attendance_date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY shift_name, week ORDER BY shift_name, week"
    }
]

# Add these just before the "Add these to your training examples" section
safe_productivity_examples = [
    {
        "question": "Which teams had productivity decline between weeks?",
        "clickhouse_sql": "SELECT Group_Name AS team, ROUND(COALESCE(prev_half_productivity, 0) - COALESCE(last_half_productivity, 0), 2) AS productivity_decline FROM (SELECT Group_Name, AVGIf(COALESCE(Productive_Percent, 0), toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-15') AS prev_half_productivity, AVGIf(COALESCE(Productive_Percent, 0), toDate(Attendance_Date) BETWEEN '2025-04-16' AND '2025-04-30') AS last_half_productivity FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY Group_Name HAVING prev_half_productivity IS NOT NULL AND last_half_productivity IS NOT NULL) WHERE prev_half_productivity > last_half_productivity ORDER BY productivity_decline DESC LIMIT 3"
    },
    {
        "question": "Show teams with most sudden productivity decline",
        "clickhouse_sql": "SELECT Group_Name AS team, ROUND(COALESCE(prev_half_productivity, 0) - COALESCE(last_half_productivity, 0), 2) AS productivity_decline FROM (SELECT Group_Name, AVGIf(COALESCE(Productive_Percent, 0), toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-15') AS prev_half_productivity, AVGIf(COALESCE(Productive_Percent, 0), toDate(Attendance_Date) BETWEEN '2025-04-16' AND '2025-04-30') AS last_half_productivity FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY Group_Name HAVING prev_half_productivity IS NOT NULL AND last_half_productivity IS NOT NULL) WHERE prev_half_productivity > last_half_productivity ORDER BY productivity_decline DESC LIMIT 3" 
    },
    {
        "question": "Compare productivity ratios between teams safely",
        "clickhouse_sql": "SELECT Group_Name AS team, AVG(Productive_Duration) AS avg_productive, AVG(Unproductive_Duration) AS avg_unproductive, AVG(Productive_Duration) / NULLIF(AVG(Unproductive_Duration), 0.001) AS productivity_ratio FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY team ORDER BY productivity_ratio DESC"
    }
]
training_examples = [
    {
        "question": "Show me employees with productivity below 50%",
        "clickhouse_sql": "SELECT employee_id, first_name, last_name, department, avg_productive_percent FROM employee_duration_summary WHERE avg_productive_percent < 50' ORDER BY avg_productive_percent ASC LIMIT 5"
    },
    {
        "question": "What is the average productivity of each department?",
        "clickhouse_sql": "SELECT department, AVG(avg_productive_percent) AS avg_productivity FROM employee_duration_summary GROUP BY department ORDER BY avg_productivity DESC"
    },
    {
        "question": "List employees with high active time but low productivity",
        "clickhouse_sql": "SELECT employee_id, first_name, last_name, department, avg_active_percent FROM employee_duration_summary WHERE avg_active_percent > 75 AND avg_productive_percent < 50 ORDER BY avg_active_percent DESC"
    },
    {
        "question": "all data tables?",
        "clickhouse_sql": "SHOW TABLES"
    },
    {
        "question": "What is the average productivity of each department?",
        "clickhouse_sql": "SELECT department, AVG(avg_productive_percent) AS avg_productivity FROM employee_duration_summary GROUP BY department ORDER BY avg_productivity DESC"
    },
    {
        "question": "List employees with high active time but low productivity",
        "clickhouse_sql": "SELECT employee_id, first_name, last_name, department, avg_active_percent FROM employee_duration_summary WHERE avg_active_percent > 75 AND avg_productive_percent < 50 ORDER BY avg_active_percent DESC"
    },
    {
        "question": "Which team has the highest average productive hours?",
        "clickhouse_sql": "SELECT Group_Name, AVG(if(isFinite(Productive_Percent), Productive_Percent, 0)) AS avg_productivity FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY Group_Name ORDER BY avg_productivity DESC LIMIT 1"
    },
    {
        "question": "give names of top 10 most productive teams",
        "clickhouse_sql": "SELECT Group_Name, AVG(if(isFinite(Productive_Percent), Productive_Percent, 0)) AS avg_productivity FROM employee_metrics WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY Group_Name ORDER BY avg_productivity DESC LIMIT 10"
    },
    {
        "question": "show me employees with low productivity",
        "clickhouse_sql": "SELECT employee_id, first_name, last_name, department, avg_productive_percent FROM employee_duration_summary WHERE avg_productive_percent < 50 ORDER BY avg_productive_percent ASC LIMIT 5"
    },
    {
        "question": "get me all teams with productivity less than 29",
        "clickhouse_sql": "SELECT Group_Name FROM employee_metrics GROUP BY Group_Name HAVING AVG(Productive_Percent) < 29"
    },
    {
        "question": "Find employees with high active time but low productivity",
        "clickhouse_sql": "SELECT employee_id, first_name, last_name, department, avg_active_percent FROM employee_duration_summary WHERE avg_active_percent > 75 AND avg_productive_percent < 50 ORDER BY avg_active_percent DESC"
    },
    {
        "question": "Which day had the highest attendance?",
        "clickhouse_sql": "SELECT attendance_date, COUNT(*) AS total_attendance FROM attendance_data GROUP BY attendance_date ORDER BY total_attendance DESC LIMIT 1"
    },
    {
        "question": "Which department has the highest average productive hours?",
        "clickhouse_sql": "SELECT department, avg_productive_duration FROM department_duration_summary ORDER BY avg_productive_duration DESC LIMIT 1"
    },
    {
        "question": "Show attendance data for IT department",
        "clickhouse_sql": "SELECT * FROM attendance_data WHERE shift_name = 'IT' ORDER BY attendance_date DESC LIMIT 20"
    },
    {
        "question": "List employees with over 80% productivity",
        "clickhouse_sql": "SELECT employee_id, first_name, last_name, department, avg_productive_percent FROM employee_duration_summary WHERE avg_productive_percent > 80 ORDER BY avg_productive_percent DESC"
    },
    {
        "question": "Show me departments with highest attendance rates",
        "clickhouse_sql": "SELECT department, AVG(attendance_rate) AS avg_attendance FROM employee_attendance_summary GROUP BY department ORDER BY avg_attendance DESC LIMIT 5"
    },
    {
        "question": "Find employees with high productivity but low active time",
        "clickhouse_sql": "SELECT employee_id, first_name, last_name, department, avg_productive_percent, avg_active_percent FROM employee_duration_summary WHERE avg_productive_percent > 75 AND avg_active_percent < 50 ORDER BY avg_productive_percent DESC, avg_active_percent ASC"
    },
    {
        "question": "Which employees show decreasing productivity over the last month?",
        "clickhouse_sql": "WITH daily_productivity AS (SELECT employee_id, attendance_date, productive_percent FROM attendance_data WHERE attendance_date >= date_sub(DAY, 30, today())) SELECT a.employee_id, a.first_name, a.last_name, a.department, corr(toDayOfMonth(d.attendance_date), d.productive_percent) AS productivity_trend FROM employee_duration_summary a JOIN daily_productivity d ON a.employee_id = d.employee_id GROUP BY a.employee_id, a.first_name, a.last_name, a.department HAVING productivity_trend < -0.5 ORDER BY productivity_trend ASC"
    },
    {
        "question": "Compare productivity between marketing and sales departments",
        "clickhouse_sql": "SELECT department, AVG(avg_productive_percent) AS avg_productivity, AVG(avg_active_percent) AS avg_activity FROM employee_duration_summary WHERE department IN ('Marketing', 'Sales') GROUP BY department ORDER BY avg_productivity DESC"
    },
    {
        "question": "Which employees have the highest keyboard activity?",
        "clickhouse_sql": "SELECT Employee, Employee_ID, Team, SUM(Key_Presses) AS total_key_presses, SUM(Mouse_Clicks) AS total_mouse_clicks FROM employee_usage_data GROUP BY Employee, Employee_ID, Team ORDER BY total_key_presses DESC LIMIT 15"
    },
    {
        "question": "Find departments with productivity below company average",
        "clickhouse_sql": "WITH company_avg AS (SELECT AVG(avg_productive_percent) AS avg_productive_percent FROM overall_summary) SELECT d.department, d.avg_productive_percent, d.avg_active_percent FROM department_duration_summary d, company_avg c WHERE d.avg_productive_percent < c.avg_productive_percent ORDER BY d.avg_productive_percent ASC"
    },
    {
        "question": "Who has the most absences in the last quarter?",
        "clickhouse_sql": "SELECT e.employee_id, e.first_name, e.last_name, e.department, e.total_days - e.days_present AS absent_days FROM employee_attendance_summary e WHERE e.total_days > 0 ORDER BY absent_days DESC LIMIT 10"
    },
    {
        "question": "Show me daily attendance trends over the past month",
        "clickhouse_sql": "SELECT attendance_date, total_employees, employees_present, (employees_present / total_employees) * 100 AS attendance_percentage FROM daily_attendance_summary WHERE attendance_date >= date_sub(DAY, 30, today()) ORDER BY attendance_date"
    },
    {
        "question": "Which day of the week has the lowest attendance rate?",
        "clickhouse_sql": "SELECT formatDateTime(attendance_date, '%A') AS day_of_week, AVG(employees_present / total_employees) * 100 AS avg_attendance_rate FROM daily_attendance_summary GROUP BY day_of_week ORDER BY avg_attendance_rate ASC LIMIT 1"
    },
    {
        "question": "Find employees who are consistently late to their shifts",
        "clickhouse_sql": "SELECT employee_id, first_name, last_name, COUNT(*) AS late_days FROM attendance_data WHERE (punch_in - shift_start_dt) > 900 AND punch_in IS NOT NULL AND shift_start_dt IS NOT NULL GROUP BY employee_id, first_name, last_name HAVING late_days >= 5 ORDER BY late_days DESC"
    },
    {
        "question": "Identify teams with perfect attendance last week",
        "clickhouse_sql": "SELECT shift_name, COUNT(DISTINCT employee_id) AS team_size FROM attendance_data WHERE attendance_date >= date_sub(DAY, 7, today()) GROUP BY shift_name HAVING COUNT(DISTINCT employee_id) = countIf(punch_in IS NOT NULL, employee_id)"
    },
    {
        "question": "Which applications do employees spend most time on?",
        "clickhouse_sql": "SELECT Application, SUM(toUInt32(splitByChar(':', Duration)[1]) * 3600 + toUInt32(splitByChar(':', Duration)[2]) * 60 + toUInt32(splitByChar(':', Duration)[3])) AS total_seconds FROM employee_usage_data GROUP BY Application ORDER BY total_seconds DESC LIMIT 10"
    },
    {
        "question": "Show me time spent on unproductive websites by department",
        "clickhouse_sql": "SELECT e.department, SUM(toUInt32(splitByChar(':', u.Duration)[1]) * 3600 + toUInt32(splitByChar(':', u.Duration)[2]) * 60 + toUInt32(splitByChar(':', u.Duration)[3])) / 3600 AS hours_spent FROM employee_usage_data u JOIN employee_duration_summary e ON u.Employee_ID = toString(e.employee_id) WHERE u.Mapping_Status = 'Unproductive' AND u.URL IS NOT NULL GROUP BY e.department ORDER BY hours_spent DESC"
    },
    {
        "question": "Compare idle time between departments",
        "clickhouse_sql": "SELECT department, avg_idle_duration, ROUND((avg_idle_duration / (avg_online_duration + 0.001)) * 100, 2) AS idle_percentage FROM department_duration_summary ORDER BY idle_percentage DESC"
    },
    {
        "question": "Find employees who spend more than 2 hours daily on social media",
        "clickhouse_sql": "SELECT u.Employee_ID, e.first_name, e.last_name, e.department, AVG(toUInt32(splitByChar(':', u.Duration)[1]) * 3600 + toUInt32(splitByChar(':', u.Duration)[2]) * 60 + toUInt32(splitByChar(':', u.Duration)[3])) / 3600 AS avg_daily_hours FROM employee_usage_data u JOIN employee_duration_summary e ON u.Employee_ID = toString(e.employee_id) WHERE u.URL LIKE '%facebook%' OR u.URL LIKE '%instagram%' OR u.URL LIKE '%twitter%' OR u.URL LIKE '%linkedin%' GROUP BY u.Employee_ID, e.first_name, e.last_name, e.department HAVING avg_daily_hours > 2 ORDER BY avg_daily_hours DESC"
    },
    {
        "question": "Which team has the highest break duration?",
        "clickhouse_sql": "SELECT shift_name AS team, AVG(break_duration) / 3600 AS avg_break_hours FROM attendance_data WHERE shift_name IS NOT NULL GROUP BY shift_name ORDER BY avg_break_hours DESC LIMIT 1"
    },
    {
        "question": "List employees who consistently miss their goal hours",
        "clickhouse_sql": "SELECT employee_id, first_name, last_name, department, Deficit FROM employee_duration_summary WHERE Deficit < 0 ORDER BY Deficit ASC LIMIT 20"
    },
    {
        "question": "Show departments exceeding their goal hours on average",
        "clickhouse_sql": "SELECT department, avg_online_goal_hours, avg_online_duration, (avg_online_duration - avg_online_goal_hours) AS avg_surplus FROM department_duration_summary WHERE avg_online_duration > avg_online_goal_hours ORDER BY avg_surplus DESC"
    },
    {
        "question": "Find employees who meet goals with minimal activity",
        "clickhouse_sql": "SELECT employee_id, first_name, last_name, department, avg_online_duration, avg_online_goal_hours, avg_active_percent FROM employee_duration_summary WHERE avg_online_duration >= avg_online_goal_hours AND avg_active_percent < 60 ORDER BY avg_active_percent ASC"
    },
    {
        "question": "Calculate what percentage of employees are meeting their goals",
        "clickhouse_sql": "SELECT (sum(Deficit >= 0) * 100.0 / count(*)) AS percentage_meeting_goals FROM employee_duration_summary"
    },
    {
        "question": "Which department has the largest deficit in goal hours?",
        "clickhouse_sql": "SELECT department, Deficit FROM department_duration_summary ORDER BY Deficit ASC LIMIT 1"
    },
    {
        "question": "Compare top and bottom 10% of employees by productivity",
        "clickhouse_sql": "WITH ranked_employees AS (SELECT employee_id, first_name, last_name, department, avg_productive_percent, NTILE(10) OVER (ORDER BY avg_productive_percent) AS productivity_decile FROM employee_duration_summary) SELECT productivity_decile, AVG(avg_productive_percent) AS avg_productivity FROM ranked_employees WHERE productivity_decile IN (1, 10) GROUP BY productivity_decile ORDER BY productivity_decile"
    },
    {
        "question": "Show employees with productivity significantly above their department average",
        "clickhouse_sql": "WITH dept_avg AS (SELECT department, AVG(avg_productive_percent) AS dept_avg_productivity FROM employee_duration_summary GROUP BY department) SELECT e.employee_id, e.first_name, e.last_name, e.department, e.avg_productive_percent, d.dept_avg_productivity, (e.avg_productive_percent - d.dept_avg_productivity) AS productivity_difference FROM employee_duration_summary e JOIN dept_avg d ON e.department = d.department WHERE (e.avg_productive_percent - d.dept_avg_productivity) > 20 ORDER BY productivity_difference DESC"
    },
    {
        "question": "Identify employees with consistently high keyboard activity but low productivity",
        "clickhouse_sql": "WITH employee_keyboard AS (SELECT Employee_ID, AVG(Key_Presses) AS avg_keys FROM employee_usage_data GROUP BY Employee_ID) SELECT e.employee_id, e.first_name, e.last_name, e.department, e.avg_productive_percent, k.avg_keys FROM employee_duration_summary e JOIN employee_keyboard k ON toString(e.employee_id) = k.Employee_ID WHERE k.avg_keys > (SELECT AVG(Key_Presses) * 1.5 FROM employee_usage_data) AND e.avg_productive_percent < 50 ORDER BY k.avg_keys DESC, e.avg_productive_percent ASC"
    },
    {
        "question": "Compare performance of new vs veteran employees",
        "clickhouse_sql": "WITH employee_tenure AS (SELECT employee_id, IF(MIN(attendance_date) > date_sub(DAY, 90, today()), 'New', 'Veteran') AS tenure FROM attendance_data GROUP BY employee_id) SELECT t.tenure, COUNT(*) AS employee_count, AVG(e.avg_productive_percent) AS avg_productivity, AVG(e.avg_active_percent) AS avg_activity FROM employee_duration_summary e JOIN employee_tenure t ON e.employee_id = t.employee_id GROUP BY t.tenure ORDER BY t.tenure"
    },
    {
        "question": "Find most improved employees in the last month",
        "clickhouse_sql": "WITH monthly_productivity AS (SELECT employee_id, toYYYYMM(attendance_date) AS month, AVG(productive_percent) AS monthly_productivity FROM attendance_data WHERE attendance_date >= date_sub(DAY, 60, today()) GROUP BY employee_id, month), improvement AS (SELECT m1.employee_id, m1.monthly_productivity - m2.monthly_productivity AS productivity_increase FROM monthly_productivity m1 JOIN monthly_productivity m2 ON m1.employee_id = m2.employee_id AND m1.month > m2.month) SELECT e.employee_id, e.first_name, e.last_name, e.department, i.productivity_increase FROM employee_duration_summary e JOIN improvement i ON e.employee_id = i.employee_id ORDER BY i.productivity_increase DESC LIMIT 10"
    },
    {
        "question": "Which websites are most visited during working hours?",
        "clickhouse_sql": "SELECT URL, COUNT(*) AS visit_count, SUM(toUInt32(splitByChar(':', Duration)[1]) * 3600 + toUInt32(splitByChar(':', Duration)[2]) * 60 + toUInt32(splitByChar(':', Duration)[3])) / 3600 AS total_hours FROM employee_usage_data WHERE URL IS NOT NULL AND URL != '' GROUP BY URL ORDER BY visit_count DESC LIMIT 20"
    },
    {
        "question": "Show me which departments use Microsoft Excel the most",
        "clickhouse_sql": "SELECT e.department, COUNT(*) AS usage_count, SUM(toUInt32(splitByChar(':', COALESCE(u.Duration, '0:0:0'))[1]) * 3600 + toUInt32(splitByChar(':', COALESCE(u.Duration, '0:0:0'))[2]) * 60 + toUInt32(splitByChar(':', COALESCE(u.Duration, '0:0:0'))[3])) / 3600 AS total_hours FROM employee_usage_data u JOIN employee_metrics e ON u.Employee_ID = e.Employee_ID WHERE u.Application LIKE '%Excel%' GROUP BY e.department ORDER BY total_hours DESC"
    },
    {
        "question": "Find employees who spend excessive time on email",
        "clickhouse_sql": "SELECT u.Employee_ID, e.first_name, e.last_name, e.department, SUM(toUInt32(splitByChar(':', u.Duration)[1]) * 3600 + toUInt32(splitByChar(':', u.Duration)[2]) * 60 + toUInt32(splitByChar(':', u.Duration)[3])) / 3600 AS email_hours FROM employee_usage_data u JOIN employee_duration_summary e ON u.Employee_ID = toString(e.employee_id) WHERE u.Application LIKE '%Outlook%' OR u.Application LIKE '%Gmail%' OR u.URL LIKE '%mail.%' GROUP BY u.Employee_ID, e.first_name, e.last_name, e.department HAVING email_hours > 4 ORDER BY email_hours DESC"
    },
    {
        "question": "Compare coding tool usage across development teams",
        "clickhouse_sql": "SELECT e.department, u.Application, COUNT(*) AS usage_count FROM employee_usage_data u JOIN employee_duration_summary e ON u.Employee_ID = toString(e.employee_id) WHERE u.Application IN ('Visual Studio Code', 'IntelliJ IDEA', 'PyCharm', 'Eclipse', 'Visual Studio') AND e.department LIKE '%Development%' GROUP BY e.department, u.Application ORDER BY e.department, usage_count DESC"
    },
    {
        "question": "Which teams spend more time in meetings?",
        "clickhouse_sql": "SELECT e.department, SUM(toUInt32(splitByChar(':', u.Duration)[1]) * 3600 + toUInt32(splitByChar(':', u.Duration)[2]) * 60 + toUInt32(splitByChar(':', u.Duration)[3])) / 3600 AS meeting_hours FROM employee_usage_data u JOIN employee_duration_summary e ON u.Employee_ID = toString(e.employee_id) WHERE u.Application LIKE '%Teams%' OR u.Application LIKE '%Zoom%' OR u.Application LIKE '%Meet%' OR u.Title LIKE '%meeting%' GROUP BY e.department ORDER BY meeting_hours DESC"
    },
    {
        "question": "Show attendance trends by day of week",
        "clickhouse_sql": "SELECT formatDateTime(attendance_date, '%A') AS day_of_week, COUNT(*) AS record_count, AVG(productive_percent) AS avg_productivity FROM attendance_data WHERE attendance_date >= date_sub(DAY, 90, today()) GROUP BY day_of_week ORDER BY toDayOfWeek(MIN(attendance_date))"
    },
    {
        "question": "When do employees typically start and end their workday?",
        "clickhouse_sql": "SELECT toHour(punch_in) AS start_hour, COUNT(*) AS frequency FROM attendance_data WHERE punch_in IS NOT NULL GROUP BY start_hour ORDER BY start_hour"
    },
    {
        "question": "Find productivity patterns throughout the day",
        "clickhouse_sql": "WITH hourly_data AS (SELECT employee_id, attendance_date, toHour(punch_in) AS hour_of_day, productive_percent FROM attendance_data WHERE punch_in IS NOT NULL) SELECT hour_of_day, AVG(productive_percent) AS avg_productivity FROM hourly_data GROUP BY hour_of_day ORDER BY hour_of_day"
    },
    {
        "question": "Show me productivity trends over the past 6 months",
        "clickhouse_sql": "SELECT toYYYYMM(attendance_date) AS month, AVG(productive_percent) AS avg_productivity, AVG(active_percent) AS avg_activity FROM attendance_data WHERE attendance_date >= date_sub(DAY, 180, today()) GROUP BY month ORDER BY month"
    },
    {
        "question": "Identify days with unusual productivity patterns",
        "clickhouse_sql": "WITH daily_stats AS (SELECT attendance_date, AVG(productive_percent) AS day_productivity FROM attendance_data GROUP BY attendance_date), overall_stats AS (SELECT AVG(productive_percent) AS avg_productivity, stddevPop(productive_percent) AS std_productivity FROM attendance_data) SELECT d.attendance_date, d.day_productivity FROM daily_stats d, overall_stats o WHERE ABS(d.day_productivity - o.avg_productivity) > 2 * o.std_productivity ORDER BY ABS(d.day_productivity - o.avg_productivity) DESC"
    },
    {
        "question": "Calculate efficiency score for each department",
        "clickhouse_sql": "SELECT department, AVG(avg_productive_percent) * 0.6 + AVG(avg_active_percent) * 0.3 + (AVG(avg_online_duration) / AVG(avg_online_goal_hours)) * 0.1 AS efficiency_score FROM employee_duration_summary GROUP BY department ORDER BY efficiency_score DESC"
    },
    {
        "question": "Create a comprehensive employee performance index",
        "clickhouse_sql": "WITH attendance_metrics AS (SELECT employee_id, attendance_rate FROM employee_attendance_summary), productivity_metrics AS (SELECT employee_id, avg_productive_percent, avg_active_percent, (avg_online_duration / avg_online_goal_hours) AS goal_achievement FROM employee_duration_summary) SELECT p.employee_id, e.first_name, e.last_name, e.department, (0.4 * p.avg_productive_percent + 0.2 * p.avg_active_percent + 0.2 * a.attendance_rate * 100 + 0.2 * LEAST(p.goal_achievement * 100, 100)) AS performance_index FROM productivity_metrics p JOIN attendance_metrics a ON p.employee_id = a.employee_id JOIN employee_duration_summary e ON p.employee_id = e.employee_id ORDER BY performance_index DESC"
    },
    {
        "question": "Identify teams with balanced work distribution",
        "clickhouse_sql": "WITH team_stats AS (SELECT shift_name, employee_id, AVG(productive_duration) AS avg_productive_time FROM attendance_data WHERE shift_name IS NOT NULL GROUP BY shift_name, employee_id), team_variance AS (SELECT shift_name, stddevPop(avg_productive_time) AS time_stddev, AVG(avg_productive_time) AS time_avg FROM team_stats GROUP BY shift_name) SELECT shift_name, time_avg / 3600 AS avg_productive_hours, time_stddev / 3600 AS stddev_hours, (time_stddev / time_avg) AS coefficient_of_variation FROM team_variance ORDER BY coefficient_of_variation ASC"
    },
    
    {
        "question": "Compare productive vs unproductive application usage patterns",
        "clickhouse_sql": """
SELECT 
    u.Mapping_Status,
    COUNT(DISTINCT u.Application) AS app_count,
    AVG(toUInt32(splitByChar(':', u.Duration)[1]) * 3600 + 
        toUInt32(splitByChar(':', u.Duration)[2]) * 60 + 
        toUInt32(splitByChar(':', u.Duration)[3])) / 3600 AS avg_app_hours,
    AVG(e.avg_productive_percent) AS avg_employee_productivity
FROM employee_usage_data u 
JOIN employee_duration_summary e ON u.Employee_ID = e.employee_id
WHERE u.Mapping_Status IN ('Productive', 'Unproductive')
GROUP BY 
    u.Mapping_Status 
ORDER BY 
    u.Mapping_Status
"""
    },
    {
        "question": "Which employees work most weekends?",
        "clickhouse_sql": """
SELECT 
    e.employee_id,
    e.first_name,
    e.last_name,
    e.department,
    COUNT(DISTINCT a.attendance_date) AS weekend_days
FROM 
    attendance_data a 
    JOIN employee_duration_summary e ON a.employee_id = e.employee_id
WHERE 
    toDayOfWeek(a.attendance_date) IN (1,7)
    AND a.punch_in IS NOT NULL
GROUP BY 
    e.employee_id,
    e.first_name,
    e.last_name,
    e.department
ORDER BY 
    weekend_days DESC
LIMIT 15
"""
    },
    {
        "question": "Find most common applications used by high performers",
        "clickhouse_sql": """
WITH high_performers AS (
    SELECT 
        employee_id 
    FROM employee_metrics 
    WHERE 
        avg_productive_percent > 80
),
top_apps AS (
    SELECT 
        u.Application,
        COUNT(*) AS usage_count 
    FROM employee_usage_data u 
        JOIN high_performers h ON u.employee_id = h.employee_id
    GROUP BY 
        u.Application
    ORDER BY 
        usage_count DESC 
    LIMIT 15
)
SELECT 
    Application,
    usage_count
FROM 
    top_apps
ORDER BY 
    usage_count DESC
"""
    },
    {
        "question": "Analyze keyboard activity vs. mouse usage by department",
        "clickhouse_sql": """
SELECT 
    e.department,
    AVG(u.Key_Presses) AS avg_key_presses,
    AVG(u.Mouse_Clicks) AS avg_mouse_clicks,
    AVG(u.Key_Presses) / NULLIF(AVG(u.Mouse_Clicks), 0) AS key_to_mouse_ratio
FROM 
    employee_usage_data u 
    JOIN employee_duration_summary e ON u.Employee_ID = e.employee_id
GROUP BY 
    e.department
ORDER BY 
    key_to_mouse_ratio DESC
"""
    },
    {
        "question": "Which teams have the highest ratio of productive to unproductive time?",
        "clickhouse_sql": """
SELECT 
    shift_name,
    AVG(productive_duration) AS avg_productive_time,
    AVG(unproductive_duration) AS avg_unproductive_time,
    AVG(productive_duration) / NULLIF(AVG(unproductive_duration), 0) AS productivity_ratio
FROM 
    employee_metrics
WHERE 
    shift_name IS NOT NULL 
GROUP BY 
    shift_name
ORDER BY 
    productivity_ratio DESC
"""
    },
    {
        "question": "Identify employees who might be experiencing burnout",
        "clickhouse_sql": """
WITH overtime_data AS (
    SELECT 
        employee_id,
        COUNT(*) AS overtime_days 
        FROM attendance_data 
        WHERE online_duration > 10 * 3600 
        GROUP BY employee_id
    ),
productivity_trend AS (
    SELECT 
        employee_id,
        corr(toDayOfMonth(attendance_date), productive_percent) AS productivity_slope 
        FROM attendance_data 
        WHERE attendance_date >= date_sub(DAY, 30, today())
        GROUP BY employee_id
    )
SELECT 
    e.employee_id,
    e.first_name,
    e.last_name,
    e.department,
    o.overtime_days,
    p.productivity_slope
FROM 
    employee_duration_summary e 
    JOIN overtime_data o ON e.employee_id = o.employee_id 
    JOIN productivity_trend p ON e.employee_id = p.employee_id 
WHERE 
    overtime_days > 5 
    AND productivity_slope < -0.5 
ORDER BY 
    overtime_days DESC,
    productivity_slope ASC
"""
    },
    {
        "question": "Show teams with the highest balanced work distribution",
        "clickhouse_sql": """
WITH employee_workloads AS (
    SELECT 
        employee_id,
        shift_name,
        SUM(online_duration) AS total_workload 
        FROM attendance_data 
        WHERE shift_name IS NOT NULL 
        GROUP BY employee_id, shift_name
    ),
team_workload_stats AS (
    SELECT 
        shift_name,
        stddevPop(total_workload) / AVG(total_workload) AS coefficient_of_variation
    FROM employee_workloads 
    GROUP BY shift_name
    HAVING 
    COUNT(*) >= 3
)
SELECT 
    shift_name,
    employees AS employee_count,
    ROUND(coefficient_of_variation, 
4) AS avg_coeff_variation
FROM 
    team_workload_stats
ORDER BY 
    coefficient_of_variation ASC LIMIT 10
"""
    },
    {
        "question": "Find managers with the most productive teams",
        "clickhouse_sql": """
SELECT 
    Manager AS manager_name,
    AVG(Productive_Percent_percent) AS avg_team_productivity
FROM employee_metrics
WHERE Manager IS NOT NULL
GROUP BY 
    Manager 
    HAVING COUNT(DISTINCT Employee_ID) >= AS team_size 
ORDER BY 
    avg_team_productivity DESC LIMIT 5
"""
    },
    {
        "question": "Show departments with significant productivity differences between morning vs. afternoon",
        "clickhouse_sql": """
WITH time_productivity AS (
    SELECT 
        e.department,
        a.employee_id,
        CASE 
            WHEN toHour(a.punch_in) < 12 THEN 'Morning' 
            ELSE 'Afternoon' 
        END AS period,
        AVG(productive_percent) AS avg_period_productivity
    FROM attendance_data a 
        JOIN employee_duration_summary e ON a.employee_id = e.employee_id
        WHERE punch_in IS NOT NULL 
        GROUP BY 
        e.employee_id,
        period,
        e.department
    ),
department_time_diff AS (
    SELECT 
        department,
        AVG(CASE WHEN period = 'Morning' THEN avg_period_productivity END AS morning_productivity,
        AVG(CASE WHEN period = 'Afternoon' THEN avg_period_productivity END AS afternoon_productivity
    FROM time_productivity 
    GROUP BY department
    )
SELECT 
    department,
    morning_productivity,
    afternoon_productivity,
    ABS(morning_productivity - afternoon_productivity) AS productivity_difference
FROM 
    department_time_diff 
ORDER BY 
    productivity_difference DESC
"""
    },
    {
        "question": "Identify employees with high productivity and low break duration",
        "clickhouse_sql": """
SELECT 
    e.employee_id,
    e.first_name,
    e.last_name,
    e.department,
    e.avg_productive_percent,
    AVG(toUInt32(splitByChar(':', b.Break_Duration)[1])) * 3600 + 
        toUInt32(splitByChar(':', b.Break_Duration)[2]) * 60 + 
        toUInt32(splitByChar(':', b.Break_Duration)[3])) / total_by_char3600 AS avg_break_hours
FROM employee_duration_summary e 
    JOIN employee_breaks b ON e.employee_id = b.Employee_ID
WHERE 
    toDate(b.Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30'
GROUP BY 
    e.employee_id,
    e.first_name,
    e.last_name,
    e.department
HAVING 
    e.avg_productive_percent > 75 AND 
    avg_break_hours < 0.5 
ORDER BY 
    avg_productive_percent DESC
"""
    },
    {
        "question": "Which teams have the most diverse application usage",
        "clickhouse_sql": """
SELECT 
    a.shift_name,
    COUNT(DISTINCT u.Application) AS unique_apps_count
 
    FROM employee_usage_data u 
    JOIN attendance_data a ON u.Employee_ID = a.employee_id 
    WHERE toDate(a.attendance_date) BETWEEN '2025-04-01' AND '2025-04-30' 
    GROUP BY a.shift_name 
    ORDER BY unique_apps_count 
DESC
"""
    },
    {
        "question": "Which departments had the highest average break duration per employee in April 2025?",
        "clickhouse_sql": """
SELECT 
    department,
    ROUND(AVG(toUInt32(splitByChar(':', COALESCE(Break_Duration, '0:0:0'))[1]) * 3600 +
           toUInt32(splitByChar(':', COALESCE(Break_Duration, '0:0:0'))[2]) * 60 +
           toUInt32(splitByChar(':', COALESCE(Break_Duration, '0:0:0'))[3])) / 3600, 2) AS avg_break_hours,
    ROUND(AVG(COALESCE(Productive_Percent, 0)), 2) AS avg_productivity
FROM employee_metrics
WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30'
GROUP BY department
ORDER BY avg_break_hours DESC
LIMIT 5
"""
    },
    {
        "question": "Identify teams with more than 10% of days with zero productivity in April 2025",
        "clickhouse_sql": """
SELECT 
    Group_Name,
    COUNTIf(COALESCE(Productive_Percent, 0) = 0) AS zero_prod_days,
    COUNT(*) AS total_days,
    ROUND((COUNTIf(COALESCE(Productive_Percent, 0) = 0) / COUNT(*) * 100), 2) AS zero_prod_percentage
FROM employee_metrics
WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30'
GROUP BY Group_Name
HAVING zero_prod_percentage > 10
ORDER BY zero_prod_percentage DESC
"""
    },
    {
        "question": "Which employees have an average online duration exceeding their shift duration by more than 1 hour in April 2025?",
        "clickhouse_sql": """
SELECT 
    Employee_ID,
    First_Name,
    Last_Name,
    department,
    ROUND(AVG(toUInt32(splitByChar(':', COALESCE(Online_Duration, '0:0:0'))[1]) * 3600 +
           toUInt32(splitByChar(':', COALESCE(Online_Duration, '0:0:0'))[2]) * 60 +
           toUInt32(splitByChar(':', COALESCE(Online_Duration, '0:0:0'))[3])) / 3600, 2) AS avg_online_hours,
    ROUND(AVG(toUInt32(splitByChar(':', COALESCE(Punch_Duration, '0:0:0'))[1]) * 3600 +
           toUInt32(splitByChar(':', COALESCE(Punch_Duration, '0:0:0'))[2]) * 60 +
           toUInt32(splitByChar(':', COALESCE(Punch_Duration, '0:0:0'))[3])) / 3600, 2) AS avg_shift_hours
FROM employee_metrics
WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30'
GROUP BY Employee_ID, First_Name, Last_Name, department
HAVING avg_online_hours > (avg_shift_hours + 1)
ORDER BY (avg_online_hours - avg_shift_hours) DESC
LIMIT 10
"""
    },
    {
        "question": "Show the top 5 applications used by teams with productivity below 40% in April 2025",
        "clickhouse_sql": """
WITH low_prod_teams AS (
    SELECT Group_Name
    FROM employee_metrics
    WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30'
    GROUP BY Group_Name
    HAVING AVG(COALESCE(Productive_Percent, 0)) < 40
)
SELECT 
    a.Application,
    COUNT(*) AS usage_count,
    ROUND(SUM(toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[1]) * 3600 +
           toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[2]) * 60 +
           toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[3])) / 3600, 2) AS total_hours
FROM employee_activity a
JOIN employee_metrics m ON a.Employee_ID = m.Employee_ID AND a.Date = m.Attendance_Date
WHERE toDate(a.Date) BETWEEN '2025-04-01' AND '2025-04-30'
AND m.Group_Name IN (SELECT Group_Name FROM low_prod_teams)
GROUP BY a.Application
ORDER BY total_hours DESC
LIMIT 5
"""
    },
    {
        "question": "Which employees had the highest average productivity during the last week of April 2025?",
        "clickhouse_sql": """
SELECT 
    Employee_ID,
    First_Name,
    Last_Name,
    department,
    ROUND(AVG(COALESCE(Productive_Percent, 0)), 2) AS avg_productivity
FROM employee_metrics
WHERE toDate(Attendance_Date) BETWEEN '2025-04-24' AND '2025-04-30'
GROUP BY Employee_ID, First_Name, Last_Name, department
ORDER BY avg_productivity DESC
LIMIT 10
"""
    },
    {
        "question": "Show teams with the highest average time spent on cloud-based apps in April 2025",
        "clickhouse_sql": """
SELECT 
    m.Group_Name AS team,
    ROUND(AVG(toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[1]) * 3600 +
           toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[2]) * 60 +
           toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[3])) / 3600, 2) AS avg_cloud_app_hours
FROM employee_activity a
JOIN employee_metrics m ON a.Employee_ID = m.Employee_ID AND a.Date = m.Attendance_Date
WHERE toDate(a.Date) BETWEEN '2025-04-01' AND '2025-04-30'
AND a.Application IN ('Google Drive', 'Dropbox', 'AWS')
GROUP BY m.Group_Name
ORDER BY avg_cloud_app_hours DESC
LIMIT 5
"""
    },
    {
        "question": "Which employees had more than 5 days with productivity below 30% and online duration > 8 hours in April 2025?",
        "clickhouse_sql": """
SELECT 
    Employee_ID,
    First_Name,
    Last_Name,
    department,
    COUNTIf(COALESCE(Productive_Percent, 0) < 30 AND 
           toUInt32(splitByChar(':', COALESCE(Online_Duration, '0:0:0'))[1]) * 3600 +
           toUInt32(splitByChar(':', COALESCE(Online_Duration, '0:0:0'))[2]) * 60 +
           toUInt32(splitByChar(':', COALESCE(Online_Duration, '0:0:0'))[3]) / 3600 > 8) AS low_prod_long_hours_days
FROM employee_metrics
WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30'
GROUP BY Employee_ID, First_Name, Last_Name, department
HAVING low_prod_long_hours_days > 5
ORDER BY low_prod_long_hours_days DESC
"""
    },
    {
        "question": "Find departments with the highest average number of applications used per employee in April 2025",
        "clickhouse_sql": """
SELECT 
    m.department,
    ROUND(AVG(COUNT(DISTINCT a.Application)), 2) AS avg_app_count
FROM employee_metrics m
JOIN employee_activity a ON m.Employee_ID = a.Employee_ID AND m.Attendance_Date = a.Date
WHERE toDate(m.Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30'
GROUP BY m.department
ORDER BY avg_app_count DESC
LIMIT 5
"""
    },
    {
        "question": "Which teams had the highest average productivity during the middle of the workday (10 AM - 2 PM) in April 2025?",
        "clickhouse_sql": """
SELECT 
    Group_Name AS team,
    ROUND(AVG(COALESCE(Productive_Percent, 0)), 2) AS avg_midday_productivity
FROM employee_metrics
WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30'
AND toHour(Punch_In) <= 10
AND (Punch_Out IS NULL OR toHour(Punch_Out) >= 14)
GROUP BY Group_Name
ORDER BY avg_midday_productivity DESC
LIMIT 5
"""
    },
    {
        "question": "Show employees with the highest average time spent on gaming apps in April 2025",
        "clickhouse_sql": """
SELECT 
    a.Employee_ID,
    m.First_Name,
    m.Last_Name,
    m.department,
    ROUND(AVG(toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[1]) * 3600 +
           toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[2]) * 60 +
           toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[3])) / 3600, 2) AS avg_gaming_hours
FROM employee_activity a
JOIN employee_metrics m ON a.Employee_ID = m.Employee_ID AND a.Date = m.Attendance_Date
WHERE toDate(a.Date) BETWEEN '2025-04-01' AND '2025-04-30'
AND a.Application LIKE '%Game%'
GROUP BY a.Employee_ID, m.First_Name, m.Last_Name, m.department
ORDER BY avg_gaming_hours DESC
LIMIT 5
"""
    },
    {
        "question": "Which departments had the highest variance in employee attendance duration in April 2025?",
        "clickhouse_sql": """
SELECT 
    department,
    ROUND(stddevPop(toUInt32(splitByChar(':', COALESCE(Punch_Duration, '0:0:0'))[1]) * 3600 +
           toUInt32(splitByChar(':', COALESCE(Punch_Duration, '0:0:0'))[2]) * 60 +
           toUInt32(splitByChar(':', COALESCE(Punch_Duration, '0:0:0'))[3])) / 3600, 2) AS variance_shift_hours
FROM employee_metrics
WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30'
GROUP BY department
ORDER BY variance_shift_hours DESC
LIMIT 5
"""
    },
    {
        "question": "Which employees worked overtime (Punch_Duration > Shift_End) for more than 10 hours total in April 2025?",
        "clickhouse_sql": """
SELECT 
    m.Employee_ID,
    m.First_Name,
    m.Last_Name,
    m.department,
    ROUND(SUM(CASE 
        WHEN toDateTime(COALESCE(m.Punch_Out, '2000-01-01 00:00:00')) > toDateTime(COALESCE(m.Shift_End, '2000-01-01 00:00:00'))
        THEN (toUnixTimestamp(toDateTime(COALESCE(m.Punch_Out, '2000-01-01 00:00:00'))) - 
              toUnixTimestamp(toDateTime(COALESCE(m.Shift_End, '2000-01-01 00:00:00')))) / 3600 
        ELSE 0 
    END), 2) AS total_overtime_hours
FROM employee_metrics m
WHERE toDate(m.Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30'
AND m.Punch_Out IS NOT NULL
AND m.Shift_End IS NOT NULL
GROUP BY m.Employee_ID, m.First_Name, m.Last_Name, m.department
HAVING total_overtime_hours > 10
ORDER BY total_overtime_hours DESC
LIMIT 10
"""
    },
    {
        "question": "Which teams had the highest 95th percentile of productivity in April 2025?",
        "clickhouse_sql": """
SELECT 
    Group_Name,
    ROUND(quantile(0.95)(COALESCE(Productive_Percent, 0)), 2) AS productivity_p95
FROM employee_metrics
WHERE toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30'
GROUP BY Group_Name
ORDER BY productivity_p95 DESC
LIMIT 5
"""
    },
    {
        "question": "Which employees switched between apps more than 100 times per day on average in April 2025?",
        "clickhouse_sql": """
SELECT 
    a.Employee_ID,
    m.First_Name,
    m.Last_Name,
    m.department,
    ROUND(AVG(COALESCE(a.app_count, 0)), 2) AS avg_app_switches
FROM employee_activity a
JOIN employee_metrics m ON a.Employee_ID = m.Employee_ID AND a.Date = m.Attendance_Date
WHERE toDate(a.Date) BETWEEN '2025-04-01' AND '2025-04-30'
GROUP BY a.Employee_ID, m.First_Name, m.Last_Name, m.department
HAVING avg_app_switches > 100 
ORDER BY avg_app_switches DESC
LIMIT 10
"""
    },
    {
        "question": "Which departments had the highest average meeting time based on calendar app activity in April 2025?",
        "clickhouse_sql": """
SELECT 
    m.department,
    ROUND(AVG(toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[1]) * 3600 +
           toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[2]) * 60 +
           toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[3])) / 3600, 2) AS avg_meeting_hours
FROM employee_activity a 
JOIN employee_metrics m ON a.Employee_ID = m.Employee_ID AND a.Date = m.Attendance_Date
WHERE toDate(a.Date) BETWEEN '2025-04-01' AND '2025-04-30'
AND a.Application IN ('Google Calendar', 'Microsoft Outlook')
GROUP BY m.department
ORDER BY avg_meeting_hours DESC
LIMIT 5
"""
    },
    {
        "question": "Find employees with more than 5 days where unproductive duration exceeded productive duration in April 2025",
        "clickhouse_sql": """
SELECT 
    Employee_ID,
    First_Name,
    Last_Name,
    department,
    COUNT(*) AS unprod_dominant_days
FROM employee_metrics
WHERE 
    toDate(Attendance_Date) BETWEEN '2025-04-01' AND '2025-04-30'
    AND toUInt32(splitByChar(':', COALESCE(Unproductive_Duration, '0:0:0'))[1]) * 3600 +
        toUInt32(splitByChar(':', COALESCE(Unproductive_Duration, '0:0:0'))[2]) * 60 +
        toUInt32(splitByChar(':', COALESCE(Unproductive_Duration, '0:0:0'))[3])) > 
    (toUInt32(splitByChar(':', COALESCE(Productive_Duration, '0:0:0'))[1]) * 3600 +
     toUInt32(splitByChar(':', COALESCE(Productive_Duration, '0:0:0'))[2]) * 60 +
     toUInt32(splitByChar(':', COALESCE(Productive_Duration, '0:0:0'))[3])))
GROUP BY 
    Employee_ID,
    First_Name,
    Last_Name,
    department
HAVING 
    unprod_dominant_days > 5
ORDER BY 
    unprod_dominant_days DESC
"""
    },
    {
        "question": "Which teams had the highest average productivity during the last 5 days of April 2025?",
        "clickhouse_sql": """
SELECT 
    Group_Name,
    ROUND(AVG(COALESCE(Productive_Percent, 0)), 2) AS avg_productivity
FROM employee_metrics
WHERE 
    toDate(Attendance_Date) BETWEEN '2025-04-25' AND '2025-04-30'
GROUP BY 
    Group_Name
ORDER BY 
    avg_productivity DESC
LIMIT 5
"""
    },
    {
        "question": "Which employees had a productivity increase of more than 15% in the second half of April compared to the first half in 2025?",
        "clickhouse_sql": """
WITH first_half AS (
    SELECT 
        Employee_ID,
        AVG(COALESCE(Productive_Percent, 0)) AS first_half_avg
    FROM employee_metrics 
    WHERE toDate(Attendance_date) BETWEEN '2025-04-01' AND '2025-04-15'
    GROUP BY Employee_ID
),
second_half AS (
    SELECT 
        Employee_ID,
        AVG(COALESCE(Productive_Percent, 0)) AS second_half_avg
    FROM employee_metrics 
    WHERE toDate(Attendance_date) BETWEEN '2025-04-16' AND '2025-04-30'
    GROUP BY Employee_ID
)
SELECT 
    s.Employee_ID,
    m.first_name,
    m.last_name,
    m.department,
    ROUND(f.first_half_avg, 2) AS first_half,
    ROUND(s.second_half_avg, 2) AS second_half,
    ROUND(s.second_half_avg - f.first_half_avg, 2) AS productivity_increase
FROM 
    second_half s 
    JOIN first_half f ON s.Employee_ID = f.Employee_ID 
    JOIN employee_metrics m ON s.Employee_ID = m.Employee_ID 
WHERE 
    toDate(m.Attendance_date) BETWEEN '2025-04-16' AND '2025-04-30'
    AND (s.second_half_avg - f.first_half_avg) > 15
GROUP BY 
    s.Employee_ID,
    m.first_name,
    m.last_name,
    m.department,
    f.first_half_avg,
    s.second_half_avg
ORDER BY 
    productivity_increase DESC 
LIMIT 10
"""
    },
    {
        "question": "Which departments had the lowest usage of collaboration tools in April 2025?",
        "clickhouse_sql": """
SELECT 
    m.department,
    ROUND(AVG(toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[1]) * 3600 +
           toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[2]) * 60 +
           toUInt32(splitByChar(':', COALESCE(a.Duration, '0:0:0'))[3])) / 3600, 2) AS avg_collaboration_hours
FROM employee_activity a 
JOIN employee_metrics m ON a.Employee_ID = m.Employee_ID AND a.Date = m.Attendance_Date 
WHERE toDate(a.Date) BETWEEN '2025-04-01' AND '2025-04-30'
AND a.Application IN ('Trello', 'Jira', 'Asana')
GROUP BY 
    m.department
ORDER BY 
    avg_collaboration_hours ASC 
LIMIT 5
"""
    },
    {
        "question": "Which employees with the highest average number of distinct applications used per day in April 2025",
        "clickhouse_sql": """
SELECT 
    a.Employee_ID,
    m.first_name,
    m.last_name,
    ROUND(AVG(COALESCE(COUNT(DISTINCT a.Application), 0)), 2) AS avg_distinct_apps
FROM employee_activity a 
JOIN employee_metrics m ON a.Employee_ID = m.Employee_ID AND a.Date = m.Attendance_Date
WHERE 
    toDate(a.Date) BETWEEN '2025-04-01' AND '2025-04-30'
GROUP BY 
    a.employee_ID,
    m.first_name,
    m.last_name,
    m.department
ORDER BY 
    avg_distinct_apps DESC 
LIMIT 10
"""
    },
    {
        "question": "Which teams had the highest correlation between key presses and productivity in April 2025",
        "clickhouse_sql": """
SELECT 
    m.Group_Name,
    corr(COALESCE(a.Key_presses, 0), COALESCE(Productive_Percent, 0)) AS key_press_productivity_corr
FROM employee_metrics m 
JOIN employee_activity a ON m.Employee_ID = a.Employee_ID AND a.Date = m.Attendance_Date
WHERE 
    toDate(a.Date) BETWEEN '2025-04-01' AND '2025-04-30'
GROUP BY 
    m.Group_Name
HAVING 
    key_press_productivity_cor > 0.2
ORDER BY 
    key_press_productivity_cor DESC
LIMIT 10
"""
    }]

# Train on these safe examples first
for example in safe_productivity_examples:
    vn.train(question=example["question"], sql=example["clickhouse_sql"])

# Add these to your training examples
for example in null_handling_examples:
    vn.train(question=example["question"], sql=example["clickhouse_sql"])

# Train on examples
for example in training_examples:
    vn.train(question=example["question"], sql=example["clickhouse_sql"])

# Test a query
print("Testing Vanna with a sample query...")
sql = vn.generate_sql("tell me the team names with most productivity decline in the previous week of last week?")
print("Generated SQL for team productivity decline:")
print(sql)

# Execute and handle results
try:
    results = vn.ask("tell me top 3 team names with most productivity decline in the previous week of last week?")
    print("Query results:")
    print(results)
except Exception as e:
    print(f"Error executing query: {e}")