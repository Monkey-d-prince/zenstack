from fastapi import FastAPI, Request, Query, HTTPException, Depends
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from vanna.chromadb import ChromaDB_VectorStore
from vanna.google import GoogleGeminiChat
import chromadb
from typing import Optional, List, Dict, Any, Tuple
from pydantic import BaseModel
from datetime import datetime, timedelta, date
import google.generativeai as genai
from decimal import Decimal
import traceback
import random
import requests
import clickhouse_connect
import uvicorn
import pandas as pd
import math
from vanna.chromadb import ChromaDB_VectorStore
from vanna.google import GoogleGeminiChat
import traceback
import logging
import re
import time
from google.generativeai import GenerativeModel
import os
import json
import aiohttp
from typing import Optional, Dict, Any
import asyncio


app = FastAPI(title="Employee Analytics Dashboard")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def connect_to_clickhouse():
    try:
        client = clickhouse_connect.get_client(
            host='20.244.1.191',
            port=8123
        )
        print("Connected to ClickHouse")
        return client
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to connect to ClickHouse: {str(e)}")
    
class Tables(BaseModel):
    tables: List[str]
    
class schema(BaseModel):
    schemas: Dict[str, List[List[Any]]] = None

class AppUsage(BaseModel):
    name: str
    hours: float
    formatted_duration: str
    user_count: int

class Diagnostic(BaseModel):
    record_count: int
    unique_employees: int
    unique_apps: int
    status_count: int

class AppUsageResponse(BaseModel):
    productive_apps: List[AppUsage]
    unproductive_apps: List[AppUsage]
    neutral_apps: List[AppUsage]
    diagnostic: Diagnostic
    
class EmployeeMetrics(BaseModel):
    employee_id: str
    name:str
    active_percent: str
    unproductive_percent: str
    productive_percent: Decimal

class MyVanna(ChromaDB_VectorStore, GoogleGeminiChat):
    def __init__(self, config=None):
        # Default config with persistent ChromaDB client
        config = config or {}
        config.setdefault('client', chromadb.PersistentClient(path="./chroma_data"))
        config.setdefault('collection_name', 'vanna')

        # Store collection name for debugging
        self.collection_name = config['collection_name']

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

vn = MyVanna()

def run_sql_for_vanna(sql: str) -> pd.DataFrame:
    client = connect_to_clickhouse()
    result = client.query(sql)
    df = pd.DataFrame(result.result_rows, columns=result.column_names)
    return df

def get_all_table_sample_data(limit: int = 5) -> dict:
    client = connect_to_clickhouse()
    tables_df = run_sql_for_vanna("SHOW TABLES")
    
    table_samples = {}
    for table_name in tables_df.iloc[:, 0]:
        try:
            query = f"SELECT * FROM {table_name} LIMIT {limit}"
            df = run_sql_for_vanna(query)
            table_samples[table_name] = df
        except Exception as e:
            print(f"Failed to fetch data from {table_name}: {e}")
    
    return table_samples


@app.get("/tables", response_model=Tables)
def get_tables():
    try:
        client = connect_to_clickhouse()
        tables = client.query("SHOW TABLES").result_rows
        table_names = [table[0] for table in tables]
                
        client.close()
        return {"tables": table_names}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch tables: {str(e)}")
    
@app.get("/schemas", response_model=Dict[str, List[List[Any]]])
def get_schemas():
    try:
        client = connect_to_clickhouse()
        tables = client.query("SHOW TABLES").result_rows
        table_names = [table[0] for table in tables]
        
        schemas = {}
        for table_name in table_names:
            try:
                schema_data = client.query(f"DESC {table_name}").result_rows
                schemas[table_name] = [[row[0], row[1]] for row in schema_data]
            except Exception as e:
                schemas[table_name] = [["error", str(e)]]
                
        client.close()
        return schemas
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch schemas: {str(e)}")
    


@app.get("/schema/{table_name}", response_model=Dict[str, List[List[Any]]])
def get_schema(table_name: str):
    try:
        client = connect_to_clickhouse()
        schema_data = client.query(f"DESC {table_name}").result_rows
        schema = [[row[0], row[1]] for row in schema_data]
        client.close()
        return {"schema": schema}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch schema: {str(e)}")
    

@app.get("/table/{table_name}", response_model=Dict[str, List[List[Any]]])
def get_table_data(table_name: str):
    try:
        client = connect_to_clickhouse()
        query = f"SELECT * FROM {table_name}"
        data = client.query(query).result_rows
        client.close()
        return {"data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch table data: {str(e)}")
    

@app.get("/overview/metrics", response_model=Dict[str, Any])
def get_overview_metrics():
    try:
        client = connect_to_clickhouse()
        
        end_date = "2025-04-30"
        start_date = "2025-04-01"
        
        query = f"""
        WITH 
        total_emp AS (
            SELECT COUNT(DISTINCT concat(First_Name, ' ', Last_Name)) AS count
            FROM employee_metrics 
            WHERE Attendance_Date BETWEEN '{start_date}' AND '{end_date}'
        ),
        total_dept AS (
            SELECT COUNT(DISTINCT Group_Name) AS count
            FROM employee_metrics 
        ),
        online_hours AS (
            SELECT 
                round(SUM(
                    toInt32OrZero(splitByChar(':', Online_Duration)[1]) * 3600 +
                    toInt32OrZero(splitByChar(':', Online_Duration)[2]) * 60 +
                    toInt32OrZero(splitByChar(':', Online_Duration)[3])
                ) / 3600.0 / 31, 1) AS daily,
                round(SUM(
                    toInt32OrZero(splitByChar(':', Online_Duration)[1]) * 3600 +
                    toInt32OrZero(splitByChar(':', Online_Duration)[2]) * 60 +
                    toInt32OrZero(splitByChar(':', Online_Duration)[3])
                ) / 3600.0, 0) AS total
            FROM employee_metrics
            WHERE Attendance_Date BETWEEN '{start_date}' AND '{end_date}'
                AND length(Online_Duration) > 0
        ),
        productive_hours AS (
            SELECT 
                round(SUM(
                    toInt32OrZero(splitByChar(':', Productive_Duration)[1]) * 3600 +
                    toInt32OrZero(splitByChar(':', Productive_Duration)[2]) * 60 +
                    toInt32OrZero(splitByChar(':', Productive_Duration)[3])
                ) / 3600.0 / 31, 1) AS daily,
                round(SUM(
                    toInt32OrZero(splitByChar(':', Productive_Duration)[1]) * 3600 +
                    toInt32OrZero(splitByChar(':', Productive_Duration)[2]) * 60 +
                    toInt32OrZero(splitByChar(':', Productive_Duration)[3])
                ) / 3600.0, 0) AS total
            FROM employee_metrics
            WHERE Attendance_Date BETWEEN '{start_date}' AND '{end_date}'
                AND length(Productive_Duration) > 0
        ),
        active_hours AS (
            SELECT 
                round(SUM(
                    toInt32OrZero(splitByChar(':', Active_Duration)[1]) * 3600 +
                    toInt32OrZero(splitByChar(':', Active_Duration)[2]) * 60 +
                    toInt32OrZero(splitByChar(':', Active_Duration)[3])
                ) / 3600.0 / 31, 1) AS daily,
                round(SUM(
                    toInt32OrZero(splitByChar(':', Active_Duration)[1]) * 3600 +
                    toInt32OrZero(splitByChar(':', Active_Duration)[2]) * 60 +
                    toInt32OrZero(splitByChar(':', Active_Duration)[3])
                ) / 3600.0, 0) AS total
            FROM employee_metrics
            WHERE Attendance_Date BETWEEN '{start_date}' AND '{end_date}'
                AND length(Active_Duration) > 0
        )
        SELECT
            total_emp.count AS total_employees,
            total_dept.count AS total_departments,
            online_hours.daily AS daily_online_hours,
            productive_hours.daily AS daily_productive_hours,
            active_hours.daily AS daily_active_hours,
            online_hours.total AS total_online_hours,
            productive_hours.total AS total_productive_hours,
            active_hours.total AS total_active_hours
        FROM 
            total_emp,
            total_dept,
            online_hours,
            productive_hours,
            active_hours
        """
        
        overview_result = client.query(query).result_rows[0]
        
        metrics = {
            "analysis_period": "30 days",
            "total_employees": overview_result[0],
            "total_departments": overview_result[1],
            "daily_online_hours": float(overview_result[2]),
            "daily_productive_hours": float(overview_result[3]),
            "daily_active_hours": float(overview_result[4]),
            "total_online_hours": int(overview_result[5]),
            "total_productive_hours": int(overview_result[6]),
            "total_active_hours": int(overview_result[7]),
            "avg_daily_active_hours": float(overview_result[4]/274),
            "avg_daily_productive_hours": float(overview_result[3]/274),
            "avg_daily_online_hours": float(overview_result[2]/274),
        }
        
        client.close()
        return metrics
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch overview metrics: {str(e)}")
        

@app.get("/overview/productivity-changes", response_model=Dict[str, Any])
def get_productivity_changes():
    try:
        client = connect_to_clickhouse()
        
        current_end_date = "2025-04-30"
        current_start_date = "2025-04-21"
        previous_end_date = "2025-04-30"
        previous_start_date = "2025-04-11"
        
        query = f"""
        WITH 
        current_period AS (
            SELECT 
                Group_Name as team,
                AVG(Productive_Percent) as current_productivity,
                COUNT(DISTINCT Employee_ID) as employee_count
            FROM employee_metrics
            WHERE Attendance_Date BETWEEN '{current_start_date}' AND '{current_end_date}'
            GROUP BY Group_Name
        ),
        previous_period AS (
            SELECT 
                Group_Name as team,
                AVG(Productive_Percent) as previous_productivity
            FROM employee_metrics
            WHERE Attendance_Date BETWEEN '{previous_start_date}' AND '{previous_end_date}'
            GROUP BY Group_Name
        )
        SELECT
            c.team,
            round(c.current_productivity, 1) as current_percent,
            round(p.previous_productivity, 1) as previous_percent,
            round(c.current_productivity - p.previous_productivity, 1) as change,
            c.employee_count
        FROM current_period c
        JOIN previous_period p ON c.team = p.team
        ORDER BY change ASC
        """
        
        results = client.query(query).result_rows
        
        teams_with_drops = []
        teams_with_rise = []
        teams_with_consistent = []
        
        for row in results:
            team, current, previous, change, employee_count = row
            change_value = float(change)
            
            team_data = {
                "team": team,
                "current_percent": float(current),
                "previous_percent": float(previous),
                "change": change_value,
                "employee_count": employee_count
            }
            
            if change_value < -3:
                drop_magnitude = abs(change_value)
                factors = []
                action = ""
                
                if drop_magnitude > 8:
                    factors = [
                        "Increased absenteeism",
                        "Inefficient workflow processes", 
                        "Lack of clear performance goals"
                    ]
                    action = "Implement a streamlined workflow and establish clear, measurable goals"
                elif drop_magnitude > 5:
                    factors = [
                        "Increased administrative burden",
                        "Insufficient staffing levels", 
                        "Lack of effective communication"
                    ]
                    action = "Implement streamlined workflows and provide additional staff training"
                else:
                    factors = [
                        "Recent system changes", 
                        "Seasonal workload variations",
                        "Training transitions"
                    ]
                    action = "Monitor closely and provide targeted support where needed"
                
                teams_with_drops.append({
                    **team_data,
                    "key_factors": factors,
                    "action_needed": action
                })
                
            elif change_value > 3:
                rise_magnitude = change_value
                success_factors = []
                recommendations = ""
                
                if rise_magnitude > 15:
                    success_factors = [
                        "Optimized route planning",
                        "Improved mobile workflow tools",
                        "Better field resource utilization"
                    ]
                    recommendations = "Maintain positive momentum and document successful practices"
                elif rise_magnitude > 5:
                    success_factors = [
                        "Successful process improvements",
                        "Effective workload management",
                        "Increased team engagement"
                    ]
                    recommendations = "Maintain positive momentum and document successful practices"
                else:
                    success_factors = [
                        "Better task prioritization",
                        "Streamlined reporting process",
                        "Improved communication channels"
                    ]
                    recommendations = "Continue reinforcing positive changes and share best practices"
                    
                teams_with_rise.append({
                    **team_data,
                    "success_factors": success_factors,
                    "recommendations": recommendations
                })
                
            else:
                stability_factors = [
                    "Established workflow patterns",
                    "Balanced resource allocation",
                    "Stable tool utilization"
                ]
                maintain_strategy = "Continue current practices while looking for optimization opportunities"
                
                teams_with_consistent.append({
                    **team_data,
                    "variance": change_value,
                    "stability_factors": stability_factors,
                    "maintain_strategy": maintain_strategy
                })
        
        client.close()
        return {
            "teams_with_drops": teams_with_drops,
            "teams_with_rise": teams_with_rise,
            "teams_with_consistent": teams_with_consistent
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch productivity changes: {str(e)}")

def parse_time_duration(time_str):
    try:
        if not time_str or time_str == "00:00:00":
            return 0
            
        parts = time_str.split(':')
        if len(parts) == 3:
            hours, minutes, seconds = parts
            return int(hours) * 3600 + int(minutes) * 60 + int(seconds)
        elif len(parts) == 2:
            hours, minutes = parts
            return int(hours) * 3600 + int(minutes) * 60
        else:
            return 0
    except:
        return 0  

@app.get("/overview/teams-performance", response_model=Dict[str, Any])
def get_teams_performance():
    try:
        client = connect_to_clickhouse()
        
        # Date range for analysis
        end_date = "2025-04-30"
        start_date = "2025-04-01"
        
        performance_query = f"""
        WITH 
        team_hours AS (
            SELECT 
                Group_Name as team,
                -- Using AVG directly on seconds for more accurate calculation
                round(AVG(
                    toInt32OrZero(splitByChar(':', Online_Duration)[1]) * 3600 + 
                    toInt32OrZero(splitByChar(':', Online_Duration)[2]) * 60 + 
                    toInt32OrZero(splitByChar(':', Online_Duration)[3])
                ) / 3600.0, 1) as online_hrs,
                
                round(AVG(
                    toInt32OrZero(splitByChar(':', Active_Duration)[1]) * 3600 + 
                    toInt32OrZero(splitByChar(':', Active_Duration)[2]) * 60 + 
                    toInt32OrZero(splitByChar(':', Active_Duration)[3])
                ) / 3600.0, 1) as active_hrs,
                
                round(AVG(
                    toInt32OrZero(splitByChar(':', Idle_Duration)[1]) * 3600 + 
                    toInt32OrZero(splitByChar(':', Idle_Duration)[2]) * 60 + 
                    toInt32OrZero(splitByChar(':', Idle_Duration)[3])
                ) / 3600.0, 1) as idle_hrs,
                
                round(AVG(
                    toInt32OrZero(splitByChar(':', Productive_Duration)[1]) * 3600 + 
                    toInt32OrZero(splitByChar(':', Productive_Duration)[2]) * 60 + 
                    toInt32OrZero(splitByChar(':', Productive_Duration)[3])
                ) / 3600.0, 1) as productive_hrs,
                
                round(AVG(
                    toInt32OrZero(splitByChar(':', Neutral_Duration)[1]) * 3600 + 
                    toInt32OrZero(splitByChar(':', Neutral_Duration)[2]) * 60 + 
                    toInt32OrZero(splitByChar(':', Neutral_Duration)[3])
                ) / 3600.0, 1) as neutral_hrs,
                
                round(AVG(
                    toInt32OrZero(splitByChar(':', Unproductive_Duration)[1]) * 3600 + 
                    toInt32OrZero(splitByChar(':', Unproductive_Duration)[2]) * 60 + 
                    toInt32OrZero(splitByChar(':', Unproductive_Duration)[3])
                ) / 3600.0, 1) as unproductive_hrs,
                
                round(AVG(
                    toInt32OrZero(splitByChar(':', Break_Duration)[1]) * 3600 + 
                    toInt32OrZero(splitByChar(':', Break_Duration)[2]) * 60 + 
                    toInt32OrZero(splitByChar(':', Break_Duration)[3])
                ) / 60.0, 0) as avg_break_minutes,
                
                round(AVG(Productive_Percent), 1) as avg_productivity,
                COUNT(DISTINCT Employee_ID) as employee_count
            FROM employee_metrics
            WHERE Attendance_Date BETWEEN '{start_date}' AND '{end_date}'
                AND toDayOfWeek(Attendance_Date) != 7  -- Exclude Sundays
                AND length(Online_Duration) > 0  -- Ensure we only include valid records
            GROUP BY Group_Name
        ),
        productivity_by_dow AS (
            SELECT
                Group_Name as team,
                toDayOfWeek(Attendance_Date) as day_of_week,
                AVG(Productive_Percent) as avg_productivity
            FROM employee_metrics
            WHERE Attendance_Date BETWEEN '{start_date}' AND '{end_date}'
                AND toDayOfWeek(Attendance_Date) != 7  -- Exclude Sundays
            GROUP BY Group_Name, toDayOfWeek(Attendance_Date)
        ),
        least_productive_day AS (
            SELECT
                team,
                day_of_week,
                avg_productivity
            FROM (
                SELECT 
                    team,
                    day_of_week,
                    avg_productivity,
                    row_number() OVER (PARTITION BY team ORDER BY avg_productivity ASC) as rn
                FROM productivity_by_dow
            ) WHERE rn = 1
        )
        SELECT
            th.team,
            th.online_hrs,
            th.active_hrs,
            th.idle_hrs,
            th.productive_hrs,
            th.neutral_hrs,
            th.unproductive_hrs,
            th.avg_break_minutes,
            th.avg_productivity,
            th.employee_count,
            lpd.day_of_week as least_productive_day,
            lpd.avg_productivity as least_productive_day_pct
        FROM team_hours th
        LEFT JOIN least_productive_day lpd ON th.team = lpd.team
        ORDER BY th.team
        """
        
        performance_results = client.query(performance_query).result_rows
        
        apps_query = f"""
        SELECT
            ea.Team as team,
            CASE
                WHEN ea.Application != '' AND ea.Application IS NOT NULL THEN ea.Application
                WHEN ea.URL != '' AND ea.URL IS NOT NULL THEN ea.URL
                ELSE 'Unknown'
            END as app_or_url,
            COUNT(*) as usage_count,
            SUM(toInt32OrZero(splitByChar(':', ea.Duration)[1]) * 3600 + 
                toInt32OrZero(splitByChar(':', ea.Duration)[2]) * 60 + 
                toInt32OrZero(splitByChar(':', ea.Duration)[3])) / 3600.0 as hours,
            concat(
                toString(floor(SUM(toInt32OrZero(splitByChar(':', ea.Duration)[1]) * 3600 + 
                toInt32OrZero(splitByChar(':', ea.Duration)[2]) * 60 + 
                toInt32OrZero(splitByChar(':', ea.Duration)[3])) / 3600)), 'h:',
                toString(floor((SUM(toInt32OrZero(splitByChar(':', ea.Duration)[1]) * 3600 + 
                toInt32OrZero(splitByChar(':', ea.Duration)[2]) * 60 + 
                toInt32OrZero(splitByChar(':', ea.Duration)[3])) % 3600) / 60)), 'm'
            ) as formatted_duration
        FROM employee_activity ea
        WHERE ea.Date BETWEEN '{start_date}' AND '{end_date}'
            AND toDayOfWeek(ea.Date) != 7  -- Exclude Sundays
        GROUP BY ea.Team, app_or_url
        ORDER BY ea.Team, hours DESC
        """

        try:
            apps_results = client.query(apps_query).result_rows
        except Exception as e:
            print(f"Error querying app data: {e}")
            apps_results = []
            
        client.close()
        
        teams_data = []
        
        def get_day_name(day_num):
            days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            return days[day_num - 1] if 1 <= day_num <= 7 else "Unknown"
        
        def determine_workload_index(online_hrs):
            if online_hrs > 40:
                return "High"
            elif online_hrs > 30:
                return "Normal"
            else:
                return "Low"
        
        def determine_alert(avg_productivity, least_productive_pct):
            if avg_productivity < 40:
                return "Productivity Dip"
            elif avg_productivity - least_productive_pct > 15:
                return "High Variability"
            return "No alerts"
        
        team_apps = {}
        app_cache_file = "team_apps_cache.json"
        try:
            for row in apps_results:
                team, app_or_url, count, hours, formatted_duration = row
                
                if team not in team_apps:
                    team_apps[team] = []
                    
                team_apps[team].append({
                    "name": app_or_url,
                    "hours": float(hours) if hours is not None else 0.0,
                    "formatted_duration": formatted_duration,
                    "usage_count": int(count) if count is not None else 0
                })
            
            for team in team_apps:
                team_apps[team] = sorted(team_apps[team], key=lambda x: x["hours"], reverse=True)
                
                if len(team_apps[team]) > 5:
                    team_apps[team] = team_apps[team][:5]
                
                for app in team_apps[team]:
                    app["hours"] = round(app["hours"], 1)
                    if not app.get("formatted_duration"):
                        hours_int = int(app["hours"])
                        minutes_int = int((app["hours"] - hours_int) * 60)
                        app["formatted_duration"] = f"{hours_int}h:{minutes_int}m"
            
            if team_apps:
                team_app_mapping = {}
                for team in team_apps:
                    if team_apps[team]:
                        team_app_mapping[team] = [app["name"] for app in team_apps[team]]
                for team_row in performance_results:
                    team_name = team_row[0]
                    if team_name not in team_apps or not team_apps[team_name]:
                        team_apps[team_name] = []
                        
                        most_similar_team = None
                        highest_similarity = 0
                        
                        for existing_team in team_app_mapping:
                            similarity = sum(1 for c in team_name if c in existing_team)
                            if similarity > highest_similarity:
                                highest_similarity = similarity
                                most_similar_team = existing_team
                        
                        if most_similar_team and team_app_mapping.get(most_similar_team):
                            for i, app_name in enumerate(team_app_mapping[most_similar_team][:5]):
                                hours = 4.0 - (i * 0.5)
                                formatted_duration = f"{int(hours)}h:{int((hours - int(hours)) * 60)}m"
                                team_apps[team_name].append({
                                    "name": app_name,
                                    "hours": hours,
                                    "formatted_duration": formatted_duration,
                                    "usage_count": 30 - (i * 5)
                                })

        except Exception as e:
            print(f"Error processing app data: {str(e)}")
            
        for team in team_apps:
            team_apps[team] = sorted(team_apps[team], key=lambda x: x["hours"], reverse=True)[:5]
            
            for app in team_apps[team]:
                app["hours"] = round(app["hours"], 1)

        if not team_apps or all(len(apps) == 0 for apps in team_apps.values()):
            sample_apps = {
                "Development": [
                    {"name": "VS Code", "hours": 15.2, "usage_count": 28},
                    {"name": "GitHub", "hours": 8.5, "usage_count": 36},
                    {"name": "Slack", "hours": 5.3, "usage_count": 42},
                    {"name": "Chrome", "hours": 4.7, "usage_count": 31},
                    {"name": "Terminal", "hours": 3.8, "usage_count": 27}
                ],
                "Marketing": [
                    {"name": "HubSpot", "hours": 12.7, "usage_count": 32},
                    {"name": "Google Analytics", "hours": 8.9, "usage_count": 22},
                    {"name": "Canva", "hours": 6.2, "usage_count": 29},
                    {"name": "Gmail", "hours": 5.4, "usage_count": 48},
                    {"name": "Slack", "hours": 4.8, "usage_count": 35}
                ],
                "Default": [
                    {"name": "Microsoft Outlook", "hours": 8.5, "usage_count": 45},
                    {"name": "Microsoft Teams", "hours": 6.3, "usage_count": 28},
                    {"name": "Excel", "hours": 4.8, "usage_count": 19},
                    {"name": "Chrome", "hours": 3.2, "usage_count": 27},
                    {"name": "PowerPoint", "hours": 2.5, "usage_count": 15}
                ]
            }
            
            for team_name in [row[0] for row in performance_results]:
                category = "Development" if "Dev" in team_name or "Engineering" in team_name else \
                          "Marketing" if "Market" in team_name or "Sales" in team_name else "Default"
                team_apps[team_name] = sample_apps[category]
        
        for row in performance_results:
            (team, online_hrs, active_hrs, idle_hrs, productive_hrs, neutral_hrs, 
             unproductive_hrs, avg_break_mins, avg_productivity, emp_count, 
             least_prod_day, least_prod_pct) = row
            
            try:
                online_hours = float(online_hrs) if online_hrs is not None else 0.0
                active_hours = float(active_hrs) if active_hrs is not None else 0.0
                idle_hours = float(idle_hrs) if idle_hrs is not None else 0.0
                productive_hours = float(productive_hrs) if productive_hrs is not None else 0.0
                neutral_hours = float(neutral_hrs) if neutral_hrs is not None else 0.0
                unproductive_hours = float(unproductive_hrs) if unproductive_hrs is not None else 0.0
                break_minutes = float(avg_break_mins) if avg_break_mins is not None else 0.0
                avg_prod = float(avg_productivity) if avg_productivity is not None else 0.0
                least_prod_pct_value = float(least_prod_pct) if least_prod_pct is not None else 0.0
                
                for value in [online_hours, active_hours, idle_hours, productive_hours, 
                              neutral_hours, unproductive_hours, break_minutes, 
                              avg_prod, least_prod_pct_value]:
                    if not math.isfinite(value):
                        value = 0.0
                    
                avg_hrs = (online_hours + productive_hours) / 2 if (online_hours + productive_hours) > 0 else 0
                
                employee_count = int(emp_count) if emp_count is not None else 0
                
            except (ValueError, TypeError):
                online_hours = active_hours = idle_hours = 0.0
                productive_hours = neutral_hours = unproductive_hours = 0.0
                break_minutes = avg_prod = least_prod_pct_value = 0.0
                avg_hrs = 0.0
                employee_count = 0
            
            top_apps = team_apps.get(team, [])
            
            workload_index = determine_workload_index(online_hours)
            
            alert = determine_alert(avg_prod, least_prod_pct_value)
            
            action_day = get_day_name(int(least_prod_day) if least_prod_day and isinstance(least_prod_day, (int, float)) else 1)
            
            if action_day == "Sunday":
                action_day = "Monday"
                
            recommendations = {
                "action_type": "Metric-Based:",
                "action": f"Optimize productivity on {action_day}s."
            }
            
            team_data = {
                "team_name": team,
                "employee_count": employee_count,
                "performance": {
                    "online": round(online_hours, 1),
                    "active": round(active_hours, 1),
                    "productive": round(productive_hours, 1),
                    "neutral": round(neutral_hours, 1),
                    "idle": round(idle_hours, 1),
                    "unproductive": round(unproductive_hours, 1),
                },
                "break_time": round(break_minutes, 0),
                "top_apps": [{
                    "name": f"{app['name']}: {app.get('formatted_duration', '0h:0m')}",
                    "hours": app["hours"],
                    "usage_count": app["usage_count"]
                } for app in top_apps],
                "workload_index": workload_index,
                "alert": alert,
                "action": {
                    "type": recommendations["action_type"],
                    "recommendation": recommendations["action"]
                },
                "least_productive_day": "Weekday" if (least_prod_day is not None and int(least_prod_day) == 7) 
                                       else get_day_name(int(least_prod_day) if least_prod_day and 
                                       isinstance(least_prod_day, (int, float)) else 1)
            }
            
            teams_data.append(team_data)
        
        return {
            "teams": teams_data
        }
        
    except Exception as e:
        error_details = traceback.format_exc()
        print(f"Error in teams_performance: {error_details}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch teams performance data: {str(e)}")
    

@app.get("/overview/weekly-trends", response_model=Dict[str, Any])
def get_weekly_trends():
    try:
        client = connect_to_clickhouse()
        
        query = f"""
        WITH daily_metrics AS (
            SELECT
                Group_Name as team,
                toDate('2025-04-01') + INTERVAL (intDiv(toUInt32(Attendance_Date - toDate('2025-04-01')), 7) * 7) DAY AS week_start,
                round(AVG(Productive_Percent), 2) as productivity,
                round(AVG(
                    toInt32OrZero(splitByChar(':', Online_Duration)[1]) * 3600 + 
                    toInt32OrZero(splitByChar(':', Online_Duration)[2]) * 60 + 
                    toInt32OrZero(splitByChar(':', Online_Duration)[3])
                ) / 3600.0, 1) as online_hours,
                COUNT(DISTINCT Employee_ID) as active_employees
            FROM employee_metrics
            WHERE Attendance_Date >= toDate('2025-04-01')
                AND Attendance_Date <= toDate('2025-04-30')
            GROUP BY team, week_start
            ORDER BY week_start ASC
        )
        SELECT
            team,
            groupArray(week_start) as weeks,
            groupArray(productivity) as productivity_trend,
            groupArray(online_hours) as hours_trend,
            groupArray(active_employees) as employee_trend
        FROM daily_metrics
        GROUP BY team
        ORDER BY team
        """
        
        results = client.query(query).result_rows
        trends_data = []
        
        for row in results:
            team, weeks, productivity, hours, employees = row
            trend_points = []
            
            for i in range(len(weeks)):
                try:
                    prod_value = float(productivity[i]) if productivity[i] is not None else 0.0
                    hours_value = float(hours[i]) if hours[i] is not None else 0.0
                    emp_value = int(employees[i]) if employees[i] is not None else 0
                    
                    if math.isinf(prod_value) or math.isnan(prod_value):
                        prod_value = 0.0
                    if math.isinf(hours_value) or math.isnan(hours_value):
                        hours_value = 0.0
                    
                    date_obj = weeks[i]
                    week_num = ((date_obj - datetime(2025, 4, 1).date()).days // 7) + 1
                    
                    trend_points.append({
                        "week": f"Week {week_num}",
                        "productivity": round(prod_value, 2),
                        "hours": round(hours_value, 1),
                        "employees": emp_value
                    })
                except (ValueError, TypeError, AttributeError) as e:
                    print(f"Error processing trend point: {e}")
                    continue
            
            if trend_points:
                trends_data.append({
                    "team": team,
                    "trends": trend_points
                })
        
        client.close()
        return {"teams": trends_data}
        
    except Exception as e:
        error_details = traceback.format_exc()
        print(f"Error in weekly_trends: {error_details}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch weekly trends: {str(e)}")

@app.get("/overview/app-usage", response_model=AppUsageResponse)
def get_app_usage():
    try:
        client = connect_to_clickhouse()

        duration_parsing = """
        IF(
            match(Duration, '\\d+h'),
            toInt32OrZero(regexpExtract(Duration, '(\\d+)h', 1)) * 3600,
            0
        ) +
        IF(
            match(Duration, '\\d+m'),
            toInt32OrZero(regexpExtract(Duration, '(\\d+)m', 1)) * 60,
            0
        ) +
        IF(
            match(Duration, '\\d+s'),
            toInt32OrZero(regexpExtract(Duration, '(\\d+)s', 1)),
            0
        )
        """

        test_query = """
        SELECT 
            COUNT(*) as record_count,
            countDistinct(Employee_ID) as unique_employees,
            count(DISTINCT 
                CASE
                    WHEN Application != '' AND Application IS NOT NULL THEN Application
                    WHEN URL != '' AND URL IS NOT NULL THEN URL
                    ELSE 'Unknown'
                END
            ) as unique_apps,
            countDistinct(Mapping_Status) as status_count
        FROM employee_activity
        """
        test_results = client.query(test_query).result_rows
        logger.info(f"Diagnostic query results: {test_results}")

        productive_apps_query = f"""
        SELECT 
            CASE
                WHEN Application != '' AND Application IS NOT NULL THEN Application
                WHEN URL != '' AND URL IS NOT NULL THEN URL
                ELSE 'Unknown'
            END as app_name,
            COUNT(DISTINCT Employee_ID) as user_count,
            SUM({duration_parsing}) as total_seconds
        FROM employee_activity
        WHERE lower(Mapping_Status) = 'productive'
        GROUP BY app_name
        ORDER BY total_seconds DESC
        Limit 10
        """

        unproductive_apps_query = f"""
        SELECT 
            CASE
                WHEN Application != '' AND Application IS NOT NULL THEN Application
                WHEN URL != '' AND URL IS NOT NULL THEN URL
                ELSE 'Unknown'
            END as app_name,
            COUNT(DISTINCT Employee_ID) as user_count,
            SUM({duration_parsing}) as total_seconds
        FROM employee_activity
        WHERE lower(Mapping_Status) = 'unproductive'
        GROUP BY app_name
        ORDER BY total_seconds DESC
        Limit 10
        """

        neutral_apps_query = f"""
        SELECT 
            CASE
                WHEN Application != '' AND Application IS NOT NULL THEN Application
                WHEN URL != '' AND URL IS NOT NULL THEN URL
                ELSE 'Unknown'
            END as app_name,
            COUNT(DISTINCT Employee_ID) as user_count,
            SUM({duration_parsing}) as total_seconds
        FROM employee_activity
        WHERE lower(Mapping_Status) = 'neutral'
        GROUP BY app_name
        ORDER BY total_seconds DESC
        Limit 10
        """

        try:
            productive_results = client.query(productive_apps_query).result_rows
            logger.info(f"Productive apps raw results: {productive_results}")
        except Exception as e:
            logger.error(f"Error in productive query: {str(e)}")
            productive_results = []

        try:
            unproductive_results = client.query(unproductive_apps_query).result_rows
            logger.info(f"Unproductive apps raw results: {unproductive_results}")
        except Exception as e:
            logger.error(f"Error in unproductive query: {str(e)}")
            unproductive_results = []

        try:
            neutral_results = client.query(neutral_apps_query).result_rows
            logger.info(f"Neutral apps raw results: {neutral_results}")
        except Exception as e:
            logger.error(f"Error in neutral query: {str(e)}")
            neutral_results = []

        productive_apps = []
        for row in productive_results:
            try:
                app_name, user_count, seconds = row
                hours = float(seconds) / 3600.0 if seconds is not None else 0
                formatted_duration = f"{int(hours)}h:{int((hours % 1) * 60)}m"
                productive_apps.append({
                    "name": app_name,
                    "hours": round(hours, 1),
                    "formatted_duration": formatted_duration,
                    "user_count": int(user_count) if user_count is not None else 0
                })
            except Exception as e:
                logger.error(f"Error processing productive row {row}: {str(e)}")

        unproductive_apps = []
        for row in unproductive_results:
            try:
                app_name, user_count, seconds = row
                hours = float(seconds) / 3600.0 if seconds is not None else 0
                formatted_duration = f"{int(hours)}h:{int((hours % 1) * 60)}m"
                unproductive_apps.append({
                    "name": app_name,
                    "hours": round(hours, 1),
                    "formatted_duration": formatted_duration,
                    "user_count": int(user_count) if user_count is not None else 0
                })
            except Exception as e:
                logger.error(f"Error processing unproductive row {row}: {str(e)}")

        neutral_apps = []
        for row in neutral_results:
            try:
                app_name, user_count, seconds = row
                hours = float(seconds) / 3600.0 if seconds is not None else 0
                formatted_duration = f"{int(hours)}h:{int((hours % 1) * 60)}m"
                neutral_apps.append({
                    "name": app_name,
                    "hours": round(hours, 1),
                    "formatted_duration": formatted_duration,
                    "user_count": int(user_count) if user_count is not None else 0
                })
            except Exception as e:
                logger.error(f"Error processing neutral row {row}: {str(e)}")

        diagnostic = {
            "record_count": test_results[0][0] if test_results else 0,
            "unique_employees": test_results[0][1] if test_results else 0,
            "unique_apps": test_results[0][2] if test_results else 0,
            "status_count": test_results[0][3] if test_results else 0
        }

        return {
            "productive_apps": productive_apps,
            "unproductive_apps": unproductive_apps,
            "neutral_apps": neutral_apps,
            "diagnostic": diagnostic
        }

    except Exception as e:
        error_details = traceback.format_exc()
        logger.error(f"Error in get_app_usage: {str(e)}\n{error_details}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch app usage data: {str(e)}")

@app.get("/overview/app-usage/{team_name}", response_model=AppUsageResponse)
def get_app_usage_by_team(team_name: str):
    try:
        if not team_name:
            raise HTTPException(status_code=400, detail="Team name is required")

        client = connect_to_clickhouse()

        duration_parsing = """
        IF(
            match(Duration, '\\d+h'),
            toInt32OrZero(regexpExtract(Duration, '(\\d+)h', 1)) * 3600,
            0
        ) +
        IF(
            match(Duration, '\\d+m'),
            toInt32OrZero(regexpExtract(Duration, '(\\d+)m', 1)) * 60,
            0
        ) +
        IF(
            match(Duration, '\\d+s'),
            toInt32OrZero(regexpExtract(Duration, '(\\d+)s', 1)),
            0
        )
        """

        test_query = f"""
        SELECT 
            COUNT(*) as record_count,
            countDistinct(Employee_ID) as unique_employees,
            count(DISTINCT 
                CASE
                    WHEN Application != '' AND Application IS NOT NULL THEN Application
                    WHEN URL != '' AND URL IS NOT NULL THEN URL
                    ELSE 'Unknown'
                END
            ) as unique_apps,
            countDistinct(Mapping_Status) as status_count
        FROM employee_activity
        WHERE Team = '{team_name}'
        """
        test_results = client.query(test_query).result_rows
        logger.info(f"Diagnostic query results for team {team_name}: {test_results}")

        productive_apps_query = f"""
        SELECT 
            CASE
                WHEN Application != '' AND Application IS NOT NULL THEN Application
                WHEN URL != '' AND URL IS NOT NULL THEN URL
                ELSE 'Unknown'
            END as app_name,
            COUNT(DISTINCT Employee_ID) as user_count,
            SUM({duration_parsing}) as total_seconds
        FROM employee_activity
        WHERE lower(Mapping_Status) = 'productive' AND Team = '{team_name}'
        GROUP BY app_name
        ORDER BY total_seconds DESC
        Limit 10
        """

        unproductive_apps_query = f"""
        SELECT 
            CASE
                WHEN Application != '' AND Application IS NOT NULL THEN Application
                WHEN URL != '' AND URL IS NOT NULL THEN URL
                ELSE 'Unknown'
            END as app_name,
            COUNT(DISTINCT Employee_ID) as user_count,
            SUM({duration_parsing}) as total_seconds
        FROM employee_activity
        WHERE lower(Mapping_Status) = 'unproductive' AND Team = '{team_name}'
        GROUP BY app_name
        ORDER BY total_seconds DESC
        Limit 10
        """

        neutral_apps_query = f"""
        SELECT 
            CASE
                WHEN Application != '' AND Application IS NOT NULL THEN Application
                WHEN URL != '' AND URL IS NOT NULL THEN URL
                ELSE 'Unknown'
            END as app_name,
            COUNT(DISTINCT Employee_ID) as user_count,
            SUM({duration_parsing}) as total_seconds
        FROM employee_activity
        WHERE lower(Mapping_Status) = 'neutral' AND Team = '{team_name}'
        GROUP BY app_name
        ORDER BY total_seconds DESC
        Limit 10
        """

        try:
            productive_results = client.query(productive_apps_query).result_rows
            logger.info(f"Productive apps raw results for team {team_name}: {productive_results}")
        except Exception as e:
            logger.error(f"Error in productive query for team {team_name}: {str(e)}")
            productive_results = []

        try:
            unproductive_results = client.query(unproductive_apps_query).result_rows
            logger.info(f"Unproductive apps raw results for team {team_name}: {productive_results}")
        except Exception as e:
            logger.error(f"Error in unproductive query for team {team_name}: {str(e)}")
            unproductive_results = []

        try:
            neutral_results = client.query(neutral_apps_query).result_rows
            logger.info(f"Neutral apps raw results for team {team_name}: {neutral_results}")
        except Exception as e:
            logger.error(f"Error in neutral query for team {team_name}: {str(e)}")
            neutral_results = []

        productive_apps = []
        for row in productive_results:
            try:
                app_name, user_count, seconds = row
                hours = float(seconds) / 3600.0 if seconds is not None else 0
                formatted_duration = f"{int(hours)}h:{int((hours % 1) * 60)}m"
                productive_apps.append({
                    "name": app_name,
                    "hours": round(hours, 1),
                    "formatted_duration": formatted_duration,
                    "user_count": int(user_count) if user_count is not None else 0
                })
            except Exception as e:
                logger.error(f"Error processing productive row for team {team_name}: {row}: {str(e)}")

        unproductive_apps = []
        for row in unproductive_results:
            try:
                app_name, user_count, seconds = row
                hours = float(seconds) / 3600.0 if seconds is not None else 0
                formatted_duration = f"{int(hours)}h:{int((hours % 1) * 60)}m"
                unproductive_apps.append({
                    "name": app_name,
                    "hours": round(hours, 1),
                    "formatted_duration": formatted_duration,
                    "user_count": int(user_count) if user_count is not None else 0
                })
            except Exception as e:
                logger.error(f"Error processing unproductive row for team {team_name}: {row}: {str(e)}")

        neutral_apps = []
        for row in neutral_results:
            try:
                app_name, user_count, seconds = row
                hours = float(seconds) / 3600.0 if seconds is not None else 0
                formatted_duration = f"{int(hours)}h:{int((hours % 1) * 60)}m"
                neutral_apps.append({
                    "name": app_name,
                    "hours": round(hours, 1),
                    "formatted_duration": formatted_duration,
                    "user_count": int(user_count) if user_count is not None else 0
                })
            except Exception as e:
                logger.error(f"Error processing neutral row for team {team_name}: {row}: {str(e)}")

        diagnostic = {
            "record_count": test_results[0][0] if test_results else 0,
            "unique_employees": test_results[0][1] if test_results else 0,
            "unique_apps": test_results[0][2] if test_results else 0,
            "status_count": test_results[0][3] if test_results else 0
        }

        return {
            "productive_apps": productive_apps,
            "unproductive_apps": unproductive_apps,
            "neutral_apps": neutral_apps,
            "diagnostic": diagnostic
        }

    except Exception as e:
        error_details = traceback.format_exc()
        logger.error(f"Error in get_app_usage_by_team for team {team_name}: {str(e)}\n{error_details}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch app usage data for team {team_name}: {str(e)}")
    

class QueryRequest(BaseModel):
    question: str

class GeminiRequest(BaseModel):
    prompt: str
    retries: Optional[int] = 2

@app.post("/api/query")
async def process_query(request: QueryRequest):
    try:
        question = request.question
        
        # Handle greetings
        greetings = ["hi", "hello", "hey", "hii", "hola", "greetings", "thanks", "thank you"]
        if question.lower().strip() in greetings or len(question.split()) < 3:
            return {
                "response": "Hello! I'm your productivity analytics assistant. How can I help you analyze employee productivity data today?"
            }
        
        # Generate and execute query with better error handling
        try:
            
            
            logger.info(f"Generating SQL for question: {question}")
            sql = vn.generate_sql(question)
            logger.info(f"Generated SQL: {sql}")
            
            # Connect with timeout
            client = connect_to_clickhouse()
            
            # Execute query with timeout
            logger.info("Executing SQL query...")
            result = client.query(sql)
            
            # Process results
            column_names = result.column_names
            rows = []
            for row in result.result_rows:
                row_dict = {}
                for i, col in enumerate(column_names):
                    if isinstance(row[i], (int, float, str, bool)) or row[i] is None:
                        row_dict[col] = row[i]
                    else:
                        row_dict[col] = str(row[i])
                rows.append(row_dict)
            
            logger.info(f"Query returned {len(rows)} results")
            return {
                "sql": sql,
                "results": rows
            }
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error executing query: {error_msg}")
            
            # For common team productivity question, fallback to reliable query
            if "productivity" in question.lower() and "team" in question.lower():
                try:
                    # Try again with a more direct approach using Vanna
                    logger.info("Attempting secondary Vanna query generation")
                    
                    # Add targeted context to help Vanna generate better SQL
                    specific_query = f"From employee_metrics table: {question}"
                    logger.info(f"Generating targeted SQL: {specific_query}")
                    
                    # Generate SQL with more specific guidance
                    fallback_sql = vn.generate_sql(specific_query)
                    
                    if not fallback_sql or len(fallback_sql) < 20:
                        # If still no good query, use a smarter fallback with pattern matching
                        logger.info("Using smart pattern fallback for team productivity")
                        fallback_sql = """
                        SELECT Group_Name AS team, 
                               AVG(Productive_Percent) AS avg_productivity
                        FROM employee_metrics 
                        GROUP BY Group_Name
                        """
                        
                        # Add conditions based on question content
                        if "between" in question.lower() and any(n in question for n in ["40", "50", "60"]):
                            # Extract numbers from the question
                            import re
                            numbers = re.findall(r'\d+', question)
                            if len(numbers) >= 2:
                                lower = numbers[0]
                                upper = numbers[1]
                                fallback_sql += f"\nHAVING AVG(Productive_Percent) BETWEEN {lower} AND {upper}"
                                
                        # Determine sort order based on question
                        if any(word in question.lower() for word in ["highest", "most", "top", "best"]):
                            fallback_sql += "\nORDER BY avg_productivity DESC"
                        else:
                            fallback_sql += "\nORDER BY avg_productivity ASC"
                        
                        # Add a reasonable limit
                        fallback_sql += "\nLIMIT 10"
                    
                    logger.info(f"Generated fallback SQL: {fallback_sql}")
                    client = connect_to_clickhouse()
                    result = client.query(fallback_sql)
                    
                    # Process results - same as before
                    column_names = result.column_names
                    rows = []
                    for row in result.result_rows:
                        row_dict = {}
                        for i, col in enumerate(column_names):
                            if isinstance(row[i], (int, float, str, bool)) or row[i] is None:
                                row_dict[col] = row[i]
                            else:
                                row_dict[col] = str(row[i])
                        rows.append(row_dict)
                    
                    return {
                        "sql": fallback_sql,
                        "results": rows
                    }
                except Exception as fallback_error:
                    logger.error(f"Fallback query also failed: {str(fallback_error)}")
            
            # Return a more helpful error message
            return {
                "response": f"I couldn't get that information due to a technical issue. Try asking in a different way or try again later. (Error: {error_msg[:100]})"
            }
    
    except Exception as e:
        logger.error(f"Unexpected error in /api/query: {str(e)}")
        return {"response": "Sorry, something went wrong with the request. Please try again later."}

@app.post("/api/performance/recommendations")
async def get_ai_recommendations(request: Request):
    try:
        data = await request.json()
        employee_data = data.get('employee_data')
        
        try:
            # Try using Gemini AI for recommendations
            prompt = f"""Based on the following employee performance metrics, provide specific, actionable recommendations:
            - Late Arrivals: {employee_data.get('late_arrivals', 0)}
            - Absent Days: {employee_data.get('absent_days', 0)}
            - Productivity Decline: {employee_data.get('productivity_decline', False)}
            - Activity Decline: {employee_data.get('activity_decline', False)}
            - Job Hunting Rise: {employee_data.get('job_hunting_sudden_rise', False)}
            - Unproductive Tools Rise: {employee_data.get('unprod_tools_sudden_rise', False)}
            - Average Active Hours: {employee_data.get('avg_active_hours', 0)}
            - Risk Score: {employee_data.get('risk_score', 0)}
            - Productivity Change: {employee_data.get('productivity_change_pct', 0)}%
            - Activity Change: {employee_data.get('activity_change_pct', 0)}%
            - Job Hunting Change: {employee_data.get('job_hunting_change_pct', 0)}%
            - Unproductive Tools Change: {employee_data.get('unprod_tools_change_pct', 0)}%
            
            DO NOT give any introduction or context, directly provide the recommendation. Please provide 2-3 specific, actionable recommendations that address the most critical issues. 
            Focus on constructive, solution-oriented advice that can help improve performance.
            Keep the response concise and professional."""

            # Call the Gemini API endpoint
            gemini_response = await call_gemini(GeminiRequest(prompt=prompt))
            if gemini_response.get("success"):
                recommendations = gemini_response["response"]
            else:
                raise Exception(gemini_response.get("message", "Failed to get AI recommendations"))

        except Exception as ai_error:
            print(f"AI recommendation error: {str(ai_error)}")
            
            # Fallback to rule-based recommendations if AI fails
            recommendations = generate_rule_based_recommendations(employee_data)
            
        return {"recommendations": recommendations}

    except Exception as e:
        print(f"Error in recommendations: {str(e)}")
        print(f"Traceback: {traceback.format_exc()}")
        return {"recommendations": "Unable to generate recommendations due to an error."}

def generate_rule_based_recommendations(employee_data):
    """Generate rule-based recommendations when AI is unavailable"""
    recommendations = []
    
    if employee_data.get('job_hunting_sudden_rise') or employee_data.get('job_hunting_change_pct', 0) > 10:
        recommendations.append("Schedule a 1:1 discussion to address career growth and job satisfaction concerns.")
    
    if employee_data.get('productivity_decline') or employee_data.get('productivity_change_pct', 0) < -10:
        recommendations.append("Review current projects and workload to identify potential blockers to productivity.")
    
    if employee_data.get('late_arrivals', 0) > 2:
        recommendations.append("Discuss attendance expectations and explore flexible work arrangements if needed.")
    
    if employee_data.get('absent_days', 0) > 1:
        recommendations.append("Check on employee well-being and provide necessary support.")
    
    if employee_data.get('unprod_tools_sudden_rise') or employee_data.get('unprod_tools_change_pct', 0) > 10:
        recommendations.append("Review tool usage patterns and current task assignments.")
    
    if employee_data.get('avg_active_hours', 0) < 6:
        recommendations.append("Investigate potential time management issues and improve active work engagement.")
        
    if len(recommendations) == 0:
        if employee_data.get('risk_score', 0) >= 4:
            recommendations.append("Monitor performance metrics closely over the next period.")
        else:
            recommendations.append("No immediate concerns. Continue to provide regular feedback and support.")
    
    # Return top 3 recommendations max, ordered by priority
    return "\n• " + "\n• ".join(recommendations[:3])

@app.get("/api/performance/departments")
async def get_departments():
    try:
        client = connect_to_clickhouse()
        query = """
        SELECT DISTINCT Group_Name as department
        FROM employee_metrics
        WHERE Group_Name IS NOT NULL AND Group_Name != ''
        ORDER BY Group_Name
        """
        print(f"Executing departments query: {query}")
        result = client.query(query)
        departments = [row[0] for row in result.result_rows]
        print(f"Found departments: {departments}")
        client.close()
        return {"departments": departments}
    except Exception as e:
        print(f"Error fetching departments: {str(e)}")
        print(f"Falling back to sample departments")
        # Return sample departments as fallback
        return {"departments": ["Engineering", "Marketing", "Sales", "Human Resources", "Finance"]}

@app.get("/api/performance/summary")
async def get_performance_summary(department: Optional[str] = Query(None, description="Department name")):
    client = None
    try:
        client = connect_to_clickhouse()

        dept_filter_condition = ""
        if department and department.lower() != "all":
            safe_dept = department.replace("'", "''").lower()
            dept_filter_condition = f"AND lower(department) = '{safe_dept}'"

        query = f"""
        WITH
          start_date AS (SELECT toDate('2025-04-01') AS d),
          end_date AS (SELECT toDate('2025-04-30') AS d),

          first_week AS (
            SELECT 
              Employee_ID AS employee_id,
              AVG(Productive_Percent) AS prod_pct_fw,
              AVG(Active_Percent) AS act_pct_fw
            FROM employee_metrics
            WHERE Attendance_Date BETWEEN (SELECT d FROM start_date) AND addDays((SELECT d FROM start_date), 6)
            GROUP BY Employee_ID
          ),

          last_week AS (
            SELECT
              Employee_ID AS employee_id,
              AVG(Productive_Percent) AS prod_pct_lw,
              AVG(Active_Percent) AS act_pct_lw
            FROM employee_metrics
            WHERE Attendance_Date BETWEEN subtractDays((SELECT d FROM end_date), 6) AND (SELECT d FROM end_date)
            GROUP BY Employee_ID
          ),

          first_week_activity AS (
            SELECT
              Employee_ID AS employee_id,
              COUNTIf(
                lower(url) LIKE '%naukri%' OR
                lower(url) LIKE '%linkedin%' OR
                lower(url) LIKE '%internshala%' OR
                lower(url) LIKE '%monster%' OR
                lower(url) LIKE '%foundit%'
              ) AS job_hunt_fw,
              COUNTIf(lower(mapping_status) = 'unproductive') AS unprod_tools_fw
            FROM employee_activity
            WHERE Date BETWEEN (SELECT d FROM start_date) AND addDays((SELECT d FROM start_date), 6)
            GROUP BY Employee_ID
          ),

          last_week_activity AS (
            SELECT
              Employee_ID AS employee_id,
              COUNTIf(
                lower(url) LIKE '%naukri%' OR
                lower(url) LIKE '%linkedin%' OR
                lower(url) LIKE '%internshala%' OR
                lower(url) LIKE '%monster%' OR
                lower(url) LIKE '%foundit%'
              ) AS job_hunt_lw,
              COUNTIf(lower(mapping_status) = 'unproductive') AS unprod_tools_lw
            FROM employee_activity
            WHERE Date BETWEEN subtractDays((SELECT d FROM end_date), 6) AND (SELECT d FROM end_date)
            GROUP BY Employee_ID
          ),

          employee_info AS (
            SELECT
              Employee_ID AS employee_id,
              any(First_Name) AS first_name,
              any(Last_Name) AS last_name,
              any(Group_Name) AS department,
              COUNTIf(Late_Arrival = 1) AS late_arrivals,
              COUNTIf(Absent = 1) AS absent_days,
              AVG(
                toInt32OrZero(splitByChar(':', Active_Duration)[1]) * 3600 +
                toInt32OrZero(splitByChar(':', Active_Duration)[2]) * 60 +
                toInt32OrZero(splitByChar(':', Active_Duration)[3])
              ) / 3600.0 AS avg_active_hours,
              AVG(Productive_Percent) AS avg_productivity
            FROM employee_metrics
            WHERE Attendance_Date BETWEEN (SELECT d FROM start_date) AND (SELECT d FROM end_date)
            GROUP BY Employee_ID
          )

        SELECT
          ei.employee_id,
          concat(ei.first_name, ' ', ei.last_name) AS name,
          ei.department,
          ei.late_arrivals,
          ei.absent_days,
          round(COALESCE(lw.prod_pct_lw, 0) - COALESCE(fw.prod_pct_fw, 0), 2) AS productivity_decline,
          round(
            CASE 
              WHEN COALESCE(fw.act_pct_fw, 0) = 0 THEN 0
              ELSE ((COALESCE(lw.act_pct_lw, 0) - COALESCE(fw.act_pct_fw, 0)) / fw.act_pct_fw) * 100
            END, 2
          ) AS activity_decline,
          round(
            (COALESCE(lwa.job_hunt_lw, 0) / 7.0 * 100) - (COALESCE(fwa.job_hunt_fw, 0) / 7.0 * 100), 2
          ) AS job_hunting_sudden_rise,
          round(
            (COALESCE(lwa.unprod_tools_lw, 0) / 7.0 * 100) - (COALESCE(fwa.unprod_tools_fw, 0) / 7.0 * 100), 2
          ) AS unprod_tools_sudden_rise,
          round(ei.avg_active_hours, 1) AS avg_active_hours,
          CASE 
            WHEN (COALESCE(lw.prod_pct_lw, 0) - COALESCE(fw.prod_pct_fw, 0)) < -10 THEN 7
            WHEN (COALESCE(lw.prod_pct_lw, 0) - COALESCE(fw.prod_pct_fw, 0)) < -5 THEN 4
            ELSE 1
          END AS risk_score,
          ei.avg_active_hours < 6 AS deficit,
          round(ei.avg_productivity, 2) AS avg_productivity
        FROM employee_info ei
        LEFT JOIN first_week fw ON ei.employee_id = fw.employee_id
        LEFT JOIN last_week lw ON ei.employee_id = lw.employee_id
        LEFT JOIN first_week_activity fwa ON ei.employee_id = fwa.employee_id
        LEFT JOIN last_week_activity lwa ON ei.employee_id = lwa.employee_id
        WHERE 1=1
          {dept_filter_condition}
        ORDER BY risk_score DESC, ei.avg_active_hours ASC
        """

        print("Executing performance query...")
        rows = client.execute(query)

        employees = []
        for row in rows:
            try:
                employee = {
                    "employee_id": str(row[0]),
                    "name": str(row[1]),
                    "department": str(row[2]),
                    "late_arrivals": int(row[3]),
                    "absent_days": int(row[4]),
                    "productivity_decline": float(row[5]),
                    "activity_decline": float(row[6]),
                    "job_hunting_sudden_rise": float(row[7]),
                    "unprod_tools_sudden_rise": float(row[8]),
                    "avg_active_hours": float(row[9]),
                    "risk_score": int(row[10]),
                    "deficit": bool(row[11]),
                    "avg_productivity": float(row[12])
                }
                employees.append(employee)
            except Exception as e:
                print(f"Error processing row: {row}\nError: {e}")
                continue

        return {"employees": employees}

    except Exception as db_error:
        print(f"Database error: {db_error}")
        print(traceback.format_exc())
        return {"employees": []}

    finally:
        if client:
            client.disconnect()


# Generate sample performance data
def generate_sample_performance_data(department=None):
    departments = ["Engineering", "Marketing", "Sales", "Human Resources", "Finance"]
    
    # Generate employee names and assign departments
    employees = []
    
    # Sample data
    first_names = ["John", "Sarah", "Michael", "Emma", "David", "Lisa", "James", "Anna", "Robert", "Emily", "Sam", "Jessica"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Davis", "Miller", "Wilson", "Taylor", "Anderson"]
    
    # Create 20 sample employees
    for i in range(1, 21):
        emp_dept = random.choice(departments)
        
        # Skip if filtering by department and this isn't the right one
        if department and department.lower() != "all" and department != emp_dept:
            continue
            
        # Create risk profile (higher ID = higher risk for demo purposes)
        risk_factor = i / 20.0  # 0.05 to 1.0
        
        employees.append({
            "employee_id": str(i),
            "name": f"{random.choice(first_names)} {random.choice(last_names)}",
            "department": emp_dept,
            "late_arrivals": int(random.randint(0, 5) * risk_factor),
            "absent_days": int(random.randint(0, 3) * risk_factor),
            "productivity_decline": risk_factor > 0.7,
            "activity_decline": risk_factor > 0.6,
            "job_hunting_sudden_rise": risk_factor > 0.8,
            "unprod_tools_sudden_rise": risk_factor > 0.75,
            "avg_active_hours": round(8 - (3 * risk_factor), 1),
            "risk_score": 7 if risk_factor > 0.8 else (4 if risk_factor > 0.6 else 1),
            "deficit": risk_factor > 0.5,
            "productivity_change_pct": round(-15 * risk_factor if risk_factor > 0.7 else random.uniform(-2, 5), 1),
            "activity_change_pct": round(-12 * risk_factor if risk_factor > 0.6 else random.uniform(-3, 4), 1),
            "job_hunting_change_pct": round(20 * risk_factor if risk_factor > 0.8 else random.uniform(-1, 3), 1),
            "unprod_tools_change_pct": round(18 * risk_factor if risk_factor > 0.75 else random.uniform(-2, 3), 1),
            "avg_productivity": round(90 - (30 * risk_factor), 1)
        })
    
    return employees

@app.get("/overview/risk-assessment", response_model=Dict[str, Any])
def get_risk_assessment():
    try:
        client = connect_to_clickhouse()
        query = """
        WITH employee_data AS (
            SELECT 
                em.Group_Name as team,
                concat(em.First_Name, ' ', em.Last_Name) as employee_name,
                em.Employee_ID,
                round(AVG(em.Active_Percent), 1) as activity,
                round(AVG(em.Productive_Percent), 1) as productivity,
                round(SUM(
                    toInt32OrZero(splitByChar(':', em.Active_Duration)[1]) * 3600 + 
                    toInt32OrZero(splitByChar(':', em.Active_Duration)[2]) * 60 + 
                    toInt32OrZero(splitByChar(':', em.Active_Duration)[3])
                ) / 3600.0, 1) as total_active_seconds,
                COUNT(DISTINCT CASE 
                    WHEN em.Punch_In IS NOT NULL AND em.Punch_In != ''
                    THEN em.Attendance_Date 
                END) as days_present,
                COUNT(DISTINCT CASE 
                    WHEN em.Punch_In IS NOT NULL AND em.Punch_In != '' AND em.Punch_In > em.Shift_Start 
                    THEN em.Attendance_Date 
                END) as late_arrivals,
                COUNT(DISTINCT CASE 
                    WHEN em.Punch_In IS NULL OR em.Punch_In = ''
            THEN em.Attendance_Date
        END) as absences,
                COUNT(DISTINCT CASE 
                    WHEN em.Punch_Out IS NOT NULL AND em.Punch_Out != '' AND em.Punch_Out < em.Shift_End 
                    THEN em.Attendance_Date 
                END) as early_departures,
                COUNT(DISTINCT CASE 
                    WHEN lower(ea.URL) LIKE '%linkedin%/jobs%' OR
                         lower(ea.URL) LIKE '%indeed.com%' OR
                         lower(ea.URL) LIKE '%monster.com%' OR
                         lower(ea.URL) LIKE '%glassdoor.com/jobs%' OR
                         lower(ea.URL) LIKE '%careers%' OR
                         lower(ea.URL) LIKE '%/jobs%'
                    THEN ea.Date
                END) as job_search_visits
            FROM employee_metrics em
            LEFT JOIN employee_activity ea 
                ON em.Employee_ID = ea.Employee_ID 
                AND ea.Date BETWEEN '2025-04-01' AND '2025-04-30'
            WHERE em.Attendance_Date BETWEEN '2025-04-01' AND '2025-04-30'
            GROUP BY em.Group_Name, em.Employee_ID, concat(em.First_Name, ' ', em.Last_Name)
        )
        SELECT 
            team,
            employee_name,
            Employee_ID,
            COALESCE(activity, 0) as activity,
            COALESCE(productivity, 0) as productivity,
            COALESCE(total_active_seconds / NULLIF(days_present * 3600.0, 0), 0) as working_hours,
            COALESCE(days_present, 0) as days_present,
            COALESCE(late_arrivals, 0) as late_arrivals,
            COALESCE(absences, 0) as absences,
            COALESCE(early_departures, 0) as early_departures,
            COALESCE(job_search_visits, 0) as job_search_visits
        FROM employee_data
        """

        results = client.query(query).result_rows
        metrics_data = []
        
        for row in results:
            # Update unpacking to match 11 columns from query
            (team, name, employee_id, activity, productivity, working_hours, 
             days_present, late, absences, early, job_search) = row
            
            # Validate and clean numeric values
            try:
                activity_val = float(activity or 0)
                productivity_val = float(productivity or 0)
                working_hours_val = float(working_hours or 0)
                days_present_val = int(days_present or 0)
                late_val = int(late or 0)
                absences_val = int(absences or 0)
                early_val = int(early or 0)
                job_search_val = int(job_search or 0)

                # Handle infinity and NaN
                if math.isinf(working_hours_val) or math.isnan(working_hours_val):
                    working_hours_val = 0.0
                if math.isinf(activity_val) or math.isnan(activity_val):
                    activity_val = 0.0
                if math.isinf(productivity_val) or math.isnan(productivity_val):
                    productivity_val = 0.0

                # Calculate punctuality percentage safely
                else:
                    punctuality_pct = 0.0

                metrics_data.append({
                    "employee_id": str(employee_id),
                    "employee_name": str(name),
                    "team": str(team),
                    "metrics": {
                        "productivity": round(productivity_val, 1),
                        "activity": round(activity_val, 1),
                        "working_hours": round(working_hours_val, 1),
                        "attendance": {
                            "days_present": days_present_val,
                            "absences": absences_val,
                            "total_working_days": 30,
                            "punctuality": {
                                "percentage": round(punctuality_pct, 1),
                                "late_arrivals": late_val,
                                "early_departures": early_val
                            }
                        },
                        "job_search_visits": job_search_val
                    }
                })

            except (ValueError, TypeError) as e:
                print(f"Error processing row for {name}: {str(e)}")
                continue

        client.close()
        return {"employees": metrics_data}
        


    except Exception as e:
        print(f"Error in risk_assessment: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/overview/employee-weekly-trends", response_model=Dict[str, Any])
def get_employee_weekly_trends():
    try:
        client = connect_to_clickhouse()
        
        query = f"""
       WITH daily_metrics AS (
            SELECT
                concat(First_Name, ' ', Last_Name) as employee_name,
                Employee_ID as employee_id,
                               Group_Name as team,
                toDate('2025-04-01') + INTERVAL (intDiv(toUInt32(Attendance_Date - toDate('2025-04-01')), 7) * 7) DAY AS week_start,
                round(AVG(Productive_Percent), 2) as productivity,
                round(AVG(
                    toInt32OrZero(splitByChar(':', Online_Duration)[1]) * 3600 + 
                    toInt32OrZero(splitByChar(':', Online_Duration)[2]) * 60 + 
                    toInt32OrZero(splitByChar(':', Online_Duration)[3])
                ) / 3600.0, 1) as online_hours,
                COUNT(DISTINCT Attendance_Date) as days_worked
            FROM employee_metrics
            WHERE Attendance_Date >= toDate('2025-04-01')
                AND Attendance_Date <= toDate('2025-04-30')
            GROUP BY employee_name, employee_id, team, week_start
            ORDER BY week_start ASC
        )
        SELECT
            employee_name,
            employee_id,
            team,
            groupArray(week_start) as weeks,
            groupArray(productivity) as productivity_trend,
            groupArray(online_hours) as hours_trend,
            groupArray(days_worked) as attendance_trend
        FROM daily_metrics
        GROUP BY employee_name, employee_id, team
        ORDER BY employee_name
        """
        
        results = client.query(query).result_rows
        trends_data = []
        
        for row in results:
            employee_name, employee_id, team, weeks, productivity, hours, attendance = row
            trend_points = []
            
            for i in range(len(weeks)):
                try:
                    prod_value = float(productivity[i]) if productivity[i] is not None else 0.0
                    hours_value = float(hours[i]) if hours[i] is not None else 0.0
                    days_value = int(attendance[i]) if attendance[i] is not None else 0
                    
                    if math.isinf(prod_value) or math.isnan(prod_value):
                        prod_value = 0.0
                    if math.isinf(hours_value) or math.isnan(hours_value):
                        hours_value = 0.0
                    
                    date_obj = weeks[i]
                    week_num = ((date_obj - datetime(2025, 4, 1).date()).days // 7) + 1
                    
                    trend_points.append({
                        "week": f"Week {week_num}",
                        "productivity": round(prod_value, 2),
                        "hours": round(hours_value, 1),
                        "days_worked": days_value
                    })
                except (ValueError, TypeError, AttributeError) as e:
                    print(f"Error processing trend point: {e}")
                    continue
            
            if trend_points:
                trends_data.append({
                    "employee_name": employee_name,
                    "employee_id": employee_id,
                    "team": team,
                    "trends": trend_points
                })
        
        client.close()
        return {"employees": trends_data}
        
    except Exception as e:
        error_details = traceback.format_exc()
        print(f"Error in employee_weekly_trends: {error_details}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch employee weekly trends: {str(e)}")


@app.get("/get-attendance")
def get_attendance():
    try:
        client = connect_to_clickhouse()
        query = """
        SELECT 
            Employee,
            SUM(CASE WHEN Status = 'Present' THEN count_status ELSE 0 END) AS Present_Count,
            SUM(CASE WHEN Status = 'Absent' THEN count_status ELSE 0 END) AS Absent_Count,
            SUM(CASE WHEN Status = 'Weekly off' THEN count_status ELSE 0 END) AS WeeklyOff_Count
        FROM (
            SELECT 
                Employee,
                Status,
                COUNT(*) AS count_status
            FROM 
                monthly_attendance
            WHERE 
                Status IN ('Present', 'Absent', 'Weekly off')
            GROUP BY 
                Employee, Status
        ) AS attendance_counts
        GROUP BY 
            Employee
        """
        
        result = client.query(query)
        
        attendance_data = []
        for row in result.result_rows:
            employee = row[0]
            present = row[1]
            absent = row[2]
            total_days = present + absent

            if total_days > 0:
                attendance_rate = round((present / total_days) * 100, 2)
            else:
                attendance_rate = 0.0  # or None, or some other logic

            attendance_data.append({
                "employee": employee,
                "attendance_rate": attendance_rate,
            })
        return {"attendance": attendance_data}
    
    except Exception as e:
        print(f"Error fetching attendance data: {str(e)}")
        return {"attendance": []}
    
@app.post("/api/team/recommendations")
async def get_team_recommendations(request: Request):
    try:
        data = await request.json()
        team_data = data.get('team_data', {})
        app_usage_data = None
        
        # Get app usage data for the team
        try:
            team_name = team_data.get('team_name')
            if team_name:
                client = connect_to_clickhouse()
                duration_parsing = """
                IF(
                    match(Duration, '\\d+h'),
                    toInt32OrZero(regexpExtract(Duration, '(\\d+)h', 1)) * 3600,
                    0
                ) +
                IF(
                    match(Duration, '\\d+m'),
                    toInt32OrZero(regexpExtract(Duration, '(\\d+)m', 1)) * 60,
                    0
                ) +
                IF(
                    match(Duration, '\\d+s'),
                    toInt32OrZero(regexpExtract(Duration, '(\\d+)s', 1)),
                    0
                )
                """

                productive_apps_query = f"""
                SELECT 
                    CASE
                        WHEN Application != '' AND Application IS NOT NULL THEN Application
                        WHEN URL != '' AND URL IS NOT NULL THEN URL
                        ELSE 'Unknown'
                    END as app_name,
                    COUNT(DISTINCT Employee_ID) as user_count,
                    SUM({duration_parsing}) as total_seconds
                FROM employee_activity
                WHERE lower(Mapping_Status) = 'productive' AND Team = '{team_name}'
                GROUP BY app_name
                ORDER BY total_seconds DESC
                Limit 10
                """

                unproductive_apps_query = f"""
                SELECT 
                    CASE
                        WHEN Application != '' AND Application IS NOT NULL THEN Application
                        WHEN URL != '' AND URL IS NOT NULL THEN URL
                        ELSE 'Unknown'
                    END as app_name,
                    COUNT(DISTINCT Employee_ID) as user_count,
                    SUM({duration_parsing}) as total_seconds
                FROM employee_activity
                WHERE lower(Mapping_Status) = 'unproductive' AND Team = '{team_name}'
                GROUP BY app_name
                ORDER BY total_seconds DESC
                Limit 10
                """

                neutral_apps_query = f"""
                SELECT 
                    CASE
                        WHEN Application != '' AND Application IS NOT NULL THEN Application
                        WHEN URL != '' AND URL IS NOT NULL THEN URL
                        ELSE 'Unknown'
                    END as app_name,
                    COUNT(DISTINCT Employee_ID) as user_count,
                    SUM({duration_parsing}) as total_seconds
                FROM employee_activity
                WHERE lower(Mapping_Status) = 'neutral' AND Team = '{team_name}'
                GROUP BY app_name
                ORDER BY total_seconds DESC
                Limit 10
                """

                productive_results = client.query(productive_apps_query).result_rows
                unproductive_results = client.query(unproductive_apps_query).result_rows
                neutral_results = client.query(neutral_apps_query).result_rows
                
                app_usage_data = {
                    "productive_apps": [],
                    "unproductive_apps": [],
                    "neutral_apps": []
                }
                
                # Process productive apps
                for row in productive_results:
                    try:
                        app_name, user_count, seconds = row
                        hours = float(seconds) / 3600.0 if seconds is not None else 0
                        formatted_duration = f"{int(hours)}h:{int((hours % 1) * 60)}m"
                        app_usage_data["productive_apps"].append({
                            "name": app_name,
                            "hours": round(hours, 1),
                            "formatted_duration": formatted_duration,
                            "user_count": int(user_count) if user_count is not None else 0
                        })
                    except Exception as e:
                        logger.error(f"Error processing productive row: {str(e)}")
                
                # Process unproductive apps
                for row in unproductive_results:
                    try:
                        app_name, user_count, seconds = row
                        hours = float(seconds) / 3600.0 if seconds is not None else 0
                        formatted_duration = f"{int(hours)}h:{int((hours % 1) * 60)}m"
                        app_usage_data["unproductive_apps"].append({
                            "name": app_name,
                            "hours": round(hours, 1),
                            "formatted_duration": formatted_duration,
                            "user_count": int(user_count) if user_count is not None else 0
                        })
                    except Exception as e:
                        logger.error(f"Error processing unproductive row: {str(e)}")
                
                # Process neutral apps
                for row in neutral_results:
                    try:
                        app_name, user_count, seconds = row
                        hours = float(seconds) / 3600.0 if seconds is not None else 0
                        formatted_duration = f"{int(hours)}h:{int((hours % 1) * 60)}m"
                        app_usage_data["neutral_apps"].append({
                            "name": app_name,
                            "hours": round(hours, 1),
                            "formatted_duration": formatted_duration,
                            "user_count": int(user_count) if user_count is not None else 0
                        })
                    except Exception as e:
                        logger.error(f"Error processing neutral row: {str(e)}")
        except Exception as e:
            print(f"Error fetching app usage data: {str(e)}")
            # Continue with what we have - app usage data is optional enhancement
        
        # Construct the prompt
        prompt = f"""
Team Name: {team_data.get('team_name')}
Employees: {team_data.get('employee_count')}
Break avg: {team_data.get('break_time')} min
Performance:
- Online: {team_data.get('performance', {}).get('online')} hrs
- Productive: {team_data.get('performance', {}).get('productive')} hrs
- Active: {team_data.get('performance', {}).get('active')} hrs
- Idle: {team_data.get('performance', {}).get('idle')} hrs
- Neutral: {team_data.get('performance', {}).get('neutral')} hrs
- Unproductive: {team_data.get('performance', {}).get('unproductive')} hrs
"""

        if app_usage_data:
            prompt += f"""
Engagement Neutral: {json.dumps(app_usage_data.get('neutral_apps', []), indent=2)} important for analysis
Engagement Productive: {json.dumps(app_usage_data.get('productive_apps', []), indent=2)} important for analysis
Engagement Unproductive: {json.dumps(app_usage_data.get('unproductive_apps', []), indent=2)} important for analysis
"""

        prompt += f"""
Least productive day: {team_data.get('least_productive_day')}
Workload Index: {team_data.get('workload_index')}
Alert: {team_data.get('alert')}

INSTRUCTIONS:
- Do NOT include any introduction or mention the team name.
- Provide only 3 to 4 concise, direct, actionable bullet points to improve productivity.
- Each bullet point must start with '*'.
- Limit each bullet point to 10-15 words.
- Address root causes like excessive social media, entertainment, or job hunting app usage.
- Recommend specific actions to reduce usage of time-wasting apps.
- Be specific, clear, and avoid vague statements.

Analyze the given data focusing on neutral and unproductive app usage. Generate 3-4 concise, actionable bullet points prioritizing reduction of neutral and unproductive app time to improve productivity. Each point should:

* Start with an action verb
* Mention app name(s) and total hours spent
* Priotize entertainment apps showing
* Provide a clear recommendation to limit or reduce usage
* Avoid any introduction or conclusion, only bullet points
* Keep bullet points 10-15 words each

Input data includes:
- team performance and engagement hours
- neutral and unproductive app lists with name and total hours
- break time and least productive day

Output only the bullet points with no extra text or explanation.
        """

        # Initialize Gemini model with appropriate config
        try:
            import google.generativeai as genai
            
            # Configure the API
            genai.configure(api_key="AIzaSyAcbqan-PqKDHO0WNlrcfa2O3JN8lbEqlk")
            
            # Create the model
            model = genai.GenerativeModel('gemini-2.0-flash')
            
            # Generate content
            response = model.generate_content(prompt)
            recommendation = response.text
        except Exception as e:
            print(f"Error with Gemini API: {str(e)}")
            # Fallback to rule-based recommendations
            recommendation = generate_team_recommendations_fallback(team_data, app_usage_data)
        
        return {"recommendation": recommendation}
        
    except Exception as e:
        print(f"Error generating team recommendation: {str(e)}")
        print(traceback.format_exc())
        return {"recommendation": "Failed to generate recommendation. Please try again."}

def generate_team_recommendations_fallback(team_data, app_usage_data=None):
    """Generate rule-based recommendations when AI fails"""
    recommendations = []
    
    # Check for performance issues
    performance = team_data.get('performance', {})
    unproductive_hours = performance.get('unproductive', 0)
    
    if unproductive_hours > 1.5:
        recommendations.append("* Reduce unproductive app time which currently averages over 1.5 hours per day.")
    
    # Check app usage if available
    if app_usage_data:
        unproductive_apps = app_usage_data.get('unproductive_apps', [])
        if unproductive_apps and len(unproductive_apps) > 0:
            top_app = unproductive_apps[0]
            recommendations.append(f"* Limit usage of {top_app.get('name')} which consumes {top_app.get('hours', 0)} hours daily.")
    
    # Check for least productive day
    least_productive_day = team_data.get('least_productive_day')
    if least_productive_day:
        recommendations.append(f"* Schedule important tasks outside of {least_productive_day}, when productivity is lowest.")
    
    # Add a generic recommendation if we don't have many
    if len(recommendations) < 3:
        recommendations.append("* Schedule regular productivity checkpoints to monitor and address efficiency issues.")
        
    if len(recommendations) < 3:
        recommendations.append("* Implement structured break times to maintain focus throughout the workday.")
    
    return "\n".join(recommendations)

@app.post("/api/employee/insight-recommendations")
async def get_employee_insight_recommendations(request: Request):
    try:
        data = await request.json()
        employee_data = data.get('employee_data', {})
        employee_name = data.get('employee_name', 'Employee')
        risk_score = float(data.get('risk_score', 0))
        
        # Extract key metrics for analysis
        metrics = employee_data or {}
        attendance_data = metrics.get('attendance', {})
        attendance_rate = float(attendance_data.get('attendance_rate', 0))
        days_present = int(attendance_data.get('days_present', 0))
        total_days = int(attendance_data.get('total_working_days', 30))
        job_search_visits = int(metrics.get('job_search_visits', 0))
        productivity_change = float(metrics.get('productivity_change', 0))
        activity_change = float(metrics.get('activity_change', 0))
        working_hours = float(metrics.get('working_hours', 0))
        productivity_week1 = float(metrics.get('productivity_week1', 0))
        productivity_week4 = float(metrics.get('productivity_week4', 0))
        activity_week1 = float(metrics.get('activity_week1', 0))
        activity_week4 = float(metrics.get('activity_week4', 0))
        
        # Determine specific scenario and urgency
        scenario = ''
        urgency = 'SCHEDULE'
        timeframe = 'this week'
        
        # Ghost Employee (Zero attendance)
        if attendance_rate == 0 or days_present == 0:
            scenario = 'GHOST_EMPLOYEE'
            urgency = 'EMERGENCY'
            timeframe = 'within 4 hours'
        # Flight Risk + Performance Issues
        elif job_search_visits >= 3 and productivity_change <= -20:
            scenario = 'ACTIVE_JOB_SEEKER_DECLINING'
            urgency = 'CRITICAL'
            timeframe = 'within 24hrs'
        # High Performer with Job Search Activity
        elif job_search_visits >= 2 and productivity_change > 20:
            scenario = 'HIGH_PERFORMER_FLIGHT_RISK'
            urgency = 'IMMEDIATE'
            timeframe = 'within 48hrs'
        # Overworked Employee
        elif working_hours > 12 and activity_change > 3:
            scenario = 'BURNOUT_RISK'
            urgency = 'URGENT'
            timeframe = 'within 24hrs'
        # Chronic Absenteeism without Performance Impact
        elif attendance_rate < 40 and abs(productivity_change) < 10:
            scenario = 'CHRONIC_ABSENCE_STABLE_PERFORMANCE'
            urgency = 'URGENT'
            timeframe = 'within 48hrs'
        # Improving Performance but Poor Attendance
        elif productivity_change > 30 and attendance_rate < 50:
            scenario = 'REMOTE_HIGH_PERFORMER'
            urgency = 'SCHEDULE'
            timeframe = 'this week'
        # Declining Everything (Triple Threat)
        elif attendance_rate < 70 and productivity_change < -10 and activity_change < -2:
            scenario = 'TRIPLE_DECLINE'
            urgency = 'CRITICAL'
            timeframe = 'within 12hrs'
        # Good Performance with Job Search
        elif job_search_visits > 0 and productivity_change > 0 and attendance_rate > 80:
            scenario = 'ENGAGED_JOB_SEEKER'
            urgency = 'SCHEDULE'
            timeframe = 'within one week'
            
        # Generate recommendations
        try:
            # Get specific context analysis based on scenario
            context_analysis = create_employee_context(scenario, employee_name, working_hours)
            
            # Format sign for display
            productivity_change_str = f"+{productivity_change}%" if productivity_change > 0 else f"{productivity_change}%"
            activity_change_str = f"+{activity_change}h" if activity_change > 0 else f"{activity_change}h"
            
            # Generate AI recommendations using our Gemini model
            model = genai.GenerativeModel('gemini-2.0-flash')
            
            prompt = f"""Act as an expert HR consultant. Do not include any introductions or generic statements. If {employee_name} has a risk score greater than 3, generate exactly 2 highly specific and personalized performance improvement recommendations based on their unique metrics and behavior patterns. If the risk score is 3 or below, provide a concise and sincere commendation highlighting one or two key strengths.

EMPLOYEE PROFILE - {employee_name}:
- Scenario Type: {scenario}
- Urgency Level: {urgency}
- Timeframe: {timeframe}
- Attendance: {attendance_rate}% ({days_present}/{total_days} days)
- Job Search Activity: {job_search_visits} visits
- Productivity: Week 1: {productivity_week1}% → Week 4: {productivity_week4}% ({productivity_change_str})
- Activity: Week 1: {activity_week1}h → Week 4: {activity_week4}h ({activity_change_str})
- Working Hours: {working_hours}hrs/day
- Risk Score: {risk_score}/10

SPECIFIC CONTEXT ANALYSIS:
{context_analysis}

PERSONALIZATION REQUIREMENTS:
1. Use {employee_name}'s name in recommendations
2. Reference their specific metrics (exact percentages, hours, days)
3. Address their unique situation, not generic templates
4. Provide different intervention strategies for each recommendation
5. Include specific actions HR can take immediately

RECOMMENDATION RULES:
- Start with urgency: {urgency}
- Include timeframe: {timeframe}
- Be 15-25 words each
- Address root cause, not just symptoms
- Provide specific next steps
- Make recommendations complementary (different approaches)

Generate exactly 2 personalized recommendations:"""
            
            response = model.generate_content(prompt)
            recommendations = response.text.strip().split('\n')
            
            # Process recommendations to match expected format
            recommendations = [line.strip() for line in recommendations if line.strip()]
            recommendations = [re.sub(r'^[•\-\d\.]\s*', '', line) for line in recommendations]
            recommendations = [re.sub(r'^\[|\]$', '', line) for line in recommendations]
            recommendations = [line for line in recommendations if len(line) > 15 and employee_name.split(' ')[0] in line]
            recommendations = recommendations[:2]
            
            if len(recommendations) < 2:
                recommendations = generate_employee_fallback_recommendations(metrics, scenario, urgency, timeframe, employee_name)
                
        except Exception as e:
            print(f"Error generating AI recommendations: {str(e)}")
            recommendations = generate_employee_fallback_recommendations(metrics, scenario, urgency, timeframe, employee_name)
        
        return {"recommendations": recommendations, "scenario": scenario, "urgency": urgency}
        
    except Exception as e:
        print(f"Error in insight recommendations: {str(e)}")
        print(traceback.format_exc())
        return {"recommendations": ["Error generating recommendations. Please try again."], "scenario": "ERROR"}

def create_employee_context(scenario, employee_name, working_hours):
    """Create context analysis for specific employee scenario"""
    if scenario == 'GHOST_EMPLOYEE':
        return f"{employee_name} has completely stopped coming to work. This suggests possible silent resignation, family emergency, or health crisis."
    elif scenario == 'ACTIVE_JOB_SEEKER_DECLINING':
        return f"{employee_name} is actively job hunting while their current performance is declining - clear disengagement pattern."
    elif scenario == 'HIGH_PERFORMER_FLIGHT_RISK':
        return f"{employee_name} is performing well but job searching - likely seeking better opportunities or career growth."
    elif scenario == 'BURNOUT_RISK':
        return f"{employee_name} is working excessive hours ({working_hours}hrs/day) which indicates potential burnout and unsustainable workload."
    elif scenario == 'CHRONIC_ABSENCE_STABLE_PERFORMANCE':
        return f"{employee_name} has poor attendance but maintains performance when present - possible personal/health issues."
    elif scenario == 'REMOTE_HIGH_PERFORMER':
        return f"{employee_name} has poor office attendance but excellent performance - thriving in remote/flexible work."
    elif scenario == 'TRIPLE_DECLINE':
        return f"{employee_name} shows decline across all metrics - comprehensive intervention needed immediately."
    elif scenario == 'ENGAGED_JOB_SEEKER':
        return f"{employee_name} is job searching but still engaged at work - proactive retention opportunity."
    else:
        return f"Performance analysis for {employee_name} requires specific targeted intervention based on metrics."

def generate_employee_fallback_recommendations(metrics, scenario, urgency, timeframe, employee_name):
    """Generate fallback recommendations when AI fails"""
    firstName = employee_name.split(' ')[0]
    recommendations = []
    
    attendance = metrics.get('attendance', {})
    attendance_rate = float(attendance.get('attendance_rate', 0))
    job_search_visits = int(metrics.get('job_search_visits', 0))
    productivity_change = float(metrics.get('productivity_change', 0))
    activity_change = float(metrics.get('activity_change', 0))
    working_hours = float(metrics.get('working_hours', 0))
    
    if scenario == 'GHOST_EMPLOYEE':
        recommendations.append(f"{urgency}: Contact {firstName}'s emergency contacts immediately - possible family/health crisis requiring immediate intervention {timeframe}.")
        recommendations.append(f"EMERGENCY: Schedule home visit for {firstName} to verify wellbeing and determine return-to-work feasibility within 24hrs.")
    elif scenario == 'ACTIVE_JOB_SEEKER_DECLINING':
        salary_text = 'competitive salary increase' if job_search_visits >= 5 else 'career advancement'
        recommendations.append(f"{urgency}: Offer {firstName} immediate role enhancement with {salary_text} {timeframe}.")
        recommendations.append(f"CRITICAL: Address {firstName}'s {abs(productivity_change)}% productivity decline with personalized performance coaching immediately.")
    elif scenario == 'HIGH_PERFORMER_FLIGHT_RISK':
        recommendations.append(f"{urgency}: Present {firstName} with leadership development track and mentorship program to retain top talent {timeframe}.")
        recommendations.append(f"IMMEDIATE: Conduct {firstName}'s compensation review with market-rate adjustment proposal to counter external offers within 48hrs.")
    elif scenario == 'BURNOUT_RISK':
        recommendations.append(f"{urgency}: Implement immediate workload redistribution for {firstName} - reduce daily hours from {working_hours} to 8hrs {timeframe}.")
        recommendations.append(f"URGENT: Provide {firstName} with wellness program enrollment and mandatory rest days to prevent burnout within 24hrs.")
    elif scenario == 'CHRONIC_ABSENCE_STABLE_PERFORMANCE':
        recommendations.append(f"{urgency}: Offer {firstName} flexible work arrangements - hybrid schedule to accommodate {attendance_rate}% attendance pattern {timeframe}.")
        recommendations.append(f"SCHEDULE: Investigate {firstName}'s personal circumstances privately and provide employee assistance program referral this week.")
    elif scenario == 'REMOTE_HIGH_PERFORMER':
        performance_text = 'excellent' if productivity_change > 0 else 'stable'
        recommendations.append(f"SCHEDULE: Formalize {firstName}'s remote work agreement recognizing {performance_text} performance despite low office presence.")
        recommendations.append(f"IMPLEMENT: Create outcome-based performance metrics for {firstName} rather than attendance-focused evaluation within one week.")
    elif scenario == 'TRIPLE_DECLINE':
        recommendations.append(f"{urgency}: Emergency intervention for {firstName} - comprehensive support plan addressing {attendance_rate}% attendance and performance decline {timeframe}.")
        recommendations.append(f"CRITICAL: Assign dedicated HR case manager to {firstName} for daily check-ins and immediate problem resolution within 12hrs.")
    elif scenario == 'ENGAGED_JOB_SEEKER':
        recommendations.append(f"SCHEDULE: Proactive career conversation with {firstName} - explore internal growth opportunities before external offers materialize {timeframe}.")
        recommendations.append(f"IMPLEMENT: Fast-track {firstName} for skill development programs and cross-functional projects to increase engagement this week.")
    else:
        recommendations.append(f"{urgency}: Schedule comprehensive performance review with {firstName} addressing {attendance_rate}% attendance and productivity patterns {timeframe}.")
        recommendations.append(f"IMPLEMENT: Create personalized development plan for {firstName} with specific goals and manager support within one week.")
    
    return recommendations

@app.post("/api/gemini/generate")
async def call_gemini(request: GeminiRequest):
    """Call Gemini API to generate content"""
    GEMINI_API_KEY = "AIzaSyAcbqan-PqKDHO0WNlrcfa2O3JN8lbEqlk"  # In production, use environment variables
    
    try:
        # Configure the Gemini API
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel('gemini-2.0-flash')
       
        
        for attempt in range(request.retries):
            try:
                # Generate content with safety settings and configuration
                response = model.generate_content(
                    request.prompt,
                    generation_config={
                        "temperature": 0.7,
                        "top_k": 40,
                        "top_p": 0.95,
                        "max_output_tokens": 1024,
                    },
                    safety_settings=[
                        {
                            "category": "HARM_CATEGORY_HARASSMENT",
                            "threshold": "BLOCK_MEDIUM_AND_ABOVE"
                        },
                        {
                            "category": "HARM_CATEGORY_HATE_SPEECH",
                            "threshold": "BLOCK_MEDIUM_AND_ABOVE"
                        },
                        {
                            "category": "HARM_CATEGORY_SEXUALLY_EXPLICIT",
                            "threshold": "BLOCK_MEDIUM_AND_ABOVE"
                        },
                        {
                            "category": "HARM_CATEGORY_DANGEROUS_CONTENT",
                            "threshold": "BLOCK_LOW_AND_ABOVE"  # Adjusted threshold
                        }
                    ]
                )
                
                # Enhanced safety check for blocked content
                if hasattr(response, 'prompt_feedback'):
                    block_reason = response.prompt_feedback.block_reason
                    if block_reason:
                        block_type = "dangerous_content" if "dangerous" in str(block_reason).lower() else "safety"
                        return {
                            "success": False,
                            "message": f"Content blocked ({block_type}): {block_reason}",
                            "block_type": block_type,
                            "response": None
                        }

                # Validate response content
                if response and hasattr(response, 'text'):
                    content = response.text.strip()
                    if content:
                        # Additional safety check on generated content
                        lower_content = content.lower()
                        if any(term in lower_content for term in ['dangerous', 'harmful', 'malicious']):
                            return {
                                "success": False,
                                "message": "Content flagged as potentially unsafe",
                                "block_type": "content_safety",
                                "response": None
                            }
                        return {
                            "success": True,
                            "response": content
                        }
                    
                return {
                    "success": False,
                    "message": "Empty response from Gemini API",
                    "response": None
                }
                    
            except Exception as e:
                error_msg = str(e).lower()
                if "safety" in error_msg or "dangerous" in error_msg or "blocked" in error_msg:
                    return {
                        "success": False,
                        "message": "Content blocked by safety filters. Please rephrase your request.",
                        "response": None
                    }
                
                if attempt == request.retries - 1:
                    raise HTTPException(
                        status_code=500,
                        detail=f"Gemini API error: {str(e)}"
                    )
                
                # Wait with exponential backoff before retrying
                await asyncio.sleep(2 ** attempt)
                continue
                
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate content: {str(e)}"
        )

# Server startup configuration
if __name__ == "__main__":
    try:
        print("Starting FastAPI server...")
        uvicorn.run(
            "app:app",
            host="127.0.0.1",
            port=3000,
            reload=True,
            log_level="info"
        )
    except Exception as e:
        print(f"Failed to start server: {str(e)}")
        logger.error(f"Server startup error: {traceback.format_exc()}")
