from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import sqlite3
import json
import time
from datetime import datetime

app = FastAPI(title="NLP Query Engine - Simple Demo")

# Simple SQLite setup
def setup_database():
    conn = sqlite3.connect("company_simple.db")
    cursor = conn.cursor()
    
    # Create tables
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS employees (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            department TEXT,
            position TEXT,
            salary REAL,
            hire_date TEXT
        )
    """)
    
    # Insert sample data
    cursor.execute("SELECT COUNT(*) FROM employees")
    if cursor.fetchone()[0] == 0:
        employees = [
            ('John Smith', 'Engineering', 'Developer', 95000, '2022-01-15'),
            ('Jane Doe', 'Engineering', 'Senior Developer', 110000, '2021-03-20'),
            ('Bob Johnson', 'Sales', 'Sales Manager', 85000, '2020-11-10'),
            ('Alice Brown', 'HR', 'HR Specialist', 75000, '2023-02-28')
        ]
        cursor.executemany(
            "INSERT INTO employees (name, department, position, salary, hire_date) VALUES (?, ?, ?, ?, ?)",
            employees
        )
        conn.commit()
    
    return conn

# Initialize database
db_conn = setup_database()

class QueryRequest(BaseModel):
    query: str

class QueryResponse(BaseModel):
    results: List[Dict[str, Any]]
    query_type: str
    response_time: float

def execute_sql_query(query: str) -> List[Dict[str, Any]]:
    """Execute SQL query and return results as dictionaries"""
    cursor = db_conn.cursor()
    cursor.execute(query)
    columns = [description[0] for description in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]

def nlp_to_sql(natural_query: str) -> str:
    """Convert natural language to SQL"""
    query_lower = natural_query.lower()
    
    if 'how many' in query_lower and 'employee' in query_lower:
        return "SELECT COUNT(*) as count FROM employees"
    elif 'average salary' in query_lower:
        return "SELECT AVG(salary) as average_salary FROM employees"
    elif 'engineering' in query_lower:
        return "SELECT * FROM employees WHERE department = 'Engineering'"
    elif 'all employee' in query_lower:
        return "SELECT * FROM employees"
    elif 'department' in query_lower:
        return "SELECT DISTINCT department FROM employees"
    else:
        return "SELECT * FROM employees LIMIT 10"

@app.get("/")
async def root():
    return {"message": "NLP Query Engine - Simple Demo", "status": "running"}

@app.post("/api/query")
async def process_query(request: QueryRequest) -> QueryResponse:
    start_time = time.time()
    
    try:
        sql_query = nlp_to_sql(request.query)
        results = execute_sql_query(sql_query)
        
        return QueryResponse(
            results=results,
            query_type="sql",
            response_time=time.time() - start_time
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")

@app.get("/api/schema")
async def get_schema():
    cursor = db_conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cursor.fetchall()]
    
    schema = {}
    for table in tables:
        cursor.execute(f"PRAGMA table_info({table})")
        columns = [{"name": row[1], "type": row[2]} for row in cursor.fetchall()]
        schema[table] = columns
    
    return {"tables": schema}

@app.get("/api/query/history")
async def get_query_history():
    return {"history": []}  # Simple demo - no history tracking

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
