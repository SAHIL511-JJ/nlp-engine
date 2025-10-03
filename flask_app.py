from flask import Flask, request, jsonify
import sqlite3
import json
import time
from datetime import datetime

app = Flask(__name__)

# Simple SQLite setup
def setup_database():
    conn = sqlite3.connect("company_flask.db")
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
            ('John Smith', 'Engineering', 'Python Developer', 95000, '2022-01-15'),
            ('Jane Doe', 'Engineering', 'Senior Developer', 110000, '2021-03-20'),
            ('Bob Johnson', 'Sales', 'Sales Manager', 85000, '2020-11-10'),
            ('Alice Brown', 'HR', 'HR Specialist', 75000, '2023-02-28'),
            ('Charlie Wilson', 'Engineering', 'Data Scientist', 105000, '2022-07-12')
        ]
        cursor.executemany(
            "INSERT INTO employees (name, department, position, salary, hire_date) VALUES (?, ?, ?, ?, ?)",
            employees
        )
        conn.commit()
    
    return conn

# Initialize database
db_conn = setup_database()

def execute_sql_query(query: str):
    """Execute SQL query and return results as dictionaries"""
    cursor = db_conn.cursor()
    cursor.execute(query)
    columns = [description[0] for description in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]

def nlp_to_sql(natural_query: str) -> str:
    """Convert natural language to SQL"""
    query_lower = natural_query.lower()
    
    if 'how many' in query_lower and 'employee' in query_lower:
        return "SELECT COUNT(*) as employee_count FROM employees"
    elif 'average salary' in query_lower:
        if 'department' in query_lower:
            return "SELECT department, AVG(salary) as average_salary FROM employees GROUP BY department"
        else:
            return "SELECT AVG(salary) as average_salary FROM employees"
    elif 'engineering' in query_lower:
        return "SELECT * FROM employees WHERE department = 'Engineering'"
    elif 'salary' in query_lower and 'department' in query_lower:
        return "SELECT department, AVG(salary) as avg_salary FROM employees GROUP BY department"
    elif 'highest paid' in query_lower:
        return "SELECT * FROM employees ORDER BY salary DESC LIMIT 5"
    elif 'all employee' in query_lower or 'list employee' in query_lower:
        return "SELECT * FROM employees"
    elif 'department' in query_lower:
        return "SELECT DISTINCT department FROM employees"
    else:
        return "SELECT * FROM employees LIMIT 10"

@app.route('/')
def root():
    return jsonify({"message": "NLP Query Engine - Flask Demo", "status": "running"})

@app.route('/api/query', methods=['POST'])
def process_query():
    start_time = time.time()
    
    data = request.get_json()
    if not data or 'query' not in data:
        return jsonify({"error": "Query is required"}), 400
    
    user_query = data['query']
    
    try:
        sql_query = nlp_to_sql(user_query)
        results = execute_sql_query(sql_query)
        
        return jsonify({
            "results": results,
            "query_type": "sql",
            "response_time": time.time() - start_time,
            "generated_sql": sql_query
        })
    except Exception as e:
        return jsonify({"error": f"Query failed: {str(e)}"}), 500

@app.route('/api/schema', methods=['GET'])
def get_schema():
    cursor = db_conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cursor.fetchall()]
    
    schema = {}
    for table in tables:
        cursor.execute(f"PRAGMA table_info({table})")
        columns = [{"name": row[1], "type": row[2]} for row in cursor.fetchall()]
        schema[table] = columns
    
    return jsonify({"tables": schema})

@app.route('/api/query/history', methods=['GET'])
def get_query_history():
    return jsonify({"history": []})

@app.route('/api/ingest/database', methods=['POST'])
def connect_database():
    return jsonify({
        "status": "success", 
        "message": "Connected to SQLite database",
        "schema": {
            "tables": 1,
            "columns": 6
        }
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
