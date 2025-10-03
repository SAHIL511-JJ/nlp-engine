from flask import Flask, request, jsonify, g
import sqlite3
import time
import os

app = Flask(__name__)
DATABASE = 'company_flask.db'

def get_db():
    """Get thread-local database connection"""
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
        db.row_factory = sqlite3.Row
    return db

@app.teardown_appcontext
def close_connection(exception):
    """Close database connection at the end of request"""
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

def init_database():
    """Initialize database with sample data"""
    with app.app_context():
        db = get_db()
        cursor = db.cursor()
        
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
        
        # Insert sample data if empty
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
            db.commit()
        
        print("Database initialized with sample data")

def execute_sql_query(query: str, params=()):
    """Execute SQL query and return results as dictionaries"""
    db = get_db()
    cursor = db.cursor()
    cursor.execute(query, params)
    return [dict(row) for row in cursor.fetchall()]

def nlp_to_sql(natural_query: str) -> tuple:
    """Convert natural language to SQL"""
    query_lower = natural_query.lower()
    
    if 'how many' in query_lower and 'employee' in query_lower:
        return "SELECT COUNT(*) as employee_count FROM employees", ()
    elif 'average salary' in query_lower:
        if 'department' in query_lower:
            return "SELECT department, ROUND(AVG(salary), 2) as average_salary FROM employees GROUP BY department", ()
        else:
            return "SELECT ROUND(AVG(salary), 2) as average_salary FROM employees", ()
    elif 'engineering' in query_lower:
        return "SELECT * FROM employees WHERE department = 'Engineering'", ()
    elif 'sales' in query_lower:
        return "SELECT * FROM employees WHERE department = 'Sales'", ()
    elif 'hr' in query_lower:
        return "SELECT * FROM employees WHERE department = 'HR'", ()
    elif 'salary' in query_lower and 'department' in query_lower:
        return "SELECT department, ROUND(AVG(salary), 2) as avg_salary FROM employees GROUP BY department ORDER BY avg_salary DESC", ()
    elif 'highest paid' in query_lower:
        return "SELECT * FROM employees ORDER BY salary DESC LIMIT 5", ()
    elif 'recent' in query_lower or 'new' in query_lower:
        return "SELECT * FROM employees ORDER BY hire_date DESC LIMIT 5", ()
    elif 'all employee' in query_lower or 'list employee' in query_lower:
        return "SELECT * FROM employees", ()
    elif 'department' in query_lower:
        return "SELECT DISTINCT department FROM employees", ()
    else:
        return "SELECT * FROM employees LIMIT 10", ()

@app.route('/')
def root():
    return jsonify({
        "message": "NLP Query Engine - Flask Demo", 
        "status": "running",
        "endpoints": {
            "GET /api/schema": "Get database schema",
            "POST /api/query": "Process natural language query",
            "GET /api/query/history": "Get query history",
            "POST /api/ingest/database": "Connect to database"
        }
    })

@app.route('/api/query', methods=['POST'])
def process_query():
    start_time = time.time()
    
    data = request.get_json()
    if not data or 'query' not in data:
        return jsonify({"error": "Query is required"}), 400
    
    user_query = data['query']
    
    try:
        sql_query, params = nlp_to_sql(user_query)
        results = execute_sql_query(sql_query, params)
        
        return jsonify({
            "results": {
                "data": results,
                "count": len(results),
                "generated_sql": sql_query
            },
            "query_type": "sql",
            "response_time": round(time.time() - start_time, 3),
            "sources": ["database"]
        })
    except Exception as e:
        return jsonify({"error": f"Query failed: {str(e)}"}), 500

@app.route('/api/schema', methods=['GET'])
def get_schema():
    try:
        db = get_db()
        cursor = db.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        schema = {"tables": []}
        for table in tables:
            cursor.execute(f"PRAGMA table_info({table})")
            columns = []
            for row in cursor.fetchall():
                columns.append({
                    "name": row[1],
                    "type": row[2],
                    "nullable": not row[3],
                    "primary_key": row[5] == 1
                })
            
            # Get sample data
            cursor.execute(f"SELECT * FROM {table} LIMIT 2")
            sample_data = [dict(zip([col[0] for col in cursor.description], row)) for row in cursor.fetchall()]
            
            schema["tables"].append({
                "name": table,
                "columns": columns,
                "sample_data": sample_data,
                "estimated_purpose": "employee_data" if "employee" in table.lower() else "general_data"
            })
        
        schema["total_tables"] = len(tables)
        schema["total_columns"] = sum(len(table["columns"]) for table in schema["tables"])
        
        return jsonify(schema)
    except Exception as e:
        return jsonify({"error": f"Schema query failed: {str(e)}"}), 500

@app.route('/api/query/history', methods=['GET'])
def get_query_history():
    return jsonify({"history": [
        {"query": "Sample query 1", "timestamp": "2025-10-03T17:00:00", "response_time": 0.15},
        {"query": "Sample query 2", "timestamp": "2025-10-03T17:01:00", "response_time": 0.08}
    ]})

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

@app.route('/api/ingest/documents', methods=['POST'])
def upload_documents():
    return jsonify({
        "job_id": "demo_123",
        "status": "completed", 
        "total_files": 0,
        "processed_files": 0,
        "message": "Demo document upload endpoint"
    })

@app.route('/api/ingest/status/<job_id>', methods=['GET'])
def get_ingestion_status(job_id):
    return jsonify({
        "job_id": job_id,
        "status": "completed",
        "progress": 100.0,
        "processed_files": 0,
        "total_files": 0
    })

if __name__ == "__main__":
    # Initialize database before starting server
    init_database()
    print("🚀 NLP Query Engine starting on http://localhost:8000")
    print("📊 Available endpoints:")
    print("   GET  /                 - Server status")
    print("   GET  /api/schema       - Database schema")
    print("   POST /api/query        - Process natural language query")
    print("   GET  /api/query/history - Query history")
    print("\n💡 Try these queries:")
    print('   curl -X POST "http://localhost:8000/api/query" -H "Content-Type: application/json" -d \'{"query": "How many employees?"}\'')
    print('   curl -X POST "http://localhost:8000/api/query" -H "Content-Type: application/json" -d \'{"query": "Average salary by department"}\'')
    
    app.run(host="0.0.0.0", port=8000, debug=False)
