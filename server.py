from http.server import HTTPServer, SimpleHTTPRequestHandler
import json
import sqlite3
import time
import os

class NLPRequestHandler(SimpleHTTPRequestHandler):
    
    def do_GET(self):
        if self.path.startswith('/api/'):
            self.handle_api_get()
        else:
            # Serve HTML, CSS, JS files
            if self.path == '/':
                self.path = '/index.html'
            return super().do_GET()
    
    def do_POST(self):
        if self.path.startswith('/api/'):
            self.handle_api_post()
        else:
            self.send_error(404)
    
    def handle_api_get(self):
        try:
            if self.path == '/api/schema':
                self.get_schema()
            elif self.path == '/api/query/history':
                self.get_query_history()
            elif self.path == '/api/ingest/status/demo_123':
                self.get_ingestion_status()
            elif self.path == '/api/':
                self.get_root()
            else:
                self.send_error(404)
        except Exception as e:
            self.send_error(500, f"Server error: {str(e)}")
    
    def handle_api_post(self):
        try:
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            
            if self.path == '/api/query':
                self.process_query(post_data)
            elif self.path == '/api/ingest/database':
                self.connect_database(post_data)
            elif self.path == '/api/ingest/documents':
                self.upload_documents(post_data)
            else:
                self.send_error(404)
        except Exception as e:
            self.send_error(500, f"Server error: {str(e)}")
    
    def get_root(self):
        self.send_json_response({
            "message": "NLP Query Engine - Combined Server", 
            "status": "running",
            "version": "1.0.0"
        })
    
    def get_schema(self):
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
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
        
        conn.close()
        self.send_json_response(schema)
    
    def process_query(self, post_data):
        data = json.loads(post_data.decode())
        query = data.get('query', '')
        start_time = time.time()
        
        # Convert natural language to SQL
        sql_query = self.nlp_to_sql(query)
        
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute(sql_query)
            results = [dict(zip([col[0] for col in cursor.description], row)) for row in cursor.fetchall()]
            
            response = {
                "results": {
                    "data": results,
                    "count": len(results),
                    "generated_sql": sql_query
                },
                "query_type": "sql",
                "response_time": round(time.time() - start_time, 3),
                "sources": ["database"],
                "cache_hit": False
            }
            
            self.send_json_response(response)
            
        except Exception as e:
            self.send_error(500, f"Query execution failed: {str(e)}")
        finally:
            conn.close()
    
    def nlp_to_sql(self, natural_query):
        query_lower = natural_query.lower()
        
        if 'how many' in query_lower and 'employee' in query_lower:
            return "SELECT COUNT(*) as employee_count FROM employees"
        elif 'average salary' in query_lower:
            if 'department' in query_lower:
                return "SELECT department, ROUND(AVG(salary), 2) as average_salary FROM employees GROUP BY department"
            else:
                return "SELECT ROUND(AVG(salary), 2) as average_salary FROM employees"
        elif 'engineering' in query_lower:
            return "SELECT * FROM employees WHERE department = 'Engineering'"
        elif 'sales' in query_lower:
            return "SELECT * FROM employees WHERE department = 'Sales'"
        elif 'hr' in query_lower or 'human resources' in query_lower:
            return "SELECT * FROM employees WHERE department = 'HR'"
        elif 'marketing' in query_lower:
            return "SELECT * FROM employees WHERE department = 'Marketing'"
        elif 'salary' in query_lower and 'department' in query_lower:
            return "SELECT department, ROUND(AVG(salary), 2) as avg_salary FROM employees GROUP BY department ORDER BY avg_salary DESC"
        elif 'highest paid' in query_lower:
            return "SELECT * FROM employees ORDER BY salary DESC LIMIT 5"
        elif 'recent' in query_lower or 'new' in query_lower:
            return "SELECT * FROM employees ORDER BY hire_date DESC LIMIT 5"
        elif 'all employee' in query_lower or 'list employee' in query_lower:
            return "SELECT * FROM employees"
        elif 'department' in query_lower:
            return "SELECT DISTINCT department FROM employees"
        elif 'python' in query_lower:
            return "SELECT * FROM employees WHERE position LIKE '%Python%' OR position LIKE '%Developer%'"
        else:
            return "SELECT * FROM employees LIMIT 10"
    
    def connect_database(self, post_data):
        self.send_json_response({
            "status": "success", 
            "message": "Connected to SQLite database",
            "schema": {
                "tables": 1,
                "columns": 6
            }
        })
    
    def upload_documents(self, post_data):
        self.send_json_response({
            "job_id": "demo_123",
            "status": "processing",
            "total_files": 0,
            "processed_files": 0
        })
    
    def get_ingestion_status(self):
        self.send_json_response({
            "job_id": "demo_123",
            "status": "completed",
            "progress": 100.0,
            "processed_files": 0,
            "total_files": 0
        })
    
    def get_query_history(self):
        self.send_json_response({
            "history": [
                {"query": "How many employees?", "timestamp": "2025-10-03T17:00:00", "response_time": 0.15},
                {"query": "Average salary by department", "timestamp": "2025-10-03T17:01:00", "response_time": 0.08}
            ]
        })
    
    def get_db_connection(self):
        conn = sqlite3.connect('company.db')
        conn.row_factory = sqlite3.Row
        return conn
    
    def send_json_response(self, data):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())

def init_database():
    """Initialize database with sample data"""
    conn = sqlite3.connect('company.db')
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
    
    # Insert sample data if empty
    cursor.execute("SELECT COUNT(*) FROM employees")
    if cursor.fetchone()[0] == 0:
        employees = [
            ('John Smith', 'Engineering', 'Python Developer', 95000, '2022-01-15'),
            ('Jane Doe', 'Engineering', 'Senior Developer', 110000, '2021-03-20'),
            ('Bob Johnson', 'Sales', 'Sales Manager', 85000, '2020-11-10'),
            ('Alice Brown', 'HR', 'HR Specialist', 75000, '2023-02-28'),
            ('Charlie Wilson', 'Engineering', 'Data Scientist', 105000, '2022-07-12'),
            ('Diana Lee', 'Marketing', 'Marketing Manager', 90000, '2021-09-05')
        ]
        cursor.executemany(
            "INSERT INTO employees (name, department, position, salary, hire_date) VALUES (?, ?, ?, ?, ?)",
            employees
        )
        conn.commit()
        print("✅ Database initialized with sample data")
    
    conn.close()

if __name__ == "__main__":
    # Initialize database
    init_database()
    
    # Start server
    port = 8000
    server = HTTPServer(('0.0.0.0', port), NLPRequestHandler)
    print(f"🚀 NLP Query Engine running on http://localhost:{port}")
    print("📊 Serving both frontend and API from same origin")
    print("💡 No CORS issues - everything works together!")
    print("\nOpen your browser to: http://localhost:8000")
    server.serve_forever()
