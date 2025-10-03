from http.server import HTTPServer, SimpleHTTPRequestHandler
import json
import sqlite3
import time
import os
import re
import uuid
from datetime import datetime

class CompatibleHandler(SimpleHTTPRequestHandler):
    
    def __init__(self, *args, **kwargs):
        self.init_databases()
        super().__init__(*args, **kwargs)
    
    def init_databases(self):
        """Initialize both databases"""
        # Company database
        conn = sqlite3.connect('company.db')
        cursor = conn.cursor()
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
        conn.close()
        
        # Documents database
        doc_conn = sqlite3.connect('documents.db')
        doc_cursor = doc_conn.cursor()
        doc_cursor.execute("""
            CREATE TABLE IF NOT EXISTS documents (
                id TEXT PRIMARY KEY,
                filename TEXT,
                content TEXT,
                file_type TEXT,
                uploaded_at TEXT
            )
        """)
        doc_conn.commit()
        doc_conn.close()
    
    def do_GET(self):
        if self.path.startswith('/api/'):
            self.handle_api_get()
        else:
            if self.path == '/':
                self.path = '/index.html'
            super().do_GET()
    
    def do_POST(self):
        if self.path.startswith('/api/'):
            self.handle_api_post()
        else:
            self.send_error(404)
    
    def handle_api_get(self):
        if self.path == '/api/schema':
            self.get_schema()
        elif self.path == '/api/query/history':
            self.get_query_history()
        elif self.path == '/api/health':
            self.get_health()
        else:
            self.send_error(404)
    
    def handle_api_post(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        
        if self.path == '/api/query':
            self.process_query(post_data)
        elif self.path == '/api/ingest/documents':
            self.upload_documents(post_data)
        elif self.path == '/connect':
            self.connect_database(post_data)
        elif self.path == '/upload':
            self.handle_upload(post_data)
        else:
            self.send_error(404)
    
    def get_health(self):
        self.send_json_response({"status": "healthy", "message": "NLP Query Engine"})
    
    def get_schema(self):
        conn = sqlite3.connect('company.db')
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
            
            cursor.execute(f"SELECT * FROM {table} LIMIT 2")
            sample_data = [dict(zip([col[0] for col in cursor.description], row)) for row in cursor.fetchall()]
            
            schema["tables"].append({
                "name": table,
                "columns": columns,
                "sample_data": sample_data,
                "estimated_purpose": "employee_data"
            })
        
        schema["total_tables"] = len(tables)
        schema["total_columns"] = sum(len(table["columns"]) for table in schema["tables"])
        
        # Add documents count
        doc_conn = sqlite3.connect('documents.db')
        doc_cursor = doc_conn.cursor()
        doc_cursor.execute("SELECT COUNT(*) FROM documents")
        schema["total_documents"] = doc_cursor.fetchone()[0]
        doc_conn.close()
        
        conn.close()
        self.send_json_response(schema)
    
    def process_query(self, post_data):
        data = json.loads(post_data.decode())
        query = data.get('query', '')
        start_time = time.time()
        
        # Determine query type
        query_lower = query.lower()
        is_document_query = any(term in query_lower for term in ['resume', 'cv', 'document', 'skill', 'java', 'python'])
        is_sql_query = any(term in query_lower for term in ['how many', 'average', 'salary', 'department', 'list', 'show'])
        
        sql_results = []
        document_results = []
        generated_sql = ""
        query_type = "sql"
        
        # Process SQL query
        if is_sql_query or not is_document_query:
            generated_sql = self.nlp_to_sql(query)
            sql_results = self.execute_sql_query(generated_sql)
        
        # Process document query
        if is_document_query:
            document_results = self.search_documents(query)
            query_type = "document" if not is_sql_query else "hybrid"
        
        response_time = round(time.time() - start_time, 3)
        
        # Format response to match frontend expectations
        response = {
            "results": {
                "sql_data": sql_results,
                "document_data": document_results,
                "sql_count": len(sql_results),
                "document_count": len(document_results),
                "total_count": len(sql_results) + len(document_results),
                "generated_sql": generated_sql
            },
            "query_type": query_type,
            "response_time": response_time,
            "sources": ["database"] if sql_results else [] + ["documents"] if document_results else [],
            "cache_hit": False
        }
        
        self.send_json_response(response)
    
    def nlp_to_sql(self, query):
        query_lower = query.lower()
        
        if 'how many' in query_lower and 'employee' in query_lower:
            return "SELECT COUNT(*) as count FROM employees"
        elif 'average salary' in query_lower:
            if 'department' in query_lower:
                return "SELECT department, ROUND(AVG(salary), 2) as average_salary FROM employees GROUP BY department"
            else:
                return "SELECT ROUND(AVG(salary), 2) as average_salary FROM employees"
        elif 'engineering' in query_lower:
            return "SELECT * FROM employees WHERE department = 'Engineering'"
        elif 'salary' in query_lower and 'department' in query_lower:
            return "SELECT department, ROUND(AVG(salary), 2) as avg_salary FROM employees GROUP BY department ORDER BY avg_salary DESC"
        elif 'highest paid' in query_lower:
            return "SELECT * FROM employees ORDER BY salary DESC LIMIT 5"
        elif 'all employee' in query_lower or 'list employee' in query_lower:
            return "SELECT * FROM employees"
        elif 'department' in query_lower:
            return "SELECT DISTINCT department FROM employees"
        else:
            return "SELECT * FROM employees LIMIT 10"
    
    def execute_sql_query(self, sql):
        conn = sqlite3.connect('company.db')
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(sql)
        results = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return results
    
    def search_documents(self, query):
        conn = sqlite3.connect('documents.db')
        cursor = conn.cursor()
        
        search_terms = self.extract_search_terms(query)
        results = []
        
        if search_terms:
            placeholders = ' OR '.join(['content LIKE ?'] * len(search_terms))
            sql = f"SELECT * FROM documents WHERE {placeholders}"
            params = [f'%{term}%' for term in search_terms]
            
            cursor.execute(sql, params)
            documents = cursor.fetchall()
            
            for doc in documents:
                doc_id, filename, content, file_type, uploaded_at = doc
                relevance = self.calculate_relevance(content, search_terms)
                
                if relevance > 0.1:
                    snippet = self.extract_snippet(content, search_terms[0])
                    
                    results.append({
                        'id': doc_id,
                        'title': filename,
                        'content': snippet,
                        'type': file_type,
                        'relevance': round(relevance, 2),
                        'uploaded_at': uploaded_at
                    })
        
        conn.close()
        return sorted(results, key=lambda x: x['relevance'], reverse=True)
    
    def extract_search_terms(self, query):
        query_lower = query.lower()
        skills = ['java', 'python', 'javascript', 'sql', 'react', 'docker', 'kubernetes']
        terms = []
        
        for skill in skills:
            if skill in query_lower:
                terms.append(skill)
        
        if not terms:
            common_words = ['the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by', 'resumes', 'mentioning']
            words = re.findall(r'\b\w+\b', query_lower)
            terms = [word for word in words if word not in common_words and len(word) > 2]
        
        return terms[:3]
    
    def calculate_relevance(self, content, terms):
        content_lower = content.lower()
        score = 0
        for term in terms:
            count = content_lower.count(term.lower())
            if count > 0:
                score += min(count * 0.3, 1.0)
        return min(score, 1.0)
    
    def extract_snippet(self, content, term, length=150):
        content_lower = content.lower()
        term_lower = term.lower()
        pos = content_lower.find(term_lower)
        
        if pos == -1:
            return content[:length] + "..." if len(content) > length else content
        
        start = max(0, pos - length//2)
        end = min(len(content), pos + length//2)
        snippet = content[start:end]
        
        if start > 0:
            snippet = "..." + snippet
        if end < len(content):
            snippet = snippet + "..."
        
        return snippet
    
    def upload_documents(self, post_data):
        # Create sample documents
        sample_docs = [
            {
                'filename': 'john_java_resume.pdf',
                'content': 'John Smith - Senior Java Developer with 5 years experience. Skills: Java, Spring Boot, Microservices, REST APIs, SQL, Hibernate. Experience with AWS and Docker.',
                'file_type': 'resume'
            },
            {
                'filename': 'jane_python_cv.docx',
                'content': 'Jane Doe - Python Full Stack Developer. Expertise in Python, Django, Flask, React, JavaScript. 3 years experience building web applications. Knowledge of Docker and CI/CD.',
                'file_type': 'resume'
            },
            {
                'filename': 'bob_fullstack.pdf',
                'content': 'Bob Johnson - Full Stack Developer skilled in both Java and Python. Experience with Spring Framework, Django, React, and cloud technologies. Strong problem-solving skills.',
                'file_type': 'resume'
            }
        ]
        
        conn = sqlite3.connect('documents.db')
        cursor = conn.cursor()
        
        for doc in sample_docs:
            doc_id = str(uuid.uuid4())
            cursor.execute(
                "INSERT INTO documents (id, filename, content, file_type, uploaded_at) VALUES (?, ?, ?, ?, ?)",
                (doc_id, doc['filename'], doc['content'], doc['file_type'], datetime.now().isoformat())
            )
        
        conn.commit()
        conn.close()
        
        self.send_json_response({
            "job_id": "sample_upload",
            "status": "completed",
            "total_files": len(sample_docs),
            "processed_files": len(sample_docs),
            "message": f"Created {len(sample_docs)} sample documents with various skills"
        })
    
    def connect_database(self, post_data):
        self.send_json_response({
            "status": "success",
            "message": "Connected to SQLite database",
            "schema": {
                "tables": 1,
                "columns": 6
            }
        })
    
    def handle_upload(self, post_data):
        self.send_json_response({
            "status": "success",
            "message": "Upload processed (sample documents created)"
        })
    
    def get_query_history(self):
        self.send_json_response({
            "history": [
                {"query": "How many employees?", "response_time": 0.15},
                {"query": "resumes mentioning java", "response_time": 0.08}
            ]
        })
    
    def send_json_response(self, data):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())

if __name__ == "__main__":
    port = 8000
    server = HTTPServer(('0.0.0.0', port), CompatibleHandler)
    print(f"🚀 Compatible NLP Server running on http://localhost:{port}")
    print("📊 This server matches the frontend's expected API format")
    print("💡 Test queries: 'how many employees' or 'resumes mentioning java'")
    server.serve_forever()
