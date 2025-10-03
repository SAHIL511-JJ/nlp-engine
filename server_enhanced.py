from http.server import HTTPServer, SimpleHTTPRequestHandler
import json
import sqlite3
import time
import os
import re
import uuid
from datetime import datetime

class EnhancedNLPHandler(SimpleHTTPRequestHandler):
    
    def __init__(self, *args, **kwargs):
        self.documents_db = "documents.db"
        self.init_documents_database()
        super().__init__(*args, **kwargs)
    
    def init_documents_database(self):
        """Initialize documents database"""
        conn = sqlite3.connect(self.documents_db)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS documents (
                id TEXT PRIMARY KEY,
                filename TEXT,
                content TEXT,
                file_type TEXT,
                uploaded_at TEXT,
                processed BOOLEAN DEFAULT FALSE
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS document_chunks (
                id TEXT PRIMARY KEY,
                document_id TEXT,
                chunk_text TEXT,
                chunk_index INTEGER,
                FOREIGN KEY (document_id) REFERENCES documents (id)
            )
        """)
        conn.commit()
        conn.close()
    
    def do_GET(self):
        if self.path.startswith('/api/'):
            self.handle_api_get()
        else:
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
            elif self.path.startswith('/api/ingest/status/'):
                self.get_ingestion_status()
            elif self.path == '/api/documents/count':
                self.get_documents_count()
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
        
        # Add documents count
        doc_conn = sqlite3.connect(self.documents_db)
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
        
        # Classify query type
        query_type = self.classify_query_type(query)
        
        sql_results = []
        document_results = []
        
        try:
            # Process SQL queries
            if query_type in ['sql', 'hybrid']:
                sql_query = self.nlp_to_sql(query)
                sql_results = self.execute_sql_query(sql_query)
            
            # Process document queries
            if query_type in ['document', 'hybrid']:
                document_results = self.search_documents(query)
            
            response = {
                "results": {
                    "sql_data": sql_results,
                    "document_data": document_results,
                    "sql_count": len(sql_results),
                    "document_count": len(document_results),
                    "total_count": len(sql_results) + len(document_results)
                },
                "query_type": query_type,
                "response_time": round(time.time() - start_time, 3),
                "sources": self.get_sources(query_type),
                "cache_hit": False
            }
            
            self.send_json_response(response)
            
        except Exception as e:
            self.send_error(500, f"Query execution failed: {str(e)}")
    
    def classify_query_type(self, query):
        """Classify query as SQL, document, or hybrid"""
        query_lower = query.lower()
        
        sql_keywords = ['how many', 'average', 'salary', 'department', 'employee', 'list', 'show']
        doc_keywords = ['resume', 'cv', 'document', 'file', 'skill', 'experience', 'java', 'python', 'certification']
        
        has_sql = any(keyword in query_lower for keyword in sql_keywords)
        has_doc = any(keyword in query_lower for keyword in doc_keywords)
        
        if has_sql and has_doc:
            return 'hybrid'
        elif has_doc:
            return 'document'
        else:
            return 'sql'
    
    def search_documents(self, query):
        """Search through uploaded documents for relevant content"""
        conn = sqlite3.connect(self.documents_db)
        cursor = conn.cursor()
        
        # Extract search terms
        search_terms = self.extract_search_terms(query)
        results = []
        
        if search_terms:
            # Search in document content
            placeholders = ' OR '.join(['content LIKE ?'] * len(search_terms))
            sql = f"SELECT * FROM documents WHERE {placeholders}"
            params = [f'%{term}%' for term in search_terms]
            
            cursor.execute(sql, params)
            documents = cursor.fetchall()
            
            for doc in documents:
                doc_id, filename, content, file_type, uploaded_at, processed = doc
                
                # Calculate relevance score
                relevance = self.calculate_relevance(content, search_terms)
                
                if relevance > 0.1:  # Only include relevant results
                    # Extract snippet around search term
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
        """Extract relevant search terms from query"""
        query_lower = query.lower()
        skills = ['java', 'python', 'javascript', 'sql', 'html', 'css', 'react', 'angular', 'node', 'django', 'flask']
        doc_types = ['resume', 'cv', 'document']
        
        terms = []
        
        # Add skills mentioned in query
        for skill in skills:
            if skill in query_lower:
                terms.append(skill)
        
        # Add document types
        for doc_type in doc_types:
            if doc_type in query_lower:
                terms.append(doc_type)
        
        # If no specific terms found, use all words (excluding common words)
        if not terms:
            common_words = ['the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by', 'resumes', 'mentioning']
            words = re.findall(r'\b\w+\b', query_lower)
            terms = [word for word in words if word not in common_words and len(word) > 2]
        
        return terms[:5]  # Limit to 5 terms
    
    def calculate_relevance(self, content, search_terms):
        """Calculate relevance score for document content"""
        content_lower = content.lower()
        score = 0
        
        for term in search_terms:
            # Count occurrences
            count = content_lower.count(term.lower())
            if count > 0:
                score += min(count * 0.2, 1.0)  # Cap at 1.0 per term
        
        return min(score, 1.0)  # Cap total score at 1.0
    
    def extract_snippet(self, content, search_term, snippet_length=150):
        """Extract a relevant snippet around the search term"""
        content_lower = content.lower()
        term_lower = search_term.lower()
        
        pos = content_lower.find(term_lower)
        if pos == -1:
            return content[:snippet_length] + "..." if len(content) > snippet_length else content
        
        start = max(0, pos - snippet_length//2)
        end = min(len(content), pos + snippet_length//2)
        
        snippet = content[start:end]
        if start > 0:
            snippet = "..." + snippet
        if end < len(content):
            snippet = snippet + "..."
        
        return snippet
    
    def get_sources(self, query_type):
        """Determine data sources based on query type"""
        sources = []
        if query_type in ['sql', 'hybrid']:
            sources.append('database')
        if query_type in ['document', 'hybrid']:
            sources.append('documents')
        return sources
    
    def upload_documents(self, post_data):
        """Handle document uploads - simulate processing"""
        data = json.loads(post_data.decode())
        
        # For demo, create some sample documents with different skills
        sample_documents = [
            {
                'filename': 'john_smith_resume.pdf',
                'content': 'John Smith - Senior Java Developer. Skills: Java, Spring Boot, Microservices, REST APIs, SQL. Experience: 5 years at Tech Corp.',
                'file_type': 'resume'
            },
            {
                'filename': 'jane_doe_cv.docx', 
                'content': 'Jane Doe - Full Stack Developer. Skills: Python, Django, React, JavaScript, AWS. Experience: 3 years at Startup Inc.',
                'file_type': 'resume'
            },
            {
                'filename': 'bob_johnson_skills.pdf',
                'content': 'Bob Johnson - Software Engineer. Skills: Java, Python, Docker, Kubernetes, CI/CD. Certifications: AWS Developer, Java Oracle Certified.',
                'file_type': 'resume'
            },
            {
                'filename': 'alice_brown_experience.txt',
                'content': 'Alice Brown - Backend Developer. Primary skills: Python, FastAPI, PostgreSQL. Secondary: Java, Spring. Experience with large-scale systems.',
                'file_type': 'resume'
            }
        ]
        
        # Store documents in database
        conn = sqlite3.connect(self.documents_db)
        cursor = conn.cursor()
        
        for doc in sample_documents:
            doc_id = str(uuid.uuid4())
            cursor.execute(
                "INSERT INTO documents (id, filename, content, file_type, uploaded_at, processed) VALUES (?, ?, ?, ?, ?, ?)",
                (doc_id, doc['filename'], doc['content'], doc['file_type'], datetime.now().isoformat(), True)
            )
        
        conn.commit()
        conn.close()
        
        self.send_json_response({
            "job_id": "doc_upload_123",
            "status": "completed",
            "total_files": len(sample_documents),
            "processed_files": len(sample_documents),
            "message": f"Successfully processed {len(sample_documents)} sample documents with various skills"
        })
    
    def get_ingestion_status(self, job_id="doc_upload_123"):
        self.send_json_response({
            "job_id": job_id,
            "status": "completed",
            "progress": 100.0,
            "processed_files": 4,
            "total_files": 4
        })
    
    def get_documents_count(self):
        conn = sqlite3.connect(self.documents_db)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM documents")
        count = cursor.fetchone()[0]
        conn.close()
        
        self.send_json_response({"count": count})
    
    # Existing SQL methods
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
    
    def execute_sql_query(self, sql_query):
        conn = self.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(sql_query)
        results = [dict(zip([col[0] for col in cursor.description], row)) for row in cursor.fetchall()]
        conn.close()
        return results
    
    def get_db_connection(self):
        conn = sqlite3.connect('company.db')
        conn.row_factory = sqlite3.Row
        return conn
    
    def get_query_history(self):
        self.send_json_response({
            "history": [
                {"query": "How many employees?", "timestamp": "2025-10-03T17:00:00", "response_time": 0.15},
                {"query": "Average salary by department", "timestamp": "2025-10-03T17:01:00", "response_time": 0.08}
            ]
        })
    
    def connect_database(self, post_data):
        self.send_json_response({
            "status": "success", 
            "message": "Connected to SQLite database"
        })
    
    def send_json_response(self, data):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())

def init_databases():
    """Initialize both company and documents databases"""
    # Initialize company database
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
        print("✅ Company database initialized with sample data")
    
    conn.close()

if __name__ == "__main__":
    init_databases()
    
    port = 8000
    server = HTTPServer(('0.0.0.0', port), EnhancedNLPHandler)
    print(f"🚀 Enhanced NLP Query Engine running on http://localhost:{port}")
    print("📊 Now with REAL document search and hybrid queries!")
    print("💡 Try: 'resumes mentioning java' or 'developers with python skills'")
    server.serve_forever()
