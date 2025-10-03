from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import uvicorn
from sqlalchemy import create_engine, text, inspect
import logging
import time
import hashlib
import json
from datetime import datetime, timedelta
import asyncio
import aiofiles
import os
import uuid
import sqlite3
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="NLP Query Engine", version="1.0.0")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# SQLite database (primary company data)
DATABASE_URL = "sqlite:///./company.db"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})

# Documents database (for document queries)
DOCUMENTS_DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "documents.db"))

# Global mutable connection string
CURRENT_DB_URL = DATABASE_URL

# Utility to reset engine and schema when user provides a new connection string
def reset_database_connection(new_connection_string: str):
	global engine, CURRENT_DB_URL, schema_discovery, current_schema, query_engine
	CURRENT_DB_URL = new_connection_string
	engine = create_engine(CURRENT_DB_URL, connect_args={"check_same_thread": False}) if CURRENT_DB_URL.startswith("sqlite") else create_engine(CURRENT_DB_URL)
	schema_discovery = SchemaDiscovery()
	current_schema = schema_discovery.analyze_database()
	query_engine = QueryEngine(current_schema)
	logger.info(f"Switched DB connection to {CURRENT_DB_URL}")

# Initialize demo data
def init_demo_data():
    """Create demo tables and data"""
    with engine.connect() as conn:
        # Create employees table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS employees (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                department TEXT,
                position TEXT,
                salary REAL,
                hire_date TEXT,
                email TEXT
            )
        """))
        
        # Create departments table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS departments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                manager_id INTEGER
            )
        """))
        
        # Ensure email column exists for existing DBs
        try:
            cols = conn.execute(text("PRAGMA table_info(employees)"))
            colnames = {row[1] for row in cols}
            if 'email' not in colnames:
                conn.execute(text("ALTER TABLE employees ADD COLUMN email TEXT"))
        except Exception:
            pass

        # Insert sample data if empty
        result = conn.execute(text("SELECT COUNT(*) FROM employees"))
        if result.scalar() == 0:
            employees = [
                {'name': 'Aarav Sharma', 'department': 'Engineering', 'position': 'Python Developer', 'salary': 95000, 'hire_date': '2022-01-15', 'email': 'aarav.sharma@company.com'},
                {'name': 'Isha Verma', 'department': 'Engineering', 'position': 'Senior Developer', 'salary': 112000, 'hire_date': '2021-03-20', 'email': 'isha.verma@company.com'},
                {'name': 'Rohan Kapoor', 'department': 'Sales', 'position': 'Sales Manager', 'salary': 88000, 'hire_date': '2020-11-10', 'email': 'rohan.kapoor@company.com'},
                {'name': 'Sneha Iyer', 'department': 'HR', 'position': 'HR Specialist', 'salary': 76000, 'hire_date': '2023-02-28', 'email': 'sneha.iyer@company.com'},
                {'name': 'Vivaan Nair', 'department': 'Engineering', 'position': 'Data Scientist', 'salary': 108000, 'hire_date': '2022-07-12', 'email': 'vivaan.nair@company.com'},
                {'name': 'Diya Patel', 'department': 'Marketing', 'position': 'Marketing Manager', 'salary': 92000, 'hire_date': '2021-09-05', 'email': 'diya.patel@company.com'},
                {'name': 'Ananya Singh', 'department': 'Engineering', 'position': 'Frontend Developer', 'salary': 89000, 'hire_date': '2022-05-19', 'email': 'ananya.singh@company.com'},
                {'name': 'Kabir Gupta', 'department': 'Engineering', 'position': 'DevOps Engineer', 'salary': 102000, 'hire_date': '2020-12-03', 'email': 'kabir.gupta@company.com'},
                {'name': 'Meera Rao', 'department': 'Finance', 'position': 'Financial Analyst', 'salary': 84000, 'hire_date': '2021-08-21', 'email': 'meera.rao@company.com'},
                {'name': 'Arjun Desai', 'department': 'Engineering', 'position': 'Backend Developer', 'salary': 97000, 'hire_date': '2023-01-10', 'email': 'arjun.desai@company.com'},
                {'name': 'Priya Chawla', 'department': 'Sales', 'position': 'Account Executive', 'salary': 78000, 'hire_date': '2022-03-11', 'email': 'priya.chawla@company.com'},
                {'name': 'Neeraj Kulkarni', 'department': 'Engineering', 'position': 'SRE', 'salary': 104000, 'hire_date': '2021-10-14', 'email': 'neeraj.kulkarni@company.com'},
                {'name': 'Tanvi Malhotra', 'department': 'HR', 'position': 'Recruiter', 'salary': 70000, 'hire_date': '2020-06-25', 'email': 'tanvi.malhotra@company.com'},
                {'name': 'Reyansh Mehta', 'department': 'Engineering', 'position': 'ML Engineer', 'salary': 115000, 'hire_date': '2022-09-02', 'email': 'reyansh.mehta@company.com'},
                {'name': 'Nisha Bose', 'department': 'Marketing', 'position': 'Content Strategist', 'salary': 82000, 'hire_date': '2021-04-30', 'email': 'nisha.bose@company.com'},
                {'name': 'Yash Tiwari', 'department': 'Engineering', 'position': 'Data Engineer', 'salary': 101000, 'hire_date': '2023-03-22', 'email': 'yash.tiwari@company.com'},
                {'name': 'Aditi Joshi', 'department': 'Engineering', 'position': 'QA Engineer', 'salary': 78000, 'hire_date': '2020-01-18', 'email': 'aditi.joshi@company.com'},
                {'name': 'Harsh Venkatesh', 'department': 'Support', 'position': 'Support Engineer', 'salary': 68000, 'hire_date': '2021-12-09', 'email': 'harsh.venkatesh@company.com'},
                {'name': 'Kritika Menon', 'department': 'Design', 'position': 'Product Designer', 'salary': 90000, 'hire_date': '2022-11-27', 'email': 'kritika.menon@company.com'},
                {'name': 'Atharv Reddy', 'department': 'Engineering', 'position': 'Security Engineer', 'salary': 109000, 'hire_date': '2021-07-07', 'email': 'atharv.reddy@company.com'},
                {'name': 'Ira Jain', 'department': 'Operations', 'position': 'Operations Manager', 'salary': 95000, 'hire_date': '2020-09-19', 'email': 'ira.jain@company.com'},
                {'name': 'Dev Mishra', 'department': 'Engineering', 'position': 'Full Stack Developer', 'salary': 99000, 'hire_date': '2023-04-04', 'email': 'dev.mishra@company.com'},
                {'name': 'Sara Dutta', 'department': 'Engineering', 'position': 'Android Developer', 'salary': 93000, 'hire_date': '2022-08-18', 'email': 'sara.dutta@company.com'},
                {'name': 'Parth Aggarwal', 'department': 'Engineering', 'position': 'iOS Developer', 'salary': 94000, 'hire_date': '2021-02-13', 'email': 'parth.aggarwal@company.com'},
                {'name': 'Jia Kapoor', 'department': 'Engineering', 'position': 'Data Analyst', 'salary': 87000, 'hire_date': '2020-05-29', 'email': 'jia.kapoor@company.com'}
            ]
            
            for emp in employees:
                conn.execute(text("""
                    INSERT INTO employees (name, department, position, salary, hire_date, email)
                    VALUES (:name, :department, :position, :salary, :hire_date, :email)
                """), emp)
            
            departments = [
                {'name': 'Engineering', 'manager_id': 2},
                {'name': 'Sales', 'manager_id': 3},
                {'name': 'HR', 'manager_id': 4},
                {'name': 'Marketing', 'manager_id': 6}
            ]
            
            for dept in departments:
                conn.execute(text("""
                    INSERT INTO departments (name, manager_id) VALUES (:name, :manager_id)
                """), dept)
            
            conn.commit()
        
        logger.info("Demo data initialized")

# Pydantic models
class DatabaseConnection(BaseModel):
    connection_string: str = "sqlite:///./company.db"

class QueryRequest(BaseModel):
    query: str
    use_cache: bool = True
    page: int = 1
    page_size: int = 25

class QueryResponse(BaseModel):
    results: Dict[str, Any]
    query_type: str
    response_time: float
    cache_hit: bool
    sources: List[str]
    generated_sql: Optional[str] = None

class DocumentUploadResponse(BaseModel):
    job_id: str
    status: str
    total_files: int
    processed_files: int

class IngestionStatus(BaseModel):
    job_id: str
    status: str
    progress: float
    processed_files: int
    total_files: int
    error_message: Optional[str] = None

# Schema Discovery
class SchemaDiscovery:
    def __init__(self):
        self.engine = engine
        self.inspector = inspect(engine)
        self.synonym_mappings = {
            'employee': ['employee', 'employees', 'emp', 'staff', 'personnel'],
            'salary': ['salary', 'compensation', 'pay', 'income'],
            'department': ['department', 'dept', 'division', 'team'],
            'name': ['name', 'full_name', 'employee_name'],
            'id': ['id', 'emp_id', 'employee_id'],
            'hire_date': ['hire_date', 'join_date', 'start_date'],
            'position': ['position', 'role', 'title', 'job_title']
        }

    def analyze_database(self) -> Dict[str, Any]:
        """Discover database schema"""
        try:
            tables = self.inspector.get_table_names()
            schema = {
                'tables': [],
                'relationships': [],
                'total_tables': len(tables),
                'total_columns': 0
            }
            
            for table_name in tables:
                table_info = self._analyze_table(table_name)
                schema['tables'].append(table_info)
                schema['total_columns'] += len(table_info['columns'])
            
            logger.info(f"Discovered {len(tables)} tables with {schema['total_columns']} columns")
            return schema
            
        except Exception as e:
            logger.error(f"Schema discovery failed: {str(e)}")
            raise

    def _analyze_table(self, table_name: str) -> Dict[str, Any]:
        """Analyze a single table"""
        columns = []
        for col in self.inspector.get_columns(table_name):
            column_data = {
                'name': col['name'],
                'type': str(col['type']),
                'nullable': col['nullable'],
                'primary_key': col.get('primary_key', False),
                'estimated_purpose': self._estimate_column_purpose(col['name'])
            }
            columns.append(column_data)
        
        # Get sample data
        sample_data = self._get_sample_data(table_name, columns)
        
        return {
            'name': table_name,
            'columns': columns,
            'sample_data': sample_data,
            'estimated_purpose': self._estimate_table_purpose(table_name)
        }

    def _estimate_table_purpose(self, table_name: str) -> str:
        """Estimate table purpose"""
        table_lower = table_name.lower()
        if any(term in table_lower for term in ['emp', 'staff']):
            return 'employee_data'
        elif any(term in table_lower for term in ['dept', 'division']):
            return 'department_data'
        else:
            return 'general_data'

    def _estimate_column_purpose(self, column_name: str) -> str:
        """Estimate column purpose"""
        col_lower = column_name.lower()
        if any(term in col_lower for term in ['name', 'full_name']):
            return 'employee_name'
        elif any(term in col_lower for term in ['salary', 'compensation']):
            return 'compensation'
        elif any(term in col_lower for term in ['date']):
            return 'date_time'
        elif any(term in col_lower for term in ['dept', 'division']):
            return 'department'
        elif any(term in col_lower for term in ['position', 'role', 'title']):
            return 'job_title'
        elif any(term in col_lower for term in ['email']):
            return 'contact_info'
        else:
            return 'general'

    def _get_sample_data(self, table_name: str, columns: List[Dict]) -> List[Dict]:
        """Get sample data from table"""
        try:
            with self.engine.connect() as conn:
                col_names = [col['name'] for col in columns]
                query = text(f"SELECT {', '.join(col_names)} FROM {table_name} LIMIT 3")
                result = conn.execute(query)
                rows = result.fetchall()
                
                sample_data = []
                for row in rows:
                    row_dict = {}
                    for i, col in enumerate(columns):
                        row_dict[col['name']] = str(row[i]) if row[i] is not None else None
                    sample_data.append(row_dict)
                
                return sample_data
        except Exception as e:
            logger.warning(f"Could not fetch sample data for {table_name}: {str(e)}")
            return []

# Query Engine
class QueryCache:
    def __init__(self, ttl_seconds: int = 300, max_size: int = 100):
        self.cache = {}
        self.ttl = timedelta(seconds=ttl_seconds)
        self.max_size = max_size

    def get(self, key: str) -> Optional[Dict]:
        if key in self.cache:
            entry = self.cache[key]
            if datetime.now() - entry['timestamp'] < self.ttl:
                return entry['data']
            else:
                del self.cache[key]
        return None

    def set(self, key: str, data: Dict):
        if len(self.cache) >= self.max_size:
            oldest_key = min(self.cache.keys(), key=lambda k: self.cache[k]['timestamp'])
            del self.cache[oldest_key]
        
        self.cache[key] = {
            'data': data,
            'timestamp': datetime.now()
        }

class QueryEngine:
    def __init__(self, schema: Dict):
        self.engine = engine
        self.schema = schema
        self.cache = QueryCache()
        self.query_history = []

    def process_query(self, user_query: str, use_cache: bool = True, page: int = 1, page_size: int = 25) -> Dict[str, Any]:
        """Process natural language query"""
        start_time = time.time()
        
        # Generate cache key
        cache_key = hashlib.md5(user_query.encode()).hexdigest()
        
        # Check cache
        if use_cache:
            cached_result = self.cache.get(cache_key)
            if cached_result:
                logger.info(f"Cache hit for query: {user_query}")
                cached_result['response_time'] = time.time() - start_time
                return cached_result
        
        # Always run both pipelines for a robust hybrid response
        try:
            sql_results = self._process_sql_query(user_query)
            doc_results = self._process_document_query(user_query)

            # Pagination (SQL only for now)
            sql_data_full = sql_results.get('data', [])
            total_sql = len(sql_data_full)
            start = max(0, (page - 1) * page_size)
            end = start + page_size
            sql_data_page = sql_data_full[start:end]

            results = {
                'sql_data': sql_data_page,
                'document_data': doc_results.get('data', []),
                'sql_count': total_sql,
                'document_count': len(doc_results.get('data', [])),
                'total_count': total_sql + len(doc_results.get('data', [])),
                'generated_sql': sql_results.get('generated_sql')
            }

            sources = []
            if total_sql:
                sources.append('database')
            if results['document_count']:
                sources.append('documents')
            query_type = 'hybrid' if len(sources) == 2 else ('sql' if 'database' in sources else 'document')
            
            response_data = {
                'results': results,
                'query_type': query_type,
                'response_time': time.time() - start_time,
                'cache_hit': False,
                'sources': sources,
                'generated_sql': results.get('generated_sql')
            }
            
            # Cache the result
            if use_cache:
                self.cache.set(cache_key, response_data)
            
            # Add to history
            self.query_history.append({
                'query': user_query,
                'timestamp': datetime.now(),
                'response_time': response_data['response_time'],
                'type': query_type
            })
            
            return response_data
            
        except Exception as e:
            logger.error(f"Query processing failed: {str(e)}")
            return {
                'results': {'error': str(e), 'data': []},
                'query_type': query_type,
                'response_time': time.time() - start_time,
                'cache_hit': False,
                'sources': [],
                'error': True
            }

    def _classify_query_type(self, query: str) -> str:
        """Kept for compatibility; currently we return hybrid by default."""
        return 'hybrid'

    def _process_sql_query(self, query: str) -> Dict[str, Any]:
        """Process SQL queries"""
        generated_sql = self._nlp_to_sql(query)
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(generated_sql))
                columns = result.keys()
                data = [dict(zip(columns, row)) for row in result.fetchall()]
                
                return {
                    'data': data,
                    'count': len(data),
                    'generated_sql': generated_sql
                }
                
        except Exception as e:
            logger.error(f"SQL execution failed: {str(e)}")
            return {
                'data': [],
                'error': str(e),
                'generated_sql': generated_sql
            }

    def _nlp_to_sql(self, query: str) -> str:
        """Convert natural language to SQL"""
        query_lower = query.lower()
        
        # Simple rule-based NLP to SQL
        if 'how many' in query_lower and 'employee' in query_lower:
            return "SELECT COUNT(*) as employee_count FROM employees"
        
        elif 'average salary' in query_lower:
            if 'department' in query_lower:
                return """
                SELECT department, ROUND(AVG(salary), 2) as average_salary 
                FROM employees 
                GROUP BY department
                """
            else:
                return "SELECT ROUND(AVG(salary), 2) as average_salary FROM employees"
        
        elif 'engineering' in query_lower:
            return "SELECT * FROM employees WHERE department = 'Engineering'"
        
        elif 'salary' in query_lower and 'department' in query_lower:
            return """
            SELECT department, ROUND(AVG(salary), 2) as avg_salary 
            FROM employees 
            GROUP BY department
            ORDER BY avg_salary DESC
            """
        
        elif 'highest paid' in query_lower:
            return "SELECT * FROM employees ORDER BY salary DESC LIMIT 5"
        
        elif 'recent' in query_lower or 'new' in query_lower:
            return "SELECT * FROM employees ORDER BY hire_date DESC LIMIT 5"
        
        elif 'department' in query_lower:
            return "SELECT * FROM departments"
        
        else:
            return "SELECT * FROM employees LIMIT 10"

    def _process_document_query(self, query: str) -> Dict[str, Any]:
        """Process document queries using keyword filter + TF-IDF ranking if available"""
        results: List[Dict[str, Any]] = []
        try:
            if not os.path.exists(DOCUMENTS_DB_PATH):
                return {'data': results, 'count': 0}

            conn = sqlite3.connect(DOCUMENTS_DB_PATH)
            cursor = conn.cursor()

            # Extract simple search terms
            terms = self._extract_search_terms(query)
            if not terms:
                conn.close()
                return {'data': results, 'count': 0}

            placeholders = ' OR '.join(['content LIKE ?'] * len(terms))
            sql = f"SELECT id, filename, content, file_type, uploaded_at FROM documents WHERE {placeholders}"
            params = [f"%{t}%" for t in terms]
            cursor.execute(sql, params)
            rows = cursor.fetchall()

            # Build corpus for TF-IDF ranking
            corpus: List[str] = []
            meta: List[tuple] = []
            for row in rows:
                doc_id, filename, content, file_type, uploaded_at = row
                corpus.append(content or "")
                meta.append((doc_id, filename, file_type, uploaded_at))

            if corpus:
                vectorizer = TfidfVectorizer(stop_words='english', max_features=5000)
                doc_matrix = vectorizer.fit_transform(corpus)
                q_vec = vectorizer.transform([query])
                sims = cosine_similarity(q_vec, doc_matrix).flatten()

                ranked = sorted(zip(range(len(corpus)), sims), key=lambda x: x[1], reverse=True)
                for idx, score in ranked:
                    doc_id, filename, file_type, uploaded_at = meta[idx]
                    content = corpus[idx]
                    snippet = self._extract_snippet(content, terms[0] if terms else '')
                    results.append({
                        'id': doc_id,
                        'title': filename,
                        'content': snippet,
                        'type': file_type,
                        'relevance': float(round(float(score), 4)),
                        'uploaded_at': uploaded_at
                    })

            conn.close()
            return {'data': results, 'count': len(results)}
        except Exception as e:
            logger.error(f"Document search failed: {str(e)}")
            return {'data': [], 'error': str(e), 'count': 0}

    def _extract_search_terms(self, query: str) -> List[str]:
        q = query.lower()
        skills = ['java', 'python', 'javascript', 'sql', 'react', 'docker', 'kubernetes']
        terms = [s for s in skills if s in q]
        if not terms:
            words = [w for w in ''.join([c if c.isalnum() else ' ' for c in q]).split() if len(w) > 2]
            common = {'the','and','for','with','but','not','you','are','can','has','have','from','this','that'}
            terms = [w for w in words if w not in common][:3]
        return terms[:3]

    def _extract_snippet(self, content: str, term: str, window: int = 80) -> str:
        idx = content.lower().find(term.lower())
        if idx == -1:
            return content[:window] + ('...' if len(content) > window else '')
        start = max(0, idx - window // 2)
        end = min(len(content), idx + window // 2)
        prefix = '...' if start > 0 else ''
        suffix = '...' if end < len(content) else ''
        return f"{prefix}{content[start:end]}{suffix}"

    def _calculate_relevance(self, content: str, terms: List[str]) -> float:
        if not content:
            return 0.0
        c = content.lower()
        score = 0
        for t in terms:
            score += c.count(t.lower())
        return min(1.0, score / max(1, len(c) // 500))

    def get_query_history(self, limit: int = 50) -> List[Dict]:
        """Get query history"""
        return self.query_history[-limit:] if self.query_history else []

# Document Processor (Simplified)
class DocumentProcessor:
    def __init__(self):
        self.processing_jobs = {}
        self.upload_dir = "uploads"
        os.makedirs(self.upload_dir, exist_ok=True)

    async def process_documents(self, files: List[UploadFile], job_id: str):
        """Process uploaded documents"""
        self.processing_jobs[job_id] = {
            'status': 'processing',
            'total_files': len(files),
            'processed_files': 0,
            'start_time': datetime.now()
        }
        
        for i, file in enumerate(files):
            try:
                # Save file
                file_path = os.path.join(self.upload_dir, f"{job_id}_{file.filename}")
                async with aiofiles.open(file_path, 'wb') as f:
                    content = await file.read()
                    await f.write(content)
                
                # Update progress
                self.processing_jobs[job_id]['processed_files'] = i + 1
                await asyncio.sleep(0.5)  # Simulate processing
                
            except Exception as e:
                logger.error(f"Failed to process {file.filename}: {str(e)}")
        
        self.processing_jobs[job_id]['status'] = 'completed'
        self.processing_jobs[job_id]['end_time'] = datetime.now()

    def get_processing_status(self, job_id: str) -> Dict[str, Any]:
        """Get processing status"""
        if job_id not in self.processing_jobs:
            return {'status': 'not_found'}
        
        job = self.processing_jobs[job_id]
        progress = (job['processed_files'] / job['total_files']) * 100
        
        return {
            'job_id': job_id,
            'status': job['status'],
            'progress': progress,
            'processed_files': job['processed_files'],
            'total_files': job['total_files']
        }

# Initialize components
init_demo_data()
schema_discovery = SchemaDiscovery()
document_processor = DocumentProcessor()
current_schema = schema_discovery.analyze_database()
query_engine = QueryEngine(current_schema)

# API Routes
@app.get("/")
async def root():
    return {"message": "NLP Query Engine API", "status": "running"}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

@app.post("/api/ingest/database")
async def connect_database(connection: DatabaseConnection):
    """Connect to database"""
    try:
        schema = schema_discovery.analyze_database()
        return {
            "status": "success", 
            "schema": schema,
            "message": f"Connected to SQLite database with {schema['total_tables']} tables"
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Database connection failed: {str(e)}")

@app.post("/api/ingest/documents")
async def upload_documents(
    background_tasks: BackgroundTasks,
    files: List[UploadFile] = File(None)
) -> DocumentUploadResponse:
    """Upload documents"""
    # If files provided, index into documents.db synchronously so results are immediate
    if files:
        try:
            os.makedirs(os.path.dirname(DOCUMENTS_DB_PATH), exist_ok=True)
            conn = sqlite3.connect(DOCUMENTS_DB_PATH)
            c = conn.cursor()
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS documents (
                    id TEXT PRIMARY KEY,
                    filename TEXT,
                    content TEXT,
                    file_type TEXT,
                    uploaded_at TEXT
                )
                """
            )
            now = datetime.now().isoformat(timespec='seconds')
            processed = 0
            for file in files:
                data = await file.read()
                text_content = await _read_bytes_text(file.filename or '', file.content_type or '', data)
                c.execute(
                    "INSERT INTO documents (id, filename, content, file_type, uploaded_at) VALUES (?,?,?,?,?)",
                    (str(uuid.uuid4()), file.filename, text_content, (file.content_type or '').split('/')[-1], now)
                )
                processed += 1
            conn.commit()
            conn.close()
            return DocumentUploadResponse(
                job_id="inline",
                status="completed",
                total_files=processed,
                processed_files=processed
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to index documents: {str(e)}")
    # else fallback to create samples
    try:
        os.makedirs(os.path.dirname(DOCUMENTS_DB_PATH), exist_ok=True)
        conn = sqlite3.connect(DOCUMENTS_DB_PATH)
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS documents (
                id TEXT PRIMARY KEY,
                filename TEXT,
                content TEXT,
                file_type TEXT,
                uploaded_at TEXT
            )
        """)
        now = datetime.now().isoformat(timespec='seconds')
        samples = [
            (str(uuid.uuid4()), 'resume_john_smith.txt', 'Experienced Python developer skilled in SQL, React, and Docker.', 'txt', now),
            (str(uuid.uuid4()), 'resume_jane_doe.txt', 'Java engineer with Kubernetes and cloud experience. Mentions SQL.', 'txt', now),
            (str(uuid.uuid4()), 'performance_review_q4.txt', 'John demonstrated strong Python and data analysis skills.', 'txt', now)
        ]
        c.executemany("INSERT INTO documents (id, filename, content, file_type, uploaded_at) VALUES (?,?,?,?,?)", samples)
        conn.commit()
        conn.close()

        return DocumentUploadResponse(
            job_id="sample",
            status="completed",
            total_files=len(samples),
            processed_files=len(samples)
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create sample documents: {str(e)}")

async def _read_bytes_text(name: str, ctype: str, data: bytes) -> str:
    name = (name or '').lower()
    ctype = (ctype or '').lower()
    try:
        if name.endswith('.txt') or 'text/plain' in ctype:
            return data.decode(errors='ignore')
        if name.endswith('.csv') or 'csv' in ctype:
            return data.decode(errors='ignore')
        if name.endswith('.pdf') or 'pdf' in ctype:
            from pypdf import PdfReader
            import io
            reader = PdfReader(io.BytesIO(data))
            return "\n".join((p.extract_text() or '') for p in reader.pages)
        if name.endswith('.docx') or 'word' in ctype:
            from docx import Document
            import io
            doc = Document(io.BytesIO(data))
            return "\n".join(p.text for p in doc.paragraphs)
        return data.decode(errors='ignore')
    except Exception as e:
        logger.warning(f"Failed to parse {name}: {e}")
        return data.decode(errors='ignore')

@app.get("/api/ingest/status/{job_id}")
async def get_ingestion_status(job_id: str) -> IngestionStatus:
    """Get ingestion status"""
    status = document_processor.get_processing_status(job_id)
    
    if status['status'] == 'not_found':
        raise HTTPException(status_code=404, detail="Job not found")
    
    return IngestionStatus(
        job_id=job_id,
        status=status['status'],
        progress=status['progress'],
        processed_files=status['processed_files'],
        total_files=status['total_files']
    )

@app.post("/api/query")
async def process_query(request: QueryRequest) -> QueryResponse:
    """Process natural language query"""
    try:
        result = query_engine.process_query(request.query, request.use_cache, request.page, request.page_size)
        return QueryResponse(**result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query processing failed: {str(e)}")

@app.get("/api/query/history")
async def get_query_history():
    """Get query history"""
    return {"history": query_engine.get_query_history()}

@app.get("/api/metrics")
async def get_metrics():
    return {
        "cache_size": len(query_engine.cache.cache),
        "recent_queries": len(query_engine.query_history[-20:]),
        "tables": current_schema.get('total_tables', 0),
        "columns": current_schema.get('total_columns', 0)
    }

@app.get("/api/export/csv")
async def export_csv(q: str):
    res = query_engine.process_query(q, use_cache=True, page=1, page_size=10000)
    rows = res.get('results', {}).get('sql_data', [])
    if not rows:
        return JSONResponse(content={"message": "No data"})
    cols = list(rows[0].keys())
    import csv
    import io
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=cols)
    writer.writeheader()
    for r in rows:
        writer.writerow(r)
    return JSONResponse(content={"filename": "export.csv", "content": buf.getvalue()})

@app.get("/api/export/json")
async def export_json(q: str):
    res = query_engine.process_query(q, use_cache=True, page=1, page_size=10000)
    rows = res.get('results', {}).get('sql_data', [])
    return JSONResponse(content={"filename": "export.json", "content": rows})

@app.get("/api/schema")
async def get_schema():
    """Get current schema"""
    doc_count = 0
    try:
        if os.path.exists(DOCUMENTS_DB_PATH):
            conn = sqlite3.connect(DOCUMENTS_DB_PATH)
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM documents")
            doc_count = c.fetchone()[0] or 0
            conn.close()
    except Exception:
        pass
    enriched = dict(current_schema)
    enriched['total_documents'] = doc_count
    return enriched

# Compatibility alias endpoints expected by the PDF spec
@app.post("/api/connect-database")
async def connect_database_alias(connection: DatabaseConnection):
    try:
        reset_database_connection(connection.connection_string)
        return {
            "status": "success",
            "schema": current_schema,
            "message": f"Connected to database: {connection.connection_string}"
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Database connection failed: {str(e)}")

@app.post("/api/upload-documents")
async def upload_documents_alias(background_tasks: BackgroundTasks, files: List[UploadFile] = File(None)):
    return await upload_documents(background_tasks, files)

@app.get("/api/ingestion-status/{job_id}")
async def ingestion_status_alias(job_id: str) -> IngestionStatus:
    return await get_ingestion_status(job_id)

if __name__ == "__main__":
    uvicorn.run(
        "backend.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )
