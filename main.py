from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import uvicorn
from sqlalchemy import create_engine, text, inspect, MetaData
import logging
import time
import hashlib
import json
from datetime import datetime, timedelta
import asyncio
import aiofiles
import os
import uuid

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

# SQLite database
DATABASE_URL = "sqlite:///./company.db"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})

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
        
        # Insert sample data if empty
        result = conn.execute(text("SELECT COUNT(*) FROM employees"))
        if result.scalar() == 0:
            employees = [
                {'name': 'John Smith', 'department': 'Engineering', 'position': 'Python Developer', 'salary': 95000, 'hire_date': '2022-01-15', 'email': 'john.smith@company.com'},
                {'name': 'Jane Doe', 'department': 'Engineering', 'position': 'Senior Developer', 'salary': 110000, 'hire_date': '2021-03-20', 'email': 'jane.doe@company.com'},
                {'name': 'Bob Johnson', 'department': 'Sales', 'position': 'Sales Manager', 'salary': 85000, 'hire_date': '2020-11-10', 'email': 'bob.johnson@company.com'},
                {'name': 'Alice Brown', 'department': 'HR', 'position': 'HR Specialist', 'salary': 75000, 'hire_date': '2023-02-28', 'email': 'alice.brown@company.com'},
                {'name': 'Charlie Wilson', 'department': 'Engineering', 'position': 'Data Scientist', 'salary': 105000, 'hire_date': '2022-07-12', 'email': 'charlie.wilson@company.com'},
                {'name': 'Diana Lee', 'department': 'Marketing', 'position': 'Marketing Manager', 'salary': 90000, 'hire_date': '2021-09-05', 'email': 'diana.lee@company.com'}
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

    def process_query(self, user_query: str, use_cache: bool = True) -> Dict[str, Any]:
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
        
        # Classify and process query
        query_type = self._classify_query_type(user_query)
        
        try:
            if query_type == 'sql':
                results = self._process_sql_query(user_query)
            else:
                results = self._process_document_query(user_query)
            
            response_data = {
                'results': results,
                'query_type': query_type,
                'response_time': time.time() - start_time,
                'cache_hit': False,
                'sources': ['database'],
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
        """Classify query type"""
        query_lower = query.lower()
        doc_indicators = ['resume', 'cv', 'document', 'file', 'review']
        
        if any(indicator in query_lower for indicator in doc_indicators):
            return 'document'
        else:
            return 'sql'

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
        """Process document queries (demo)"""
        return {
            'data': [
                {
                    'title': 'Sample_Resume_John_Smith.pdf',
                    'content': f'Relevant content matching: {query}',
                    'relevance': 0.95,
                    'type': 'resume'
                },
                {
                    'title': 'Performance_Review_Q4.docx', 
                    'content': f'Review data related to: {query}',
                    'relevance': 0.82,
                    'type': 'review'
                }
            ],
            'count': 2
        }

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
    files: List[UploadFile] = File(...)
) -> DocumentUploadResponse:
    """Upload documents"""
    if not files:
        raise HTTPException(status_code=400, detail="No files provided")
    
    job_id = str(uuid.uuid4())
    
    # Start background processing
    background_tasks.add_task(document_processor.process_documents, files, job_id)
    
    return DocumentUploadResponse(
        job_id=job_id,
        status="processing",
        total_files=len(files),
        processed_files=0
    )

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
        result = query_engine.process_query(request.query, request.use_cache)
        return QueryResponse(**result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query processing failed: {str(e)}")

@app.get("/api/query/history")
async def get_query_history():
    """Get query history"""
    return {"history": query_engine.get_query_history()}

@app.get("/api/schema")
async def get_schema():
    """Get current schema"""
    return current_schema

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )
