from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from datetime import datetime

class DatabaseConnection(BaseModel):
    connection_string: str
    database_type: str = "postgresql"

class TableSchema(BaseModel):
    name: str
    columns: List[Dict[str, Any]]
    sample_data: List[Dict[str, Any]]
    estimated_purpose: str

class DatabaseSchema(BaseModel):
    tables: List[TableSchema]
    relationships: List[Dict[str, Any]]
    total_tables: int
    total_columns: int

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
