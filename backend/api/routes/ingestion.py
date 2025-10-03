from fastapi import APIRouter, UploadFile, File, HTTPException, BackgroundTasks
from typing import List
import os
import uuid
import tempfile

from models.database_models import DatabaseConnection, DocumentUploadResponse, IngestionStatus
from services.schema_discovery import SchemaDiscovery
from services.document_processor import DocumentProcessor
from main import schema_discovery, document_processor, current_schema, query_engine

router = APIRouter(prefix="/ingest", tags=["Data Ingestion"])

@router.post("/database")
async def connect_database(connection: DatabaseConnection):
    """Connect to database and auto-discover schema"""
    try:
        schema = schema_discovery.analyze_database(connection.connection_string)
        
        # Update global state
        global current_schema, query_engine
        current_schema = schema
        query_engine = QueryEngine(connection.connection_string, schema)
        
        return {
            "status": "success",
            "schema": schema,
            "message": f"Discovered {schema['total_tables']} tables with {schema['total_columns']} columns"
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Database connection failed: {str(e)}")

@router.post("/documents")
async def upload_documents(
    background_tasks: BackgroundTasks,
    files: List[UploadFile] = File(...)
) -> DocumentUploadResponse:
    """Upload and process multiple documents"""
    if not files:
        raise HTTPException(status_code=400, detail="No files provided")
    
    # Create job ID
    job_id = str(uuid.uuid4())
    
    # Save files temporarily
    file_paths = []
    for file in files:
        # Validate file type
        allowed_extensions = ['.pdf', '.docx', '.txt', '.csv']
        file_ext = os.path.splitext(file.filename)[1].lower()
        if file_ext not in allowed_extensions:
            raise HTTPException(status_code=400, detail=f"Unsupported file type: {file_ext}")
        
        # Save file to temporary location
        temp_dir = tempfile.gettempdir()
        file_path = os.path.join(temp_dir, f"{job_id}_{file.filename}")
        
        with open(file_path, "wb") as f:
            content = await file.read()
            f.write(content)
        
        file_paths.append(file_path)
    
    # Start background processing
    background_tasks.add_task(document_processor.process_documents, file_paths, job_id)
    
    return DocumentUploadResponse(
        job_id=job_id,
        status="processing",
        total_files=len(files),
        processed_files=0
    )

@router.get("/status/{job_id}")
async def get_ingestion_status(job_id: str) -> IngestionStatus:
    """Get status of document processing job"""
    status = document_processor.get_processing_status(job_id)
    
    if status['status'] == 'not_found':
        raise HTTPException(status_code=404, detail="Job not found")
    
    return IngestionStatus(
        job_id=job_id,
        status=status['status'],
        progress=status['progress'],
        processed_files=status['processed_files'],
        total_files=status['total_files'],
        error_message="; ".join(status['errors']) if status['errors'] else None
    )
