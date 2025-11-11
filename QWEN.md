# NLP Query Engine - Project Context

## Project Overview

The NLP Query Engine is a natural language query system that allows users to query mixed structured employee data and unstructured documents using plain English. The system supports dynamic schema discovery, hybrid (SQL + document) results, caching, pagination, metrics, and export functionality.

### Key Technologies
- **Backend**: FastAPI (Python 3.10+)
- **Database**: SQLite (default), PostgreSQL/MySQL support
- **Frontend**: Pure HTML/CSS/JavaScript (no build step required)
- **Document Processing**: TF-IDF ranking for document search
- **Document Formats**: PDF, DOCX, TXT, CSV

### Architecture
- **Backend API**: FastAPI server with REST endpoints
- **Frontend UI**: Static HTML file that communicates with the API
- **Database Layer**: SQLAlchemy for structured data, SQLite for documents
- **Document Processing**: Built-in parsing for multiple formats with TF-IDF ranking

## Building and Running

### Prerequisites
- Python 3.10+ (tested up to 3.13)
- Browser to open the frontend
- Optional: PostgreSQL/MySQL if using non-SQLite DB

### Setup Commands

For Windows:
```powershell
cd C:\nlp\nlp-engine
py -m venv .venv
.\.venv\Scripts\activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m uvicorn backend.main:app --reload --host 0.0.0.0 --port 8000
```

For Linux/WSL:
```bash
cd /path/to/nlp-engine
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m uvicorn backend.main:app --reload --host 0.0.0.0 --port 8000
```

### Running the Application
1. Start the backend: `python -m uvicorn backend.main:app --reload --host 0.0.0.0 --port 8000`
2. Open `frontend/index.html` in your browser
3. Connect to a database URL (e.g., `sqlite:///./company.db`)
4. Upload documents or create sample documents
5. Enter natural language queries

### Health Checks
- Health: `curl http://localhost:8000/health`
- Schema: `curl http://localhost:8000/api/schema`
- Swagger UI: `http://localhost:8000/docs`

## API Endpoints

### Database Operations
- `POST /api/connect-database` - Connect to database with connection string
- `GET /api/schema` - Get current schema information

### Document Operations
- `POST /api/ingest/documents` - Upload documents (multipart) or create samples
- `GET /api/ingestion-status/{job_id}` - Check ingestion job status

### Query Operations
- `POST /api/query` - Process natural language query
- `GET /api/query/history` - Get query history
- `GET /api/metrics` - Get system metrics

### Export Operations
- `GET /api/export/csv?q=...` - Export SQL results as CSV
- `GET /api/export/json?q=...` - Export SQL results as JSON

## Core Components

### Backend Structure
- `backend/main.py`: Main FastAPI application with all business logic
- `backend/config.yml`: Configuration file for pool size, cache, logging
- `backend/api/`: API route definitions
- `backend/models/`: Data models
- `backend/services/`: Business logic services

### Frontend Structure
- `frontend/index.html`: Single-page application with all UI components

### Key Classes in Backend
- `SchemaDiscovery`: Analyzes database structure and estimates column purposes
- `QueryEngine`: Processes natural language queries and generates SQL
- `QueryCache`: Implements TTL-based caching for query results
- `DocumentProcessor`: Handles document ingestion and status tracking

## Development Conventions

### Coding Style
- Follows Python PEP 8 standards
- Uses FastAPI/Pydantic patterns for request/response handling
- Type hints are used throughout the codebase
- Logging is implemented using Python's logging module

### Query Processing
- Dynamic NLP to SQL conversion with rule-based matching
- Supports both database queries and document searches
- Implements pagination for large result sets
- Includes basic caching with TTL and size limits

### Document Handling
- Supports PDF, DOCX, TXT, and CSV formats
- Uses TF-IDF for relevance ranking of document results
- Stores processed documents in SQLite database

## Testing

Run tests with:
```bash
pytest -q
```

## Configuration

### Environment Variables
- `DATABASE_URL`: Database connection string (defaults to `sqlite:///./company.db`)

### Configuration File
- `backend/config.yml`: Contains settings for connection pooling, cache, and logging

## Troubleshooting

- Backend won't start: Ensure venv is active and requirements are installed
- DB connection fails: Verify connection string and DB accessibility
- Uploaded docs not appearing: Check that `documents.db` exists and has content
- CORS issues: Backend allows all origins by default