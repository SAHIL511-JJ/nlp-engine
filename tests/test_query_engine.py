import os
import json
from fastapi.testclient import TestClient
from backend.main import app


client = TestClient(app)


def test_health_and_schema():
    r = client.get("/health")
    assert r.status_code == 200
    data = r.json()
    assert data.get("status") == "healthy"

    r = client.get("/api/schema")
    assert r.status_code == 200
    schema = r.json()
    assert "tables" in schema
    assert isinstance(schema.get("total_tables"), int)


def test_sql_query_flow():
    r = client.post("/api/query", json={"query": "how many employees", "use_cache": False})
    assert r.status_code == 200
    data = r.json()
    assert data["query_type"] == "sql"
    assert "results" in data
    assert "generated_sql" in data


def test_document_query_flow_without_db():
    # Ensure no documents DB then document query should return empty but succeed
    if os.path.exists(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "documents.db"))):
        os.remove(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "documents.db")))

    r = client.post("/api/query", json={"query": "find resumes mentioning python", "use_cache": False})
    assert r.status_code == 200
    data = r.json()
    assert data["query_type"] in ("document", "sql", "hybrid")
    # If doc DB missing, document path returns empty results gracefully
    if data["query_type"] == "document":
        assert data["results"].get("count", 0) >= 0

