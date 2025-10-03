from fastapi import APIRouter, HTTPException
from typing import List

from models.database_models import QueryRequest, QueryResponse
from main import query_engine, current_schema

router = APIRouter(prefix="/query", tags=["Query"])

@router.post("", response_model=QueryResponse)
async def process_query(request: QueryRequest):
    """Process natural language query"""
    if not query_engine:
        raise HTTPException(status_code=400, detail="No database connected. Please connect to a database first.")
    
    if not request.query.strip():
        raise HTTPException(status_code=400, detail="Query cannot be empty")
    
    try:
        result = query_engine.process_query(request.query, request.use_cache)
        return QueryResponse(**result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query processing failed: {str(e)}")

@router.get("/history")
async def get_query_history(limit: int = 50):
    """Get query history"""
    if not query_engine:
        return {"history": []}
    
    history = query_engine.get_query_history(limit)
    return {"history": history}
