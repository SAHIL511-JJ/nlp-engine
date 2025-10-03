from fastapi import APIRouter, HTTPException

from main import current_schema

router = APIRouter(prefix="/schema", tags=["Schema"])

@router.get("")
async def get_schema():
    """Get current discovered schema"""
    if not current_schema:
        raise HTTPException(status_code=404, detail="No schema discovered. Please connect to a database first.")
    
    return current_schema
