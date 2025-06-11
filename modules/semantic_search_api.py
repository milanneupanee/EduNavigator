import logging
from typing import List, Optional
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from utils.gemini_utils import generate_embedding
from utils.db_utils import search_vector_similarity, get_synced_conn

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(title="University and Course Semantic Search API")

# Response models
class University(BaseModel):
    id: int
    name: str
    country: str
    description: str
    similarity_score: float

class Course(BaseModel):
    id: int
    name: str
    university_name: str
    description: str
    degree_type: str
    field_of_study: str
    starting_date: Optional[str] = None
    duration: Optional[str] = None
    fee_structure: Optional[str] = None
    language_of_study: Optional[str] = None
    similarity_score: float

class SearchResponse(BaseModel):
    universities: List[University] = []
    courses: List[Course] = []

@app.get("/search", response_model=SearchResponse)
async def search(
    query: str = Query(..., description="Search query"),
    limit: int = Query(5, description="Maximum number of results to return"),
    search_type: str = Query("all", description="Type of search: 'all', 'universities', or 'courses'")
):
    """
    Perform semantic search over universities and courses.
    """
    logger.info(f"Search request: query='{query}', limit={limit}, type={search_type}")
    
    # Generate query embedding
    embedding = generate_embedding(query, 'RETRIEVAL_QUERY')
    
    if not embedding:
        raise HTTPException(status_code=500, detail="Failed to generate embedding for query")
    
    # Search for relevant data
    universities = []
    courses = []
    
    conn = get_synced_conn()
    try:
        if search_type in ["all", "universities"]:
            universities = search_vector_similarity(conn, "university", embedding, limit=limit)
            
        if search_type in ["all", "courses"]:
            courses = search_vector_similarity(conn, "course", embedding, limit=limit)
            
    except Exception as e:
        logger.error(f"Error searching for relevant data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database search error: {str(e)}")
    finally:
        conn.close()
    
    # Format response
    response = SearchResponse(
        universities=[University(**u) for u in universities],
        courses=[Course(**c) for c in courses]
    )
    
    return response

def run_api(host: str = "0.0.0.0", port: int = 8000):
    """
    Run the semantic search API.
    
    Args:
        host: Host to bind the server to
        port: Port to bind the server to
    """
    import uvicorn
    uvicorn.run(app, host=host, port=port)

def main():
    """Run the semantic search API as a standalone application."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Semantic search API for university and course data')
    parser.add_argument('--host', type=str, default='0.0.0.0', help='Host to bind the server to')
    parser.add_argument('--port', type=int, default=8000, help='Port to bind the server to')
    
    args = parser.parse_args()
    
    run_api(args.host, args.port)

if __name__ == '__main__':
    main() 
