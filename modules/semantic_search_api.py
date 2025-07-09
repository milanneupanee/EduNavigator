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
    # Primary key (unique ID for each university)
    id: int
    # Country where the university is located
    country: str
    # Full name of the university
    university_name: str
    # City where the university is based
    city: str
    # Website URL
    university_url: str
    # Offered UG programs (as text or list)
    undergraduate_programs: Optional[str] = None
    # Offered PG programs (as text or list)
    graduate_programs: Optional[str] = None
    # Tuition fee for undergraduate programs (assuming float for numerical value)
    tuition_undergrad: Optional[float] = None
    # Tuition fee for graduate programs (assuming float for numerical value)
    tuition_grad: Optional[float] = None
    # Estimated cost of living (assuming float for numerical value)
    living_cost: Optional[float] = None
    # Admission deadlines
    application_deadlines: Optional[str] = None
    # Requirements to apply (e.g., GPA, IELTS)
    admission_requirements: Optional[str] = None
    # Scholarships available to all international students
    scholarships_international: Optional[str] = None
    # Scholarships specifically for Nepali students
    scholarships_nepali: Optional[str] = None
    # Available campus services and facilities
    campus_facilities: Optional[str] = None
    # Similarity score from the search
    similarity_score: float

class Course(BaseModel):
    # ID of the course
    id: int
    # Name of the course
    name: str
    # Name of the university offering the course
    university_name: str
    # Description of the course
    description: str
    # Type of degree (e.g., Bachelor's, Master's)
    degree_type: str
    # Field of study for the course
    field_of_study: str
    # Optional: Starting date for the course
    starting_date: Optional[str] = None
    # Optional: Duration of the course
    duration: Optional[str] = None
    # Optional: Fee structure details
    fee_structure: Optional[str] = None
    # Optional: Language of instruction
    language_of_study: Optional[str] = None
    # Similarity score from the search
    similarity_score: float

class SearchResponse(BaseModel):
    # List of matching universities
    universities: List[University] = []
    # List of matching courses
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
    
    # Initialize lists for results
    universities_results = []
    courses_results = []
    
    conn = get_synced_conn()
    try:
        if search_type in ["all", "universities"]:
            # Search for universities using vector similarity
            # The search_vector_similarity function is expected to return dictionaries
            # with keys matching the University Pydantic model fields.
            universities_results = search_vector_similarity(conn, "university", embedding, limit=limit)
            
        if search_type in ["all", "courses"]:
            # Search for courses using vector similarity
            # The search_vector_similarity function is expected to return dictionaries
            # with keys matching the Course Pydantic model fields.
            # NOTE: This assumes a separate 'courses' table exists in your database
            # with the fields defined in the Course Pydantic model.
            courses_results = search_vector_similarity(conn, "course", embedding, limit=limit)
            
    except Exception as e:
        logger.error(f"Error searching for relevant data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database search error: {str(e)}")
    finally:
        conn.close()
    
    # Format response by instantiating Pydantic models
    response = SearchResponse(
        universities=[University(**u) for u in universities_results],
        courses=[Course(**c) for c in courses_results]
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
