import logging
import sqlite3
from typing import Dict, Any, List, Optional, Tuple

from utils.gemini_utils import generate_embedding, classify_query_intent, generate_chat_response
from utils.db_utils import search_vector_similarity

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ChatModule:
    def __init__(self, db_path: str):
        """
        Initialize the chat module.
        
        Args:
            db_path: Path to SQLite database
        """
        self.db_path = db_path
    
    def process_query(self, query: str) -> str:
        """
        Process a user query and generate a response.
        
        Args:
            query: User query string
            
        Returns:
            Response string
        """
        logger.info(f"Processing query: {query}")
        
        # Determine if query requires data lookup
        requires_lookup = self._requires_data_lookup(query)
        
        if requires_lookup:
            logger.info("Query requires data lookup")
            
            # Generate query embedding
            query_embedding = generate_embedding(query, 'RETRIEVAL_QUERY')
            
            if not query_embedding:
                logger.warning("Failed to generate query embedding, falling back to direct response")
                return generate_chat_response(query)
            
            # Search for relevant data
            context = self._search_relevant_data(query, query_embedding)
            
            # Generate response with context
            response = generate_chat_response(query, context)
        else:
            logger.info("Query does not require data lookup")
            
            # Generate response without context
            response = generate_chat_response(query)
        
        return response
    
    def _requires_data_lookup(self, query: str) -> bool:
        """
        Determine if a query requires university/course data lookup.
        
        Args:
            query: User query string
            
        Returns:
            True if data lookup is required, False otherwise
        """
        # Use LangChain to classify query intent
        return classify_query_intent(query)
    
    def _search_relevant_data(self, query: str, query_embedding: bytes) -> Dict[str, Any]:
        """
        Search for relevant university and course data based on query embedding.
        
        Args:
            query: User query string
            query_embedding: Query embedding vector (already serialized)
            
        Returns:
            Dictionary containing relevant universities and courses
        """
        # Determine if query is more about universities or courses
        university_keywords = ["university", "college", "institution", "campus", "school"]
        course_keywords = ["course", "program", "degree", "study", "major", "bachelor", "master"]
        
        university_score = sum(1 for keyword in university_keywords if keyword.lower() in query.lower())
        course_score = sum(1 for keyword in course_keywords if keyword.lower() in query.lower())
        
        # Search in the appropriate table or both
        universities = []
        courses = []
        
        conn = sqlite3.connect(self.db_path)
        try:
            # Always search courses as they're more specific
            courses = search_vector_similarity(conn, 'course', query_embedding, limit=3, query_text=query)
            
            # If query seems to be about universities or is ambiguous, search universities too
            if university_score >= course_score:
                universities = search_vector_similarity(conn, 'university', query_embedding, limit=2, query_text=query)
                
        except Exception as e:
            logger.error(f"Error searching for relevant data: {str(e)}")
        finally:
            conn.close()
        
        # Format context
        context = {
            "universities": universities,
            "courses": courses
        }
        
        return context

def main():
    """Run the chat module as a standalone interactive application."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Interactive chat module for university and course queries')
    parser.add_argument('--db-path', type=str, default='data/database.db', help='Path to SQLite database')
    
    args = parser.parse_args()
    
    chat_module = ChatModule(args.db_path)
    
    print("University and Course Chat Assistant")
    print("Type 'exit' or 'quit' to end the conversation")
    
    while True:
        query = input("\nYour query: ")
        
        if query.lower() in ['exit', 'quit']:
            break
            
        response = chat_module.process_query(query)
        print(f"\nAssistant: {response}")

if __name__ == '__main__':
    main() 
