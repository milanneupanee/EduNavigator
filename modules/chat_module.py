import logging
from typing import Dict, Any, List, Optional, Tuple
from utils.gemini_utils import generate_embedding, classify_query_intent, generate_chat_response
from utils.db_utils import search_vector_similarity, get_synced_conn

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ChatModule:
    def __init__(self):
        pass
    
    def process_query(self, query: str) -> str:
        """
        Process a user query and generate a response.
        
        Args:
            query: User query string
            
        Returns:
            Response string
        """
        logger.info(f"Processing query: {query}")
        intent = classify_query_intent(query)
        requires_lookup = intent.get("requires_lookup", False)
        target = intent.get("target", "both")
        reason = intent.get("reason", "")
        logger.info(f"Query intent: requires_lookup={requires_lookup}, target={target}, reason={reason}")
        
        if requires_lookup:
            logger.info("Query requires data lookup")
            
            # Generate query embedding
            query_embedding = generate_embedding(query, 'RETRIEVAL_QUERY')
            
            if not query_embedding:
                logger.warning("Failed to generate query embedding, falling back to direct response")
                return generate_chat_response(query)
            
            # Search for relevant data
            context = self._search_relevant_data(query, query_embedding, target)
            
            # Generate response with context
            response = generate_chat_response(query, context)
        else:
            logger.info("Query does not require data lookup")
            
            # Generate response without context
            response = generate_chat_response(query)
        
        return response
    
    
    def _search_relevant_data(self, query: str, query_embedding: bytes, target: str) -> Dict[str, Any]:
        """
        Search for relevant university and course data based on query embedding.
        
        Args:
            query: User query string
            query_embedding: Query embedding vector (already serialized)
            target: Target entity type ("course", "university", or "both")
            
        Returns:
            Dictionary containing relevant universities and courses
        """
        universities = []
        courses = []
        conn = get_synced_conn()
        try:
            if target in ("course", "both"):
                courses = search_vector_similarity(conn, 'course', query_embedding, limit=3, query_text=query)
            if target in ("university", "both"):
                universities = search_vector_similarity(conn, 'university', query_embedding, limit=2, query_text=query)
        except Exception as e:
            logger.error(f"Error searching for relevant data: {str(e)}")
        finally:
            conn.close()

        # Log the number of results found
        logger.info(f"Found {len(universities)} universities and {len(courses)} courses for query: {query}")
        
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
    
    args = parser.parse_args()
    
    chat_module = ChatModule()
    
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
