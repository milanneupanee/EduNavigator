import logging
import sqlite3

from utils.db_utils import setup_vector_extension
from utils.gemini_utils import generate_embedding

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EmbeddingGenerator:
    def __init__(self, db_path: str):
        """
        Initialize the embedding generator.
        
        Args:
            db_path: Path to SQLite database
        """
        self.db_path = db_path
        
        # Setup vector extension (but continue even if it fails)
        try:
            setup_vector_extension(self.db_path)
        except Exception as e:
            logger.warning(f"Failed to set up vector extension: {str(e)}")
            logger.warning("Vector search will not be available, but embeddings will still be stored.")
        
    def generate_embeddings(self):
        """Generate and store embeddings for university and course descriptions."""
        # Generate university embeddings
        self._generate_university_embeddings()
        
        # Generate course embeddings
        self._generate_course_embeddings()
        
    def _generate_university_embeddings(self):
        """Generate and store embeddings for university descriptions."""
        conn = sqlite3.connect(self.db_path)

        try:
            # Get universities without embeddings
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, name, description, country 
                FROM universities 
                WHERE id NOT IN (SELECT university_id FROM university_embeddings)
            """)
            universities = cursor.fetchall()
            
            logger.info(f"Generating embeddings for {len(universities)} universities")
            
            for univ_id, name, description, country in universities:
                # Create text to embed (combine relevant fields)
                text_to_embed = f"University: {name}\nCountry: {country}\nDescription: {description}"
                
                # Generate embedding - this now returns a serialized binary blob
                embedding = generate_embedding(text_to_embed, 'RETRIEVAL_DOCUMENT')
                
                if embedding:
                    # Store embedding
                    cursor.execute("""
                        INSERT INTO university_embeddings (university_id, embedding)
                        VALUES (?, ?)
                    """, (univ_id, embedding))
                    
                    logger.info(f"Stored embedding for university: {name}")
                else:
                    logger.warning(f"Failed to generate embedding for university: {name}")
            
            conn.commit()
            
        except Exception as e:
            print(e)
            conn.rollback()
            logger.error(f"Error generating university embeddings: {str(e)}")
        finally:
            conn.close()
    
    def _generate_course_embeddings(self):
        """Generate and store embeddings for course descriptions."""
        conn = sqlite3.connect(self.db_path)
        try:
            # Get courses without embeddings
            cursor = conn.cursor()
            cursor.execute("""
                SELECT c.id, c.name, c.description, c.field_of_study, c.degree_type,
                       u.name as university_name
                FROM courses c
                JOIN universities u ON c.university_id = u.id
                WHERE c.id NOT IN (SELECT course_id FROM course_embeddings)
            """)
            courses = cursor.fetchall()
            
            logger.info(f"Generating embeddings for {len(courses)} courses")
            
            for course_id, name, description, field, degree_type, university_name in courses:
                # Create text to embed (combine relevant fields)
                text_to_embed = (
                    f"Course: {name}\n"
                    f"University: {university_name}\n"
                    f"Field of Study: {field}\n"
                    f"Degree Type: {degree_type}\n"
                    f"Description: {description}"
                )
                
                # Generate embedding
                embedding = generate_embedding(text_to_embed, 'RETRIEVAL_DOCUMENT')
                
                if embedding:
                    # Store embedding
                    cursor.execute("""
                        INSERT INTO course_embeddings (course_id, embedding)
                        VALUES (?, ?)
                    """, (course_id, embedding))
                    
                    logger.info(f"Stored embedding for course: {name}")
                else:
                    logger.warning(f"Failed to generate embedding for course: {name}")
            
            conn.commit()
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error generating course embeddings: {str(e)}")
        finally:
            conn.close()

def main():
    """Run the embedding generator as a standalone module."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate embeddings for university and course data')
    parser.add_argument('--db-path', type=str, default='data/database.db', help='Path to SQLite database')
    
    args = parser.parse_args()
    
    generator = EmbeddingGenerator(args.db_path)
    generator.generate_embeddings()

if __name__ == '__main__':
    main() 
