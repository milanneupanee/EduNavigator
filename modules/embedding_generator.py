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
        """Generate and store embeddings for university data."""
        # Generate university embeddings
        self._generate_university_embeddings()
        
        # Removed _generate_course_embeddings for now, as the provided DB schema
        # indicates program information might be within the university table.
        # If you have a separate 'courses' table, you can re-introduce and adapt it.
        # self._generate_course_embeddings() 
        
    def _generate_university_embeddings(self):
        """Generate and store embeddings for university descriptions."""
        conn = sqlite3.connect(self.db_path)

        try:
            cursor = conn.cursor()
            # Fetch all relevant fields from the main university table
            # Assuming your main table is named 'universities' based on your provided field list.
            # You might need to confirm the actual table name in your database.
            cursor.execute("""
                SELECT 
                    id, 
                    country, 
                    university_name, 
                    city, 
                    university_url, 
                    undergraduate_programs, 
                    graduate_programs, 
                    tuition_undergrad, 
                    tuition_grad, 
                    living_cost, 
                    application_deadlines, 
                    admission_requirements, 
                    scholarships_international, 
                    scholarships_nepali, 
                    campus_facilities
                FROM universities 
                WHERE id NOT IN (SELECT university_id FROM university_embeddings)
            """)
            universities = cursor.fetchall()
            
            logger.info(f"Generating embeddings for {len(universities)} universities")
            
            for univ_data in universities:
                (univ_id, country, university_name, city, university_url, 
                 undergraduate_programs, graduate_programs, tuition_undergrad, 
                 tuition_grad, living_cost, application_deadlines, 
                 admission_requirements, scholarships_international, 
                 scholarships_nepali, campus_facilities) = univ_data
                
                # Construct a comprehensive text to embed
                text_to_embed = f"University: {university_name}\n" \
                                f"Country: {country}\n" \
                                f"City: {city}\n" \
                                f"Website: {university_url}\n"

                if undergraduate_programs:
                    text_to_embed += f"Undergraduate Programs: {undergraduate_programs}\n"
                if graduate_programs:
                    text_to_embed += f"Graduate Programs: {graduate_programs}\n"
                if tuition_undergrad is not None: # Check for None explicitly for numerical fields
                    text_to_embed += f"Undergraduate Tuition: {tuition_undergrad}\n"
                if tuition_grad is not None:
                    text_to_embed += f"Graduate Tuition: {tuition_grad}\n"
                if living_cost is not None:
                    text_to_embed += f"Estimated Living Cost: {living_cost}\n"
                if application_deadlines:
                    text_to_embed += f"Application Deadlines: {application_deadlines}\n"
                if admission_requirements:
                    text_to_embed += f"Admission Requirements: {admission_requirements}\n"
                if scholarships_international:
                    text_to_embed += f"International Scholarships: {scholarships_international}\n"
                if scholarships_nepali:
                    text_to_embed += f"Scholarships for Nepali Students: {scholarships_nepali}\n"
                if campus_facilities:
                    text_to_embed += f"Campus Facilities: {campus_facilities}\n"
                
                # Generate embedding
                embedding = generate_embedding(text_to_embed, 'RETRIEVAL_DOCUMENT')
                
                if embedding:
                    # Store embedding in university_embeddings table
                    cursor.execute("""
                        INSERT INTO university_embeddings (university_id, embedding)
                        VALUES (?, ?)
                    """, (univ_id, embedding))
                    
                    logger.info(f"Stored embedding for university: {university_name}")
                else:
                    logger.warning(f"Failed to generate embedding for university: {university_name}")
            
            conn.commit()
            
        except sqlite3.OperationalError as e:
            logger.error(f"SQLite Operational Error (check table/column names): {str(e)}")
            conn.rollback()
        except Exception as e:
            logger.error(f"Error generating university embeddings: {str(e)}")
            conn.rollback()
        finally:
            conn.close()
    
    # Removed _generate_course_embeddings as per assumption about your DB schema.
    # If you have a dedicated 'courses' table, you'll need to define its schema
    # and adapt this method accordingly.
    # def _generate_course_embeddings(self):
    #     """Generate and store embeddings for course descriptions."""
    #     conn = sqlite3.connect(self.db_path)
    #     try:
    #         # ... (your existing course embedding logic, but verify table and column names)
    #     except Exception as e:
    #         conn.rollback()
    #         logger.error(f"Error generating course embeddings: {str(e)}")
    #     finally:
    #         conn.close()

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