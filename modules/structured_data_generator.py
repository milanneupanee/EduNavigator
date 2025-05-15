import sqlite3
from typing import Dict, Any
import logging
from pathlib import Path

from utils.gemini_utils import extract_structured_data
from utils.db_utils import init_db, insert_university, insert_course

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class StructuredDataGenerator:
    def __init__(self, raw_data_dir: str, db_path: str):
        """
        Initialize the structured data generator.
        
        Args:
            raw_data_dir: Directory containing raw scraped files
            db_path: Path to SQLite database
        """
        self.raw_data_dir = Path(raw_data_dir)
        self.db_path = db_path
        
        # Initialize database
        init_db(self.db_path)
        
    def process_files(self):
        """Process all files in the raw data directory."""
        if not self.raw_data_dir.exists():
            logger.error(f"Raw data directory {self.raw_data_dir} does not exist")
            return
            
        file_count = 0
        success_count = 0
        
        for file_path in self.raw_data_dir.glob('*.*'):
            if file_path.suffix.lower() in ['.json', '.txt', '.html']:
                file_count += 1
                logger.info(f"Processing file: {file_path}")
                
                try:
                    # Read file content
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    
                    # Extract structured data using Gemini
                    structured_data = extract_structured_data(content)
                    
                    if structured_data:
                        # Store data in database
                        self._store_data(structured_data)
                        success_count += 1
                    else:
                        logger.warning(f"No structured data extracted from {file_path}")
                        
                except Exception as e:
                    logger.error(f"Error processing file {file_path}: {str(e)}")
        
        logger.info(f"Processed {file_count} files, successfully extracted data from {success_count} files")
    
    def _store_data(self, data: Dict[str, Any]):
        """
        Store extracted data in the database.
        
        Args:
            data: Structured data extracted from a file
        """
        conn = sqlite3.connect(self.db_path)
        try:
            # Extract university data
            university_data = {
                'name': data.get('university_name', ''),
                'country': data.get('country', ''),
                'description': data.get('university_description', '')
            }
            
            # Insert university and get its ID
            university_id = insert_university(conn, university_data)
            
            # Extract course data
            course_data = {
                'university_id': university_id,
                'name': data.get('course_name', ''),
                'description': data.get('description', ''),
                'degree_type': data.get('degree_type', ''),
                'starting_date': data.get('starting_date', ''),
                'duration': data.get('duration', ''),
                'scholarship': data.get('scholarship', ''),
                'fee_structure': data.get('fee_structure', ''),
                'language_of_study': data.get('language_of_study', ''),
                'field_of_study': data.get('field_of_study', '')
            }
            
            # Insert course
            insert_course(conn, course_data)
            
            conn.commit()
            logger.info(f"Stored data for university: {university_data['name']}, course: {course_data['name']}")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error storing data: {str(e)}")
        finally:
            conn.close()

def main():
    """Run the structured data generator as a standalone module."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate structured data from raw scraped files')
    parser.add_argument('--raw-dir', type=str, default='data/raw', help='Directory containing raw scraped files')
    parser.add_argument('--db-path', type=str, default='data/database.db', help='Path to SQLite database')
    
    args = parser.parse_args()
    
    generator = StructuredDataGenerator(args.raw_dir, args.db_path)
    generator.process_files()

if __name__ == '__main__':
    main() 
