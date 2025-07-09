import logging
from typing import Dict, Any, List, Optional
from pathlib import Path
from utils.gemini_utils import extract_structured_data, generate_embedding
from utils.db_utils import insert_university, insert_course, get_synced_conn

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class StructuredDataGenerator:
    def __init__(self, raw_data_dir: str):
        """
        Initialize the structured data generator.

        Args:
            raw_data_dir: Directory containing raw scraped files
        """
        self.raw_data_dir = Path(raw_data_dir)

    def process_files(self):
        """Process all files in the raw data directory."""
        if not self.raw_data_dir.exists():
            logger.error(f"Raw data directory {self.raw_data_dir} does not exist")
            return

        file_count = 0
        success_count = 0

        for file_path in self.raw_data_dir.glob("*.*"):
            if file_path.suffix.lower() in [".json", ".txt", ".html"]:
                file_count += 1
                logger.info(f"Processing file: {file_path}")

                try:
                    # Read file content
                    with open(file_path, "r", encoding="utf-8") as f:
                        content = f.read()

                    # Extract structured data using Gemini
                    # This function is expected to return a dictionary like:
                    # {
                    #   "universities": [
                    #     {
                    #       "university_name": "...",
                    #       "country": "...",
                    #       "city": "...",
                    #       "university_url": "...",
                    #       "undergraduate_programs": "...",
                    #       "graduate_programs": "...",
                    #       "tuition_undergrad": "...",
                    #       "tuition_grad": "...",
                    #       "living_cost": "...",
                    #       "application_deadlines": "...",
                    #       "admission_requirements": "...",
                    #       "scholarships_international": "...",
                    #       "scholarships_nepali": "...",
                    #       "campus_facilities": "...",
                    #       "courses": [ # Optional, if courses are nested
                    #         {
                    #           "name": "...",
                    #           "description": "...",
                    #           "degree_type": "...",
                    #           "field_of_study": "...",
                    #           "starting_date": "...",
                    #           "duration": "...",
                    #           "fee_structure": "...",
                    #           "language_of_study": "...",
                    #         }
                    #       ]
                    #     }
                    #   ]
                    # }
                    structured_data = extract_structured_data(content)

                    if structured_data:
                        # Store data in database
                        self._store_data(structured_data)
                        success_count += 1
                    else:
                        logger.warning(f"No structured data extracted from {file_path}")

                except Exception as e:
                    logger.error(f"Error processing file {file_path}: {str(e)}")

        logger.info(
            f"Processed {file_count} files, successfully extracted data from {success_count} files"
        )

    def _store_data(self, data: Dict[str, Any]):
        """
        Store extracted data in the database.

        Args:
            data: Structured data extracted from a file
        """
        conn = get_synced_conn()
        try:
            for university_data in data.get("universities", []):
                # Extract all university data fields based on the provided schema
                # Ensure these keys match what 'extract_structured_data' returns
                university_to_insert = {
                    "country": university_data.get("country", ""),
                    "university_name": university_data.get("university_name", ""),
                    "city": university_data.get("city", ""),
                    "university_url": university_data.get("university_url", ""),
                    "undergraduate_programs": university_data.get("undergraduate_programs", ""),
                    "graduate_programs": university_data.get("graduate_programs", ""),
                    "tuition_undergrad": university_data.get("tuition_undergrad"), # Keep as is, might be None
                    "tuition_grad": university_data.get("tuition_grad"), # Keep as is, might be None
                    "living_cost": university_data.get("living_cost"), # Keep as is, might be None
                    "application_deadlines": university_data.get("application_deadlines", ""),
                    "admission_requirements": university_data.get("admission_requirements", ""),
                    "scholarships_international": university_data.get("scholarships_international", ""),
                    "scholarships_nepali": university_data.get("scholarships_nepali", ""),
                    "campus_facilities": university_data.get("campus_facilities", ""),
                }

                # Construct a comprehensive text for university embedding
                text_to_embed_university = (
                    f"University: {university_to_insert['university_name']}\n"
                    f"Country: {university_to_insert['country']}\n"
                    f"City: {university_to_insert['city']}\n"
                    f"Website: {university_to_insert['university_url']}\n"
                )
                if university_to_insert["undergraduate_programs"]:
                    text_to_embed_university += f"Undergraduate Programs: {university_to_insert['undergraduate_programs']}\n"
                if university_to_insert["graduate_programs"]:
                    text_to_embed_university += f"Graduate Programs: {university_to_insert['graduate_programs']}\n"
                if university_to_insert["tuition_undergrad"] is not None:
                    text_to_embed_university += f"Undergraduate Tuition: {university_to_insert['tuition_undergrad']}\n"
                if university_to_insert["tuition_grad"] is not None:
                    text_to_embed_university += f"Graduate Tuition: {university_to_insert['tuition_grad']}\n"
                if university_to_insert["living_cost"] is not None:
                    text_to_embed_university += f"Estimated Living Cost: {university_to_insert['living_cost']}\n"
                if university_to_insert["application_deadlines"]:
                    text_to_embed_university += f"Application Deadlines: {university_to_insert['application_deadlines']}\n"
                if university_to_insert["admission_requirements"]:
                    text_to_embed_university += f"Admission Requirements: {university_to_insert['admission_requirements']}\n"
                if university_to_insert["scholarships_international"]:
                    text_to_embed_university += f"International Scholarships: {university_to_insert['scholarships_international']}\n"
                if university_to_insert["scholarships_nepali"]:
                    text_to_embed_university += f"Scholarships for Nepali Students: {university_to_insert['scholarships_nepali']}\n"
                if university_to_insert["campus_facilities"]:
                    text_to_embed_university += f"Campus Facilities: {university_to_insert['campus_facilities']}\n"

                university_to_insert["embedding"] = generate_embedding(text_to_embed_university, 'RETRIEVAL_DOCUMENT')

                # Insert university and get its ID
                # The insert_university function in db_utils.py must be updated
                # to accept all these new fields.
                university_id = insert_university(conn, university_to_insert)

                # Process nested courses if they exist
                for course_data in university_data.get("courses", []):
                    # Extract course data. Ensure these keys match what 'extract_structured_data' returns for courses.
                    course_to_insert = {
                        "university_id": university_id,
                        "name": course_data.get("name", ""),
                        "description": course_data.get("description", ""),
                        "degree_type": course_data.get("degree_type", ""),
                        "field_of_study": course_data.get("field_of_study", ""),
                        "starting_date": course_data.get("starting_date", ""),
                        "duration": course_data.get("duration", ""),
                        "fee_structure": course_data.get("fee_structure", ""),
                        "language_of_study": course_data.get("language_of_study", ""),
                        # Assuming 'scholarship' is part of course data if present
                        "scholarship": course_data.get("scholarship", ""),
                    }

                    # Generate embedding for course
                    text_to_embed_course = (
                        f"Course: {course_to_insert['name']}\n"
                        f"University: {university_to_insert['university_name']}\n" # Use university name from parent
                        f"Field of Study: {course_to_insert['field_of_study']}\n"
                        f"Degree Type: {course_to_insert['degree_type']}\n"
                        f"Description: {course_to_insert['description']}"
                    )
                    course_to_insert["embedding"] = generate_embedding(text_to_embed_course, 'RETRIEVAL_DOCUMENT')

                    # Insert course
                    # The insert_course function in db_utils.py must be updated
                    # to accept all these new fields.
                    insert_course(conn, course_to_insert)

                    logger.info(
                        f"Stored data for university: {university_to_insert['university_name']}, "
                        f"course: {course_to_insert['name']}"
                    )
            conn.commit() # Commit once after processing all universities and their courses
        except Exception as e:
            conn.rollback()
            logger.error(f"Error storing data: {str(e)}")
        finally:
            conn.close()

def main():
    """Run the structured data generator as a standalone module."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate structured data from raw scraped files"
    )
    parser.add_argument(
        "--raw-dir",
        type=str,
        default="data/raw",
        help="Directory containing raw scraped files",
    )

    args = parser.parse_args()

    generator = StructuredDataGenerator(args.raw_dir)
    generator.process_files()

if __name__ == "__main__":
    main()
