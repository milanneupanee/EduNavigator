from typing import Dict, Any
import logging
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
                # Extract university data with new comprehensive schema
                university_data_dict = {
                    "country": university_data.get("country", ""),
                    "university_name": university_data.get("university_name", ""),
                    "city": university_data.get("city", ""),
                    "university_url": university_data.get("university_url", ""),
                    "undergraduate_programs": university_data.get("undergraduate_programs", ""),
                    "graduate_programs": university_data.get("graduate_programs", ""),
                    "tuition_undergrad": university_data.get("tuition_undergrad", ""),
                    "tuition_grad": university_data.get("tuition_grad", ""),
                    "living_cost": university_data.get("living_cost", ""),
                    "application_deadlines": university_data.get("application_deadlines", ""),
                    "admission_requirements": university_data.get("admission_requirements", ""),
                    "scholarships_international": university_data.get("scholarships_international", ""),
                    "scholarships_nepali": university_data.get("scholarships_nepali", ""),
                    "campus_facilities": university_data.get("campus_facilities", ""),
                }

                # Generate embedding for university using comprehensive data
                text_to_embed = (
                    f"University: {university_data_dict['university_name']}\n"
                    f"Country: {university_data_dict['country']}\n"
                    f"City: {university_data_dict['city']}\n"
                    f"Undergraduate Programs: {university_data_dict['undergraduate_programs']}\n"
                    f"Graduate Programs: {university_data_dict['graduate_programs']}\n"
                    f"Tuition (UG): {university_data_dict['tuition_undergrad']}\n"
                    f"Tuition (Grad): {university_data_dict['tuition_grad']}\n"
                    f"Living Cost: {university_data_dict['living_cost']}\n"
                    f"Admission Requirements: {university_data_dict['admission_requirements']}\n"
                    f"Scholarships (International): {university_data_dict['scholarships_international']}\n"
                    f"Scholarships (Nepali): {university_data_dict['scholarships_nepali']}\n"
                    f"Campus Facilities: {university_data_dict['campus_facilities']}"
                )
                university_data_dict["embedding"] = generate_embedding(text_to_embed, 'RETRIEVAL_DOCUMENT')

                # Insert university and get its ID
                university_id = insert_university(conn, university_data_dict)

                # Extract course data with new simplified schema
                course_data = {
                    "university_id": university_id,
                    "name": university_data.get("course_name", ""),
                    "description": university_data.get("course_description", ""),
                    "degree_type": university_data.get("degree_type", ""),
                    "field_of_study": university_data.get("field_of_study", ""),
                    "duration": university_data.get("duration", ""),
                    "tuition_fee": university_data.get("tuition_fee", ""),
                    "application_deadline": university_data.get("application_deadline", ""),
                    "admission_requirements": university_data.get("course_admission_requirements", ""),
                    "scholarships": university_data.get("course_scholarships", ""),
                }

                # Generate embedding for course
                text_to_embed = (
                    f"Course: {course_data['name']}\n"
                    f"University: {university_data_dict['university_name']}\n"
                    f"Field of Study: {course_data['field_of_study']}\n"
                    f"Degree Type: {course_data['degree_type']}\n"
                    f"Description: {course_data['description']}\n"
                    f"Duration: {course_data['duration']}\n"
                    f"Tuition Fee: {course_data['tuition_fee']}\n"
                    f"Admission Requirements: {course_data['admission_requirements']}\n"
                    f"Scholarships: {course_data['scholarships']}"
                )
                course_data["embedding"] = generate_embedding(text_to_embed, 'RETRIEVAL_DOCUMENT')

                # Insert course
                insert_course(conn, course_data)

                conn.commit()
                logger.info(
                    f"Stored data for university: {university_data_dict['university_name']}, course: {course_data['name']}"
                )

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
