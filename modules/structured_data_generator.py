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
                # Extract university data
                university_data_dict = {
                    "name": university_data.get("university_name", ""),
                    "country": university_data.get("country", ""),
                    "description": university_data.get("university_description", ""),
                }

                # Generate embedding for university
                text_to_embed = f"University: {university_data_dict['name']}\nCountry: {university_data_dict['country']}\nDescription: {university_data_dict['description']}"
                university_data_dict["embedding"] = generate_embedding(text_to_embed, 'RETRIEVAL_DOCUMENT')

                # Insert university and get its ID
                university_id = insert_university(conn, university_data_dict)

                # Extract course data
                course_data = {
                    "university_id": university_id,
                    "name": university_data.get("course_name", ""),
                    "description": university_data.get("description", ""),
                    "degree_type": university_data.get("degree_type", ""),
                    "starting_date": university_data.get("starting_date", ""),
                    "duration": university_data.get("duration", ""),
                    "scholarship": university_data.get("scholarship", ""),
                    "fee_structure": university_data.get("fee_structure", ""),
                    "language_of_study": university_data.get("language_of_study", ""),
                    "field_of_study": university_data.get("field_of_study", ""),
                }

                # Generate embedding for course
                text_to_embed = (
                    f"Course: {course_data['name']}\n"
                    f"University: {university_data_dict['name']}\n"
                    f"Field of Study: {course_data['field_of_study']}\n"
                    f"Degree Type: {course_data['degree_type']}\n"
                    f"Description: {course_data['description']}"
                )
                course_data["embedding"] = generate_embedding(text_to_embed, 'RETRIEVAL_DOCUMENT')

                # Insert course
                insert_course(conn, course_data)

                conn.commit()
                logger.info(
                    f"Stored data for university: {university_data_dict['name']}, course: {course_data['name']}"
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
