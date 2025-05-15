#!/usr/bin/env python3
"""
Reset the database and set up the vector tables with a fixed dimension.
"""

import argparse
import logging
import os
import sqlite3
import struct
from pathlib import Path
from consts import DIMENSION
import sqlite_vec

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def setup_directories():
    """Create necessary directories if they don't exist."""
    Path("data/raw").mkdir(parents=True, exist_ok=True)
    Path("data/processed").mkdir(parents=True, exist_ok=True)


def reset_database(db_path: str, dimension: int = DIMENSION):
    """
    Reset the database and set up the vector tables with a fixed dimension.

    Args:
        db_path: Path to SQLite database
        dimension: Dimension for vector embeddings
    """
    # Delete the database file if it exists
    if os.path.exists(db_path):
        os.remove(db_path)
        logger.info(f"Deleted existing database at {db_path}")

    # Create a new database
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()

        # Create universities table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS universities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            country TEXT,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)

        # Create courses table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS courses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            university_id INTEGER,
            name TEXT NOT NULL,
            description TEXT,
            degree_type TEXT,
            starting_date TEXT,
            duration TEXT,
            scholarship TEXT,
            fee_structure TEXT,
            language_of_study TEXT,
            field_of_study TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (university_id) REFERENCES universities (id)
        )
        """)

        # Create tables for embeddings
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS university_embeddings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            university_id INTEGER,
            embedding BLOB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (university_id) REFERENCES universities (id)
        )
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS course_embeddings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            course_id INTEGER,
            embedding BLOB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (course_id) REFERENCES courses (id)
        )
        """)

        # Set up vector extension
        conn.enable_load_extension(True)
        sqlite_vec.load(conn)

        # Create virtual tables with the fixed dimension
        cursor.execute(f"""
        CREATE VIRTUAL TABLE IF NOT EXISTS vec_university USING vec0(
            embedding FLOAT[{dimension}]
        )
        """)

        cursor.execute(f"""
        CREATE VIRTUAL TABLE IF NOT EXISTS vec_course USING vec0(
            embedding FLOAT[{dimension}]
        )
        """)

        # Create a test vector to verify the setup
        test_vector = [0.1] * dimension
        serialized_vector = struct.pack(f"{dimension}f", *test_vector)

        # Insert a test row to verify the setup
        cursor.execute(
            """
        INSERT INTO universities (name, country, description)
        VALUES (?, ?, ?)
        """,
            ("Test University", "Test Country", "Test Description"),
        )

        university_id = cursor.lastrowid

        cursor.execute(
            """
        INSERT INTO university_embeddings (university_id, embedding)
        VALUES (?, ?)
        """,
            (university_id, serialized_vector),
        )

        cursor.execute(
            """
        INSERT INTO vec_university (rowid, embedding)
        VALUES (?, ?)
        """,
            (university_id, serialized_vector),
        )

        # Test a query to verify the setup
        cursor.execute(
            """
        SELECT 
            u.id, u.name, u.country, u.description, 
            v.distance as similarity_score
        FROM vec_university v
        JOIN universities u ON v.rowid = u.id
        WHERE v.embedding MATCH ? AND K = 3
        ORDER BY distance
        """,
            (serialized_vector,),
        )

        result = cursor.fetchone()
        if result:
            logger.info(f"Vector search test successful: {result}")
        else:
            logger.warning("Vector search test failed")

        # Clean up the test data
        cursor.execute("DELETE FROM vec_university")
        cursor.execute("DELETE FROM university_embeddings")
        cursor.execute("DELETE FROM universities")

        conn.commit()
        logger.info(
            f"Database reset and initialized successfully with dimension {dimension}"
        )

    except Exception as e:
        conn.rollback()
        logger.error(f"Error resetting database: {str(e)}")
    finally:
        try:
            conn.enable_load_extension(False)
        except:
            pass
        conn.close()


def main():
    """Reset the database."""
    parser = argparse.ArgumentParser(
        description="Reset the database and set up vector tables"
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default="data/database.db",
        help="Path to SQLite database",
    )
    parser.add_argument(
        "--dimension", type=int, default=DIMENSION, help="Dimension for vector embeddings"
    )

    args = parser.parse_args()

    # Create necessary directories
    setup_directories()

    # Reset the database
    logger.info(f"Resetting database at {args.db_path} with dimension {args.dimension}")
    reset_database(args.db_path, args.dimension)

    logger.info("Database reset complete")


if __name__ == "__main__":
    main()

