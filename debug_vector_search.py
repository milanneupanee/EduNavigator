#!/usr/bin/env python3
"""
Debug script for vector search functionality.
"""

import argparse
import logging
import sqlite3
import struct
import os
from pathlib import Path
import sqlite_vec
from consts import DIMENSION
from utils.db_utils import debug_vector_tables, setup_vector_extension
from utils.gemini_utils import generate_embedding

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def test_vector_search(db_path: str, query: str):
    """
    Test vector search functionality.
    
    Args:
        db_path: Path to SQLite database
        query: Query string to test
    """
    logger.info(f"Testing vector search with query: {query}")
    
    # Generate query embedding
    embedding = generate_embedding(query, 'RETRIEVAL_QUERY')
    
    if not embedding:
        logger.error("Failed to generate embedding for query")
        return
    
    # Connect to database
    conn = sqlite3.connect(db_path)
    try:
        # Enable extension loading
        conn.enable_load_extension(True)
        sqlite_vec.load(conn)
        
        cursor = conn.cursor()
        
        # Try to search universities
        try:
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
                (embedding,),
            )
            
            rows = cursor.fetchall()
            logger.info(f"University search returned {len(rows)} results")
            
            for row in rows:
                logger.info(f"University: {row[1]}, Distance: {row[4]}")
        except Exception as e:
            logger.error(f"University search failed: {str(e)}")
        
        # Try to search courses
        try:
            cursor.execute(
                """
                SELECT 
                    c.id, c.name, u.name, c.description, c.degree_type, c.field_of_study,
                    c.starting_date, c.duration, c.fee_structure, c.language_of_study,
                    v.distance as similarity_score
                FROM vec_course v
                JOIN courses c ON v.rowid = c.id
                JOIN universities u ON c.university_id = u.id
                WHERE v.embedding MATCH ? AND K = 3
                ORDER BY distance
                """,
                (embedding,),
            )
            
            rows = cursor.fetchall()
            logger.info(f"Course search returned {len(rows)} results")
            
            for row in rows:
                logger.info(f"Course: {row[1]} at {row[2]}, Distance: {row[10]}")
        except Exception as e:
            logger.error(f"Course search failed: {str(e)}")
        
    except Exception as e:
        logger.error(f"Error testing vector search: {str(e)}")
    finally:
        # Disable extension loading for security
        try:
            conn.enable_load_extension(False)
        except:
            pass
        conn.close()

def fix_vector_tables(db_path: str):
    """
    Fix vector tables by recreating them and repopulating with embeddings.
    
    Args:
        db_path: Path to SQLite database
    """
    logger.info("Fixing vector tables")
    
    conn = sqlite3.connect(db_path)
    try:
        # Enable extension loading
        conn.enable_load_extension(True)
        sqlite_vec.load(conn)
        
        cursor = conn.cursor()
        
        # Drop existing virtual tables
        cursor.execute("DROP TABLE IF EXISTS vec_university")
        cursor.execute("DROP TABLE IF EXISTS vec_course")
        
        # Create virtual tables with the correct dimension
        cursor.execute(f"""
        CREATE VIRTUAL TABLE IF NOT EXISTS vec_university USING vec0(
            embedding FLOAT[{DIMENSION}]
        )
        """)
        
        cursor.execute(f"""
        CREATE VIRTUAL TABLE IF NOT EXISTS vec_course USING vec0(
            embedding FLOAT[{DIMENSION}]
        )
        """)
        
        # Get all embeddings
        cursor.execute("SELECT university_id, embedding FROM university_embeddings")
        university_embeddings = cursor.fetchall()
        
        # Populate vec_university
        for university_id, embedding in university_embeddings:
            # Check if the university exists
            cursor.execute("SELECT id FROM universities WHERE id = ?", (university_id,))
            if cursor.fetchone():
                # Check embedding dimension
                dimension = len(embedding) // 4
                if dimension == DIMENSION:
                    cursor.execute(
                        "INSERT INTO vec_university(rowid, embedding) VALUES (?, ?)",
                        (university_id, embedding),
                    )
                else:
                    logger.warning(f"Skipping university embedding {university_id} due to dimension mismatch: {dimension} != {DIMENSION}")
            else:
                logger.warning(f"University {university_id} does not exist, skipping embedding")
        
        # Get all course embeddings
        cursor.execute("SELECT course_id, embedding FROM course_embeddings")
        course_embeddings = cursor.fetchall()
        
        # Populate vec_course
        for course_id, embedding in course_embeddings:
            # Check if the course exists
            cursor.execute("SELECT id FROM courses WHERE id = ?", (course_id,))
            if cursor.fetchone():
                # Check embedding dimension
                dimension = len(embedding) // 4
                if dimension == DIMENSION:
                    cursor.execute(
                        "INSERT INTO vec_course(rowid, embedding) VALUES (?, ?)",
                        (course_id, embedding),
                    )
                else:
                    logger.warning(f"Skipping course embedding {course_id} due to dimension mismatch: {dimension} != {DIMENSION}")
            else:
                logger.warning(f"Course {course_id} does not exist, skipping embedding")
        
        conn.commit()
        logger.info("Vector tables fixed successfully")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error fixing vector tables: {str(e)}")
    finally:
        # Disable extension loading for security
        try:
            conn.enable_load_extension(False)
        except:
            pass
        conn.close()

def main():
    """Debug vector search functionality."""
    parser = argparse.ArgumentParser(description="Debug vector search functionality")
    parser.add_argument("--db-path", type=str, default="data/database.db", help="Path to SQLite database")
    parser.add_argument("--query", type=str, default="computer science", help="Query string to test")
    parser.add_argument("--fix", action="store_true", help="Fix vector tables")
    
    args = parser.parse_args()
    
    # Debug vector tables
    debug_vector_tables(args.db_path)
    
    # Fix vector tables if requested
    if args.fix:
        fix_vector_tables(args.db_path)
        debug_vector_tables(args.db_path)
    
    # Test vector search
    test_vector_search(args.db_path, args.query)

if __name__ == "__main__":
    main() 
