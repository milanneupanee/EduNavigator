#!/usr/bin/env python3
"""
Initialize the database with the required tables.
"""

import argparse
import logging
import os
from pathlib import Path
from utils.db_utils import init_db, setup_vector_extension

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def setup_directories():
    """Create necessary directories if they don't exist."""
    Path("data/raw").mkdir(parents=True, exist_ok=True)
    Path("data/processed").mkdir(parents=True, exist_ok=True)

def main():
    """Initialize the database."""
    parser = argparse.ArgumentParser(description="Initialize the database")
    parser.add_argument("--db-path", type=str, default="data/database.db", help="Path to SQLite database")
    
    args = parser.parse_args()
    
    # Create necessary directories
    setup_directories()
    
    # Initialize the database
    logger.info(f"Initializing database at {args.db_path}")
    init_db(args.db_path)
    
    # Try to set up the vector extension
    logger.info("Setting up vector extension")
    if setup_vector_extension(args.db_path):
        logger.info("Vector extension set up successfully")
    else:
        logger.warning("Failed to set up vector extension")
        logger.warning("Vector search will not be available, but embeddings will still be stored.")
    
    logger.info("Database initialization complete")

if __name__ == "__main__":
    main() 