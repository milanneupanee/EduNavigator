import sqlite3
import logging
from consts import DIMENSION
import sqlite_vec
from typing import Dict, Any, List

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def init_db(db_path: str):
    """
    Initialize the database with required tables.

    Args:
        db_path: Path to SQLite database
    """
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

        conn.commit()
        logger.info("Database initialized successfully")

    except Exception as e:
        conn.rollback()
        logger.error(f"Error initializing database: {str(e)}")
    finally:
        conn.close()


def setup_vector_extension(db_path: str):
    """
    Set up the SQLite vector extension for vector similarity search.
    If the extension is not available, it will log a warning but continue.

    Args:
        db_path: Path to SQLite database

    Returns:
        True if the extension was loaded successfully, False otherwise
    """
    conn = sqlite3.connect(db_path)
    extension_loaded = False

    try:
        # Enable extension loading
        conn.enable_load_extension(True)

        try:
            # Try to load the extension
            sqlite_vec.load(conn)

            # Check if the extension loaded correctly
            cursor = conn.cursor()
            cursor.execute("SELECT sqlite_version(), vec_version()")
            sqlite_version, vec_version = cursor.fetchone()
            logger.info(
                f"SQLite version: {sqlite_version}, sqlite-vec version: {vec_version}"
            )
            extension_loaded = True

            # Try to create virtual tables
            try:
                # Drop existing virtual tables if they exist
                cursor.execute("DROP TABLE IF EXISTS vec_university")
                cursor.execute("DROP TABLE IF EXISTS vec_course")

                # Get dimension from existing embeddings or use default
                dimension = DIMENSION

                # Create virtual tables with the correct dimension
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

                # Populate the virtual tables with existing embeddings
                cursor.execute(
                    "SELECT university_id, embedding FROM university_embeddings"
                )
                university_embeddings = cursor.fetchall()

                for university_id, embedding in university_embeddings:
                    # Ensure the embedding has the correct dimension
                    if len(embedding) // 4 != dimension:
                        logger.warning(
                            f"Skipping university embedding {university_id} due to dimension mismatch"
                        )
                        continue

                    cursor.execute(
                        "INSERT INTO vec_university(rowid, embedding) VALUES (?, ?)",
                        (university_id, embedding),
                    )

                cursor.execute("SELECT course_id, embedding FROM course_embeddings")
                course_embeddings = cursor.fetchall()

                for course_id, embedding in course_embeddings:
                    # Ensure the embedding has the correct dimension
                    if len(embedding) // 4 != dimension:
                        logger.warning(
                            f"Skipping course embedding {course_id} due to dimension mismatch"
                        )
                        continue

                    cursor.execute(
                        "INSERT INTO vec_course(rowid, embedding) VALUES (?, ?)",
                        (course_id, embedding),
                    )

                conn.commit()
                logger.info("Vector tables created and populated successfully")

            except sqlite3.OperationalError as e:
                logger.error(f"Error creating virtual tables: {str(e)}")
                logger.error(
                    "Vector search will not be available, but embeddings will still be stored."
                )
                extension_loaded = False

        except Exception as e:
            logger.error(f"Error loading sqlite-vec extension: {str(e)}")
            logger.error(
                "Vector search will not be available, but embeddings will still be stored."
            )
            extension_loaded = False

    except Exception as e:
        conn.rollback()
        logger.error(f"Error setting up vector extension: {str(e)}")
        extension_loaded = False

    finally:
        # Disable extension loading for security
        try:
            conn.enable_load_extension(False)
        except:
            pass
        conn.close()

    return extension_loaded


def insert_university(conn: sqlite3.Connection, data: Dict[str, Any]) -> int:
    """
    Insert or update university data in the database.

    Args:
        conn: SQLite connection
        data: University data dictionary

    Returns:
        University ID
    """
    cursor = conn.cursor()

    # Check if university already exists
    cursor.execute(
        "SELECT id FROM universities WHERE name = ? AND country = ?",
        (data["name"], data["country"]),
    )
    result = cursor.fetchone()

    if result:
        # Update existing university
        university_id = result[0]
        cursor.execute(
            """
            UPDATE universities
            SET description = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (data["description"], university_id),
        )
        logger.info(f"Updated university: {data['name']}")
    else:
        # Insert new university
        cursor.execute(
            """
            INSERT INTO universities (name, country, description)
            VALUES (?, ?, ?)
            """,
            (data["name"], data["country"], data["description"]),
        )
        university_id = cursor.lastrowid
        logger.info(f"Inserted new university: {data['name']}")

    assert university_id is not None, "Failed to retrieve university ID after insertion"

    return university_id


def insert_course(conn: sqlite3.Connection, data: Dict[str, Any]) -> int:
    """
    Insert or update course data in the database.

    Args:
        conn: SQLite connection
        data: Course data dictionary

    Returns:
        Course ID
    """
    cursor = conn.cursor()

    # Check if course already exists
    cursor.execute(
        "SELECT id FROM courses WHERE name = ? AND university_id = ?",
        (data["name"], data["university_id"]),
    )
    result = cursor.fetchone()

    if result:
        # Update existing course
        course_id = result[0]
        cursor.execute(
            """
            UPDATE courses
            SET description = ?, degree_type = ?, starting_date = ?,
                duration = ?, scholarship = ?, fee_structure = ?,
                language_of_study = ?, field_of_study = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                data["description"],
                data["degree_type"],
                data["starting_date"],
                data["duration"],
                data["scholarship"],
                data["fee_structure"],
                data["language_of_study"],
                data["field_of_study"],
                course_id,
            ),
        )
        logger.info(f"Updated course: {data['name']}")
    else:
        # Insert new course
        cursor.execute(
            """
            INSERT INTO courses (
                university_id, name, description, degree_type, starting_date,
                duration, scholarship, fee_structure, language_of_study, field_of_study
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                data["university_id"],
                data["name"],
                data["description"],
                data["degree_type"],
                data["starting_date"],
                data["duration"],
                data["scholarship"],
                data["fee_structure"],
                data["language_of_study"],
                data["field_of_study"],
            ),
        )
        course_id = cursor.lastrowid
        logger.info(f"Inserted new course: {data['name']}")

    assert course_id is not None, "Failed to retrieve course ID after insertion"

    return course_id


def search_vector_similarity(
    conn: sqlite3.Connection,
    entity_type: str,
    query_embedding: bytes,
    limit: int = 5,
    query_text: str = None,  # Add query_text parameter for fallback
) -> List[Dict[str, Any]]:
    """
    Search for entities similar to the query embedding using sqlite-vec.
    Falls back to text search if vector search fails.

    Args:
        conn: SQLite connection
        entity_type: Type of entity to search for ('university' or 'course')
        query_embedding: Query embedding vector (already serialized)
        limit: Maximum number of results to return
        query_text: Original query text for fallback search

    Returns:
        List of matching entities with similarity scores
    """
    # Try vector search first
    try:
        # Enable extension loading for this connection
        conn.enable_load_extension(True)
        sqlite_vec.load(conn)
        
        cursor = conn.cursor()

        if entity_type == "university":
            # Search for universities using vec0 virtual table
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
                (query_embedding,),
            )

            rows = cursor.fetchall()
            logger.info(f"Vector search for universities returned {len(rows)} results")
            
            results = []
            for row in rows:
                # Convert distance to similarity (1 - normalized distance)
                distance = row[4]
                similarity = 1.0 - min(distance / 2.0, 1.0)  # Normalize and invert
                
                results.append(
                    {
                        "id": row[0],
                        "name": row[1],
                        "country": row[2],
                        "description": row[3],
                        "similarity_score": similarity,
                    }
                )

            return results

        elif entity_type == "course":
            # Search for courses using vec0 virtual table
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
                (query_embedding, ),
            )

            rows = cursor.fetchall()
            logger.info(f"Vector search for courses returned {len(rows)} results")
            
            results = []
            for row in rows:
                # Convert distance to similarity (1 - normalized distance)
                distance = row[10]
                similarity = 1.0 - min(distance / 2.0, 1.0)  # Normalize and invert
                
                results.append(
                    {
                        "id": row[0],
                        "name": row[1],
                        "university_name": row[2],
                        "description": row[3],
                        "degree_type": row[4],
                        "field_of_study": row[5],
                        "starting_date": row[6],
                        "duration": row[7],
                        "fee_structure": row[8],
                        "language_of_study": row[9],
                        "similarity_score": similarity,
                    }
                )

            return results

        else:
            raise ValueError(f"Invalid entity type: {entity_type}")

    except Exception as e:
        logger.error(f"Vector search failed: {str(e)}")
        logger.info("Falling back to text search")
        
        # Fall back to text search if query_text is provided
        if query_text:
            return search_text_similarity(conn, entity_type, query_text, limit)
        else:
            logger.error("No query text provided for fallback search")
            return []
    finally:
        # Disable extension loading for security
        try:
            conn.enable_load_extension(False)
        except:
            pass


def search_text_similarity(
    conn: sqlite3.Connection,
    entity_type: str,
    query: str,
    limit: int = 5,
) -> List[Dict[str, Any]]:
    """
    Fallback search method using simple text matching when vector search is not available.

    Args:
        conn: SQLite connection
        entity_type: Type of entity to search for ('university' or 'course')
        query: Text query
        limit: Maximum number of results to return

    Returns:
        List of matching entities
    """
    cursor = conn.cursor()

    try:
        if entity_type == "university":
            # Search for universities using text matching
            cursor.execute(
                """
                SELECT 
                    id, name, country, description
                FROM universities
                WHERE 
                    name LIKE ? OR 
                    description LIKE ? OR
                    country LIKE ?
                LIMIT ?
                """,
                (f"%{query}%", f"%{query}%", f"%{query}%", limit),
            )

            results = []
            for row in cursor.fetchall():
                results.append(
                    {
                        "id": row[0],
                        "name": row[1],
                        "country": row[2],
                        "description": row[3],
                        "similarity_score": 0.5,  # Default similarity score
                    }
                )

            return results

        elif entity_type == "course":
            # Search for courses using text matching
            cursor.execute(
                """
                SELECT 
                    c.id, c.name, u.name, c.description, c.degree_type, c.field_of_study,
                    c.starting_date, c.duration, c.fee_structure, c.language_of_study
                FROM courses c
                JOIN universities u ON c.university_id = u.id
                WHERE 
                    c.name LIKE ? OR 
                    c.description LIKE ? OR
                    c.field_of_study LIKE ?
                LIMIT ?
                """,
                (f"%{query}%", f"%{query}%", f"%{query}%", limit),
            )

            results = []
            for row in cursor.fetchall():
                results.append(
                    {
                        "id": row[0],
                        "name": row[1],
                        "university_name": row[2],
                        "description": row[3],
                        "degree_type": row[4],
                        "field_of_study": row[5],
                        "starting_date": row[6],
                        "duration": row[7],
                        "fee_structure": row[8],
                        "language_of_study": row[9],
                        "similarity_score": 0.5,  # Default similarity score
                    }
                )

            return results

        else:
            raise ValueError(f"Invalid entity type: {entity_type}")

    except Exception as e:
        logger.error(f"Error in text search: {str(e)}")
        return []


def check_and_fix_embeddings(db_path: str):
    """
    Check and fix any dimension mismatches in existing embeddings.

    Args:
        db_path: Path to SQLite database
    """
    conn = sqlite3.connect(db_path)
    try:
        # Enable extension loading
        conn.enable_load_extension(True)
        sqlite_vec.load(conn)

        cursor = conn.cursor()

        # Check university embeddings
        cursor.execute("SELECT id, university_id, embedding FROM university_embeddings")
        university_embeddings = cursor.fetchall()

        if university_embeddings:
            # Get the dimension of the first embedding
            first_embedding = university_embeddings[0][2]
            expected_dimension = len(first_embedding) // 4  # 4 bytes per float32

            logger.info(
                f"Expected university embedding dimension: {expected_dimension}"
            )

            # Check all embeddings for consistency
            for id, university_id, embedding in university_embeddings:
                dimension = len(embedding) // 4
                if dimension != expected_dimension:
                    logger.warning(
                        f"Dimension mismatch for university embedding {id}: {dimension} != {expected_dimension}"
                    )

        # Check course embeddings
        cursor.execute("SELECT id, course_id, embedding FROM course_embeddings")
        course_embeddings = cursor.fetchall()

        if course_embeddings:
            # Get the dimension of the first embedding
            first_embedding = course_embeddings[0][2]
            expected_dimension = len(first_embedding) // 4  # 4 bytes per float32

            logger.info(f"Expected course embedding dimension: {expected_dimension}")

            # Check all embeddings for consistency
            for id, course_id, embedding in course_embeddings:
                dimension = len(embedding) // 4
                if dimension != expected_dimension:
                    logger.warning(
                        f"Dimension mismatch for course embedding {id}: {dimension} != {expected_dimension}"
                    )

        # Recreate the virtual tables with the correct dimensions
        setup_vector_extension(db_path)

        # Populate the virtual tables with existing embeddings
        cursor.execute("DELETE FROM vec_university")
        cursor.execute("DELETE FROM vec_course")

        for id, university_id, embedding in university_embeddings:
            cursor.execute(
                "INSERT INTO vec_university(rowid, embedding) VALUES (?, ?)",
                (university_id, embedding),
            )

        for id, course_id, embedding in course_embeddings:
            cursor.execute(
                "INSERT INTO vec_course(rowid, embedding) VALUES (?, ?)",
                (course_id, embedding),
            )

        conn.commit()
        logger.info("Embeddings checked and fixed successfully")

    except Exception as e:
        conn.rollback()
        logger.error(f"Error checking and fixing embeddings: {str(e)}")
    finally:
        # Disable extension loading for security
        conn.enable_load_extension(False)
        conn.close()


def debug_vector_tables(db_path: str):
    """
    Debug function to check if vector tables are properly populated.
    
    Args:
        db_path: Path to SQLite database
    """
    conn = sqlite3.connect(db_path)
    try:
        # Enable extension loading
        conn.enable_load_extension(True)
        sqlite_vec.load(conn)
        
        cursor = conn.cursor()
        
        # Check if vector tables exist
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='vec_university'")
        if not cursor.fetchone():
            logger.error("vec_university table does not exist")
        else:
            # Count rows in vec_university
            cursor.execute("SELECT COUNT(*) FROM vec_university")
            count = cursor.fetchone()[0]
            logger.info(f"vec_university has {count} rows")
            
            # Sample a row
            if count > 0:
                cursor.execute("SELECT rowid FROM vec_university LIMIT 1")
                rowid = cursor.fetchone()[0]
                logger.info(f"Sample rowid in vec_university: {rowid}")
                
                # Check if the rowid exists in universities table
                cursor.execute("SELECT id FROM universities WHERE id = ?", (rowid,))
                if cursor.fetchone():
                    logger.info(f"Rowid {rowid} exists in universities table")
                else:
                    logger.error(f"Rowid {rowid} does not exist in universities table")
        
        # Check if vec_course table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='vec_course'")
        if not cursor.fetchone():
            logger.error("vec_course table does not exist")
        else:
            # Count rows in vec_course
            cursor.execute("SELECT COUNT(*) FROM vec_course")
            count = cursor.fetchone()[0]
            logger.info(f"vec_course has {count} rows")
            
            # Sample a row
            if count > 0:
                cursor.execute("SELECT rowid FROM vec_course LIMIT 1")
                rowid = cursor.fetchone()[0]
                logger.info(f"Sample rowid in vec_course: {rowid}")
                
                # Check if the rowid exists in courses table
                cursor.execute("SELECT id FROM courses WHERE id = ?", (rowid,))
                if cursor.fetchone():
                    logger.info(f"Rowid {rowid} exists in courses table")
                else:
                    logger.error(f"Rowid {rowid} does not exist in courses table")
        
        # Try a simple vector search
        # Create a test vector
        import struct
        test_vector = [0.1] * DIMENSION
        serialized_vector = struct.pack(f"{DIMENSION}f", *test_vector)
        
        # Try to search
        try:
            cursor.execute(
                """
                SELECT rowid, distance
                FROM vec_university
                WHERE embedding MATCH ? AND K = 3
                ORDER BY distance
                """,
                (serialized_vector,),
            )
            result = cursor.fetchone()
            if result:
                logger.info(f"Test vector search successful: {result}")
            else:
                logger.warning("Test vector search returned no results")
        except Exception as e:
            logger.error(f"Test vector search failed: {str(e)}")
        
    except Exception as e:
        logger.error(f"Error debugging vector tables: {str(e)}")
    finally:
        # Disable extension loading for security
        try:
            conn.enable_load_extension(False)
        except:
            pass
        conn.close()


