import os
import libsql
import logging
from consts import DIMENSION
from typing import Dict, Any, List

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def get_synced_conn():
    url = os.getenv("TURSO_DATABASE_URL")
    auth_token = os.getenv("TURSO_AUTH_TOKEN")
    conn = libsql.connect("local.db", sync_url=url, auth_token=auth_token)
    conn.sync()
    return conn

def initialize_database():
    """
    Initialize the database with required tables and vector search index.
    Uses Turso sync connection.
    """
    conn = get_synced_conn()
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS universities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            country TEXT,
            description TEXT,
            embedding F32_BLOB({DIMENSION}),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        cursor.execute(f"""
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
            embedding F32_BLOB({DIMENSION}),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (university_id) REFERENCES universities (id)
        )
        """)
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS universities_embedding_idx ON universities (libsql_vector_idx(embedding));
        """)
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS courses_embedding_idx ON courses (libsql_vector_idx(embedding));
        """)
        conn.commit()
        logger.info("Database and vector indexes initialized successfully.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Database initialization failed: {str(e)}")
        raise
    finally:
        conn.close()

def insert_university(conn, data: Dict[str, Any]) -> int:
    cursor = conn.cursor()
    cursor.execute(
        "SELECT id FROM universities WHERE name = ? AND country = ?",
        (data["name"], data["country"]),
    )
    result = cursor.fetchone()
    embedding_str = data.get("embedding")
    if isinstance(embedding_str, list):
        embedding_str = str(embedding_str)
    if result:
        university_id = result[0]
        cursor.execute(
            f"""
            UPDATE universities
            SET description = ?, embedding = vector(?), updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (data["description"], embedding_str, university_id),
        )
        logger.info(f"Updated university: {data['name']}")
    else:
        cursor.execute(
            f"""
            INSERT INTO universities (name, country, description, embedding)
            VALUES (?, ?, ?, vector(?))
            """,
            (data["name"], data["country"], data["description"], embedding_str),
        )
        university_id = cursor.lastrowid
        logger.info(f"Inserted new university: {data['name']}")
    assert university_id is not None, "Failed to retrieve university ID after insertion"
    return university_id

def insert_course(conn, data: Dict[str, Any]) -> int:
    cursor = conn.cursor()
    cursor.execute(
        "SELECT id FROM courses WHERE name = ? AND university_id = ?",
        (data["name"], data["university_id"]),
    )
    result = cursor.fetchone()
    embedding_str = data.get("embedding")
    if isinstance(embedding_str, list):
        embedding_str = str(embedding_str)
    if result:
        course_id = result[0]
        cursor.execute(
            f"""
            UPDATE courses
            SET description = ?, degree_type = ?, starting_date = ?,
                duration = ?, scholarship = ?, fee_structure = ?,
                language_of_study = ?, field_of_study = ?, embedding = vector(?), updated_at = CURRENT_TIMESTAMP
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
                embedding_str,
                course_id,
            ),
        )
        logger.info(f"Updated course: {data['name']}")
    else:
        cursor.execute(
            f"""
            INSERT INTO courses (
                university_id, name, description, degree_type, starting_date,
                duration, scholarship, fee_structure, language_of_study, field_of_study, embedding
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, vector(?))
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
                embedding_str,
            ),
        )
        course_id = cursor.lastrowid
        logger.info(f"Inserted new course: {data['name']}")
    assert course_id is not None, "Failed to retrieve course ID after insertion"
    return course_id

def search_vector_similarity(conn, entity_type: str, query_embedding, limit: int = 5, query_text: str = None) -> List[Dict[str, Any]]:
    cursor = conn.cursor()
    if isinstance(query_embedding, list):
        embedding_str = str(query_embedding)
    else:
        embedding_str = query_embedding
    if entity_type == "university":
        cursor.execute(
            """
            SELECT u.id, u.name, u.country, u.description, vector_distance_cos(u.embedding, ?) as distance
            FROM universities u
            ORDER BY distance
            LIMIT ?
            """,
            (embedding_str, limit),
        )
        rows = cursor.fetchall()
        results = []
        for row in rows:
            results.append({
                "id": row[0],
                "name": row[1],
                "country": row[2],
                "description": row[3],
                "similarity_score": 1.0 - min(row[4] / 2.0, 1.0),
            })
        return results
    elif entity_type == "course":
        cursor.execute(
            """
            SELECT c.id, c.name, u.name, c.description, c.degree_type, c.field_of_study,
                   c.starting_date, c.duration, c.fee_structure, c.language_of_study,
                   vector_distance_cos(c.embedding, ?) as distance
            FROM courses c
            JOIN universities u ON c.university_id = u.id
            ORDER BY distance
            LIMIT ?
            """,
            (embedding_str, limit),
        )
        rows = cursor.fetchall()
        results = []
        for row in rows:
            results.append({
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
                "similarity_score": 1.0 - min(row[10] / 2.0, 1.0),
            })
        return results
    else:
        raise ValueError(f"Invalid entity type: {entity_type}")


