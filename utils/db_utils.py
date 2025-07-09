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
    """
    Get a database connection using libsql.
    For Turso cloud databases, use the URL and auth_token.
    For local development, use a local file.
    """
    url = os.getenv("TURSO_DATABASE_URL")
    auth_token = os.getenv("TURSO_AUTH_TOKEN")
    
    # For testing, use local SQLite if Turso is not available
    if not url or not auth_token:
        print("Warning: Using local SQLite database for testing")
        return libsql.connect("local.db")
    
    try:
        print(f"Connecting to Turso database: {url}")
        conn = libsql.connect('local.db', sync_url=url, auth_token=auth_token)
        return conn
    except Exception as e:
        print(f"Warning: Failed to connect to Turso: {e}")
        print("Falling back to local SQLite database for testing")
        return libsql.connect("local.db")

def initialize_database():
    """
    Initialize the database with required tables and vector search index.
    Uses Turso sync connection.
    """
    conn = get_synced_conn()
    try:
        cursor = conn.cursor()
        cursor.execute("""select 1""")
        result = cursor.fetchone()
        print(f"Connection successful! Result: {result}")
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS universities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            country TEXT,
            university_name TEXT NOT NULL,
            city TEXT,
            university_url TEXT,
            undergraduate_programs TEXT,
            graduate_programs TEXT,
            tuition_undergrad TEXT,
            tuition_grad TEXT,
            living_cost TEXT,
            application_deadlines TEXT,
            admission_requirements TEXT,
            scholarships_international TEXT,
            scholarships_nepali TEXT,
            campus_facilities TEXT,
            embedding BLOB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        cursor.execute("""select 2""")
        result = cursor.fetchone()
        print(f"Connection successful! Result: {result}")
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS courses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            university_id INTEGER,
            name TEXT NOT NULL,
            description TEXT,
            degree_type TEXT,
            field_of_study TEXT,
            duration TEXT,
            tuition_fee TEXT,
            application_deadline TEXT,
            admission_requirements TEXT,
            scholarships TEXT,
            embedding BLOB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (university_id) REFERENCES universities (id)
        )
        """)
        # Create vector indexes separately
        try:
            cursor.execute("""
            CREATE INDEX IF NOT EXISTS universities_embedding_idx ON universities (libsql_vector_idx(embedding));
            """)
            cursor.execute("""
            CREATE INDEX IF NOT EXISTS courses_embedding_idx ON courses (libsql_vector_idx(embedding));
            """)
        except Exception as e:
            logger.warning(f"Could not create vector indexes: {e}")
            logger.info("Tables created successfully without vector indexes")
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
        "SELECT id FROM universities WHERE university_name = ? AND country = ?",
        (data["university_name"], data["country"]),
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
            SET city = ?, university_url = ?, undergraduate_programs = ?, graduate_programs = ?,
                tuition_undergrad = ?, tuition_grad = ?, living_cost = ?, application_deadlines = ?,
                admission_requirements = ?, scholarships_international = ?, scholarships_nepali = ?,
                campus_facilities = ?, embedding = vector(?), updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                data.get("city"),
                data.get("university_url"),
                data.get("undergraduate_programs"),
                data.get("graduate_programs"),
                data.get("tuition_undergrad"),
                data.get("tuition_grad"),
                data.get("living_cost"),
                data.get("application_deadlines"),
                data.get("admission_requirements"),
                data.get("scholarships_international"),
                data.get("scholarships_nepali"),
                data.get("campus_facilities"),
                embedding_str,
                university_id,
            ),
        )
        logger.info(f"Updated university: {data['university_name']}")
    else:
        cursor.execute(
            f"""
            INSERT INTO universities (
                country, university_name, city, university_url, undergraduate_programs,
                graduate_programs, tuition_undergrad, tuition_grad, living_cost,
                application_deadlines, admission_requirements, scholarships_international,
                scholarships_nepali, campus_facilities, embedding
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, vector(?))
            """,
            (
                data.get("country"),
                data["university_name"],
                data.get("city"),
                data.get("university_url"),
                data.get("undergraduate_programs"),
                data.get("graduate_programs"),
                data.get("tuition_undergrad"),
                data.get("tuition_grad"),
                data.get("living_cost"),
                data.get("application_deadlines"),
                data.get("admission_requirements"),
                data.get("scholarships_international"),
                data.get("scholarships_nepali"),
                data.get("campus_facilities"),
                embedding_str,
            ),
        )
        university_id = cursor.lastrowid
        logger.info(f"Inserted new university: {data['university_name']}")
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
            SET description = ?, degree_type = ?, field_of_study = ?, duration = ?,
                tuition_fee = ?, application_deadline = ?, admission_requirements = ?,
                scholarships = ?, embedding = vector(?), updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                data.get("description"),
                data.get("degree_type"),
                data.get("field_of_study"),
                data.get("duration"),
                data.get("tuition_fee"),
                data.get("application_deadline"),
                data.get("admission_requirements"),
                data.get("scholarships"),
                embedding_str,
                course_id,
            ),
        )
        logger.info(f"Updated course: {data['name']}")
    else:
        cursor.execute(
            f"""
            INSERT INTO courses (
                university_id, name, description, degree_type, field_of_study,
                duration, tuition_fee, application_deadline, admission_requirements,
                scholarships, embedding
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, vector(?))
            """,
            (
                data["university_id"],
                data["name"],
                data.get("description"),
                data.get("degree_type"),
                data.get("field_of_study"),
                data.get("duration"),
                data.get("tuition_fee"),
                data.get("application_deadline"),
                data.get("admission_requirements"),
                data.get("scholarships"),
                embedding_str,
            ),
        )
        course_id = cursor.lastrowid
        logger.info(f"Inserted new course: {data['name']}")
    assert course_id is not None, "Failed to retrieve course ID after insertion"
    return course_id

def search_vector_similarity(conn, entity_type: str, query_embedding, limit: int = 5, query_text: str | None = None) -> List[Dict[str, Any]]:
    cursor = conn.cursor()
    if isinstance(query_embedding, list):
        embedding_str = str(query_embedding)
    else:
        embedding_str = query_embedding
    if entity_type == "university":
        cursor.execute(
            """
            SELECT u.id, u.university_name, u.country, u.city, u.university_url,
                   u.undergraduate_programs, u.graduate_programs, u.tuition_undergrad,
                   u.tuition_grad, u.living_cost, u.application_deadlines,
                   u.admission_requirements, u.scholarships_international,
                   u.scholarships_nepali, u.campus_facilities,
                   vector_distance_cos(u.embedding, ?) as distance
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
                "university_name": row[1],
                "country": row[2],
                "city": row[3],
                "university_url": row[4],
                "undergraduate_programs": row[5],
                "graduate_programs": row[6],
                "tuition_undergrad": row[7],
                "tuition_grad": row[8],
                "living_cost": row[9],
                "application_deadlines": row[10],
                "admission_requirements": row[11],
                "scholarships_international": row[12],
                "scholarships_nepali": row[13],
                "campus_facilities": row[14],
                "similarity_score": 1.0 - min(row[15] / 2.0, 1.0),
            })
        return results
    elif entity_type == "course":
        cursor.execute(
            """
            SELECT c.id, c.name, u.university_name, c.description, c.degree_type,
                   c.field_of_study, c.duration, c.tuition_fee, c.application_deadline,
                   c.admission_requirements, c.scholarships,
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
                "duration": row[6],
                "tuition_fee": row[7],
                "application_deadline": row[8],
                "admission_requirements": row[9],
                "scholarships": row[10],
                "similarity_score": 1.0 - min(row[11] / 2.0, 1.0),
            })
        return results
    else:
        raise ValueError(f"Invalid entity type: {entity_type}")


