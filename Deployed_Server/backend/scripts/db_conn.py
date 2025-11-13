import pymysql
import logging

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    "host": "localhost",
    "user": "abc", #replace with new
    "password": "xyz", #replace with new
    "database": "tcna_cancer_db",
    "port": 3310,
    "cursorclass": pymysql.cursors.DictCursor,
}

def get_connection():
    """Create and return a database connection."""
    try:
        conn = pymysql.connect(**DB_CONFIG)
        logger.info("Successfully connected to the database")
        return conn
    except pymysql.MySQLError as e:
        logger.error(f"Failed to connect to the database: {e}")
        raise
