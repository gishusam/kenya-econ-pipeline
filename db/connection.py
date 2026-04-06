import os
import psycopg2
from contextlib import contextmanager
from dotenv import load_dotenv
from utils.logger import get_logger

load_dotenv()
logger = get_logger(__name__)

def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", 5432),
        dbname=os.getenv("POSTGRES_DB", "kenya_econ"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )

@contextmanager
def get_cursor():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            yield cur
        conn.commit()
        logger.info("Transaction committed")
    except Exception as e:
        conn.rollback()
        logger.error(f"Transaction rolled back: {e}")
        raise
    finally:
        conn.close()