import os
import glob
from db.connection import get_cursor
from utils.logger import get_logger

logger = get_logger(__name__)

MIGRATIONS_DIR = os.path.join(os.path.dirname(__file__), "migrations")

def run_migrations():
    sql_files = sorted(glob.glob(f"{MIGRATIONS_DIR}/*.sql"))
    with get_cursor() as cur:
        for filepath in sql_files:
            filename = os.path.basename(filepath)
            logger.info(f"Running migration: {filename}")
            with open(filepath, "r") as f:
                cur.execute(f.read())
            logger.info(f"Migration complete: {filename}")

if __name__ == "__main__":
    run_migrations()