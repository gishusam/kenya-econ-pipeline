import json
import glob
import os
from datetime import timezone, datetime
from db.connection import get_cursor
from utils.logger import get_logger

logger = get_logger(__name__)

def load_worldbank_file(filepath: str, table: str, value_col: str):
    with open(filepath) as f:
        raw = json.load(f)

    records = raw[1]  # World Bank returns [metadata, data]
    inserted = 0

    with get_cursor() as cur:
        for record in records:
            if record.get("value") is None:
                continue

            cur.execute(f"""
                INSERT INTO raw.{table}
                    (country_code, country_name, indicator_id,
                     indicator_name, year, value, source_file)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                record["countryiso3code"],
                record["country"]["value"],
                record["indicator"]["id"],
                record["indicator"]["value"],
                record["date"],
                record["value"],
                os.path.basename(filepath),
            ))
            inserted += 1

    logger.info(f"Inserted {inserted} rows into raw.{table} from {os.path.basename(filepath)}")
    return inserted

def load_to_staging_gdp():
    with get_cursor() as cur:
        cur.execute("""
            INSERT INTO staging.stg_worldbank_gdp
                (country_code, country_name, indicator_id, year, value_usd)
            SELECT
                r.country_code,
                r.country_name,
                r.indicator_id,
                r.year::INTEGER,
                r.value
            FROM raw.worldbank_gdp r
            WHERE NOT EXISTS (
                SELECT 1 FROM staging.stg_worldbank_gdp s
                WHERE s.country_code = r.country_code
                  AND s.year = r.year::INTEGER
                  AND s.is_current = TRUE
                  AND s.value_usd = r.value
            )
            ON CONFLICT DO NOTHING
        """)

        logger.info("Staging GDP load complete (SCD2 dedup applied)")

def load_to_staging_inflation():
    with get_cursor() as cur:
        cur.execute("""
            INSERT INTO staging.stg_worldbank_inflation
                (country_code, country_name, indicator_id, year, inflation_pct)
            SELECT
                r.country_code,
                r.country_name,
                r.indicator_id,
                r.year::INTEGER,
                r.value
            FROM raw.worldbank_inflation r
            WHERE NOT EXISTS (
                SELECT 1 FROM staging.stg_worldbank_inflation s
                WHERE s.country_code = r.country_code
                  AND s.year = r.year::INTEGER
                  AND s.is_current = TRUE
                  AND s.inflation_pct = r.value
            )
            ON CONFLICT DO NOTHING
        """)
        logger.info("Staging inflation load complete")

def run():
    gdp_files = sorted(glob.glob("data/raw/worldbank/gdp_ke_*.json"))
    inflation_files = sorted(glob.glob("data/raw/worldbank/inflation_ke_*.json"))

    for f in gdp_files:
        load_worldbank_file(f, "worldbank_gdp", "value_usd")
    for f in inflation_files:
        load_worldbank_file(f, "worldbank_inflation", "inflation_pct")

    load_to_staging_gdp()
    load_to_staging_inflation()

if __name__ == "__main__":
    run()