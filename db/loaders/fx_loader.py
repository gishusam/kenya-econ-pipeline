import json
import glob
import os
from datetime import datetime, timezone
from db.connection import get_cursor
from utils.logger import get_logger

logger = get_logger(__name__)

def load_fx_file(filepath: str):
    with open(filepath) as f:
        data = json.load(f)

    rates = data["rates"]
    base = data["base"]
    rate_ts = datetime.fromtimestamp(data["timestamp"], tz=timezone.utc)
    kes_rate = rates.get("KES")
    filename = os.path.basename(filepath)

    with get_cursor() as cur:
        for currency, rate in rates.items():
            cur.execute("""
                INSERT INTO raw.fx_rates
                    (base_currency, target_currency, rate, rate_timestamp, source_file)
                VALUES (%s, %s, %s, %s, %s)
            """, (base, currency, rate, rate_ts, filename))

    logger.info(f"Loaded {len(rates)} FX pairs from {filename}")
    return kes_rate

def load_to_staging_fx(filepath: str):
    with open(filepath) as f:
        data = json.load(f)

    rates = data["rates"]
    rate_ts = datetime.fromtimestamp(data["timestamp"], tz=timezone.utc)
    kes = rates.get("KES")
    eur = rates.get("EUR")
    gbp = rates.get("GBP")

    if not kes:
        logger.warning(f"No KES rate in {filepath}, skipping staging load")
        return

    usd_per_kes = round(1 / kes, 8) if kes else None

    with get_cursor() as cur:
        cur.execute("""
            INSERT INTO staging.stg_fx_rates
                (base_currency, target_currency, rate, usd_per_kes, rate_timestamp)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, ("USD", "KES", kes, usd_per_kes, rate_ts))

    logger.info(f"Staged FX: 1 USD = {kes} KES | 1 KES = {usd_per_kes} USD | ts={rate_ts}")

def run():
    fx_files = sorted(glob.glob("data/raw/fx_rates/fx_ke_*.json"))
    for f in fx_files:
        load_fx_file(f)
        load_to_staging_fx(f)

if __name__ == "__main__":
    run()
