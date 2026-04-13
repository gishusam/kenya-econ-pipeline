import json
import os
from datetime import datetime, timezone
from kafka import KafkaConsumer
from dotenv import load_dotenv
from db.connection import get_cursor
from utils.logger import get_logger

load_dotenv()
logger = get_logger(__name__)

KAFKA_TOPIC = "fx_rates_ke"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_GROUP = "fx_postgres_writer"

def create_consumer():
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

def write_to_postgres(message: dict):
    if message.get("target_currency") != "KES":
        return

    rate = message["rate"]
    usd_per_kes = round(1 / rate, 8) if rate else None
    event_ts = datetime.fromtimestamp(
        message["event_timestamp"], tz=timezone.utc
    )

    with get_cursor() as cur:
        cur.execute("""
            INSERT INTO staging.stg_fx_rates
                (base_currency, target_currency, rate,
                 usd_per_kes, rate_timestamp)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (base_currency, target_currency, rate_timestamp)
            DO NOTHING
        """, (
            message["base_currency"],
            message["target_currency"],
            rate,
            usd_per_kes,
            event_ts,
        ))

    logger.info(
        f"Written: {message['base_currency']}/{message['target_currency']} "
        f"= {rate} at {event_ts}"
    )

def run():
    logger.info(f"Starting FX consumer — group: {KAFKA_GROUP}")
    consumer = create_consumer()
    try:
        for message in consumer:
            try:
                write_to_postgres(message.value)
            except Exception as e:
                logger.error(f"Write error: {e}")
    except KeyboardInterrupt:
        logger.info("Consumer stopped")
    finally:
        consumer.close()

if __name__ == "__main__":
    run()