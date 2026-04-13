import json
import os
import time
from datetime import datetime, timezone
from kafka import KafkaProducer
from dotenv import load_dotenv
from ingestion.fx_rates import FXRatesIngester
from utils.logger import get_logger

load_dotenv()
logger = get_logger(__name__)

KAFKA_TOPIC = "fx_rates_ke"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
POLL_INTERVAL = int(os.getenv("FX_POLL_INTERVAL_SECONDS", 60))

def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
    )

def fetch_and_publish(producer: KafkaProducer):
    ingester = FXRatesIngester()
    raw = ingester.fetch()

    rates = raw.get("rates", {})
    ts = raw.get("timestamp", int(datetime.now(timezone.utc).timestamp()))

    for currency, rate in rates.items():
        message = {
            "base_currency": raw["base"],
            "target_currency": currency,
            "rate": rate,
            "event_timestamp": ts,
            "published_at": datetime.now(timezone.utc).isoformat(),
        }
        producer.send(KAFKA_TOPIC, value=message)
        logger.info(f"Published: {raw['base']}/{currency} = {rate}")

    producer.flush()
    logger.info(f"Flushed {len(rates)} messages to topic {KAFKA_TOPIC}")

def run():
    logger.info(f"Starting FX producer — polling every {POLL_INTERVAL}s")
    producer = create_producer()
    try:
        while True:
            try:
                fetch_and_publish(producer)
            except Exception as e:
                logger.error(f"Fetch/publish error: {e}")
            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        logger.info("Producer stopped")
    finally:
        producer.close()

if __name__ == "__main__":
    run()