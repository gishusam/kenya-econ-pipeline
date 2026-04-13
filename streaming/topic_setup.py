import os
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from utils.logger import get_logger

logger = get_logger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

def create_topics():
    admin = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        client_id="kenya_econ_admin",
    )

    topics = [
        NewTopic(
            name="fx_rates_ke",
            num_partitions=1,
            replication_factor=1,
        )
    ]

    try:
        admin.create_topics(new_topics=topics)
        logger.info("Topic fx_rates_ke created")
    except TopicAlreadyExistsError:
        logger.info("Topic fx_rates_ke already exists")
    finally:
        admin.close()

if __name__ == "__main__":
    create_topics()