import os
import json
import logging
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
from pybgpstream import BGPStream

logger = logging.getLogger(__name__)

def kafka_message_key(message):
    """Generate a unique key for each Kafka message based on relevant BGP fields."""
    return f"{message['prefix']}-{message['peer_as']}-{message['timestamp']}"

def bgpstream_format(collector, elem):
    """Format BGPStream elements into a dict format for Kafka."""
    if elem.type == "R" or elem.type == "A":  # 'R' is for Route, 'A' is for Announcement
        as_path = elem.fields["as-path"]
        if len(as_path) > 0:
            origins = elem.fields["as-path"].split()
            typ = "F" if elem.type == "R" else "U"
            yield {
                "type": typ,
                "timestamp": elem.time,
                "collector": collector,
                "peer_as": elem.peer_asn,
                "peer_ip": elem.peer_address,
                "prefix": elem.fields["prefix"],
                "origins": origins,
                "as_path": as_path
            }
    elif elem.type == "W":  # 'W' is for Withdrawal
        yield {
            "type": "W",
            "timestamp": elem.time,
            "collector": collector,
            "peer_as": elem.peer_asn,
            "peer_ip": elem.peer_address,
            "prefix": elem.fields["prefix"],
            "origins": None,
            "as_path": None
        }

def produce_to_kafka(producer, topic, messages):
    """Send messages to Kafka with deduplication based on message keys."""
    for message in messages:
        key = kafka_message_key(message).encode('utf-8')
        value = json.dumps(message).encode('utf-8')
        try:
            producer.send(topic, key=key, value=value)
        except KafkaError as e:
            logger.error(f"Failed to send message to Kafka: {e}")
    producer.flush()

def iterate_stream(stream, collector):
    """Iterate over the BGPStream and yield formatted messages."""
    for elem in stream:  # Directly iterate over the stream
        for message in bgpstream_format(collector, elem):
            yield message

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Fetch configuration from environment variables
    kafka_servers = os.getenv("KAFKA_SERVERS", "localhost:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "bgp-updates")
    collector = os.getenv("BGP_COLLECTOR", "ris-live")

    # Initialize Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=kafka_servers.split(","),
        acks='all',  # Wait for all replicas to acknowledge
        retries=5,  # Retry on failure
    )

    # Set up BGPStream for real-time streaming without filters
    stream = BGPStream(
        project=collector  # Specify project (e.g., "ris-live")
    )

    # Process the stream and produce messages to Kafka
    count = 0
    for batch in iterate_stream(stream, collector):
        produce_to_kafka(producer, kafka_topic, [batch])

        # Log progress every 5000 messages
        count += 1
        if count % 5000 == 0:
            logger.info(f"Processed {count} messages")
