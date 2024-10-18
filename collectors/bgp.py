import os
import json
import logging
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
from pybgpstream import BGPStream, BGPRecord

logger = logging.getLogger(__name__)

def kafka_message_key(message):
    """Generate a unique key for each Kafka message based on relevant BGP fields."""
    return f"{message['prefix']}-{message['peer_as']}-{message['timestamp']}"

def bgpstream_format(collector, elem):
    """Format BGPStream elements into a dict format for Kafka."""
    if elem.type == "R" or elem.type == "A":
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
    elif elem.type == "W":
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
    rec = BGPRecord()
    while stream.get_next_record(rec):
        elem = rec.get_next_elem()
        while elem:
            for message in bgpstream_format(collector, elem):
                yield message
            elem = rec.get_next_elem()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Fetch configuration from environment variables
    kafka_servers = os.getenv("KAFKA_SERVERS", "localhost:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "bgp-updates")
    collector = os.getenv("BGP_COLLECTOR", "ris-live")

    # Initialize Kafka producer
    producer = KafkaProducer(bootstrap_servers=kafka_servers.split(","))

    # Set up BGPStream for real-time streaming
    stream = BGPStream()

    # Filter by the desired project (e.g., RIS Live)
    stream.add_filter('project', collector)

    # Stream only real-time updates (no past RIBs, just current and future updates)
    stream.add_filter('record-type', 'updates')

    # Start the stream for real-time updates
    stream.start()

    count = 0
    for batch in iterate_stream(stream, collector):
        produce_to_kafka(producer, kafka_topic, [batch])

        # Log progress
        count += 1
        logger.info(f"Processed {count} messages")
