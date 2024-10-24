from confluent_kafka import KafkaError, Consumer, Producer
from sqlalchemy.ext.asyncio import create_async_engine
from datetime import datetime, timedelta
from typing import List
from io import BytesIO
import fastavro
import logging
import asyncio
import struct
import socket
import json
import os
import re

# Get the hostname and process ID
hostname = socket.gethostname()  # Get the machine's hostname

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Kafka Consumer configuration
consumer_conf = {
    'bootstrap.servers': 'node01.kafka-pub.ris.ripe.net:9094,node02.kafka-pub.ris.ripe.net:9094,node03.kafka-pub.ris.ripe.net:9094',
    'group.id': f"bgpdata-{hostname}",
    'partition.assignment.strategy': 'roundrobin',
    'enable.auto.commit': False,  # Disable automatic offset commit, handle manually
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': os.getenv("SASL_KAFKA_USERNAME"),
    'sasl.password': os.getenv("SASL_KAFKA_PASSWORD"),
}

# Kafka Producer configuration
producer_config = {
    'bootstrap.servers': 'openbmp-kafka:29092', # OpenBMP Kafka broker
    'client.id': 'bmp-message-producer',        # Custom client ID for the producer
    'linger.ms': 5,                             # Slight delay to batch messages
    'acks': 'all',                              # Wait for all brokers to acknowledge the message
    'retries': 5                                # Retry sending up to 5 times on failure
}
producer = Producer(producer_config)

avro_schema = {
    "type": "record",
    "name": "RisLiveBinary",
    "namespace": "net.ripe.ris.live",
    "fields": [
        {
          "name": "type",
          "type": {
            "type": "enum",
            "name": "MessageType",
            "symbols": ["STATE", "OPEN", "UPDATE", "NOTIFICATION", "KEEPALIVE"],
          },
        },
        { "name": "timestamp", "type": "long" },
        { "name": "host", "type": "string" },
        { "name": "peer", "type": "bytes" },
        {
          "name": "attributes",
          "type": { "type": "array", "items": "int" },
          "default": [],
        },
        {
          "name": "prefixes",
          "type": { "type": "array", "items": "bytes" },
          "default": [],
        },
        { "name": "path", "type": { "type": "array", "items": "long" }, "default": [] },
        { "name": "ris_live", "type": "string" },
        { "name": "raw", "type": "string" },
    ],
}

class BMPMessageFactory:
    """
    A class with static methods to construct BMP messages from exaBGP JSON data.
    """

    @staticmethod
    def construct_bmp_update_message(peer_ip: str, peer_asn: int, timestamp: float, path: List[int], next_hop: str, prefix: str) -> bytes:
        """
        Constructs a BMP Route Monitoring message for BGP UPDATE messages.
        
        :param peer_ip: IP address of the peer
        :param peer_asn: ASN of the peer
        :param timestamp: Timestamp of the BGP message
        :param path: AS path for the BGP announcement
        :param next_hop: Next-hop IP address
        :param prefix: The IP prefix being announced
        :return: BMP Route Monitoring message in bytes
        """
        # BMP Common Header
        version = 3  # BMP version 3
        msg_type = 0x00  # Route Monitoring message type
        msg_length = 48  # Placeholder for now, adjusted after message construction
        bmp_header = struct.pack('!BIB', version, msg_length, msg_type)

        # BMP Per-Peer Header (48 bytes)
        peer_type = 0  # Assume IPv4, non-RR client
        peer_distinguisher = 0  # Not using peer distinguisher
        peer_ip_bytes = struct.pack('!4s', socket.inet_aton(peer_ip))
        peer_as_bytes = struct.pack('!I', peer_asn)
        peer_bgp_id = socket.inet_aton(peer_ip)  # Use peer IP as BGP ID
        timestamp_sec, timestamp_usec = divmod(int(timestamp * 1_000_000), 1_000_000)

        per_peer_header = struct.pack(
            '!BII4sI4sII',
            peer_type,  # Peer Type
            peer_distinguisher,  # Peer Distinguisher
            peer_ip_bytes,  # Peer Address
            peer_as_bytes,  # Peer AS
            peer_bgp_id,  # Peer BGP ID
            timestamp_sec,  # Timestamp seconds
            timestamp_usec  # Timestamp microseconds
        )

        # BGP Update message
        bgp_update = b''

        # AS_PATH segment
        as_path_segment = struct.pack('!B', 2)  # Type = 2 (AS_SEQUENCE)
        as_path_len = struct.pack('!B', len(path))  # Path length
        as_path_data = b''.join([struct.pack('!I', asn) for asn in path])
        bgp_update += as_path_segment + as_path_len + as_path_data

        # Next-hop
        bgp_update += struct.pack('!4s', socket.inet_aton(next_hop))

        # Prefix (NLRI) announcement
        prefix_parts = prefix.split('/')
        prefix_ip = prefix_parts[0]
        prefix_length = int(prefix_parts[1])
        prefix_bytes = struct.pack('!4sB', socket.inet_aton(prefix_ip), prefix_length)
        bgp_update += prefix_bytes

        # Update the total message length
        total_msg_length = 6 + len(per_peer_header) + len(bgp_update)
        bmp_header = struct.pack('!BIB', version, total_msg_length, msg_type)

        # Construct the final BMP message
        full_bmp_message = bmp_header + per_peer_header + bgp_update
        return full_bmp_message

    @staticmethod
    def construct_bmp_keepalive_message(peer_ip: str, peer_asn: int, timestamp: float) -> bytes:
        """
        Constructs a BMP Keepalive message.
        
        :param peer_ip: IP address of the peer
        :param peer_asn: ASN of the peer
        :param timestamp: Timestamp of the Keepalive message
        :return: BMP Keepalive message in bytes
        """
        # BMP Common Header
        version = 3  # BMP version 3
        msg_type = 0x00  # Route Monitoring message type
        msg_length = 48  # Placeholder for now, adjusted after message construction
        bmp_header = struct.pack('!BIB', version, msg_length, msg_type)

        # BMP Per-Peer Header (48 bytes)
        peer_type = 0  # Assume IPv4, non-RR client
        peer_distinguisher = 0  # Not using peer distinguisher
        peer_ip_bytes = struct.pack('!4s', socket.inet_aton(peer_ip))
        peer_as_bytes = struct.pack('!I', peer_asn)
        peer_bgp_id = socket.inet_aton(peer_ip)  # Use peer IP as BGP ID
        timestamp_sec, timestamp_usec = divmod(int(timestamp * 1_000_000), 1_000_000)

        per_peer_header = struct.pack(
            '!BII4sI4sII',
            peer_type,  # Peer Type
            peer_distinguisher,  # Peer Distinguisher
            peer_ip_bytes,  # Peer Address
            peer_as_bytes,  # Peer AS
            peer_bgp_id,  # Peer BGP ID
            timestamp_sec,  # Timestamp seconds
            timestamp_usec  # Timestamp microseconds
        )

        # BGP Keepalive message is simply 19 bytes (no additional information)
        bgp_keepalive = b'\xFF' * 16 + b'\x00\x13' + b'\x04'  # 16-byte marker, 2-byte length, 1-byte type (KEEPALIVE)

        # Update the total message length
        total_msg_length = 6 + len(per_peer_header) + len(bgp_keepalive)
        bmp_header = struct.pack('!BIB', version, total_msg_length, msg_type)

        # Construct the final BMP message (KEEPALIVE)
        full_bmp_message = bmp_header + per_peer_header + bgp_keepalive
        return full_bmp_message

    @staticmethod
    def construct_bmp_peer_state_message(peer_ip: str, peer_asn: int, timestamp: float, state: str) -> bytes:
        """
        Constructs a BMP Peer Up/Down/Connected message based on the state.
        
        :param peer_ip: IP address of the peer
        :param peer_asn: ASN of the peer
        :param timestamp: Timestamp of the peer state message
        :param state: The state of the peer ("up", "down", "connected")
        :return: BMP Peer State message in bytes
        """
        # BMP Common Header
        version = 3  # BMP version 3
        msg_type = 0x00  # Route Monitoring message type (for now)
        msg_length = 48  # Placeholder for now, adjusted after message construction
        bmp_header = struct.pack('!BIB', version, msg_length, msg_type)

        # BMP Per-Peer Header (48 bytes)
        peer_type = 0  # Assume IPv4, non-RR client
        peer_distinguisher = 0  # Not using peer distinguisher
        peer_ip_bytes = struct.pack('!4s', socket.inet_aton(peer_ip))
        peer_as_bytes = struct.pack('!I', peer_asn)
        peer_bgp_id = socket.inet_aton(peer_ip)  # Use peer IP as BGP ID
        timestamp_sec, timestamp_usec = divmod(int(timestamp * 1_000_000), 1_000_000)

        per_peer_header = struct.pack(
            '!BII4sI4sII',
            peer_type,  # Peer Type
            peer_distinguisher,  # Peer Distinguisher
            peer_ip_bytes,  # Peer Address
            peer_as_bytes,  # Peer AS
            peer_bgp_id,  # Peer BGP ID
            timestamp_sec,  # Timestamp seconds
            timestamp_usec  # Timestamp microseconds
        )

        # Encode state message (use custom encoding for peer state)
        state_bytes = state.encode('utf-8')

        # Construct the BMP Peer State message
        full_bmp_message = bmp_header + per_peer_header + state_bytes
        return full_bmp_message

    @staticmethod
    def exabgp_to_bmp(exabgp_message: str) -> List[bytes]:
        """
        Convert an exaBGP JSON message to a list of BMP messages.
        
        :param exabgp_message: The exaBGP JSON string
        :return: A list of BMP Route Monitoring, Keepalive, or Peer State messages in bytes
        """
        data = json.loads(exabgp_message)

        # Extract relevant fields from exaBGP message
        peer_ip = data['peer']
        peer_asn = int(data['peer_asn'])
        timestamp = data['timestamp']
        msg_type = data['type']

        bmp_messages = []

        # Handle UPDATE messages
        if msg_type == "UPDATE":
            path = data.get('path', [])
            announcements = data.get('announcements', [])
            withdrawals = data.get('withdrawals', [])

            # Process Announcements
            for announcement in announcements:
                next_hop = announcement['next_hop']
                for prefix in announcement['prefixes']:
                    bmp_message = BMPMessageFactory.construct_bmp_update_message(
                        peer_ip=peer_ip,
                        peer_asn=peer_asn,
                        timestamp=timestamp,
                        path=path,
                        next_hop=next_hop,
                        prefix=prefix
                    )
                    bmp_messages.append(bmp_message)

        # Handle KEEPALIVE messages
        elif msg_type == "KEEPALIVE":
            bmp_message = BMPMessageFactory.construct_bmp_keepalive_message(
                peer_ip=peer_ip,
                peer_asn=peer_asn,
                timestamp=timestamp
            )
            bmp_messages.append(bmp_message)

        # Handle RIS_PEER_STATE messages
        elif msg_type == "RIS_PEER_STATE":
            state = data['state']
            bmp_message = BMPMessageFactory.construct_bmp_peer_state_message(
                peer_ip=peer_ip,
                peer_asn=peer_asn,
                timestamp=timestamp,
                state=state
            )
            bmp_messages.append(bmp_message)

        return bmp_messages


# Callback to handle partition rebalancing
def on_rebalance(consumer, partitions):
    """
    Callback function to handle partition rebalancing.

    This function is called when the consumer's partitions are rebalanced. It logs the
    assigned partitions and handles any errors that occur during the rebalancing process.

    Args:
        consumer: The Kafka consumer instance.
        partitions: A list of TopicPartition objects representing the newly assigned partitions.
    """
    try:
        if partitions[0].error:
            logger.error(f"Rebalance error: {partitions[0].error}")
        else:
            logger.info(f"Assigned partitions: {[p.partition for p in partitions]}")
            consumer.assign(partitions)
    except Exception as e:
        logger.error(f"Error handling rebalance: {e}", exc_info=True)

async def log_status(timestamp, time_lag, poll_interval):
    """
    Periodically logs the most recent timestamp, time lag, and current poll interval.
    This coroutine runs concurrently with the main processing loop.
    
    Args:
        timestamp (datetime): The most recent timestamp of the messages.
        time_lag (timedelta): The current time lag of the messages.
        poll_interval (float): The current polling interval in seconds.
    """
    while True:
        await asyncio.sleep(5)  # Sleep for 5 seconds before logging
        
        logger.info(f"Timestamp: {timestamp}, "
                    f"Time lag: {time_lag.total_seconds()} seconds, "
                    f"Poll interval: {poll_interval} seconds")

async def main():
    """
    Main function to consume messages from Kafka, process them, and insert into the database.

    This asynchronous function sets up a Kafka consumer, subscribes to the specified topic,
    and continuously polls for messages. It processes messages in batches, dynamically
    adjusts polling intervals based on message lag, and handles various error scenarios.

    The function performs the following key operations:
    1. Sets up a Kafka consumer with specified configuration and callbacks.
    2. Implements batch processing of messages with a configurable threshold.
    3. Dynamically adjusts polling intervals based on message time lag.
    4. Processes messages, including deserialization and transcoding to BMP.
    5. Inserts processed data into the OpenBMP Kafka topic.
    6. Handles various error scenarios and implements retry logic.

    The function runs indefinitely until interrupted or an unhandled exception occurs.
    """
    # Create Kafka consumer
    consumer = Consumer(consumer_conf)
    consumer.subscribe(['ris-live'])

    # Set up callbacks
    consumer.subscribe(
        ['ris-live'],
        on_assign=on_rebalance,
        on_revoke=lambda c, p: logger.info(f"Revoked partitions: {[part.partition for part in p]}")
    )

    # Initialize settings for batch processing
    BATCH_THRESHOLD = 20000
    CATCHUP_POLL_INTERVAL = 1.0               # Fast poll when behind in time
    NORMAL_POLL_INTERVAL = 5.0                # Slow poll when caught up
    TIME_LAG_THRESHOLD = timedelta(minutes=5) # Consider behind if messages are older than 5 minutes
    FAILURE_RETRY_DELAY = 5                   # Delay in seconds before retrying a failed message

    messages_batch = []
    messages_size = 0

    poll_interval = NORMAL_POLL_INTERVAL  # Initialize with the normal poll interval
    time_lag = timedelta(0)               # Initialize time lag
    timestamp = datetime.now()            # Initialize timestamp

    # Start logging task that is updated within the loop
    logging_task = asyncio.create_task(log_status(timestamp, time_lag, poll_interval))

    try:
        while True:
            msg = consumer.poll(timeout=poll_interval)  # Use dynamically set poll_interval
            
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info(f"End of partition reached: {err}")
                elif msg.error().code() == KafkaError._THROTTLING:
                    logger.warning(f"Kafka throttle event: {err}")
                else:
                    logger.error(f"Kafka error: {err}", exc_info=True)
                continue

            try:
                value = msg.value()

                # Remove the first 5 bytes
                value = value[5:]
                
                # Deserialize the Avro encoded exaBGP message
                parsed = fastavro.schemaless_reader(BytesIO(value), avro_schema)

                # Check if the message is significantly behind the current time
                timestamp = parsed['timestamp']
                time_lag = datetime.now() - datetime.fromtimestamp(timestamp)

                # Convert to BMP messages
                messages = BMPMessageFactory.exabgp_to_bmp(parsed)

                # Adjust polling interval based on how far behind the messages are
                if time_lag > TIME_LAG_THRESHOLD:
                    poll_interval = CATCHUP_POLL_INTERVAL # Faster polling if we're behind
                else:
                    poll_interval = NORMAL_POLL_INTERVAL # Slow down if we're caught up

                # Add messages to the batch
                messages_batch.extend(messages)
                messages_size += len(messages)

                # Process the batch when it reaches the threshold (non-strict)
                if messages_size >= BATCH_THRESHOLD:
                    # Send messages to Kafka
                    for message in messages_batch:
                        producer.send('openbmp.bmp_raw', message)
                    
                    logger.info(f"Ingested {len(messages_batch)} messages.")

                    # Commit the offset after successful processing
                    producer.flush()

                    # Reset the batch
                    messages_batch = []
                    messages_size = 0

            except Exception as e:
                logger.error("Failed to process message, retrying in %d seconds...", FAILURE_RETRY_DELAY, exc_info=True)
                # Wait before retrying the message to avoid overwhelming Kafka
                await asyncio.sleep(FAILURE_RETRY_DELAY)

    except Exception as e:
        logger.error("Fatal error", exc_info=True)
    finally:
        consumer.close()
        # Cancel the logging task when exiting
        logging_task.cancel()

if __name__ == "__main__":
    asyncio.run(main())
