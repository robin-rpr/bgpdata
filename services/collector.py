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
import zlib
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

import json
import struct
import socket
from typing import List

class BMPConverter:
    """
    A class to convert exaBGP JSON messages into BMP (RFC7854) messages.
    https://datatracker.ietf.org/doc/html/rfc7854
    """
    def __init__(self):
        """
        Initialize the BMPConverter class.

        This constructor sets up the necessary configurations and state for the BMPConverter class.
        It prepares the class to convert exaBGP JSON messages into BMP messages, which can be used
        for monitoring and managing BGP sessions.

        The BMPConverter class provides methods to build various BGP and BMP messages, including
        KEEPALIVE, NOTIFICATION, UPDATE, and Peer Up/Down Notification messages. It also includes
        utility functions to encode prefixes and path attributes as per BGP specifications.

        Attributes:
            None

        Methods:
            exabgp_to_bmp(exabgp_message: str) -> List[bytes]:
                Convert an exaBGP JSON message to a list of BMP messages.
            build_bgp_keepalive_message() -> bytes:
                Build the BGP KEEPALIVE message in bytes.
            build_bgp_notification_message(notification_message: dict) -> bytes:
                Build the BGP NOTIFICATION message in bytes.
            build_bgp_update_message(update_message: dict) -> bytes:
                Build the BGP UPDATE message in bytes.
            build_bmp_per_peer_header(peer_ip: str, peer_asn: int, timestamp: float) -> bytes:
                Build the BMP Per-Peer Header.
            construct_bmp_peer_up_message(peer_ip: str, peer_asn: int, timestamp: float) -> bytes:
                Construct a BMP Peer Up Notification message.
            construct_bmp_peer_down_message(peer_ip: str, peer_asn: int, timestamp: float, notification_message: dict) -> bytes:
                Construct a BMP Peer Down Notification message.
            encode_prefix(prefix: str) -> bytes:
                Encode a prefix into bytes as per BGP specification.
        """
    def exabgp_to_bmp(self, exabgp_message: str) -> List[bytes]:
        """
        Convert an exaBGP JSON message to a list of BMP messages.

        Args:
            exabgp_message (str): The exaBGP JSON string

        Returns:
            List[bytes]: A list of BMP Route Monitoring, Keepalive, or Peer State messages in bytes
        """
        data = json.loads(exabgp_message)

        # Extract relevant fields from exaBGP message
        peer_ip = data['peer']
        peer_asn = int(data['peer_asn'])
        timestamp = data['timestamp']
        msg_type = data['type'].upper()

        bmp_messages = []

        # Handle UPDATE messages
        if msg_type == "UPDATE":
            # Extract path attributes
            path = data.get('path', [])
            origin = data.get('origin', 'IGP').lower()
            community = data.get('community', [])
            announcements = data.get('announcements', [])
            withdrawals = data.get('withdrawals', [])

            attributes = {
                'origin': origin,
                'as-path': path,
                'community': community
            }

            # Process Announcements
            if announcements:
                update_message = {
                    'attribute': attributes,
                    'announce': {}
                }
                for announcement in announcements:
                    next_hop = announcement['next_hop']
                    prefixes = announcement['prefixes']
                    attributes['next-hop'] = next_hop
                    afi_safi = 'ipv4 unicast' if ':' not in prefixes[0] else 'ipv6 unicast'
                    update_message['announce'][afi_safi] = {}
                    for prefix in prefixes:
                        update_message['announce'][afi_safi][prefix] = {}
                # Build the BGP UPDATE message
                bmp_message = self.construct_bmp_route_monitoring_message(
                    peer_ip=peer_ip,
                    peer_asn=peer_asn,
                    timestamp=timestamp,
                    update_message=update_message
                )
                bmp_messages.append(bmp_message)

            # Process Withdrawals
            if withdrawals:
                update_message = {
                    'attribute': attributes,
                    'withdraw': {}
                }
                afi_safi = 'ipv4 unicast' if ':' not in withdrawals[0] else 'ipv6 unicast'
                update_message['withdraw'][afi_safi] = withdrawals
                # Build the BGP UPDATE message with withdrawals
                bmp_message = self.construct_bmp_route_monitoring_message(
                    peer_ip=peer_ip,
                    peer_asn=peer_asn,
                    timestamp=timestamp,
                    update_message=update_message
                )
                bmp_messages.append(bmp_message)

        # Handle KEEPALIVE messages
        elif msg_type == "KEEPALIVE":
            bmp_message = self.construct_bmp_keepalive_message(
                peer_ip=peer_ip,
                peer_asn=peer_asn,
                timestamp=timestamp
            )
            bmp_messages.append(bmp_message)

        # Handle RIS_PEER_STATE messages
        elif msg_type == "RIS_PEER_STATE":
            state = data['state']
            if state.lower() == 'connected':
                # Peer Up message
                bmp_message = self.construct_bmp_peer_up_message(
                    peer_ip=peer_ip,
                    peer_asn=peer_asn,
                    timestamp=timestamp
                )
                bmp_messages.append(bmp_message)
            elif state.lower() == 'down':
                # Peer Down message
                bmp_message = self.construct_bmp_peer_down_message(
                    peer_ip=peer_ip,
                    peer_asn=peer_asn,
                    timestamp=timestamp,
                    notification_message={}
                )
                bmp_messages.append(bmp_message)

        return bmp_messages

    def construct_bmp_route_monitoring_message(self, peer_ip, peer_asn, timestamp, update_message):
        """
        Construct a BMP Route Monitoring message containing a BGP UPDATE message.

        Args:
            peer_ip (str): The peer IP address
            peer_asn (int): The peer AS number
            timestamp (float): The timestamp
            update_message (dict): The BGP UPDATE message in dictionary form

        Returns:
            bytes: The BMP message in bytes
        """
        # Build the BGP UPDATE message
        bgp_update = self.build_bgp_update_message(update_message)

        # Build the BMP Common Header
        bmp_msg_type = 0  # Route Monitoring
        per_peer_header = self.build_bmp_per_peer_header(peer_ip, peer_asn, timestamp)

        total_length = 6 + len(per_peer_header) + len(bgp_update)
        bmp_common_header = struct.pack('!BIB', 3, total_length, bmp_msg_type)

        bmp_message = bmp_common_header + per_peer_header + bgp_update

        return bmp_message

    def construct_bmp_keepalive_message(self, peer_ip, peer_asn, timestamp):
        """
        Construct a BMP Route Monitoring message containing a BGP KEEPALIVE message.

        Args:
            peer_ip (str): The peer IP address.
            peer_asn (int): The peer AS number.
            timestamp (float): The timestamp.

        Returns:
            bytes: The BMP message in bytes.
        """
        # Build the BGP KEEPALIVE message
        bgp_keepalive = self.build_bgp_keepalive_message()

        # Build the BMP Common Header
        bmp_msg_type = 0  # Route Monitoring
        per_peer_header = self.build_bmp_per_peer_header(peer_ip, peer_asn, timestamp)

        total_length = 6 + len(per_peer_header) + len(bgp_keepalive)
        bmp_common_header = struct.pack('!BIB', 3, total_length, bmp_msg_type)

        bmp_message = bmp_common_header + per_peer_header + bgp_keepalive

        return bmp_message

    def construct_bmp_peer_up_message(self, peer_ip, peer_asn, timestamp):
        """
        Construct a BMP Peer Up Notification message.

        Args:
            peer_ip (str): The peer IP address.
            peer_asn (int): The peer AS number.
            timestamp (float): The timestamp.

        Returns:
            bytes: The BMP message in bytes.
        """
        # For simplicity, we will not include all optional fields
        bmp_msg_type = 3  # Peer Up Notification
        per_peer_header = self.build_bmp_per_peer_header(peer_ip, peer_asn, timestamp)

        # Local Address (16 bytes), Local Port (2 bytes), Remote Port (2 bytes), Sent OPEN Message, Received OPEN Message
        # For simplicity, we'll use placeholders
        local_address = b'\x00' * 16
        local_port = struct.pack('!H', 0)
        remote_port = struct.pack('!H', 179)
        sent_open_message = b''
        received_open_message = b''

        peer_up_msg = local_address + local_port + remote_port + sent_open_message + received_open_message

        total_length = 6 + len(per_peer_header) + len(peer_up_msg)
        bmp_common_header = struct.pack('!BIB', 3, total_length, bmp_msg_type)

        bmp_message = bmp_common_header + per_peer_header + peer_up_msg

        return bmp_message

    def construct_bmp_peer_down_message(self, peer_ip, peer_asn, timestamp, notification_message):
        """
        Construct a BMP Peer Down Notification message.

        Args:
            peer_ip (str): The peer IP address.
            peer_asn (int): The peer AS number.
            timestamp (float): The timestamp.
            notification_message (dict): The BGP Notification message in dictionary form.

        Returns:
            bytes: The BMP message in bytes.
        """
        # Build the BGP Notification message
        bgp_notification = self.build_bgp_notification_message(notification_message)

        # Build the BMP Common Header
        bmp_msg_type = 2  # Peer Down Notification
        per_peer_header = self.build_bmp_per_peer_header(peer_ip, peer_asn, timestamp)

        # Reason: 1-byte code indicating the reason. For simplicity, use 1 (Local system closed the session)
        reason = struct.pack('!B', 1)  # Reason Code 1

        total_length = 6 + len(per_peer_header) + len(reason) + len(bgp_notification)
        bmp_common_header = struct.pack('!BIB', 3, total_length, bmp_msg_type)

        bmp_message = bmp_common_header + per_peer_header + reason + bgp_notification

        return bmp_message

    def build_bgp_update_message(self, update_message):
        """
        Build the BGP UPDATE message in bytes.

        Args:
            update_message (dict): The update message dictionary

        Returns:
            bytes: The BGP UPDATE message in bytes
        """
        # Initialize components
        withdrawn_routes = b''
        withdrawn_routes_length = 0
        total_path_attribute_length = 0
        path_attributes = b''
        nlri = b''

        # Process 'withdraw'
        if 'withdraw' in update_message:
            # Withdrawn Routes
            withdraw = update_message['withdraw']
            for afi_safi in withdraw:
                prefixes = withdraw[afi_safi]
                for prefix in prefixes:
                    prefix_bytes = self.encode_prefix(prefix)
                    withdrawn_routes += prefix_bytes

            withdrawn_routes_length = len(withdrawn_routes)

        # Process 'attribute'
        if 'attribute' in update_message:
            # Path Attributes
            attributes = update_message['attribute']
            path_attributes = self.encode_path_attributes(attributes)
            total_path_attribute_length = len(path_attributes)

        # Process 'announce'
        if 'announce' in update_message:
            # NLRI
            announce = update_message['announce']
            for afi_safi in announce:
                prefixes_dict = announce[afi_safi]
                for prefix in prefixes_dict:
                    prefix_bytes = self.encode_prefix(prefix)
                    nlri += prefix_bytes

        # Build the UPDATE message
        # Withdrawn Routes Length (2 bytes)
        bgp_update = struct.pack('!H', withdrawn_routes_length)
        bgp_update += withdrawn_routes
        # Total Path Attribute Length (2 bytes)
        bgp_update += struct.pack('!H', total_path_attribute_length)
        bgp_update += path_attributes
        # NLRI
        bgp_update += nlri

        # Now build the BGP Message Header
        # Marker: 16 bytes of 0xFF
        marker = b'\xFF' * 16
        length = 19 + len(bgp_update)
        msg_type = 2  # UPDATE message

        bgp_message = marker + struct.pack('!HB', length, msg_type) + bgp_update

        return bgp_message

    def encode_prefix(self, prefix):
        """
        Encode a prefix into bytes as per BGP specification.

        Args:
            prefix (str): The prefix string, e.g., '192.0.2.0/24'

        Returns:
            bytes: The encoded prefix in bytes
        """
        # Split prefix and prefix length
        ip, prefix_length = prefix.split('/')
        prefix_length = int(prefix_length)
        if ':' in ip:
            # IPv6
            ip_bytes = socket.inet_pton(socket.AF_INET6, ip)
        else:
            # IPv4
            ip_bytes = socket.inet_pton(socket.AF_INET, ip)

        # Calculate the number of octets required to represent the prefix
        num_octets = (prefix_length + 7) // 8
        # Truncate the ip_bytes to num_octets
        ip_bytes = ip_bytes[:num_octets]
        # Build the prefix in bytes
        prefix_bytes = struct.pack('!B', prefix_length) + ip_bytes
        return prefix_bytes

    def encode_path_attributes(self, attributes):
        """
        Encode path attributes into bytes as per BGP specification.

        Args:
            attributes (dict): Dictionary of path attributes

        Returns:
            bytes: The encoded path attributes in bytes
        """
        path_attributes = b''

        # Origin
        if 'origin' in attributes:
            origin = attributes['origin']
            # Origin is 1 byte: 0=IGP, 1=EGP, 2=INCOMPLETE
            origin_value = {'igp': 0, 'egp': 1, 'incomplete': 2}.get(origin.lower(), 2)
            # Attribute Flags: Optional (0), Transitive (1), Partial (0), Extended Length (0)
            attr_flags = 0x40  # Transitive
            attr_type = 1
            attr_length = 1  # 1 byte
            attr_value = struct.pack('!B', origin_value)
            path_attributes += struct.pack('!BBB', attr_flags, attr_type, attr_length) + attr_value

        # AS_PATH
        if 'as-path' in attributes:
            as_path = attributes['as-path']
            # AS_PATH is a sequence of AS_PATH segments
            # For simplicity, assume only AS_SEQUENCE
            attr_flags = 0x40  # Transitive
            attr_type = 2
            as_numbers = as_path  # list of AS numbers
            segment_type = 2  # AS_SEQUENCE
            segment_length = len(as_numbers)
            segment_value = b''
            for asn in as_numbers:
                segment_value += struct.pack('!I', int(asn))
            attr_value = struct.pack('!BB', segment_type, segment_length) + segment_value
            attr_length = len(attr_value)
            if attr_length > 255:
                # Extended Length
                attr_flags |= 0x10  # Set Extended Length flag
                path_attributes += struct.pack('!BBH', attr_flags, attr_type, attr_length)
            else:
                path_attributes += struct.pack('!BBB', attr_flags, attr_type, attr_length)
            path_attributes += attr_value

        # NEXT_HOP
        if 'next-hop' in attributes:
            next_hop = attributes['next-hop']
            if ':' in next_hop:
                logger.info(f"IPv6 NEXT_HOP: {next_hop}")
                next_hop_bytes = socket.inet_pton(socket.AF_INET6, next_hop)
                attr_flags = 0x80  # Optional
                attr_type = 14  # MP_REACH_NLRI
                afi = 2  # IPv6
                safi = 1  # Unicast
                # Build MP_REACH_NLRI attribute
                mp_reach_value = struct.pack('!HBB', afi, safi, len(next_hop_bytes)) + next_hop_bytes + b'\x00'
                attr_length = len(mp_reach_value)
                if attr_length > 255:
                    attr_flags |= 0x10  # Extended Length
                    path_attributes += struct.pack('!BBH', attr_flags, attr_type, attr_length)
                else:
                    path_attributes += struct.pack('!BBB', attr_flags, attr_type, attr_length)
                path_attributes += mp_reach_value
            else:
                next_hop_bytes = socket.inet_aton(next_hop)
                attr_flags = 0x40  # Transitive
                attr_type = 3
                attr_length = 4  # IPv4 address
                attr_value = next_hop_bytes
                path_attributes += struct.pack('!BBB', attr_flags, attr_type, attr_length) + attr_value

        # COMMUNITY
        if 'community' in attributes:
            community = attributes['community']
            if community:
                attr_flags = 0xC0  # Optional and Transitive
                attr_type = 8
                community_value = b''
                for comm in community:
                    asn, value = comm
                    community_value += struct.pack('!HH', int(asn), int(value))
                attr_length = len(community_value)
                if attr_length > 255:
                    attr_flags |= 0x10  # Extended Length
                    path_attributes += struct.pack('!BBH', attr_flags, attr_type, attr_length)
                else:
                    path_attributes += struct.pack('!BBB', attr_flags, attr_type, attr_length)
                path_attributes += community_value

        # Additional attributes can be added similarly

        return path_attributes

    def build_bgp_keepalive_message(self):
        """
        Build the BGP KEEPALIVE message.

        Args:
            None

        Returns:
            bytes: The BGP KEEPALIVE message in bytes.
        """
        # Marker: 16 bytes of 0xFF
        marker = b'\xFF' * 16
        length = 19  # Header only
        msg_type = 4  # KEEPALIVE message
        bgp_message = marker + struct.pack('!HB', length, msg_type)
        return bgp_message

    def build_bgp_notification_message(self, notification_message):
        """
        Build the BGP NOTIFICATION message in bytes.

        Args:
            notification_message (dict): The notification message dictionary.

        Returns:
            bytes: The BGP NOTIFICATION message in bytes.
        """
        # Extract error code and subcode
        error_code = int(notification_message.get('code', 0))
        error_subcode = int(notification_message.get('subcode', 0))
        data = notification_message.get('data', b'')

        # Build the NOTIFICATION message
        notification = struct.pack('!BB', error_code, error_subcode) + data

        # Now build the BGP Message Header
        # Marker: 16 bytes of 0xFF
        marker = b'\xFF' * 16
        length = 19 + len(notification)
        msg_type = 3  # NOTIFICATION message

        bgp_message = marker + struct.pack('!HB', length, msg_type) + notification

        return bgp_message

    def build_bmp_per_peer_header(self, peer_ip, peer_asn, timestamp):
        """
        Build the BMP Per-Peer Header.

        Args:
            peer_ip (str): The peer IP address.
            peer_asn (int): The peer AS number.
            timestamp (float): The timestamp.

        Returns:
            bytes: The Per-Peer Header in bytes.
        """
        peer_type = 0  # Global Instance Peer
        peer_flags = 0
        # Peer Distinguisher (8 bytes): set to zero for Global Instance Peer
        peer_distinguisher = b'\x00' * 8
        # Peer Address (16 bytes): IPv4 mapped into IPv6
        if ':' in peer_ip:
            # IPv6 address
            peer_address = socket.inet_pton(socket.AF_INET6, peer_ip)
        else:
            # IPv4 address
            peer_address = b'\x00' * 12 + socket.inet_pton(socket.AF_INET, peer_ip)

        peer_as = int(peer_asn)
        # For Peer BGP ID, we'll use zeros (could be improved)
        peer_bgp_id = b'\x00' * 4

        ts_seconds = int(timestamp)
        ts_microseconds = int((timestamp - ts_seconds) * 1e6)

        per_peer_header = struct.pack('!BB8s16sI4sII',
                                      peer_type,
                                      peer_flags,
                                      peer_distinguisher,
                                      peer_address,
                                      peer_as,
                                      peer_bgp_id,
                                      ts_seconds,
                                      ts_microseconds)
        return per_peer_header


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
    Main function to consume messages from Kafka, process them, and insert into OpenBMP.

    This asynchronous function sets up a Kafka consumer, subscribes to the specified topic,
    and continuously polls for messages. It processes messages in batches, dynamically
    adjusts polling intervals based on message lag, and handles various error scenarios.

    The function performs the following key operations:
    1. Sets up a Kafka consumer with specified configuration and callbacks.
    2. Implements batch processing of messages with a configurable threshold.
    3. Dynamically adjusts polling intervals based on message time lag.
    4. Processes messages, including deserialization to exaBGP JSON and conversion to BMP (RFC7854).
    5. Inserts processed BMP messages into the OpenBMP Kafka topic.
    6. Handles various error scenarios and implements retry logic.

    The function runs indefinitely until interrupted or an unhandled exception occurs.
    """
    # Create Kafka consumer
    consumer = Consumer(consumer_conf)
    consumer.subscribe(['ris-live'])

    # Initialize the converter
    converter = BMPConverter()

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
                timestamp = parsed['timestamp'] / 100
                time_lag = datetime.now() - datetime.fromtimestamp(timestamp)

                # Convert to BMP messages
                messages = converter.exabgp_to_bmp(parsed['ris_live'])

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
