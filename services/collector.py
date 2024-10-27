"""
BGPDATA - BGP Data Collection and Analytics Service

This software is part of the BGPDATA project, which is designed to collect, process, and analyze BGP data from various sources.
It helps researchers and network operators get insights into their network by providing a scalable and reliable way to analyze and inspect historical and live BGP data from RIPE NCC RIS.

Author: Robin Röper

© 2024 BGPDATA. All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
1. Redistributions of source code must retain the above copyright notice, this list of conditions, and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions, and the following disclaimer in the documentation and/or other materials provided with the distribution.
3. Neither the name of BGPDATA nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""
from confluent_kafka import KafkaError, Consumer
from datetime import datetime, timedelta
from protocols.bmp import BMPv3
from typing import List
from io import BytesIO
import fastavro
import logging
import asyncio
import socket
import time
import json
import sys
import os

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

def mrt_to_bmp(data: str) -> List[bytes]:
    """
    Convert an MRT RIB files to a list of BMPv3 (RFC7854) messages.
    (https://www.ripe.net/data-tools/services/mrt-rib-files)

    Args:
        data (str): The MRT RIB file

    Returns:
        List[bytes]: A list of BMP Route Monitoring, Keepalive, or Peer State messages in bytes
    """
    pass

def exabgp_to_bmp(data: dict) -> List[bytes]:
    """
    Convert an exaBGP JSON message to a list of BMPv3 (RFC7854) messages.
    (https://ris-live.ripe.net/)

    Args:
        data (dict): The exaBGP JSON

    Returns:
        List[bytes]: A list of BMP Route Monitoring, Keepalive, or Peer State messages in bytes
    """

    # Extract relevant fields from exaBGP message
    peer_ip = data['peer']
    peer_asn = int(data['peer_asn'])
    timestamp = data['timestamp'] / 1000 # Cast from int to datetime float
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

        # Common attributes
        common_attributes = {
            'origin': origin,
            'as-path': path,
            'community': community
        }

        if 'med' in data:
            common_attributes['med'] = data['med']

        # Process Announcements
        for announcement in announcements:
            next_hop = announcement['next_hop']
            prefixes = announcement['prefixes']
            # Split next_hop into a list of addresses
            next_hop_addresses = [nh.strip() for nh in next_hop.split(',')]

            # Determine the AFI based on the first prefix
            afi = 1  # IPv4
            if ':' in prefixes[0]:
                afi = 2  # IPv6
            safi = 1  # Unicast

            # Build attributes for this announcement
            attributes = common_attributes.copy()
            attributes.update({
                'next-hop': next_hop_addresses,
                'afi': afi,
                'safi': safi,
            })

            # For IPv6, include NLRI in attributes
            if afi == 2:
                # Build NLRI
                nlri = b''
                for prefix in prefixes:
                    nlri += BMPv3.encode_prefix(prefix)
                attributes['nlri'] = nlri
                update_message = {
                    'attribute': attributes,
                }
            else:
                # For IPv4, include NLRI in the update_message
                nlri = b''
                for prefix in prefixes:
                    nlri += BMPv3.encode_prefix(prefix)
                update_message = {
                    'attribute': attributes,
                    'nlri': nlri,
                }

            # Build BMP message
            bmp_message = BMPv3.construct_bmp_route_monitoring_message(
                peer_ip=peer_ip,
                peer_asn=peer_asn,
                timestamp=timestamp,
                update_message=update_message
            )
            bmp_messages.append(bmp_message)

        # Process Withdrawals
        if withdrawals:
            afi = 1  # IPv4
            if ':' in withdrawals[0]:
                afi = 2  # IPv6
            safi = 1  # Unicast

            # Build attributes for withdrawals
            attributes = common_attributes.copy()
            attributes.update({
                'afi': afi,
                'safi': safi,
            })

            if afi == 2:
                # For IPv6, withdrawals are included in MP_UNREACH_NLRI
                nlri = b''
                for prefix in withdrawals:
                    nlri += BMPv3.encode_prefix(prefix)
                attributes['withdrawn_nlri'] = nlri
                update_message = {
                    'attribute': attributes,
                }
            else:
                # For IPv4, withdrawals are in the BGP UPDATE message body
                withdrawn_routes = b''
                for prefix in withdrawals:
                    withdrawn_routes += BMPv3.encode_prefix(prefix)
                update_message = {
                    'attribute': attributes,
                    'withdrawn_routes': withdrawn_routes,
                }

            # Build BMP message
            bmp_message = BMPv3.construct_bmp_route_monitoring_message(
                peer_ip=peer_ip,
                peer_asn=peer_asn,
                timestamp=timestamp,
                update_message=update_message
            )
            bmp_messages.append(bmp_message)

    # Handle KEEPALIVE messages
    elif msg_type == "KEEPALIVE":
        bmp_message = BMPv3.construct_bmp_keepalive_message(
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
            bmp_message = BMPv3.construct_bmp_peer_up_message(
                peer_ip=peer_ip,
                peer_asn=peer_asn,
                timestamp=timestamp
            )
            bmp_messages.append(bmp_message)
        elif state.lower() == 'down':
            # Peer Down message
            bmp_message = BMPv3.construct_bmp_peer_down_message(
                peer_ip=peer_ip,
                peer_asn=peer_asn,
                timestamp=timestamp,
                notification_message={}
            )
            bmp_messages.append(bmp_message)

    return bmp_messages


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

async def log_status(status):
    """
    Periodically logs the most recent timestamp, time lag, current poll interval, and consumption rate.
    This coroutine runs concurrently with the main processing loop.
    
    Args:
        status (dict): A dictionary containing the following keys:
            - timestamp (datetime): The most recent timestamp of the messages.
            - time_lag (timedelta): The current time lag of the messages.
            - poll_interval (float): The current polling interval in seconds.
            - bytes_sent_since_last_log (int): The number of bytes sent since the last log.
    """
    while True:
        seconds = 5
        await asyncio.sleep(seconds)  # Sleep for 5 seconds before logging

        # Compute kbps_counter
        bytes_sent = status['bytes_sent_since_last_log']
        kbps_counter = (bytes_sent * 8) / seconds / 1000  # Convert bytes to kilobits per second

        logger.info(f"At time: {status['timestamp']}, "
                    f"Time lag: {status['time_lag'].total_seconds()} seconds, "
                    f"Poll interval: {status['poll_interval']} seconds, "
                    f"Transmitting at ~{kbps_counter:.2f} kbit/s")

        # Reset bytes_sent_since_last_log
        status['bytes_sent_since_last_log'] = 0

async def main():
    """
    Main function to consume messages from RIS Raw Data Dumps and RIS Live Kafka, process them, and insert them into OpenBMP.

    This asynchronous function sets up a Kafka consumer, subscribes to the specified topic,
    and continuously polls for messages. It processes messages in batches, dynamically
    adjusts polling intervals based on message lag, and handles various error scenarios.

    The function performs the following key operations:
    1. Sets up a Kafka consumer with specified configuration and callbacks.
    2. Establishes a persistent TCP connection to the OpenBMP collector.
    3. May perform initial catchup by Route Injecting MRT records from RIS Raw Data Dumps.
    4. Dynamically adjusts RIS Live Kafka polling intervals based on message time lag.
    5. Processes messages, including deserialization to exaBGP JSON and translation to BMPv3 (RFC7854).
    6. Inserts processed BMP messages into the OpenBMP Kafka topic.
    7. Handles various error scenarios and implements retry logic.

    The function runs indefinitely until interrupted or an unhandled exception occurs.
    """
    # Create Kafka consumer
    consumer = Consumer(consumer_conf)
    consumer.subscribe(['ris-live'])

    # Create OpenBMP Socket with retry until timeout
    timeout = int(os.getenv('OPENBMP_COLLECTOR_TIMEOUT', 30))  # Default to 30 seconds if not set
    host = os.getenv('OPENBMP_COLLECTOR_HOST')
    port = int(os.getenv('OPENBMP_COLLECTOR_PORT'))

    start_time = time.time()

    while True:
        try:
            # Create a non-blocking socket
            reader, writer = await asyncio.open_connection(host, port)
            logger.info(f"Connected to OpenBMP collector at {host}:{port}")
            break  # Success
        except (OSError, socket.error) as e:
            elapsed_time = time.time() - start_time
            if elapsed_time >= timeout:
                logger.error(f"Unable to connect to OpenBMP collector within {timeout} seconds")
                raise Exception(f"Unable to connect to OpenBMP collector within {timeout} seconds") from e
            else:
                logger.warning(f"Connection failed, retrying in 10 seconds... ({elapsed_time:.1f}s elapsed)")
                await asyncio.sleep(10)  # Wait before retrying
        except Exception as e:
            # Handle other exceptions
            logger.error("An unexpected error occurred while trying to connect to OpenBMP collector", exc_info=True)
            raise

    # Set up callbacks
    consumer.subscribe(
        ['ris-live'],
        on_assign=on_rebalance,
        on_revoke=lambda c, p: logger.info(f"Revoked partitions: {[part.partition for part in p]}")
    )

    # Initialize settings
    CATCHUP_POLL_INTERVAL = 1.0               # Fast poll when behind in time
    NORMAL_POLL_INTERVAL = 5.0                # Slow poll when caught up
    TIME_LAG_THRESHOLD = timedelta(minutes=5) # Consider behind if messages are older than 5 minutes
    FAILURE_RETRY_DELAY = 5                   # Delay in seconds before retrying a failed message

    # Rate limiting variables
    MESSAGES_PER_SECOND = 100                 # Messages per second (increase if needed)
    last_send_time = time.time()
    messages_sent = 0

    # Initialize status dictionary to share variables between main and log_status
    status = {
        'timestamp': datetime.now(),           # Initialize timestamp
        'time_lag': timedelta(0),              # Initialize time lag
        'poll_interval': NORMAL_POLL_INTERVAL, # Initialize with the normal poll interval
        'bytes_sent_since_last_log': 0,        # Initialize bytes sent counter
    }

    # Start logging task that is updated within the loop
    logging_task = asyncio.create_task(log_status(status))

    try:
        while True:
            msg = consumer.poll(timeout=status['poll_interval'])  # Use dynamically set poll_interval
            
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
                # Adjust polling interval based on how far behind the messages are
                if status['time_lag'] > TIME_LAG_THRESHOLD:
                    status['poll_interval'] = CATCHUP_POLL_INTERVAL # Faster polling if we're behind
                else:
                    status['poll_interval'] = NORMAL_POLL_INTERVAL # Slow down if we're caught up

                
                # We've not reached the batch threshold, process the message.
                value = msg.value()

                # Remove the first 5 bytes
                value = value[5:]
                
                # Deserialize the Avro encoded exaBGP message
                parsed = fastavro.schemaless_reader(BytesIO(value), avro_schema)

                # Check if the message is significantly behind the current time
                status['timestamp'] = datetime.fromtimestamp(parsed['timestamp'] / 1000)
                status['time_lag'] = datetime.now() - status['timestamp']

                # Convert to BMP messages
                messages = exabgp_to_bmp(json.loads(parsed['ris_live']))
                
                # Send each BMP message individually over the persistent TCP connection
                for message in messages:
                    try:
                        # Send the message over the persistent TCP connection
                        writer.write(message)
                        await writer.drain()
                        # Update the bytes sent counter
                        status['bytes_sent_since_last_log'] += len(message)
                        messages_sent += 1

                        # Check if rate limit is exceeded
                        if messages_sent >= MESSAGES_PER_SECOND:
                            # Sleep for the remainder of the second
                            current_time = time.time()
                            elapsed_time = current_time - last_send_time
                            sleep_time = 1.0 - elapsed_time
                            if sleep_time > 0:
                                await asyncio.sleep(sleep_time)
                            messages_sent = 0
                            last_send_time = time.time()
                    except Exception as e:
                        logger.error("Error occurred while sending data over the socket.", exc_info=True)
                        # Wait 10 seconds before retrying
                        await asyncio.sleep(10)
                        # Exit the service
                        sys.exit("Service terminated due to broken TCP connection, exiting 1.")

                # Commit the offset after successful processing and transmission
                consumer.commit()

            except Exception as e:
                logger.error("Failed to process message, retrying in %d seconds...", FAILURE_RETRY_DELAY, exc_info=True)
                # Wait before retrying the message to avoid overwhelming Kafka
                await asyncio.sleep(FAILURE_RETRY_DELAY)

    except Exception as e:
        logger.error("Fatal error", exc_info=True)
    finally:
        await consumer.stop()
        writer.close()
        await writer.wait_closed()
        # Cancel the logging task when exiting
        logging_task.cancel()
        try:
            await logging_task
        except asyncio.CancelledError:
            pass
