"""
BGPDATA - BGP Data Collection and Analytics Service

This software is part of the BGPDATA project, which is designed to collect, process, and analyze BGP data from various sources.
It helps researchers and network operators get insights into their network by providing a scalable and reliable way to analyze and inspect historical and live BGP data from Route Collectors around the world.

Author: Robin Röper

© 2024 BGPDATA. All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
1. Redistributions of source code must retain the above copyright notice, this list of conditions, and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions, and the following disclaimer in the documentation and/or other materials provided with the distribution.
3. Neither the name of BGPDATA nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""
from confluent_kafka import KafkaError, Consumer, TopicPartition
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from protocols.bmp import BMPv3
from bs4 import BeautifulSoup
import queue as queueio
from io import BytesIO
import threading
import rocksdbpy
import fastavro
import requests
import logging
import asyncio
import bgpkit
import signal
import select
import socket
import struct
import time
import json
import os

# Get the hostname and process ID
hostname = socket.gethostname()  # Get the machine's hostname

# Configure logging
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Get list of collectors
routeviews_collectors = [tuple(collector.strip().split(':')) for collector in (
    os.getenv('ROUTEVIEWS_COLLECTORS') or '').split(',') if collector]
ris_collectors = [collector.strip() for collector in (
    os.getenv('RIS_COLLECTORS') or '').split(',') if collector]
openbmp_collectors = [tuple(collector.split(':')) for collector in (
    os.getenv('OPENBMP_COLLECTORS') or '').split(',') if collector]

# Route Views Kafka Consumer configuration
routeviews_consumer_conf = {
    'bootstrap.servers': 'stream.routeviews.org:9092',
    'group.id': f"bgpdata-{hostname}",
    'partition.assignment.strategy': 'roundrobin',
    'enable.auto.commit': False,
    'auto.offset.reset': 'earliest',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': os.getenv('ROUTEVIEWS_USERNAME'),
    'sasl.password': os.getenv('ROUTEVIEWS_PASSWORD'),
}

# RIS Kafka Consumer configuration
ris_consumer_conf = {
    'bootstrap.servers': 'node01.kafka-pub.ris.ripe.net:9094,node02.kafka-pub.ris.ripe.net:9094,node03.kafka-pub.ris.ripe.net:9094',
    'group.id': f"bgpdata-{hostname}",
    'partition.assignment.strategy': 'roundrobin',
    'enable.auto.commit': False,
    'auto.offset.reset': 'earliest',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': os.getenv('RIS_USERNAME'),
    'sasl.password': os.getenv('RIS_PASSWORD'),
}

# RIS Avro Encoding schema
ris_avro_schema = {
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
        {"name": "timestamp", "type": "long"},
        {"name": "host", "type": "string"},
        {"name": "peer", "type": "bytes"},
        {
            "name": "attributes",
            "type": {"type": "array", "items": "int"},
            "default": [],
        },
        {
            "name": "prefixes",
            "type": {"type": "array", "items": "bytes"},
            "default": [],
        },
        {"name": "path", "type": {"type": "array", "items": "long"}, "default": []},
        {"name": "ris_live", "type": "string"},
        {"name": "raw", "type": "string"},
    ],
}


def on_assign(consumer, partitions, db):
    """
    Callback function to handle partition assigning/rebalancing.

    This function is called when the consumer's partitions are assigned/rebalanced. It logs the
    assigned partitions and handles any errors that occur during the rebalancing process.

    Args:
        consumer: The Kafka consumer instance.
        partitions: A list of TopicPartition objects representing the newly assigned partitions.
        db: The RocksDB database.
    """
    try:
        if partitions[0].error:
            logger.error(f"Rebalance error: {partitions[0].error}")
        else:
            logger.info(
                f"Assigned partitions: {[p.partition for p in partitions]}")

            # Set the offset for each partition
            for partition in partitions:
                last_offset = db.get(
                    f'{partition.topic}_{partition.partition}'.encode('utf-8')) or None
                # If the offset is stored, set it
                if last_offset is not None:
                    # +1 because we start from the next message
                    partition.offset = int.from_bytes(
                        last_offset, byteorder='big') + 1
                    logger.info(
                        f"Setting offset for partition {partition.partition} of {partition.topic} to {partition.offset}")

            # Assign the partitions to the consumer
            consumer.assign(partitions)
    except Exception as e:
        logger.error(f"Error handling assignment: {e}", exc_info=True)

async def logging_task(status, queue):
    """
    Asynchronous task to periodically log the most recent timestamp, time lag, current poll interval, and consumption rate.
    This task runs within the main event loop.

    Args:
        status (dict): A dictionary containing the following keys:
            - time_lag (timedelta): The current time lag of the messages.
            - bytes_sent (int): The number of bytes sent since the last log.
            - activity (str): The current activity of the collector.
        queue (queueio.Queue): The queue containing the messages to send.
    """
    while True:
        seconds = 10
        await asyncio.sleep(seconds)  # Sleep for n seconds before logging

        # Compute kbps_counter
        bytes_sent = status['bytes_sent']
        # Convert bytes to kilobits per second
        kbps_counter = (bytes_sent * 8) / seconds / 1000

        if status['activity'] == "INITIALIZING":
            # Initializing
            logger.info(f"{status['activity']}{(17 - len(status['activity'])) * ' '}| "
                        f"Transmitting at ~{kbps_counter:.2f} kbit/s")
        elif status['activity'] == "RIB_INJECTION":
            # RIB Injection
            logger.info(f"{status['activity']}{(17 - len(status['activity'])) * ' '}| "
                        f"Transmitting at ~{kbps_counter:.2f} kbit/s, "
                        f"Queue size: ~{queue.qsize()}")
        elif status['activity'] == "KAFKA_POLLING":
            # Kafka Polling
            logger.info(f"{status['activity']}{(17 - len(status['activity'])) * ' '}| "
                        f"Time lag: ~{status['time_lag'].total_seconds()} seconds, "
                        f"Transmitting at ~{kbps_counter:.2f} kbit/s, "
                        f"Queue size: ~{queue.qsize()}")
        elif status['activity'] == "TERMINATING":
            # Terminating
            logger.info(f"{status['activity']}{(17 - len(status['activity'])) * ' '}| "
                        f"Transmitting at ~{kbps_counter:.2f} kbit/s, "
                        f"Queue size: ~{queue.qsize()}")

        # Reset bytes_sent
        status['bytes_sent'] = 0


def rib_task(queue, db, status, timestamps, collectors, provider, events):
    """
    Synchronous task to inject RIB messages from MRT Data Dumps into the queue.

    Args:
        queue (queueio.Queue): The queue to add the messages to.
        db (rocksdbpy.DB): The RocksDB database.
        status (dict): A dictionary containing the following keys:
            - time_lag (timedelta): The current time lag of the messages.
            - bytes_sent (int): The number of bytes sent since the last log.
            - activity (str): The current activity of the collector.
        timestamps (dict): A dictionary containing the latest timestamps of the RIBs.
        collectors (list): A list of tuples containing the host and URL of the RIB Data Dumps.
        provider (str): The provider of the MRT Data Dumps.
        events (dict): A dictionary containing the following keys:
            - route-views_injection (threading.Event): The event to wait for before starting.
            - route-views_provision (threading.Event): The event to wait for before starting.
            - ris_injection (threading.Event): The event to wait for before starting.
            - ris_provision (threading.Event): The event to wait for before starting.
    """

    # If the event is set, the provider is already initialized, skip
    if events[f"{provider}_provision"].is_set():
        return

    # Set the activity
    status['activity'] = "RIB_INJECTION"
    logger.info(f"Initiating RIB Injection from {provider} collectors...")

    try:
        for host, url in collectors:
            logger.info(f"Injecting RIB from {provider} of {host} via {url}")

            batch = []

            if host not in timestamps:
                timestamps[host] = -1

            while True:
                try:
                    # Parse the RIB Data Dump via BGPKit
                    # Learn more at https://bgpkit.com/
                    parser = bgpkit.Parser(url=url, cache_dir='.cache')

                    for elem in parser:
                        # Update the timestamp if it's the freshest
                        if elem['timestamp'] > timestamps[host]:
                            timestamps[host] = elem['timestamp']

                        # Construct the BMP message
                        messages = BMPv3.construct(
                            host,
                            elem['peer_ip'],
                            elem['peer_asn'],
                            elem['timestamp'],
                            "UPDATE",
                            [
                                [int(asn) for asn in part[1:-1].split(',')] if part.startswith('{') and part.endswith('}')
                                else int(part)
                                for part in elem['as_path'].split()
                            ],
                            elem['origin'],
                            [
                                # Only include compliant communities with 2 or 3 parts that are all valid integers
                                [int(part) for part in comm.split(
                                    ":")[1:] if part.isdigit()]
                                for comm in (elem.get("communities") or [])
                                if len(comm.split(":")) in {2, 3} and all(p.isdigit() for p in comm.split(":")[1:])
                            ],
                            [
                                {
                                    "next_hop": elem["next_hop"],
                                    "prefixes": [elem["prefix"]]
                                }
                            ],
                            [],
                            None,
                            0
                        )

                        # Add the messages to the batch
                        batch.extend(messages)

                    break  # Exit retry loop when successful

                except Exception as e:
                    logger.warning(
                        f"Retrieving RIB from {provider} {host} via {url} failed, retrying...", exc_info=True)
                    time.sleep(10)  # Wait 10 seconds before retrying

            # Add the messages to the queue
            for message in batch:
                queue.put((message, 0, provider, host, -1))

    except Exception as e:
        logger.error(
            f"Error injecting RIB from {provider} collectors: {e}", exc_info=True)
        raise e

    events[f"{provider}_injection"].set()


def kafka_task(configuration, timestamps, collectors, topics, queue, db, status, batch_size, provider, events):
    """
    Synchronous task to poll a batch of messages from Kafka and add them to the queue.

    Args:
        configuration (dict): The configuration of the Kafka consumer.
        timestamps (dict): A dictionary containing the latest timestamps of the RIBs.
        collectors (list): A list of tuples containing the host and topic of the collectors.
        topics (list): A list of topics to subscribe to.
        queue (queueio.Queue): The queue to add the messages to.
        db (rocksdbpy.DB): The RocksDB database.
        status (dict): A dictionary containing the following keys:
            - time_lag (timedelta): The current time lag of the messages.
            - bytes_sent (int): The number of bytes sent since the last log.
            - activity (str): The current activity of the collector.
        batch_size (int): Number of messages to fetch at once.
        provider (str): The provider of the messages.
        events (dict): A dictionary containing the following keys:
            - route-views_injection (threading.Event): The event to wait for before starting.
            - route-views_provision (threading.Event): The event to wait for before starting.
            - ris_injection (threading.Event): The event to wait for before starting.
            - ris_provision (threading.Event): The event to wait for before starting.
    """

    # Wait for possible RIB injection to finish
    for key in events.keys():
        if key.endswith("_injection"):
            events[key].wait()

    # Set the activity
    status['activity'] = "KAFKA_POLLING"
    logger.info(f"Subscribing to {provider} Kafka Consumer...")

    # Create Kafka Consumer
    consumer = Consumer(configuration)

    # Subscribe to Kafka Consumer
    consumer.subscribe(
        topics,
        on_assign=lambda c, p: on_assign(c, p, db),
        on_revoke=lambda c, p: logger.info(
            f"Revoked partitions: {[part.partition for part in p]}")
    )

    # If RIBs are injected but not yet provisioned
    if not events[f"{provider}_provision"].is_set():
        # Seek to desired offsets based on timestamps
        # Define a time delta (e.g., 5 hours)
        time_delta = timedelta(hours=5)

        # Keep track of the oldest timestamp for each topic
        # Why? In case multiple collectors stream to the same topic
        oldest_timestamps = {}

        for host, topic in collectors:
            # Verify that the collector is known
            if host not in timestamps:
                raise Exception(
                    f"Attempted to provision {host} of {provider} but it's unknown")

            # Assure the oldest timestamp for the topic (see comment above)
            oldest_timestamps[topic] = min(oldest_timestamps.get(
                topic, timestamps[host]), timestamps[host])

            # Get the timestamp of the recorded RIB
            timestamp = datetime.fromtimestamp(oldest_timestamps[topic])

            # Calculate the target time
            target_time = timestamp - time_delta
            # Convert to milliseconds
            target_timestamp_ms = int(target_time.timestamp() * 1000)

            # Get metadata to retrieve all partitions for the topic
            metadata = consumer.list_topics(topic, timeout=10)
            partitions = metadata.topics[topic].partitions.keys()

            # Create TopicPartition instances with the target timestamp
            topic_partitions = [TopicPartition(
                topic, p, target_timestamp_ms) for p in partitions]

            # Query Kafka for the offsets
            offsets = consumer.offsets_for_times(topic_partitions, timeout=10)

            # Extract the found offsets
            found_offsets = []
            for tp in offsets:
                if tp.offset == -1:
                    # Timestamp is greater than all message timestamps in the partition
                    # Start from the latest offset
                    latest = consumer.get_watermark_offsets(tp)
                    tp.offset = latest[1]
                found_offsets.append(tp)

            # Assign the found offsets
            consumer.assign(found_offsets)

            # Log the assigned offsets
            for tp in found_offsets:
                logger.info(
                    f"Assigned topic {tp.topic} partition {tp.partition} to offset {tp.offset}")

            # Mark Consumer as provisioned
            events[f"{provider}_provision"].set()

            # Wait for all consumers to be provisioned
            for key in events.keys():
                if key.endswith("_provision"):
                    events[key].wait()

            # Mark the RIBs injection as fulfilled
            db.set(b'injection_ended', b'\x01')

    # Poll messages from Kafka
    while True:
        # Poll a batch of messages
        msgs = consumer.consume(batch_size, timeout=0.1)

        if not msgs:
            continue

        for msg in msgs:
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info(f"End of partition reached: {msg.error()}")
                else:
                    logger.error(f"Kafka error: {msg.error()}", exc_info=True)
                continue

            # Process the message
            value = msg.value()
            topic = msg.topic()
            partition = msg.partition()

            # Initialize the messages list
            messages = []

            match provider:
                case 'route-views':
                    # Skip the raw binary header (we don't need the fields)
                    value = value[76 + struct.unpack("!H", value[54:56])[
                        0] + struct.unpack("!H", value[72:74])[0]:]

                    # TODO (1): Skip messages from unknown collectors.

                    # TODO (2): Parse the message and replace the peer_distinguisher with our own hash representation
                    #           Of the Route Views Collector name (SHA256) through the BMPv3.construct() function (e.g. the host).

                    # TODO (3): We need to keep track of the timestamp of the message
                    #           We do this to be able to show the time lag of the messages.

                    # HACK: Dummy timestamp for now
                    timestamp = datetime.now()

                    # HACK: Untempered message for now
                    messages.append(value)
                case 'ris':
                    # Remove the first 5 bytes (we don't need them)
                    value = value[5:]

                    # Parse the Avro encoded exaBGP message
                    parsed = fastavro.schemaless_reader(
                        BytesIO(value), ris_avro_schema)
                    # Cast from int to datetime float
                    timestamp = parsed['timestamp'] / 1000
                    host = parsed['host']  # Extract Host

                    # Skip unknown collectors
                    # TODO: We should probably log this, but not as an error, and not for every message
                    if host not in timestamps:
                        continue

                    # Check if the RIB injection for this collector is corrupted
                    if timestamps[host] == -1:
                        raise Exception(
                            f"RIB injection for {host} is corrupted, no MRT records ingested")

                    # Skip messages before the ingested collector's RIB or before the collector was seen
                    if timestamp > timestamps[host]:
                        # TODO: We are estimating the time gap between the message and the ingested RIB very statically,
                        #       but we should approach this more accurately, e.g. approximate the time gap through reverse graph analysis.
                        continue

                    # Parse to BMP messages and add to the queue
                    marshal = json.loads(parsed['ris_live'])
                    messages.extend(BMPv3.construct(
                        host,
                        marshal['peer'],
                        marshal['peer_asn'],
                        # Cast from int to datetime float
                        marshal['timestamp'] / 1000,
                        marshal['type'],
                        marshal['path'],
                        marshal['origin'],
                        marshal['community'],
                        marshal['announcements'],
                        marshal['withdrawals'],
                        marshal.get('state', None),
                        marshal.get('med', None)
                    ))

            # Update the approximated time lag preceived by the consumer
            status['time_lag'] = datetime.now(
            ) - datetime.fromtimestamp(timestamp)

            for message in messages:
                queue.put((message, msg.offset(), provider, topic, partition))


def sender_task(queue, host, port, db, status):
    """
    Synchronous task to transmit messages from the queue to the TCP socket.
    Only updates offset in RocksDB once message is successfully sent.

    Args:
        queue (queueio.Queue): The queue containing the messages to send.
        host (str): The host of the OpenBMP collector.
        port (int): The port of the OpenBMP collector.
        db (rocksdbpy.DB): The RocksDB database to store the offset.
        status (dict): A dictionary containing the following keys:
            - time_lag (timedelta): The current time lag of the messages.
            - bytes_sent (int): The number of bytes sent since the last log.
            - activity (str): The current activity of the collector.
    """
    # Whether a message has been sent
    sent_message = False

    # Establish a blocking TCP connection
    try:
        with socket.create_connection((host, port), timeout=60) as sock:
            logger.info(f"Connected to OpenBMP collector at {host}:{port}")

            while True:
                try:
                    logger.debug("Checking connection")
                    # Check if the connection is still alive
                    ready_to_read, _, _ = select.select([sock], [], [], 0)
                    if ready_to_read:
                        # Attempt to read data to verify connection is open
                        if not sock.recv(1, socket.MSG_PEEK):
                            raise ConnectionError("TCP connection closed by the peer")

                    # Get the message from the queue
                    message, offset, _, topic, partition = queue.get()

                    # Send the message
                    sock.sendall(message)
                    status['bytes_sent'] += len(message)

                    if not sent_message:
                        # At least one message has been sent
                        sent_message = True  # Mark that a message has been sent
                        db.set(b'injection_started', b'\x01')
                    
                    if partition != -1:
                        # Save offset to RocksDB only if it's from Kafka
                        key = f'{topic}_{partition}'.encode('utf-8')
                        db.set(key, offset.to_bytes(16, byteorder='big'))

                    # Mark the message as done
                    queue.task_done()

                except queueio.Empty:
                    # If the queue is empty, let the loop rest a bit
                    time.sleep(0.1)
                except ConnectionError as e:
                    logger.error("TCP connection lost", exc_info=True)
                    raise e
                except Exception as e:
                    logger.error(
                        "Error sending message over TCP", exc_info=True)
                    raise e

    except Exception as e:
        logger.error(f"Connection to OpenBMP collector at {host}:{port} failed", exc_info=True)
        raise e


async def main():
    """
    Main function to consume messages from MRT Dumps and Kafka, process them, and insert them into OpenBMP.

    This asynchronous function sets up a Kafka consumer, subscribes to the specified topic,
    and continuously polls for messages. It processes messages in batches, dynamically
    adjusts polling intervals based on message lag, and handles various error scenarios.

    The function performs the following key operations:
    1. Sets up RocksDB and a Kafka consumer with specified configuration and callbacks.
    2. Establishes a persistent TCP connection to the OpenBMP collector.
    3. May perform initial catchup by Route Injecting MRT records from MRT Data Dumps.
    4. Dynamically adjusts Kafka polling intervals based on message time lag.
    5. Processes messages, including possible deserialization and translation to BMPv3 (RFC7854).
    6. Sends processed BMP messages over TCP to the OpenBMP collector and updates the offset in RocksDB.
    7. Handles various error scenarios and implements retry logic.

    The function runs indefinitely until interrupted or an unhandled exception occurs.
    This script will be able to recover gracefully through the use of RocksDB.
    """

    # Log the start
    logger.info("Starting...")

    # Wait to not stress the system
    time.sleep(5)

    # Number of messages to queue to the OpenBMP collector (1M is ~1GB Memory)
    QUEUE_SIZE = 10000000
    BATCH_SIZE = 10000    # Number of messages to fetch at once from Kafka

    # Get the running loop
    loop = asyncio.get_running_loop()

    # Create RocksDB database
    db = rocksdbpy.open_default("rocks.db")

    # Define queue with a limit
    queue = queueio.Queue(maxsize=QUEUE_SIZE)

    # Initialize status dictionary to share logging information
    status = {
        'time_lag': timedelta(0),              # Initialize time lag
        'bytes_sent': 0,                       # Initialize bytes sent counter
        'activity': "INITIALIZING",            # Initialize activity
    }

    # Keep track of freshest timestamps of the RIBs
    timestamps = {}

    # Create a ThreadPoolExecutor for sender tasks
    workers = (2 if len(ris_collectors) > 0 else 0) + \
        (2 if len(routeviews_collectors) > 0 else 0) + len(openbmp_collectors)
    executor = ThreadPoolExecutor(max_workers=workers)

    # Start logging task that is updated within the loop
    task = asyncio.create_task(logging_task(status, queue))

    # Define shutdown function
    def shutdown(signum, _=None):
        # Set the activity
        status['activity'] = "TERMINATING"

        # Log the shutdown signal and frame information
        logger.warning(
            f"Shutdown signal ({signum}) received, exiting...")
        
        # Close the database
        db.close()

        # Shutdown the executor
        executor.shutdown(wait=False)

        # Cancel the task
        task.cancel()

        # Exit the program
        os._exit(signum)

    # Register signal handlers
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, shutdown)

    # Log the collectors
    logger.info(f"Routeviews collectors: {routeviews_collectors}")
    logger.info(f"RIS collectors: {ris_collectors}")
    logger.info(f"OpenBMP collectors: {openbmp_collectors}")

    try:
        # Create readyness events
        events = {}

        if len(routeviews_collectors) > 0:
            events["route-views_injection"] = threading.Event()
            events["route-views_provision"] = threading.Event()

        if len(ris_collectors) > 0:
            events["ris_injection"] = threading.Event()
            events["ris_provision"] = threading.Event()

        # Whether the RIBs injection started
        injection_started = True if db.get(
            b'injection_started') == b'\x01' else False
        # Whether the RIBs injection ended (important)
        injection_ended = True if db.get(
            b'injection_ended') == b'\x01' else False

        # We need to ensure all RIBs are fully injected
        if injection_started and injection_ended:
            # Everything is initialized
            if len(routeviews_collectors) > 0:
                events['route-views_injection'].set()
                events['route-views_provision'].set()
            if len(ris_collectors) > 0:
                events['ris_injection'].set()
                events['ris_provision'].set()

        elif injection_started and not injection_ended:
            # Database is corrupted, we need to exit
            logger.error("RIBs injection is corrupted, exiting...")
            raise Exception("Database is corrupted, exiting...")

        if len(routeviews_collectors) > 0:
            # HACK: For Route Views, we need to manually fetch the latest RIBs
            rv_rib_urls = []
            for host, _ in routeviews_collectors:
                if host == "route-views2":
                    # Route Views 2 is hosted on the root folder
                    index = f"https://archive.routeviews.org/bgpdata/{datetime.now().year}.{datetime.now().month}/RIBS/"
                else:
                    # Construct the URL for the latest RIBs
                    index = f"https://archive.routeviews.org/{host}/bgpdata/{datetime.now().year}.{datetime.now().month}/RIBS/"

                # Crawl the index page to find the latest RIB file (with beautifulsoup its also a apache file server)
                response = requests.get(index, timeout=30)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')
                latest_rib = soup.find_all('a')[-1].text
                rv_rib_urls.append(f"{index}{latest_rib}")

        # Initialize futures
        futures = []

        # RIB Tasks
        if len(routeviews_collectors) > 0:
            # Only if there are Route Views collectors
            future = loop.run_in_executor(executor, rib_task, queue, db, status, timestamps, list(zip(list(set(
                [f"{host}.routeviews.org" for host, _ in routeviews_collectors])), rv_rib_urls)), 'route-views', events)
            futures.append(asyncio.wrap_future(future))

        if len(ris_collectors) > 0:  
            # Only if there are RIS collectors
            future = loop.run_in_executor(executor, rib_task, queue, db, status, timestamps, list(zip(list(set(
                [f"{host}.ripe.net" for host in ris_collectors])), [f"https://data.ris.ripe.net/{i}/latest-bview.gz" for i in ris_collectors])), 'ris', events)
            futures.append(asyncio.wrap_future(future))

        # Kafka Tasks
        if len(routeviews_collectors) > 0:
            # Only if there are Route Views collectors
            future = loop.run_in_executor(executor, kafka_task, routeviews_consumer_conf, timestamps, list(zip([f"{host}.routeviews.org" for host, _ in routeviews_collectors],
                [f'routeviews.{host}.{peer}.bmp_raw' for host, peer in routeviews_collectors])), [f'routeviews.{host}.{peer}.bmp_raw' for host, peer in routeviews_collectors], queue, db, status, BATCH_SIZE, 'route-views', events)
            futures.append(asyncio.wrap_future(future))

        if len(ris_collectors) > 0:
            # Only if there are RIS collectors
            future = loop.run_in_executor(executor, kafka_task, ris_consumer_conf, timestamps, list(zip([f"{host}.ripe.net" for host in ris_collectors],
                ['ris-live' for _ in ris_collectors])), ['ris-live'], queue, db, status, BATCH_SIZE, 'ris', events)
            futures.append(asyncio.wrap_future(future))

        # Sender Tasks
        for host, port in openbmp_collectors:
            future = loop.run_in_executor(
                executor, sender_task, queue, host, port, db, status)
            futures.append(asyncio.wrap_future(future))

        # Keep the logging task and main loop alive
        await asyncio.gather(task, *futures)
        logger.info("All tasks finished")
    except Exception as e:
        logger.error("Fatal error", exc_info=True)
    finally:
        shutdown(1)
