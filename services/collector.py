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
from concurrent.futures import ThreadPoolExecutor
from protocols.bmp import BMPv3
from bs4 import BeautifulSoup
from typing import List
from io import BytesIO
import rocksdbpy
import fastavro
import requests
import logging
import asyncio
import bgpkit
import socket
import struct
import time
import json
import sys
import os

# Get the hostname and process ID
hostname = socket.gethostname()  # Get the machine's hostname

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# List of Route Views collectors with locations
# https://www.routeviews.org/
rv_collectors = [
    "amsix.ams",             # AMS-IX Amsterdam, Netherlands
    "cix.atl",               # CIX-ATL Atlanta, Georgia
    "decix.jhb",             # DE-CIX KUL, Johor Bahru, Malaysia
    "iraq-ixp.bgw",          # IRAQ-IXP Baghdad, Iraq
    "pacwave.lax",           # Pacific Wave, Los Angeles, California
    "pit.scl",               # PIT Chile Santiago, Chile
    "pitmx.qro",             # PIT Chile MX, Querétaro, Mexico
    # "route-views",         # U of Oregon, Eugene, Oregon (Only has Cisco's `show ip bgp` data, not relevant for us)
    "route-views.amsix",     # AMS-IX AM6, Amsterdam, Netherlands
    "route-views.bdix",      # BDIX, Dhaka, Bangladesh
    "route-views.bknix",     # BKNIX, Bangkok, Thailand
    "route-views.chicago",   # Equinix CH1, Chicago, Illinois
    "route-views.chile",     # NIC.cl Santiago, Chile
    "route-views.eqix",      # Equinix DC, Ashburn, Virginia
    "route-views.flix",      # FL-IX, Miami, Florida
    "route-views.fortaleza", # IX.br (PTT.br), Fortaleza, Brazil
    "route-views.gixa",      # GIXA, Ghana, Africa
    "route-views.gorex",     # GOREX, Guam, US Territories
    # "route-views.jinx",    # JINX, Johannesburg, South Africa (RETIRED)
    "route-views.kixp",      # KIXP, Nairobi, Kenya
    "route-views.linx",      # LINX, London, United Kingdom
    "route-views.mwix",      # FD-IX, Indianapolis, Indiana
    "route-views.napafrica", # NAPAfrica, Johannesburg, South Africa
    "route-views.nwax",      # NWAX, Portland, Oregon
    "route-views.ny",        # DE-CIX NYC, New York, USA
    "route-views.isc",       # PAIX (ISC), Palo Alto, California
    "route-views.perth",     # West Australian Internet Exchange, Perth, Australia
    "route-views.peru",      # Peru IX, Lima, Peru
    "route-views.phoix",     # University of the Philippines, Diliman, Quezon City, Philippines
    "route-views.rio",       # IX.br (PTT.br), Rio de Janeiro, Brazil
    # "route-views.saopaulo",# SAOPAULO (PTT Metro, NIC.br), Sao Paulo, Brazil (RETIRED)
    "route-views2.saopaulo", # SAOPAULO (PTT Metro, NIC.br), Sao Paulo, Brazil
    "route-views.sfmix",     # San Francisco Metro IX, San Francisco, California
    # "route-views.siex",    # Southern Italy Exchange (SIEX), Rome, Italy (OFFLINE)
    "route-views.sg",        # Equinix SG1, Singapore, Singapore
    "route-views.soxrs",     # Serbia Open Exchange, Belgrade, Serbia
    "route-views.sydney",    # Equinix SYD1, Sydney, Australia
    "route-views.telxatl",   # TELXATL, Atlanta, Georgia
    "route-views.uaeix",     # UAE-IX, Dubai, United Arab Emirates
    "route-views.wide",      # DIXIE (NSPIXP), Tokyo, Japan
    # Multi-hop collectors
    "route-views2",          # U of Oregon, Eugene, Oregon
    "route-views3",          # U of Oregon, Eugene, Oregon
    "route-views4",          # U of Oregon, Eugene, Oregon
    "route-views5",          # U of Oregon, Eugene, Oregon
    "route-views6",          # U of Oregon, Eugene, Oregon (IPv6)
    "route-views7",          # U of Oregon, Eugene, Oregon
    # Applications
    # "bgpmon",              # BGPMon, Colorado State University Fort Collins, Colorado (OFFLINE)
    # "archive",             # Archive, includes asn.routeviews.org zone files, U of Oregon, Eugene, Oregon
    # "bgplay",              # BGPlay, BGP update player (RETIRED), U of Oregon, Eugene, Oregon
    # "zebra"                # BGP Beacon prefix 192.135.183.0 (RETIRED), U of Oregon, Eugene, Oregon
]

# List of RRCs with locations
# https://ris.ripe.net
ris_collectors = [
    "rrc00",                 # Amsterdam, NL - multihop, global
    "rrc01",                 # London, GB - IXP, LINX, LONAP
    # "rrc02",               # Paris, FR - IXP, SFINX (Historic)
    "rrc03",                 # Amsterdam, NL - IXP, AMS-IX, NL-IX
    "rrc04",                 # Geneva, CH - IXP, CIXP
    "rrc05",                 # Vienna, AT - IXP, VIX
    "rrc06",                 # Otemachi, JP - IXP, DIX-IE, JPIX
    "rrc07",                 # Stockholm, SE - IXP, Netnod
    # "rrc08",               # San Jose, CA, US - IXP, MAE-WEST (Historic)
    # "rrc09",               # Zurich, CH - IXP, TIX (Historic)
    "rrc10",                 # Milan, IT - IXP, MIX
    "rrc11",                 # New York, NY, US - IXP, NYIIX
    "rrc12",                 # Frankfurt, DE - IXP, DE-CIX
    "rrc13",                 # Moscow, RU - IXP, MSK-IX
    "rrc14",                 # Palo Alto, CA, US - IXP, PAIX
    "rrc15",                 # Sao Paolo, BR - IXP, PTTMetro-SP
    "rrc16",                 # Miami, FL, US - IXP, Equinix Miami
    "rrc18",                 # Barcelona, ES - IXP, CATNIX
    "rrc19",                 # Johannesburg, ZA - IXP, NAP Africa JB
    "rrc20",                 # Zurich, CH - IXP, SwissIX
    "rrc21",                 # Paris, FR - IXP, France-IX Paris and Marseille
    "rrc22",                 # Bucharest, RO - IXP, Interlan
    "rrc23",                 # Singapore, SG - IXP, Equinix Singapore
    "rrc24",                 # Montevideo, UY - multihop, LACNIC region
    "rrc25",                 # Amsterdam, NL - multihop, global
    "rrc26",                 # Dubai, AE - IXP, UAE-IX"
]

# Route Views Kafka Consumer configuration
rv_consumer_conf = {
    'bootstrap.servers': 'stream.routeviews.org:9092',
    'group.id': f"bgpdata-{hostname}",
    'partition.assignment.strategy': 'roundrobin',
    'enable.auto.commit': True,  # Enable automatic offset commit, we use RocksDB to store the offset
    'auto.offset.reset': 'earliest',
}

# RIS Kafka Consumer configuration
ris_consumer_conf = {
    'bootstrap.servers': 'node01.kafka-pub.ris.ripe.net:9094,node02.kafka-pub.ris.ripe.net:9094,node03.kafka-pub.ris.ripe.net:9094',
    'group.id': f"bgpdata-{hostname}",
    'partition.assignment.strategy': 'roundrobin',
    'enable.auto.commit': True,  # Enable automatic offset commit, we use RocksDB to store the offset
    'auto.offset.reset': 'earliest',
    # SASL Authentication (required for RIS Kafka)
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'public',
    'sasl.password': 'public',
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

def on_assign(consumer, partitions, db):
    """
    Callback function to handle partition assigning/rebalancing.

    This function is called when the consumer's partitions are assigned/rebalanced. It logs the
    assigned partitions and handles any errors that occur during the rebalancing process.

    Args:
        consumer: The Kafka consumer instance.
        partitions: A list of TopicPartition objects representing the newly assigned partitions.
        db: The RocksDB database to store the offset.
    """
    try:
        if partitions[0].error:
            logger.error(f"Rebalance error: {partitions[0].error}")
        else:
            logger.info(f"Assigned partitions: {[p.partition for p in partitions]}")

            # Set the offset for each partition
            for partition in partitions:
                partition.offset = int.from_bytes(db.get(f'{consumer.topic()}'.encode('utf-8')) or b'\x00', byteorder='big')
                logger.info(f"Setting offset for partition {partition.partition} of {consumer.topic()} to {partition.offset}")
            
            # Assign the partitions to the consumer
            consumer.assign(partitions)
    except Exception as e:
        logger.error(f"Error handling assignment: {e}", exc_info=True)

async def log_status(status, queue):
    """
    Periodically logs the most recent timestamp, time lag, current poll interval, and consumption rate.
    This coroutine runs concurrently with the main processing loop.
    
    Args:
        status (dict): A dictionary containing the following keys:
            - timestamp (datetime): The most recent timestamp of the messages.
            - time_lag (timedelta): The current time lag of the messages.
            - bytes_sent (int): The number of bytes sent since the last log.
        queue (asyncio.Queue): The queue containing the messages to send.
    """
    while True:
        seconds = 5
        await asyncio.sleep(seconds)  # Sleep for 5 seconds before logging

        # Compute kbps_counter
        bytes_sent = status['bytes_sent']
        kbps_counter = (bytes_sent * 8) / seconds / 1000  # Convert bytes to kilobits per second

        if status['activity'] == "CONNECTING":
            # Connecting
            logger.info(f"Activity: {status['activity']}, "
                        f"Transmitting at ~{kbps_counter:.2f} kbit/s")
        elif status['activity'] == "RIB_INJECTION":
            # RIB Injection
            logger.info(f"Activity: {status['activity']}, "
                        f"Transmitting at ~{kbps_counter:.2f} kbit/s, "
                        f"Queue size: ~{queue.qsize()}")
        elif status['activity'] == "KAFKA_POLLING":
            # Kafka Polling
            logger.info(f"Activity: {status['activity']}, "
                        f"Time lag: ~{status['time_lag'].total_seconds()} seconds, "
                        f"Transmitting at ~{kbps_counter:.2f} kbit/s, "
                        f"Queue size: ~{queue.qsize()}")

        # Reset bytes_sent
        status['bytes_sent'] = 0

async def rib_task(queue, status, timestamps, collectors, provider, events):
    """
    Task to inject RIB messages from MRT Data Dumps into the queue.

    Args:
        queue (asyncio.Queue): The queue to add the messages to.
        status (dict): The status dictionary to update the time lag.
        timestamps (dict): A dictionary containing the latest timestamps of the RIBs.
        collectors (list): A list of tuples containing the host and URL of the RIB Data Dumps.
        provider (str): The provider of the MRT Data Dumps.
        events (dict): A dictionary containing the following keys:
            - route-views (asyncio.Event): The event to wait for before starting.
            - ris (asyncio.Event): The event to wait for before starting.
    """

    status['activity'] = "RIB_INJECTION"

    logger.info(f"Beginning RIB Injection from {provider} collectors")
    
    try:
        # Perform RIB Injection from each collector
        for host, url in collectors:
            logger.info(f"Injecting RIB from {provider} {host} via {url}")

            # Initialize timestamps[host] before using it
            if host not in timestamps:
                timestamps[host] = -1

            # Parse the RIB Data Dump via BGPKit
            # Learn more at https://bgpkit.com/
            parser = bgpkit.Parser(url=url)
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
                        [int(part) for part in comm.split(":")[1:] if part.isdigit()]
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

                # Add the message to the queue
                for message in messages:
                    await queue.put((message, 0, provider, host))

    except Exception as e:
        logger.error(f"Error injecting RIB from {provider} collectors: {e}", exc_info=True)
        logger.error(f"Serious data collection error with possible data corruption, exiting...")
        raise e

    # We are done, mark as ready
    events[provider].set()


async def kafka_task(consumer, timestamps, topics, queue, db, status, batch_size, provider, events):
    """
    Task to poll a batch of messages from Kafka and add them to the queue.

    Args:
        consumer (confluent_kafka.Consumer): The Kafka consumer instance.
        timestamps (dict): A dictionary containing the latest timestamps of the RIBs.
        topics (list): A list of topics to subscribe to.
        queue (asyncio.Queue): The queue to add the messages to.
        db (rocksdbpy.DB): The RocksDB database to store the offset.
        status (dict): The status dictionary to update the time lag.
        batch_size (int): Number of messages to fetch at once.
        provider (str): The provider of the messages.
        events (dict): A dictionary containing the following keys:
            - route-views (asyncio.Event): The event to wait for before starting.
            - ris (asyncio.Event): The event to wait for before starting.
    """

    # Wait for all providers to be ready
    await asyncio.gather(*[events[p].wait() for p in events])

    status['activity'] = "KAFKA_POLLING"

    logger.info(f"Subscribing to {provider} Kafka Consumer")

    # Subscribe to Kafka Consumer
    consumer.subscribe(
        topics,
        on_assign=lambda c, p: on_assign(c, p, db),
        on_revoke=lambda c, p: logger.info(f"Revoked partitions: {[part.partition for part in p]}")
    )

    loop = asyncio.get_running_loop()
    
    while True:
        # Poll a batch of messages
        msgs = await loop.run_in_executor(None, consumer.consume, batch_size, 0.1)
        
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

            match provider:
                case 'route-views':
                    # Skip the raw binary header (we don't need the fields)
                    value = value[76 + struct.unpack("!H", value[54:56])[0] + struct.unpack("!H", value[72:74])[0]:]

                    # TODO (1): Skip messages before the ingested collector's RIB.

                    # TODO (2): Parse the message and replace the peer_distinguisher with our own hash representation
                    #           Of the Route Views Collector name (SHA256) through the BMPv3.construct() function (e.g. the host).

                    # Add the message
                    messages.append(value)
                case 'ris':
                    # Remove the first 5 bytes (we don't need them)
                    value = value[5:]

                    # Parse the Avro encoded exaBGP message
                    parsed = fastavro.schemaless_reader(BytesIO(value), ris_avro_schema)
                    timestamp = parsed['timestamp'] / 1000  # Cast from int to datetime float
                    host = parsed['host'] # Extract Host

                    # Check if the collector is known
                    if host not in timestamps:
                        raise Exception(f"Unknown collector {host}")
                    
                    # Check if the RIB injection for this collector is corrupted
                    if timestamps[host] == -1:
                        raise Exception(f"RIB injection for {host} is corrupted, no MRT records ingested")
                    
                    # Skip messages before the ingested collector's RIB or before the collector was seen
                    if timestamp > timestamps[host]:
                        # TODO: We are estimating the time gap between the message and the ingested RIB very statically,
                        #       but we should approach this more accurately, e.g. approximate the time gap through reverse graph analysis.
                        continue

                    # Parse to BMP messages and add to the queue
                    marshal = json.loads(parsed['ris_live'])
                    messages = BMPv3.construct(
                        host,
                        marshal['peer'],
                        marshal['peer_asn'],
                        marshal['timestamp'] / 1000, # Cast from int to datetime float
                        marshal['type'],
                        marshal['path'],
                        marshal['origin'],
                        marshal['community'],
                        marshal['announcements'],
                        marshal['withdrawals'],
                        marshal['state'],
                        marshal['med']
                    )
                
            # Update the timestamp and time lag
            status['timestamp'] = datetime.fromtimestamp(timestamp)
            status['time_lag'] = datetime.now() - status['timestamp']

            for message in messages:
                await queue.put((message, msg.offset(), provider, topic))

def sender_task(queue, host, port, db, status):
    """
    Synchronous task to transmit messages from the queue to the TCP socket.
    Only updates offset in RocksDB once message is successfully sent.

    Args:
        queue (asyncio.Queue): The queue containing the messages to send.
        host (str): The host of the OpenBMP collector.
        port (int): The port of the OpenBMP collector.
        db (rocksdbpy.DB): The RocksDB database to store the offset.
        status (dict): The status dictionary to update the bytes sent counter.
    """
    # Establish a blocking TCP connection
    try:
        with socket.create_connection((host, port), timeout=60) as sock:
            logger.info(f"Connected to OpenBMP collector at {host}:{port}")

            while True:
                try:
                    message, offset, provider, topic = queue.get_nowait()  # Non-blocking get
                    sock.sendall(message)
                    status['bytes_sent'] += len(message)

                    # Save offset to RocksDB
                    db.set(
                        f'{provider}_{topic}'.encode('utf-8'),
                        offset.to_bytes(16, byteorder='big')
                    )

                    # Mark the message as done
                    queue.task_done()

                except asyncio.QueueEmpty:
                    # If the queue is empty, let the loop rest a bit
                    time.sleep(0.1)
                except Exception as e:
                    logger.error("Error sending message over TCP", exc_info=True)

    except Exception as e:
        logger.error("Socket connection failed, exiting...", exc_info=True)
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

    QUEUE_SIZE = 1000000 # Number of messages to queue to the OpenBMP collector
    BATCH_SIZE = 10000   # Number of messages to fetch at once from Kafka

    # Wait for 10 seconds before starting (prevents possible self-inflicted dos attack)
    await asyncio.sleep(10)

    # Create RocksDB database
    db = rocksdbpy.open_default("offset.db")

    # Create Route Views Kafka Consumer
    rv_consumer = Consumer(rv_consumer_conf)

    # Create RIS Live Kafka Consumer
    ris_consumer = Consumer(ris_consumer_conf)

    # Define queue with a limit
    queue = asyncio.Queue(maxsize=QUEUE_SIZE)

    # Initialize status dictionary to share variables between main and log_status
    status = {
        'timestamp': datetime.now(),           # Initialize timestamp
        'time_lag': timedelta(0),              # Initialize time lag
        'bytes_sent': 0,                       # Initialize bytes sent counter
        'activity': "CONNECTING",              # Initialize activity
    }

    # Keep track of freshest timestamps of the RIBs
    timestamps = {}

    # Start logging task that is updated within the loop
    logging_task = asyncio.create_task(log_status(status, queue))

    # Create OpenBMP Socket with retry until timeout
    collectors = [tuple(collector.split(':')) for collector in os.getenv('OPENBMP_COLLECTORS').split(',')]
    logger.info(f"Found {len(collectors)} collectors: {collectors}")

    # Create a ThreadPoolExecutor for sender tasks
    executor = ThreadPoolExecutor(max_workers=len(collectors))

    try:
        # Create readyness events
        events = {
            "route-views": asyncio.Event(),
            "ris": asyncio.Event(),
        }

        # Verify database initialization
        initialized = False

        for key, value in db.iterator():
            if value == b'\x00' * 16:
                # RIB Injection failed, detected unexpected null offset
                raise Exception("RIB Injection failed, data may be corrupted, exiting...")
            else:
                logger.info(f"Found offset for {key.decode('utf-8')}: {value.hex()}")
                initialized = True

        if initialized:
            # Everything is initialized
            logger.info("Offset database is intact.")
            events['route-views'].set()
            events['ris'].set()

        # HACK: For Route Views, we need to manually fetch the latest RIBs
        rv_rib_urls = []
        for i in rv_collectors:
            if i == "route-views2":
                # Route Views 2 is hosted on the root folder
                index = f"https://archive.routeviews.org/bgpdata/{datetime.now().year}.{datetime.now().month}/RIBS/"
            else:
                # Construct the URL for the latest RIBs
                index = f"https://archive.routeviews.org/{i}/bgpdata/{datetime.now().year}.{datetime.now().month}/RIBS/"

            # Crawl the index page to find the latest RIB file (with beautifulsoup its also a apache file server)
            response = requests.get(index, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            latest_rib = soup.find_all('a')[-1].text
            rv_rib_urls.append(f"{index}{latest_rib}")

        # Create tasks list starting with RIB and Kafka consumers
        tasks = [
            # RIB Consumers
            rib_task(queue, status, timestamps, list(zip([f"{i}.routeviews.org" for i in rv_collectors], rv_rib_urls)), 'route-views', events),
            rib_task(queue, status, timestamps, list(zip([f"{i}.ripe.net" for i in ris_collectors], [f"https://data.ris.ripe.net/{i}/latest-bview.gz" for i in ris_collectors])), 'ris', events),
            # Kafka Consumers
            kafka_task(rv_consumer, timestamps, [f'bmp.rv.routeviews.{i}' for i in rv_collectors], queue, db, status, BATCH_SIZE, 'route-views', events),
            kafka_task(ris_consumer, timestamps, ['ris-live'], queue, db, status, BATCH_SIZE, 'ris', events),
            # Logging
            logging_task
        ]

        # Add a multithreaded sender task for each collector
        # Create tasks for all sender_task instances in separate threads
        loop = asyncio.get_running_loop()
        for host, port in collectors:
            loop.run_in_executor(executor, sender_task, queue, host, port, db, status)

        # Start all tasks
        await asyncio.gather(*tasks)
    except Exception as e:
        logger.error("Fatal error", exc_info=True)
    finally:
        # Close the RocksDB database
        db.close()

        # Shutdown the ThreadPoolExecutor
        executor.shutdown()
        
        # Cancel the logging task when exiting
        logging_task.cancel()
        try:
            await logging_task
        except asyncio.CancelledError:
            pass
