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
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
import queue as queueio
from config import *
from tasks import *
import threading
import rocksdbpy
import requests
import logging
import asyncio
import signal
import time
import os

# Logger
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, log_level, logging.INFO))

# Collectors
routeviews_collectors = [tuple(c.split(':')) for c in os.getenv('ROUTEVIEWS_COLLECTORS', '').split(',') if c.strip()]
ris_collectors = [c.strip() for c in os.getenv('RIS_COLLECTORS', '').split(',') if c]
openbmp_collectors = [tuple(c.split(':')) for c in os.getenv('OPENBMP_COLLECTORS', '').split(',') if c.strip()]

# RocksDB database
db = rocksdbpy.open_default(".rocksdb")

# Multi-threading
executor = ThreadPoolExecutor(max_workers=sum([
    2 * bool(ris_collectors),
    2 * bool(routeviews_collectors), 
    len(openbmp_collectors),
    1
]))

# Asyncio loop
loop = asyncio.get_running_loop()

# Message queue
queue = queueio.Queue(maxsize=10000000)

# Status dictionary
status = {
    'time_lag': {},
    'time_preceived': {},
    'bytes_sent': 0,
    'bytes_received': 0,
    'activity': None,
}

# AsyncIO Futures
futures = []

# Readyness events
events = {}
for collector_type in ['route-views', 'ris']:
    for event_type in ['injection', 'provision']:
        events[f'{collector_type}_{event_type}'] = threading.Event()

def on_start():
    """
    Callback function to handle the start of the collector.
    """

    # Set started flag
    db.put(b'started', b'\x01')

    # Loop through all unique hosts
    for host in list(set([f"{host}" for host, _ in routeviews_collectors])) + [f"{host}.ripe.net" for host in ris_collectors]:
        # Initialize timestamp for this host if not already set
        if db.get(f'timestamps_{host}'.encode('utf-8')) is None:
            db.set(f'timestamps_{host}'.encode('utf-8'), struct.pack('>d', 0.0))

        # Send peer up message
        message = BMPv3.construct(
            host,
            '127.0.0.1',
            0,
            time.time(),
            'PEER_STATE',
            None,
            None,
            None,
            None,
            None,
            'connected',
            None
        )

        # Add the message to the queue
        queue.put((message, 0, None, -1))

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
            # Set the offset for each partition
            for partition in partitions:
                last_offset = db.get(
                    f'offsets_{partition.topic}_{partition.partition}'.encode('utf-8')) or None
                
                # If the offset is stored, set it
                if last_offset is not None:
                    # +1 because we start from the next message
                    partition.offset = int.from_bytes(last_offset, byteorder='big') + 1
                
                # Log the assigned offset
                logger.info(
                    f"Assigned offset for partition {partition.partition} of {partition.topic} to {partition.offset}")
                
            # Assign the partitions to the consumer
            consumer.assign(partitions)
    except Exception as e:
        logger.error(f"Error handling assignment: {e}", exc_info=True)

def on_shutdown(signum, _=None):
    """
    Callback function for shutdown.
    """
    db.close()
    executor.shutdown(wait=False)

    # Wait (5 seconds)
    time.sleep(5)

    # Exit the program
    os._exit(signum)

async def main():
    """
    Collector main function.

    This function coordinates the initialization and execution of multiple tasks that:
    1. Sets up and configures a RocksDB database for storing Kafka offsets and state tracking.
    2. Creates and manages a queue for buffering BGP messages to be sent to an OpenBMP collector.
    3. Initializes Kafka consumers for RIS and Route Views data sources, with configured batch polling.
    4. Handles MRT RIB data injection for synchronization and recovery from RIB dump files.
    5. Establishes TCP connections for transmitting processed BGP messages to OpenBMP collectors.
    6. Logs statistics and tracks system performance, such as message processing rates and time lag.
    7. Monitors and gracefully handles shutdowns, ensuring proper resource cleanup.

    This function runs indefinitely and can handle interruptions or unexpected errors, resuming
    from the last known state using RocksDB for persistence.

    Note:
        - The function is designed to be fault-tolerant, with retry mechanisms and event handling.
        - It registers signal handlers to manage graceful exits on receiving termination signals.
    """

    BATCH_SIZE = 100000 # Number of messages to transfer in a batch

    # Register signal handlers
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, on_shutdown)

    # Validate database state
    if db.get(b'started') == b'\x01':
        if db.get(b'ready') == b'\x01':
            # Database is consistent, set the events
            for collector_type, collectors in [
                ('route-views', routeviews_collectors),
                ('ris', ris_collectors)
            ]:
                if collectors:
                    events[f'{collector_type}_injection'].set()
                    events[f'{collector_type}_provision'].set()
        else:
            # Database is in an inconsistent state
            # Likely the RIBs injection started but never completed
            raise RuntimeError("Database is inconsistent")

    try:
        # Start
        on_start()

        # HACK: For Route Views, we need to manually fetch the latest RIBs
        if len(routeviews_collectors) > 0:
            # Initialize the RIB URLs
            rv_rib_urls = []

            # Loop through all Route Views collectors
            for host, _ in routeviews_collectors:
                if host == "route-views2":
                    # Route Views 2 is hosted on the root folder
                    index = f"https://archive.routeviews.org/bgpdata/{datetime.now().year}.{datetime.now().month:02d}/RIBS/"
                else:
                    # Construct the URL for the latest RIBs
                    index = f"https://archive.routeviews.org/{host}/bgpdata/{datetime.now().year}.{datetime.now().month:02d}/RIBS/"

                # Crawl the index page to find the latest RIB file (with beautifulsoup its also a apache file server)
                response = requests.get(index, timeout=30)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')
                latest_rib = soup.find_all('a')[-1].text
                rv_rib_urls.append(f"{index}{latest_rib}")

        # ------------------------------
        # RIB Tasks
        # ------------------------------
        if len(routeviews_collectors) > 0:
            # Only if there are Route Views collectors
            future = loop.run_in_executor(executor, rib_task, queue, db, status, list(zip(list(set(
                [f"{host}.routeviews.org" for host, _ in routeviews_collectors])), rv_rib_urls)), 'route-views', events, logger)
            futures.append(asyncio.wrap_future(future))

        if len(ris_collectors) > 0:  
            # Only if there are RIS collectors
            future = loop.run_in_executor(executor, rib_task, queue, db, status, list(zip(list(set(
                [f"{host}.ripe.net" for host in ris_collectors])), [f"https://data.ris.ripe.net/{i}/latest-bview.gz" for i in ris_collectors])), 'ris', events, logger)
            futures.append(asyncio.wrap_future(future))

        # ------------------------------
        # Kafka Tasks
        # ------------------------------
        if len(routeviews_collectors) > 0:
            # Only if there are Route Views collectors
            future = loop.run_in_executor(executor, kafka_task, routeviews_consumer_conf, list(zip([f"{host}.routeviews.org" for host, _ in routeviews_collectors],
                [f'{host.replace("-","")}.{peer}.bmp_raw' for host, peer in routeviews_collectors])), [f'{host.replace("-","")}.{peer}.bmp_raw' for host, peer in routeviews_collectors], queue, db, status, BATCH_SIZE, 'route-views', events, logger)
            futures.append(asyncio.wrap_future(future))

        if len(ris_collectors) > 0:
            # Only if there are RIS collectors
            future = loop.run_in_executor(executor, kafka_task, ris_consumer_conf, list(zip([f"{host}.ripe.net" for host in ris_collectors],
                ['ris-live' for _ in ris_collectors])), ['ris-live'], queue, db, status, BATCH_SIZE, 'ris', events, logger)
            futures.append(asyncio.wrap_future(future))

        # ------------------------------
        # Sender Tasks
        # ------------------------------
        for host, port in openbmp_collectors:
            future = loop.run_in_executor(
                executor, sender_task, queue, host, port, db, status, logger)
            futures.append(asyncio.wrap_future(future))

        # ------------------------------
        # Logging Task
        # ------------------------------
        future = loop.run_in_executor(executor, logging_task, status, queue, db, [f"{host}.routeviews.org" for host, _ in routeviews_collectors], [f"{host}.ripe.net" for host in ris_collectors], logger)
        futures.append(asyncio.wrap_future(future))

        # Keep loop alive
        await asyncio.gather(*futures)
    except Exception as e:
        logger.critical(e, exc_info=True)
    finally:
        on_shutdown(1)

if __name__ == "__main__":
    main()