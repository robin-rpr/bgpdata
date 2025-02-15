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
from collector.src.kafka import kafka_task
from collector.src.rib import rib_task
from collector.src.sender import sender_task
from collector.src.logging import logging_task
from concurrent.futures import ThreadPoolExecutor
from libs.bmp import BMPv3
import queue as queueio
import rocksdbpy
import threading
import asyncio
import logging
import signal
import sys
import os

# Logger
logger = logging.getLogger(__name__)
log_level = os.getenv('COLLECTOR_LOG_LEVEL', 'INFO').upper()
logger.setLevel(getattr(logging, log_level, logging.INFO))

# Environment variables
OPENBMP_CONNECT = os.getenv('COLLECTOR_OPENBMP_CONNECT')
KAFKA_CONNECT = os.getenv('COLLECTOR_KAFKA_CONNECT')
HOST = os.getenv('COLLECTOR_HOST')

# Signal handler
def handle_shutdown(signum, frame, shutdown_event):
    """
    Signal handler for shutdown.

    Args:
        signum (int): The signal number.
        frame (frame): The signal frame.
        shutdown_event (asyncio.Event): The shutdown event.
    """
    logger.info(f"Signal {signum}. Triggering shutdown...")
    shutdown_event.set()

# Main Coroutine
async def main():
    memory = {
        'task': None,
        'time_lag': {},
        'bytes_sent': 0,
        'bytes_received': 0,
        'rows_processed': 0,
    }

    events = {
        'injection': threading.Event()
    }

    # Queue
    queue = queueio.Queue(maxsize=10000000)

    # Executor
    executor = ThreadPoolExecutor()

    # Database
    db = rocksdbpy.open_default("/var/lib/rocksdb")

    # Futures
    futures = []

    try:
        logger.info("Starting up...")

        # Create an event to signal shutdown
        shutdown_event = asyncio.Event()

        # Register SIGTERM handler
        loop = asyncio.get_event_loop()
        loop.add_signal_handler(signal.SIGTERM, handle_shutdown, signal.SIGTERM, None, shutdown_event)
        loop.add_signal_handler(signal.SIGINT, handle_shutdown, signal.SIGINT, None, shutdown_event)  # Handle Ctrl+C

        # Validate database state
        if db.get(b'started') == b'\x01':
            if not db.get(b'ready') == b'\x01':
                # Database is in an inconsistent state
                raise RuntimeError("Corrupted database")

        # Initialize the BMP connection
        message = BMPv3.init_message(
            router_name=f'{HOST}.ripe.net' if HOST.startswith('rrc') else HOST,
            router_descr=f'{HOST}.ripe.net' if HOST.startswith('rrc') else f'{HOST}.routeviews.org'
        )
        queue.put((message, 0, None, -1, False))

        # Check for exceptions in each task
        def check_exception(fut):
            exc = fut.exception()
            if exc is not None:
                shutdown_event.set()
                raise exc

        # Start rib task
        future = asyncio.wrap_future(
            loop.run_in_executor(executor, rib_task, HOST, queue, db, logger, events, memory)
        )
        future.add_done_callback(check_exception)
        futures.append(future)

        # Start kafka task
        future = asyncio.wrap_future(
            loop.run_in_executor(executor, kafka_task, HOST, KAFKA_CONNECT, queue, db, logger, events, memory)
        )
        future.add_done_callback(check_exception)
        futures.append(future)

        # Start sender task
        future = asyncio.wrap_future(
            loop.run_in_executor(executor, sender_task, OPENBMP_CONNECT, queue, db, logger, memory)
        )
        future.add_done_callback(check_exception)
        futures.append(future)

        # Start logging task
        future = asyncio.wrap_future(
            loop.run_in_executor(executor, logging_task, HOST, queue, logger, memory)
        )
        future.add_done_callback(check_exception)
        futures.append(future)

        # Wait for the shutdown event
        await shutdown_event.wait()
    finally:
        logger.info("Shutting down...")

        # Terminate the BMP connection
        message = BMPv3.term_message(
            reason_code=1
        )
        queue.put((message, 0, None, -1, False))

        # Shutdown the executor
        executor.shutdown(wait=False, cancel_futures=True)

        logger.info("Shutdown complete.")

        # Terminate
        os._exit(1)


if __name__ == "__main__":
    main()