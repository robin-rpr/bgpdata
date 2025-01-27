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
from datetime import timedelta
from models.collector import Collector
from protocols.bmp import BMPv3
from collectors.ris.tasks.kafka import kafka_task
from collectors.ris.tasks.rib import rib_task
from collectors.ris.tasks.sender import sender_task
from collectors.ris.tasks.logging import logging_task
import logging
import signal
import os

# Logger
logger = logging.getLogger(__name__)
log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
logger.setLevel(getattr(logging, log_level, logging.INFO))

# Environment variables
openbmp = os.getenv('OPENBMP_FQDN')
host = os.getenv('COLLECTOR_HOST')

# Collectors
collectors = {
    'rrc13': [8470,8359,6939,6866,44030,41722,41617,41095,39821,35598,3267,28917,2854,25091,24482,210756,20764],
}

def before_start(self):
    # Validate database state
    if self.db.get(b'started') == b'\x01':
        if self.db.get(b'ready') == b'\x01':
            # Database is consistent, set the events
            self.events['injection'].set()
            self.events['provision'].set()
        else:
            # Database is in an inconsistent state
            raise RuntimeError("Inconsistent database")

    # Initialize the BMP connection
    messages = BMPv3.construct(
        collector=f'{self.host}.ripe.net',
        msg_type='INIT'
    )
    for message in messages:
        self.queue.put((message, 0, None, -1))

def before_stop(_, self):
    # Terminate the BMP connection
    messages = BMPv3.construct(
        collector=f'{self.host}.ripe.net',
        msg_type='TERM'
    )
    for message in messages:
        self.queue.put((message, 0, None, -1))

async def main():
    collector = Collector(
        before_start=before_start,
        before_stop=before_stop,
        after_stop=lambda signum, _: os._exit(signum),
        openbmp=openbmp,
        host=host,
        memory={
            'time_lag': timedelta(0),
            'time_preceived': None,
            'bytes_sent': 0,
            'bytes_received': 0,
            'source': None,
            'kafka_topics': [f'{host}.{peer}.bmp_raw' for peer in collectors[host]]
        },
        events=[
            'injection',
            'provision',
        ],
        tasks=[
            rib_task,
            kafka_task,
            sender_task,
            logging_task
        ]
    )

    # Register signal handlers
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, collector.stop)

    # Start Collector
    await collector.start()