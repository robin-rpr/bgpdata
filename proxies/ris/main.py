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
from models.proxy import Proxy
from protocols.bmp import BMPv3
from proxies.routeviews.org.config import *
from proxies.routeviews.org.tasks.kafka import kafka_task
from proxies.routeviews.org.tasks.rib import rib_task
from proxies.routeviews.org.tasks.sender import sender_task
from proxies.routeviews.org.tasks.logging import logging_task
import logging
import signal
import time
import os

# Logger
logger = logging.getLogger(__name__)
log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
logger.setLevel(getattr(logging, log_level, logging.INFO))

# Environment variables
target = os.getenv('PROXY_TARGET')
router = os.getenv('PROXY_ROUTER')

# Route Views Collectors
collectors = {
    'amsix.ams': [293,1103,1140,3320,6204,6777,20253,28283,34177,37271,39120,49544,56987,213241,271253,328832,398465],
    'route-views.amsix': [1103,3214,3399,6762,6830,8888,9002,12779,12859,16509,16552,30844,38880,39591,42541,44324,47957,50763,51019,51088,56662,58511,60144,60150,61955,199524,200242,208753,211398,213151,216311,267613],
    'route-views.bknix': [59238,63529],
    'route-views.chicago': [293,852,3257,8220,14630,16509,16552,17350,19151,19653,20253,24115,32709,49544,53828,199524,398465],
    'route-views.chile': [27678,27986],
    'cix.atl': [13335,16509,20253,20940,63221],
    'decix.jhb': [16509,38137,38194,199524],
    'route-views.eqix': [293,2914,3257,3320,6057,6079,6762,6830,6939,8220,8781,11039,16509,16552,17350,19151,20253,32098,37468,37721,40934,41095,49544,57695,199524,398465],
    'route-views.flix': [1031,6939,7195,15280,16509,16552,19151,20253,28283,52320,52468,63221,263237,264409],
    'route-views.fortaleza': [1031,20253,26162,28624,52320,199524,262462,263945,264409,264479],
    'route-views.gorex': [16509,40300,65534],
    'route-views.isc': [3320,6762,6939,7575,16509,19151,49544,199524],
    'route-views.kixp': [6939,16509,37271,37704,63293,328475,328977],
    'route-views.linx': [1031,2914,3170,3257,3491,5413,5511,6424,6453,6667,6762,6830,6939,8455,8714,9002,13030,13237,14537,16509,16552,31500,34288,37271,38182,38880,39122,41811,47957,48070,49544,58511,59605,267613,398465],
    'route-views.napafrica': [3491,16509,16552,32653,37271,37468,49544,199524,328137,328206,328266,328320,328512,328964,329035],
    'route-views.ny': [11399,13335,20253,20940,28213,32934,49544,63034,64289,398465],
    'route-views.perth': [7594,7606,17766,49544,58511,136557,140627,199524],
    'route-views.rio': [999,1031,6057,20253,26162,52320,52468,199524,264409,264479,267613],
    'route-views2.saopaulo': [1031,7195,13786,16552,26162,28329,37468,49544,52468,52863,52873,60503,61832,199524,262791,263009,263237,263541,264409,264479,268976,271253],
    'route-views3': [209,3216,3257,3561,5645,6939,8289,9268,11537,14315,19653,22388,23367,29479,38001,38136,39120,40387,45352,46450,55222,61568,63927,202365],
    'route-views4': [1299,1351,2518,2914,14041,19754,19782,24482,30950,32653,34288,36236,38726,38883,45437,56665,57050,58511,58682,63956,133950,204028],
    'route-views5': [955,1221,11537,13058,19529,22296,33185,37721,41666,49544,56987,58511,60539,132884,137409,199518,207934,208594],
    'route-views6': [209,701,1403,2497,2914,3130,3257,6939,7018,7660,18106,20130,20912,22652,23673,37100,49788,57463,57866,58511,140731,209306],
    'route-views7': [260,924,4641,5580,8582,16260,30371,37989,40864,44620,44901,48297,51999,57344,132213,150369,199310,206271,328977,401021],
    'route-views.sfmix': [16509,20253,34927,35008,49544,63055,64289,397131],
    'route-views.sg': [3491,4637,6762,7713,8220,9002,9902,16509,16552,17660,18106,24115,24482,24516,37468,38182,38880,49544,58511,58952,59318,63927,132337,136106,136557,151326,199524],
    'route-views.soxrs': [13004,199524],
    'route-views.sydney': [3491,4826,7575,7594,8888,9266,16552,24115,24516,58511,63956,132847,135895,148968,199524,398465],
    'route-views.telxatl': [4181,6082,6939,16509,19151,20253,32299,53828],
    'route-views.uaeix': [16509,42473,49544,60924,61374,199524],
    'route-views.wide': [2497,2500,2516,7500],
}

def before_start(target, router, queue, db, events):
    # Validate database state
    if db.get(b'started') == b'\x01':
        if db.get(b'ready') == b'\x01':
            # Database is consistent, set the events
            events['injection'].set()
            events['provision'].set()
        else:
            # Database is in an inconsistent state
            raise RuntimeError("Inconsistent database")

    messages = []

    # Send ROUTER INIT message
    messages.extend(BMPv3.construct(
        collector=f'{router}.ripe.net',
        local_ip='127.0.0.1',
        local_port=179,
        bgp_id='192.0.2.1',
        my_as=64512,
        msg_type='ROUTER_INIT',
        hold_time=180,
        optional_params=b'' # No optional parameters
    ))

    # Send PEER UP message
    messages.extend(BMPv3.construct(
        collector=f'{router}.ripe.net',
        peer_ip='127.0.0.1',
        peer_asn=64513, # Private ASN
        timestamp=time.time(),
        msg_type='PEER_STATE',
        path=[],
        origin='INCOMPLETE',
        community=[],
        announcements=[],
        withdrawals=[],
        state='CONNECTED',
        my_as=64512, # Private ASN
        hold_time=180, # 3 minutes
        bgp_id='192.0.2.1', # BGP Identifier (Using a unique TEST-NET-1 IP)
        optional_params=b'' # No optional parameters
    ))

    # Add the messages to the queue
    for message in messages:
        queue.put((message, 0, None, -1))

async def main():
    proxy = Proxy(
        before_start=before_start,
        after_stop=lambda signum: os._exit(signum),
        target=target,
        router=router,
        memory={
            'time_lag': timedelta(0),
            'time_preceived': None,
            'bytes_sent': 0,
            'bytes_received': 0,
            'active_stage': None,
            'kafka_topics': ['ris-live']
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
        signal.signal(sig, proxy.stop)

    # Start Proxy
    await proxy.start()