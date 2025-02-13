"""
BGPDATA - BGP Data Collection and Analytics Service

This software is part of the BGPDATA project, which is designed to collect, process, and analyze BGP data from various sources.
It helps researchers and network operators get insights into their network by providing a scalable and reliable way to analyze and inspect historical and live BGP data from Route Collectors around the world.

Author: Robin Röper
Co-Author: Tim Evens

Eclipse Public License - v 1.0

THE ACCOMPANYING PROGRAM IS PROVIDED UNDER THE TERMS OF THIS ECLIPSE PUBLIC
LICENSE ("AGREEMENT"). ANY USE, REPRODUCTION OR DISTRIBUTION OF THE PROGRAM
CONSTITUTES RECIPIENT'S ACCEPTANCE OF THIS AGREEMENT.
"""
from bs4 import BeautifulSoup
from datetime import datetime
from libs.bmp import BMPv3
from libs.mrt import MRT
import functools
import requests
import tempfile
import struct
import time
import os

def rib_task(host, queue, db, logger, events, memory):
    """
    Task to inject RIB messages from MRT Data Dumps into the queue.
    """

    # Broadcast the stage
    memory['task'] = "rib"

    class MessageBucket:
        def __init__(self, peer, raw_path_attributes, initial_prefix, mp_reach, collector, timestamp):
            """
            Message Bucket constructor.

            Args:
                peer: dict containing at least 'ip_address' and 'asn'
                raw_path_attributes: bytes (concatenated BGP path attributes)
                initial_prefix: bytes (encoded NLRI for one prefix)
                mp_reach: bytes or None (optional MP_REACH attribute value)
                collector: string (collector name/identifier)
                timestamp: float (timestamp from the MRT entry)
            """
            self.peer = peer
            self.raw_path_attributes = raw_path_attributes
            self.mp_reach = mp_reach
            self.prefixes = [initial_prefix]
            self.collector = collector
            self.timestamp = timestamp

        def add_prefix(self, prefix):
            self.prefixes.append(prefix)

        def finalize_bucket(self):
            """
            Construct a BGP UPDATE message from the aggregated prefixes and wrap it in a BMP monitoring message.
            """
            # Concatenate all NLRI prefixes
            nlri = b"".join(self.prefixes)
            # Use the MP_REACH attribute if present; otherwise, use the original path attributes
            path_attributes = self.mp_reach if self.mp_reach is not None else self.raw_path_attributes

            # Build the BGP UPDATE body.
            # BGP UPDATE format:
            #   Withdrawn Routes Length (2 bytes) | Withdrawn Routes (0 since we don't have any)
            #   Total Path Attribute Length (2 bytes) | Path Attributes | NLRI
            withdrawn_routes_length = 0
            total_path_attr_length = len(path_attributes)
            bgp_update_body = (struct.pack('!H', withdrawn_routes_length) +
                            struct.pack('!H', total_path_attr_length) +
                            path_attributes +
                            nlri)

            # Build the BGP header:
            #   Marker (16 bytes of 0xff) | Length (2 bytes) | Type (1 byte, 2 = UPDATE)
            marker = b'\xff' * 16
            total_length = 19 + len(bgp_update_body)  # 19 = 16 (marker) + 2 (length) + 1 (type)
            bgp_header = marker + struct.pack('!H', total_length) + struct.pack('!B', 2)

            bgp_update = bgp_header + bgp_update_body

            # Wrap the BGP update in a BMP monitoring message using your BMPv3 library.
            # (Parameters: peer_ip, peer_asn, timestamp, bgp_update, collector)
            bmp_message = BMPv3.monitoring_message(self.peer['ip_address'],
                                                self.peer['asn'],
                                                self.timestamp,
                                                bgp_update,
                                                self.collector)
            return bmp_message

    # Get RIB file URL
    if host.startswith("rrc"):
        # RIS Route Collectors
        url = f"https://data.ris.ripe.net/{host}/latest-bview.gz"
    else:
        # Route Views Collectors
        if host == "route-views2":
            index = f"https://archive.routeviews.org/bgpdata/{datetime.now().year}.{datetime.now().month:02d}/RIBS/"
        else:
            index = f"https://archive.routeviews.org/{host}/bgpdata/{datetime.now().year}.{datetime.now().month:02d}/RIBS/"

        response = requests.get(index, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        latest = soup.find_all('a')[-1].text
        url = f'{index}{latest}'

    # Download the RIB file
    tmp_dir = tempfile.gettempdir()
    tmp_file = os.path.join(tmp_dir, latest)

    class BytesTrackingAdapter(requests.adapters.HTTPAdapter):
        def send(self, request, **kwargs):
            kwargs.pop('stream', None)
            response = super().send(request, stream=True, **kwargs)
            response.raw.read = functools.partial(self._track_bytes, response.raw.read)
            return response

        def _track_bytes(self, original_read, *args, **kwargs):
            data = original_read(*args, **kwargs)
            if data:
                memory['bytes_received'] += len(data)
            return data

    session = requests.Session()
    session.mount('http://', BytesTrackingAdapter())
    session.mount('https://', BytesTrackingAdapter())

    logger.info(f"Downloading {url}")
    response = session.get(url, timeout=30)
    response.raise_for_status()
    
    with open(tmp_file, 'wb') as f:
        f.write(response.content)

    timestamps = {}
    messages = []
    buckets = {}
    peers = []

    # Extract the list of peers
    iterator = MRT(tmp_file)
    for entry in iterator:
        if entry['mrt_header']['type'] == 13 and entry['mrt_header']['subtype'] == 1:
            # Create list of dicts.
            peers = entry['mrt_entry']['peer_list']
            break

    # Iterate through the RIB entries
    iterator = MRT(tmp_file)
    for entry in iterator:
        # Process only TABLE_DUMP_V2 RIB_IPV4_UNICAST entries.
        # NOTE: Assumes MRT type 13 with subtype 2; adjust if needed.
        if entry['mrt_header']['type'] == 13 and entry['mrt_header']['subtype'] == 2:
            memory['rows_processed'] += 1
            # Obtain the raw prefix NLRI
            raw_prefix_nlri = entry['mrt_entry']['raw_prefix_nlri']
            # Loop through each RIB entry within the MRT record
            for rib_entry in entry['mrt_entry']['rib_entries']:
                # Check if we have already provisioned this peer
                if db.get(f'timestamp_{peers[rib_entry["peer_index"]]["asn"]}'.encode('utf-8')) is not None:
                    continue

                # Compute a bucket key based on the peer index and the hash of the path attributes
                raw_path_attributes = b"".join(rib_entry['raw_bgp_attributes'])
                bucket_key = f"{rib_entry['peer_index']}_{hash(raw_path_attributes)}"

                # Update the timestamp if we find an earlier one for the peer
                if peers[rib_entry['peer_index']]['asn'] not in timestamps or float(entry['mrt_header']['timestamp']) > timestamps[peers[rib_entry['peer_index']]['asn']]:
                    timestamps[peers[rib_entry['peer_index']]['asn']] = float(entry['mrt_header']['timestamp'])

                if bucket_key in buckets:
                    buckets[bucket_key].add_prefix(raw_prefix_nlri)
                else:
                    # Retrieve peer information using openbmp; this function must be provided.
                    peer = peers[rib_entry['peer_index']]
                    # Get the MP_REACH attribute if it exists
                    mp_reach = rib_entry.get('raw_mp_reach_nlri', {}).get('value')
                    buckets[bucket_key] = MessageBucket(peer,
                                                        raw_path_attributes,
                                                        raw_prefix_nlri,
                                                        mp_reach,
                                                        host,
                                                        entry['mrt_header']['timestamp'])

    logger.info(f"Completed processing of RIB file. Preparing {len(buckets)} BMP messages")

    # Finalize each bucket to build the BMP messages and update metrics
    for bucket in buckets.values():
        bmp_message = bucket.finalize_bucket()
        messages.append(bmp_message)

    # Store the timestamp for each new peer
    for peer, timestamp in timestamps.items():
        db.set(f'timestamp_{peer}'.encode('utf-8'), struct.pack('>d', timestamp))

    if len(messages) == 0:
        logger.info("Collector's RIB database is already up-to-date")

    # Enqueue each final BMP message
    for message in messages:
        queue.put((message, 0, None, -1, True))

    # Wait for completion
    while not queue.empty():
        time.sleep(2)

    # Set database as ready
    db.set(b'ready', b'\x01')

    # Set the injection event
    events['injection'].set()
