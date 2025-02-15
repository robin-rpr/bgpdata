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
        def __init__(self, peer, raw_path_attribute, initial_prefix, mp_reach, collector, timestamp):
            """
            Args:
                peer (dict): Should contain at least 'ip_address' and 'asn'.
                raw_path_attribute (bytes): The complete raw BGP path attributes (excluding MP_REACH).
                initial_prefix (bytes): The first NLRI prefix.
                mp_reach (bytes or None): The MP_REACH attribute data (without header), if applicable.
                collector (str): Collector identifier.
                timestamp (float): Timestamp of the update.
            """
            self.peer = peer
            self.raw_path_attribute = raw_path_attribute
            self.mp_reach = mp_reach  # Can be None if not used.
            self.prefixes = [initial_prefix]
            self.collector = collector
            self.timestamp = timestamp

        def add_prefix(self, prefix):
            """
            Append another NLRI prefix to the bucket.
            Optionally, you can flush the bucket if it becomes too large.
            """
            # (Optional) Flush if accumulated prefixes are too large.
            if sum(len(p) for p in self.prefixes) >= 4000:
                # In a real implementation you might want to return or enqueue the message here.
                msg = self.__send_message()
                self.prefixes = []
                return msg
            self.prefixes.append(prefix)
            return None

        def get_mp_reach_attribute_header(self, attr_length):
            """
            Build the MP_REACH attribute header.

            The attribute flag for MP_REACH is 144 (binary 10010000) and the type code is 14.
            """
            attr_flag = 144  # 0x90, binary 10010000
            attr_type_code = 14
            return struct.pack("!B B H", attr_flag, attr_type_code, attr_length)

        def finalize_bucket(self):
            """
            Finalize the bucket and build the BMP message. If there are any remaining prefixes,
            they will be assembled into the final BGP UPDATE message.
            """
            if self.prefixes:
                return self.__send_message()
            return None

        def __send_message(self):
            # Concatenate all NLRI prefixes.
            nlri = b"".join(self.prefixes)

            if self.mp_reach is not None:
                # Calculate the total length of the MP_REACH attribute:
                # the MP_REACH data plus the NLRI (which is embedded in the attribute).
                mp_reach_length = len(self.mp_reach + nlri)
                # Build the full MP_REACH attribute by prepending the proper header.
                full_mp_reach_attribute = (
                    self.get_mp_reach_attribute_header(mp_reach_length) +
                    self.mp_reach +
                    nlri
                )
                # The final path attributes are the original ones plus the full MP_REACH attribute.
                path_attributes = self.raw_path_attribute + full_mp_reach_attribute
                # When using MP_REACH, the NLRI is part of the attribute; no separate NLRI field.
                nlri_field = b""
            else:
                # No MP_REACH attribute provided: simply use the raw path attribute and append the NLRI.
                path_attributes = self.raw_path_attribute
                nlri_field = nlri

            # Build the BGP UPDATE body.
            # Format: Withdrawn Routes Length (2 bytes) | Withdrawn Routes (none) |
            #         Total Path Attribute Length (2 bytes) | Path Attributes | NLRI (if applicable)
            withdrawn_routes_length = 0
            total_path_attr_length = len(path_attributes)
            bgp_update_body = (
                struct.pack("!H", withdrawn_routes_length) +
                struct.pack("!H", total_path_attr_length) +
                path_attributes +
                nlri_field
            )

            # Build the BGP header:
            # Marker (16 bytes of 0xff) | Length (2 bytes) | Type (1 byte, where 2 = UPDATE)
            marker = b'\xff' * 16
            total_length = 19 + len(bgp_update_body)  # 19 bytes for header fields.
            bgp_header = marker + struct.pack("!H", total_length) + struct.pack("!B", 2)

            bgp_update = bgp_header + bgp_update_body

            # Wrap the BGP UPDATE in a BMP monitoring message using BMPv3.
            bmp_message = BMPv3.monitoring_message(
                self.peer['ip_address'],
                self.peer['asn'],
                self.timestamp,
                bgp_update,
                self.collector
            )
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

    max_timestamps = {}
    min_timestamps = {}
    messages = []
    buckets = {}
    peers = []

    # Extract the list of peers
    iterator = MRT(tmp_file)
    for entry in iterator:
        memory['rows_processed'] += 1
        if entry['mrt_header']['type'] == 13 and entry['mrt_header']['subtype'] == 1:
            # Create list of dicts.
            peers = entry['mrt_entry']['peer_list']
            break

    # Iterate through the RIB entries
    iterator = MRT(tmp_file)
    peer_indexes = set()
    for entry in iterator:
        memory['rows_processed'] += 1
        # Process only TABLE_DUMP_V2 RIB_IPV4_UNICAST entries.
        # NOTE: Assumes MRT type 13 with subtype 2; adjust if needed.
        if entry['mrt_header']['type'] == 13 and entry['mrt_header']['subtype'] == 2:
            # Obtain the raw prefix NLRI
            raw_prefix_nlri = entry['mrt_entry']['raw_prefix_nlri']
            # Loop through each RIB entry within the MRT record
            for rib_entry in entry['mrt_entry']['rib_entries']:
                # Check if we have already provisioned this peer
                if db.get(f'timestamp_{peers[rib_entry["peer_index"]]["asn"]}'.encode('utf-8')) is not None:
                    continue

                # Add the peer index to the set of updating peer indexes
                peer_indexes.add(rib_entry['peer_index'])

                # Compute a bucket key based on the peer index and the hash of the path attributes
                raw_path_attributes = b"".join(rib_entry['raw_bgp_attributes'])
                bucket_key = f"{rib_entry['peer_index']}_{hash(raw_path_attributes)}"

                # Update the maximum timestamp if we find a newer one for the peer
                if peers[rib_entry['peer_index']]['asn'] not in max_timestamps or float(entry['mrt_header']['timestamp']) > max_timestamps[peers[rib_entry['peer_index']]['asn']]:
                    max_timestamps[peers[rib_entry['peer_index']]['asn']] = float(entry['mrt_header']['timestamp'])

                # Update the minimum timestamp if we find an older one for the peer
                if peers[rib_entry['peer_index']]['asn'] not in min_timestamps or float(entry['mrt_header']['timestamp']) < min_timestamps[peers[rib_entry['peer_index']]['asn']]:
                    min_timestamps[peers[rib_entry['peer_index']]['asn']] = float(entry['mrt_header']['timestamp'])

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

    logger.info(f"Completed processing RIB file.")

    # Queue the BMP Peer Up messages
    for peer_index in peer_indexes:
        bmp_message = BMPv3.peer_up_message(
            peers[peer_index]['ip_address'],
            peers[peer_index]['asn'],
            min_timestamps[peers[peer_index]['asn']] - 1, host)
        messages.append(bmp_message)

    # Queue the BMP Monitoring Update messages
    for bucket in buckets.values():
        bmp_message = bucket.finalize_bucket()
        if bmp_message is not None:
            messages.append(bmp_message)

    # Queue the BMP Monitoring End-of-RIB messages
    for peer_index in peer_indexes:
        bgp_update = bytes.fromhex("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF00130200000000")
        bmp_message = BMPv3.monitoring_message(
            peers[peer_index]['ip_address'],
            peers[peer_index]['asn'],
            max_timestamps[peers[peer_index]['asn']] + 1,
            bgp_update,
            host
        )
        messages.append(bmp_message)

    # Store the maximum timestamp for each new peer
    for peer, timestamp in max_timestamps.items():
        db.set(f'timestamp_{peer}'.encode('utf-8'), struct.pack('>d', timestamp))

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
