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
from libs.bmp import BMPv3
from libs.mrt import MRT
import requests
import tempfile
import struct
import time
import os

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

def rib_task(openbmp, host, queue, db, logger, events, memory):
    """
    Task to inject RIB messages from MRT Data Dumps into the queue.
    """

    # Return if already done
    if events['provision'].is_set():
        return

    # Broadcast the stage
    memory['source'] = "rib"

    try:
        # Find latest RIB file
        url = f'https://data.ris.ripe.net/{host}/latest-bview.gz'

        # Download the RIB file
        tmp_dir = tempfile.gettempdir()
        tmp_file = os.path.join(tmp_dir, 'latest-bview.gz')

        # Try downloading with retries
        max_retries = 10        
        for attempt in range(max_retries):
            try:
                logger.info(f"Downloading {url}")
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                
                with open(tmp_file, 'wb') as f:
                    f.write(response.content)
                break # Success - exit the retry loop
                
            except (requests.RequestException, IOError) as e:
                if attempt == max_retries - 1: # Last attempt
                    raise # Re-raise the last exception if all retries failed
                logger.warning(f"Download failed: {e}. Retrying in 30s")
                time.sleep(30)

        # Store the timestamp (equal to 0)
        db.set(b'target_timestamp', b'\x00\x00\x00\x00\x00\x00\x00\x00')

        timestamp = 0.0
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
            # Update the timestamp if we find an earlier one
            if float(entry['mrt_header']['timestamp']) > timestamp:
                timestamp = float(entry['mrt_header']['timestamp'])

            # Process only TABLE_DUMP_V2 RIB_IPV4_UNICAST entries.
            # NOTE: Assumes MRT type 13 with subtype 2; adjust if needed.
            if entry['mrt_header']['type'] == 13 and entry['mrt_header']['subtype'] == 2:
                # Obtain the raw prefix NLRI
                raw_prefix_nlri = entry['mrt_entry']['raw_prefix_nlri']
                # Loop through each RIB entry within the MRT record
                for rib_entry in entry['mrt_entry']['rib_entries']:
                    # Compute a bucket key based on the peer index and the hash of the path attributes
                    raw_path_attributes = b"".join(rib_entry['raw_bgp_attributes'])
                    bucket_key = f"{rib_entry['peer_index']}_{hash(raw_path_attributes)}"

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

        # Finalize each bucket to build the BMP messages and update metrics
        for bucket in buckets.values():
            bmp_message = bucket.finalize_bucket()
            memory['bytes_received'] += len(bmp_message)
            messages.append(bmp_message)

        # Store the timestamp
        db.set(b'target_timestamp', struct.pack('>d', timestamp))

        # Enqueue each final BMP message
        for message in messages:
            queue.put((message, 0, None, -1, True))

    except Exception as e:
        logger.error(
            f"RIB Injection Failed: {e}", exc_info=True)
        raise e

    # Set the injection event
    events['injection'].set()
