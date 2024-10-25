"""
BGPDATA - BGP Data Collection and Analytics Service

This software is part of the BGPDATA project, which is designed to collect, process, and analyze BGP data from various sources.
It helps researchers and network operators get insights into their network by providing a scalable and reliable way to collect and process BGP data.

Author: Robin Röper

© 2024 BGPDATA. All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
1. Redistributions of source code must retain the above copyright notice, this list of conditions, and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions, and the following disclaimer in the documentation and/or other materials provided with the distribution.
3. Neither the name of BGPDATA nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""
from confluent_kafka import KafkaError, Consumer
from sqlalchemy.ext.asyncio import create_async_engine
from datetime import datetime, timedelta
from typing import List
from io import BytesIO
import fastavro
import logging
import asyncio
import struct
import socket
import time
import json
import zlib
import sys
import os
import re

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

class BMPConverter:
    """
    A class to convert data into BMP (RFC7854) messages.
    https://datatracker.ietf.org/doc/html/rfc7854

    # Author: Robin Röper <rroeper@ripe.net>
    """
    # BMP header lengths (not counting the version in the common hdr)
    BMP_HDRv3_LEN = 5             # BMP v3 header length, not counting the version
    BMP_HDRv1v2_LEN = 43
    BMP_PEER_HDR_LEN = 42         # BMP peer header length
    BMP_INFO_TLV_HDR_LEN = 4      # BMP info message header length, does not count the info field
    BMP_MIRROR_TLV_HDR_LEN = 4    # BMP route mirroring TLV header length
    BMP_TERM_MSG_LEN = 4          # BMP term message header length, does not count the info field
    BMP_PEER_UP_HDR_LEN = 20      # BMP peer up event header size not including the recv/sent open param message
    BMP_PACKET_BUF_SIZE = 68000   # Size of the BMP packet buffer (memory)

    # BGP constants
    BGP_MAX_MSG_SIZE = 65535      # Max payload size - Larger than RFC4271 of 4096
    BGP_MSG_HDR_LEN = 19          # BGP message header size
    BGP_OPEN_MSG_MIN_LEN = 29     # Includes the expected header size
    BGP_VERSION = 4
    BGP_CAP_PARAM_TYPE = 2
    BGP_AS_TRANS = 23456          # BGP ASN when AS exceeds 16bits

    def __init__(self):
        """
        Initialize the BMPConverter class.

        This constructor sets up the necessary configurations and state for the BMPConverter class.
        It prepares the class to convert exaBGP JSON messages into BMP messages, which can be used
        for monitoring and managing BGP sessions.

        The BMPConverter class provides methods to build various BGP and BMP messages, including
        KEEPALIVE, NOTIFICATION, UPDATE, and Peer Up/Down Notification messages. It also includes
        utility functions to encode prefixes and path attributes as per BGP specifications.

        Attributes:
            None

        Methods:
            exabgp_to_bmp(exabgp_message: str) -> List[bytes]:
                Convert an exaBGP JSON message to a list of BMP messages.
            build_bgp_keepalive_message() -> bytes:
                Build the BGP KEEPALIVE message in bytes.
            build_bgp_notification_message(notification_message: dict) -> bytes:
                Build the BGP NOTIFICATION message in bytes.
            build_bgp_update_message(update_message: dict) -> bytes:
                Build the BGP UPDATE message in bytes.
            build_bmp_per_peer_header(peer_ip: str, peer_asn: int, timestamp: float) -> bytes:
                Build the BMP Per-Peer Header.
            construct_bmp_peer_up_message(peer_ip: str, peer_asn: int, timestamp: float) -> bytes:
                Construct a BMP Peer Up Notification message.
            construct_bmp_peer_down_message(peer_ip: str, peer_asn: int, timestamp: float, notification_message: dict) -> bytes:
                Construct a BMP Peer Down Notification message.
            encode_prefix(prefix: str) -> bytes:
                Encode a prefix into bytes as per BGP specification.
        """
    def exabgp_to_bmp(self, exabgp_message: str) -> List[bytes]:
        """
        Convert an exaBGP JSON message to a list of BMP messages.

        Args:
            exabgp_message (str): The exaBGP JSON string

        Returns:
            List[bytes]: A list of BMP Route Monitoring, Keepalive, or Peer State messages in bytes
        """
        data = json.loads(exabgp_message)

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
                        nlri += self.encode_prefix(prefix)
                    attributes['nlri'] = nlri
                    update_message = {
                        'attribute': attributes,
                    }
                else:
                    # For IPv4, include NLRI in the update_message
                    nlri = b''
                    for prefix in prefixes:
                        nlri += self.encode_prefix(prefix)
                    update_message = {
                        'attribute': attributes,
                        'nlri': nlri,
                    }

                # Build BMP message
                bmp_message = self.construct_bmp_route_monitoring_message(
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
                        nlri += self.encode_prefix(prefix)
                    attributes['withdrawn_nlri'] = nlri
                    update_message = {
                        'attribute': attributes,
                    }
                else:
                    # For IPv4, withdrawals are in the BGP UPDATE message body
                    withdrawn_routes = b''
                    for prefix in withdrawals:
                        withdrawn_routes += self.encode_prefix(prefix)
                    update_message = {
                        'attribute': attributes,
                        'withdrawn_routes': withdrawn_routes,
                    }

                # Build BMP message
                bmp_message = self.construct_bmp_route_monitoring_message(
                    peer_ip=peer_ip,
                    peer_asn=peer_asn,
                    timestamp=timestamp,
                    update_message=update_message
                )
                bmp_messages.append(bmp_message)

        # Handle KEEPALIVE messages
        elif msg_type == "KEEPALIVE":
            bmp_message = self.construct_bmp_keepalive_message(
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
                bmp_message = self.construct_bmp_peer_up_message(
                    peer_ip=peer_ip,
                    peer_asn=peer_asn,
                    timestamp=timestamp
                )
                bmp_messages.append(bmp_message)
            elif state.lower() == 'down':
                # Peer Down message
                bmp_message = self.construct_bmp_peer_down_message(
                    peer_ip=peer_ip,
                    peer_asn=peer_asn,
                    timestamp=timestamp,
                    notification_message={}
                )
                bmp_messages.append(bmp_message)

        return bmp_messages

    def construct_bmp_route_monitoring_message(self, peer_ip, peer_asn, timestamp, update_message):
        """
        Construct a BMP Route Monitoring message containing a BGP UPDATE message.

        Args:
            peer_ip (str): The peer IP address
            peer_asn (int): The peer AS number
            timestamp (float): The timestamp
            update_message (dict): The BGP UPDATE message in dictionary form

        Returns:
            bytes: The BMP message in bytes
        """
        # Build the BGP UPDATE message
        bgp_update = self.build_bgp_update_message(update_message)

        # Build the BMP Common Header
        bmp_msg_type = 0  # Route Monitoring
        per_peer_header = self.build_bmp_per_peer_header(peer_ip, peer_asn, timestamp)

        total_length = self.BMP_HDRv3_LEN + len(per_peer_header) + len(bgp_update)

        bmp_common_header = struct.pack('!BIB', 3, total_length, bmp_msg_type)

        # Build the full BMP message
        bmp_message = bmp_common_header + per_peer_header + bgp_update

        return bmp_message

    def construct_bmp_keepalive_message(self, peer_ip, peer_asn, timestamp):
        """
        Construct a BMP Route Monitoring message containing a BGP KEEPALIVE message.

        Args:
            peer_ip (str): The peer IP address.
            peer_asn (int): The peer AS number.
            timestamp (float): The timestamp.

        Returns:
            bytes: The BMP message in bytes.
        """
        # Build the BGP KEEPALIVE message
        bgp_keepalive = self.build_bgp_keepalive_message()

        # Build the BMP Common Header
        bmp_msg_type = 0  # Route Monitoring
        per_peer_header = self.build_bmp_per_peer_header(peer_ip, peer_asn, timestamp)

        total_length = self.BMP_HDRv3_LEN + len(per_peer_header) + len(bgp_keepalive)

        bmp_common_header = struct.pack('!BIB', 3, total_length, bmp_msg_type)

        # Build the full BMP message
        bmp_message = bmp_common_header + per_peer_header + bgp_keepalive

        return bmp_message

    def construct_bmp_peer_up_message(self, peer_ip, peer_asn, timestamp):
        """
        Construct a BMP Peer Up Notification message.

        Args:
            peer_ip (str): The peer IP address.
            peer_asn (int): The peer AS number.
            timestamp (float): The timestamp.

        Returns:
            bytes: The BMP message in bytes.
        """
        # For simplicity, we will not include all optional fields
        bmp_msg_type = 3  # Peer Up Notification
        per_peer_header = self.build_bmp_per_peer_header(peer_ip, peer_asn, timestamp)

        # Local Address (16 bytes), Local Port (2 bytes), Remote Port (2 bytes), Sent OPEN Message, Received OPEN Message
        # For simplicity, we'll use placeholders
        local_address = b'\x00' * 16
        local_port = struct.pack('!H', 0)
        remote_port = struct.pack('!H', 179)
        sent_open_message = b''
        received_open_message = b''

        peer_up_msg = local_address + local_port + remote_port + sent_open_message + received_open_message

        total_length = self.BMP_HDRv3_LEN + len(per_peer_header) + len(peer_up_msg)

        bmp_common_header = struct.pack('!BIB', 3, total_length, bmp_msg_type)

        # Build the full BMP message
        bmp_message = bmp_common_header + per_peer_header + peer_up_msg

        return bmp_message

    def construct_bmp_peer_down_message(self, peer_ip, peer_asn, timestamp, notification_message):
        """
        Construct a BMP Peer Down Notification message.

        Args:
            peer_ip (str): The peer IP address.
            peer_asn (int): The peer AS number.
            timestamp (float): The timestamp.
            notification_message (dict): The BGP Notification message in dictionary form.

        Returns:
            bytes: The BMP message in bytes.
        """
        # Build the BGP Notification message
        bgp_notification = self.build_bgp_notification_message(notification_message)

        # Build the BMP Common Header
        bmp_msg_type = 2  # Peer Down Notification
        per_peer_header = self.build_bmp_per_peer_header(peer_ip, peer_asn, timestamp)

        # Reason: 1-byte code indicating the reason. For simplicity, use 1 (Local system closed the session)
        reason = struct.pack('!B', 1)  # Reason Code 1

        total_length = self.BMP_HDRv3_LEN + len(per_peer_header) + len(reason) + len(bgp_notification)

        bmp_common_header = struct.pack('!BIB', 3, total_length, bmp_msg_type)

        # Build the full BMP message
        bmp_message = bmp_common_header + per_peer_header + reason + bgp_notification

        return bmp_message

    def build_bgp_update_message(self, update_message):
        """
        Build the BGP UPDATE message in bytes.

        Args:
            update_message (dict): The update message dictionary

        Returns:
            bytes: The BGP UPDATE message in bytes
        """
        # Initialize components
        withdrawn_routes = b''
        withdrawn_routes_length = 0
        total_path_attribute_length = 0
        path_attributes = b''
        nlri = b''

        # Process 'withdraw'
        #if 'withdrawn_routes' in update_message:
        #    # Withdrawn Routes
        #    withdraw = update_message['withdrawn_routes']
        #    for afi_safi in withdraw:
        #        prefixes = withdraw[afi_safi]
        #        for prefix in prefixes:
        #            prefix_bytes = self.encode_prefix(prefix)
        #            withdrawn_routes += prefix_bytes
#
        #    withdrawn_routes_length = len(withdrawn_routes)

        # Process 'withdrawn_routes'
        if 'withdrawn_routes' in update_message:
            withdrawn_routes = update_message['withdrawn_routes']
            withdrawn_routes_length = len(withdrawn_routes)

        # Process 'attribute'
        if 'attribute' in update_message:
            # Path Attributes
            attributes = update_message['attribute']
            path_attributes = self.encode_path_attributes(attributes)
            total_path_attribute_length = len(path_attributes)

        # Process 'announce'
        if 'announce' in update_message:
            # NLRI
            announce = update_message['announce']
            for afi_safi in announce:
                prefixes_dict = announce[afi_safi]
                for prefix in prefixes_dict:
                    prefix_bytes = self.encode_prefix(prefix)
                    if ':' in prefix:
                        nlri += b'' # Empty for IPv6
                    else:
                        nlri += prefix_bytes

        # Process the Network Layer Reachability Information or NLRI
        if 'nlri' in update_message:
            nlri = update_message['nlri']

        # Build the UPDATE message
        # Withdrawn Routes Length (2 bytes)
        bgp_update = struct.pack('!H', withdrawn_routes_length)
        bgp_update += withdrawn_routes
        # Total Path Attribute Length (2 bytes)
        bgp_update += struct.pack('!H', total_path_attribute_length)
        bgp_update += path_attributes
        # NLRI
        bgp_update += nlri

        # Now build the BGP Message Header
        # Marker: 16 bytes of 0xFF
        marker = b'\xFF' * 16
        length = 19 + len(bgp_update)
        msg_type = 2  # UPDATE message

        bgp_message = marker + struct.pack('!HB', length, msg_type) + bgp_update

        return bgp_message

    def encode_prefix(self, prefix):
        """
        Encode a prefix into bytes as per BGP specification.

        Args:
            prefix (str): The prefix string, e.g., '192.0.2.0/24'

        Returns:
            bytes: The encoded prefix in bytes
        """
        # Split prefix and prefix length
        ip, prefix_length = prefix.split('/')
        prefix_length = int(prefix_length)
        if ':' in ip:
            # IPv6
            ip_bytes = socket.inet_pton(socket.AF_INET6, ip)
        else:
            # IPv4
            ip_bytes = socket.inet_pton(socket.AF_INET, ip)

        # Calculate the number of octets required to represent the prefix
        num_octets = (prefix_length + 7) // 8
        # Truncate the ip_bytes to num_octets
        ip_bytes = ip_bytes[:num_octets]
        # Build the prefix in bytes
        prefix_bytes = struct.pack('!B', prefix_length) + ip_bytes
        return prefix_bytes

    def encode_path_attributes(self, attributes):
        """
        Encode path attributes into bytes as per BGP specification.

        Args:
            attributes (dict): Dictionary of path attributes

        Returns:
            bytes: The encoded path attributes in bytes
        """
        path_attributes = b''

        # Origin
        if 'origin' in attributes:
            origin = attributes['origin']
            # Origin is 1 byte: 0=IGP, 1=EGP, 2=INCOMPLETE
            origin_value = {'igp': 0, 'egp': 1, 'incomplete': 2}.get(origin.lower(), 2)
            # Attribute Flags: Optional (0), Transitive (1), Partial (0), Extended Length (0)
            attr_flags = 0x40  # Transitive
            attr_type = 1
            attr_length = 1  # 1 byte
            attr_value = struct.pack('!B', origin_value)
            path_attributes += struct.pack('!BBB', attr_flags, attr_type, attr_length) + attr_value

        # AS_PATH
        if 'as-path' in attributes:
            as_path = attributes['as-path']
            attr_flags = 0x40  # Transitive
            attr_type = 2
            as_path_value = b''

            # Process each segment in the AS_PATH
            segments = []
            current_segment = []
            for element in as_path:
                if isinstance(element, list):
                    # AS_SET
                    if current_segment:
                        # Flush previous AS_SEQUENCE segment
                        segments.append((2, current_segment))
                        current_segment = []
                    segments.append((1, element))  # AS_SET
                else:
                    # AS_SEQUENCE
                    current_segment.append(element)

            if current_segment:
                segments.append((2, current_segment))  # AS_SEQUENCE

            # Build the AS_PATH attribute
            for segment_type, as_numbers in segments:
                segment_length = len(as_numbers)
                segment_value = b''
                for asn in as_numbers:
                    segment_value += struct.pack('!I', int(asn))
                as_path_value += struct.pack('!BB', segment_type, segment_length) + segment_value

            attr_length = len(as_path_value)
            if attr_length > 255:
                # Extended Length
                attr_flags |= 0x10  # Set Extended Length flag
                path_attributes += struct.pack('!BBH', attr_flags, attr_type, attr_length)
            else:
                path_attributes += struct.pack('!BBB', attr_flags, attr_type, attr_length)
            path_attributes += as_path_value

        # NEXT_HOP and MP_REACH_NLRI
        if 'next-hop' in attributes:
            next_hop = attributes['next-hop']
            afi = attributes.get('afi', 1)  # Default to IPv4
            safi = attributes.get('safi', 1)  # Default to unicast
            if afi == 2:
                # IPv6
                # Handle MP_REACH_NLRI
                next_hop_bytes = b''
                for nh in next_hop:
                    # Determine if next hop is IPv4 or IPv6
                    if ':' in nh:
                        # Next hop is IPv6
                        next_hop_bytes += socket.inet_pton(socket.AF_INET6, nh)
                    else:
                        # Next hop is IPv4, encode per RFC 5549
                        next_hop_bytes += socket.inet_pton(socket.AF_INET, nh)

                attr_flags = 0x80  # Optional
                attr_type = 14  # MP_REACH_NLRI
                nlri = attributes.get('nlri', b'')
                nh_length = len(next_hop_bytes)
                mp_reach_nlri = struct.pack('!HBB', afi, safi, nh_length) + next_hop_bytes + b'\x00' + nlri
                attr_length = len(mp_reach_nlri)
                if attr_length > 255:
                    attr_flags |= 0x10  # Extended Length
                    path_attributes += struct.pack('!BBH', attr_flags, attr_type, attr_length)
                else:
                    path_attributes += struct.pack('!BBB', attr_flags, attr_type, attr_length)
                path_attributes += mp_reach_nlri
            else:
                # IPv4
                # NEXT_HOP attribute
                next_hop_bytes = socket.inet_pton(socket.AF_INET, next_hop[0])
                attr_flags = 0x40  # Transitive
                attr_type = 3
                attr_length = 4  # IPv4 address
                attr_value = next_hop_bytes
                path_attributes += struct.pack('!BBB', attr_flags, attr_type, attr_length) + attr_value

        # Handle MP_UNREACH_NLRI for IPv6 withdrawals
        if 'withdrawn_nlri' in attributes:
            afi = attributes.get('afi', 2)
            safi = attributes.get('safi', 1)
            withdrawn_nlri = attributes['withdrawn_nlri']
            attr_flags = 0x80  # Optional
            attr_type = 15  # MP_UNREACH_NLRI
            mp_unreach_nlri = struct.pack('!HB', afi, safi) + withdrawn_nlri
            attr_length = len(mp_unreach_nlri)
            if attr_length > 255:
                attr_flags |= 0x10  # Extended Length
                path_attributes += struct.pack('!BBH', attr_flags, attr_type, attr_length)
            else:
                path_attributes += struct.pack('!BBB', attr_flags, attr_type, attr_length)
            path_attributes += mp_unreach_nlri

        # COMMUNITY
        if 'community' in attributes:
            community = attributes['community']
            if community:
                attr_flags = 0xC0  # Optional and Transitive
                attr_type = 8
                community_value = b''
                for comm in community:
                    asn, value = comm
                    community_value += struct.pack('!HH', int(asn), int(value))
                attr_length = len(community_value)
                if attr_length > 255:
                    attr_flags |= 0x10  # Extended Length
                    path_attributes += struct.pack('!BBH', attr_flags, attr_type, attr_length)
                else:
                    path_attributes += struct.pack('!BBB', attr_flags, attr_type, attr_length)
                path_attributes += community_value

        # MED
        if 'med' in attributes:
            med = int(attributes['med'])
            attr_flags = 0x80  # Optional
            attr_type = 4
            attr_length = 4
            attr_value = struct.pack('!I', med)
            path_attributes += struct.pack('!BBB', attr_flags, attr_type, attr_length) + attr_value

        # TODO:Additional attributes can be added similarly

        return path_attributes

    def build_bgp_keepalive_message(self):
        """
        Build the BGP KEEPALIVE message.

        Args:
            None

        Returns:
            bytes: The BGP KEEPALIVE message in bytes.
        """
        # Marker: 16 bytes of 0xFF
        marker = b'\xFF' * 16
        length = 19  # Header only
        msg_type = 4  # KEEPALIVE message
        bgp_message = marker + struct.pack('!HB', length, msg_type)
        return bgp_message

    def build_bgp_notification_message(self, notification_message):
        """
        Build the BGP NOTIFICATION message in bytes.

        Args:
            notification_message (dict): The notification message dictionary.

        Returns:
            bytes: The BGP NOTIFICATION message in bytes.
        """
        # Extract error code and subcode
        error_code = int(notification_message.get('code', 0))
        error_subcode = int(notification_message.get('subcode', 0))
        data = notification_message.get('data', b'')

        # Build the NOTIFICATION message
        notification = struct.pack('!BB', error_code, error_subcode) + data

        # Now build the BGP Message Header
        # Marker: 16 bytes of 0xFF
        marker = b'\xFF' * 16
        length = 19 + len(notification)
        msg_type = 3  # NOTIFICATION message

        bgp_message = marker + struct.pack('!HB', length, msg_type) + notification

        return bgp_message

    def build_bmp_per_peer_header(self, peer_ip, peer_asn, timestamp):
        """
        Build the BMP Per-Peer Header.

        Args:
            peer_ip (str): The peer IP address.
            peer_asn (int): The peer AS number.
            timestamp (float): The timestamp.

        Returns:
            bytes: The Per-Peer Header in bytes.
        """
        logger.debug(f"Building BMP Per-Peer Header for {peer_ip} with AS{peer_asn}")
        peer_type = 0  # Global Instance Peer
        peer_flags = 0
        # Peer Distinguisher (8 bytes): set to zero for Global Instance Peer
        peer_distinguisher = b'\x00' * 8
        # Peer Address (16 bytes): IPv4 mapped into IPv6
        if ':' in peer_ip:
            # IPv6 address
            peer_address = socket.inet_pton(socket.AF_INET6, peer_ip)
        else:
            # IPv4 address
            peer_address = b'\x00' * 12 + socket.inet_pton(socket.AF_INET, peer_ip)

        # For Peer BGP ID, we'll use zeros (could be improved)
        peer_bgp_id = b'\x00' * 4

        # Convert peer_asn to 4-byte big-endian byte array
        peer_as_bytes = struct.pack('!I', peer_asn)

        ts_seconds = int(timestamp)
        ts_microseconds = int((timestamp - ts_seconds) * 1e6)

        per_peer_header = struct.pack('!BB8s16s4s4sII',
                                      peer_type,
                                      peer_flags,
                                      peer_distinguisher,
                                      peer_address,
                                      peer_as_bytes,
                                      peer_bgp_id,
                                      ts_seconds,
                                      ts_microseconds)
        return per_peer_header


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
        await asyncio.sleep(5)  # Sleep for 5 seconds before logging

        # Compute kbps_counter
        bytes_sent = status['bytes_sent_since_last_log']
        kbps_counter = (bytes_sent * 8) / 5 / 1000  # Convert bytes to kilobits per second

        logger.info(f"At time: {status['timestamp']}, "
                    f"Time lag: {status['time_lag'].total_seconds()} seconds, "
                    f"Poll interval: {status['poll_interval']} seconds, "
                    f"Transmitting at ~{kbps_counter:.2f} kbit/s")

        # Reset bytes_sent_since_last_log
        status['bytes_sent_since_last_log'] = 0

async def main():
    """
    Main function to consume messages from Kafka, process them, and insert into OpenBMP.

    This asynchronous function sets up a Kafka consumer, subscribes to the specified topic,
    and continuously polls for messages. It processes messages in batches, dynamically
    adjusts polling intervals based on message lag, and handles various error scenarios.

    The function performs the following key operations:
    1. Sets up a Kafka consumer with specified configuration and callbacks.
    2. Implements batch processing of messages with a configurable threshold.
    3. Dynamically adjusts polling intervals based on message time lag.
    4. Processes messages, including deserialization to exaBGP JSON and conversion to BMP (RFC7854).
    5. Inserts processed BMP messages into the OpenBMP Kafka topic.
    6. Handles various error scenarios and implements retry logic.

    The function runs indefinitely until interrupted or an unhandled exception occurs.
    """
    # Create Kafka consumer
    consumer = Consumer(consumer_conf)
    consumer.subscribe(['ris-live'])

    # Create OpenBMP Socket with retry until timeout
    timeout = int(os.getenv('OPENBMP_COLLECTOR_TIMEOUT', 30))  # Default to 30 seconds if not set
    host = os.getenv('OPENBMP_COLLECTOR_HOST')
    port = int(os.getenv('OPENBMP_COLLECTOR_PORT'))

    loop = asyncio.get_event_loop()
    start_time = time.time()

    while True:
        try:
            # Create a non-blocking socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setblocking(False)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)  # Disable Nagle's algorithm for low-latency sending
            await loop.sock_connect(sock, (host, port))  # Attempt to connect
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

    # Initialize the converter
    converter = BMPConverter()

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
                messages = converter.exabgp_to_bmp(parsed['ris_live'])
                
                # Send each BMP message individually over the persistent TCP connection
                try:
                    for message in messages:
                        # Send the message over the persistent TCP connection
                        sock.sendall(message)

                        # Update the bytes sent counter
                        status['bytes_sent_since_last_log'] += len(message)

                except Exception as e:
                    # Handle exceptions
                    logger.error("TCP connection ended unexpectedly or encountered an error. The service will terminate in 10 seconds.", exc_info=True)
                    await asyncio.sleep(10)
                    sys.exit("Service terminated due to broken TCP connection, exiting.")

                # Commit the offset after successful processing and transmission
                consumer.commit()

            except Exception as e:
                logger.error("Failed to process message, retrying in %d seconds...", FAILURE_RETRY_DELAY, exc_info=True)
                # Wait before retrying the message to avoid overwhelming Kafka
                await asyncio.sleep(FAILURE_RETRY_DELAY)

    except Exception as e:
        logger.error("Fatal error", exc_info=True)
    finally:
        consumer.close()
        # Cancel the logging task when exiting
        logging_task.cancel()

if __name__ == "__main__":
    asyncio.run(main())
