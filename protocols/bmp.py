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
from typing import List
import hashlib
import struct
import socket

class BMPv3:
    """
    Turn structured data into BMPv3 (RFC7854) messages.
    https://datatracker.ietf.org/doc/html/rfc7854

    # Author: Robin Röper <rroeper@ripe.net>

    The BMPv3 class provides methods to build various BGP and BMP messages, including
    KEEPALIVE, NOTIFICATION, UPDATE, and Peer Up/Down Notification messages. It also includes
    utility functions to encode prefixes and path attributes as per BGP specifications.

    Attributes:
        None

    Methods:
        build_bgp_keepalive_message() -> bytes:
            Build the BGP KEEPALIVE message in bytes.
        build_bgp_notification_message(notification_message: dict) -> bytes:
            Build the BGP NOTIFICATION message in bytes.
        build_bgp_update_message(update_message: dict) -> bytes:
            Build the BGP UPDATE message in bytes.
        build_bmp_per_peer_header(peer_ip: str, peer_asn: int, timestamp: float, collector: str) -> bytes:
            Build the BMP Per-Peer Header.
        construct_bmp_peer_up_message(peer_ip: str, peer_asn: int, timestamp: float) -> bytes:
            Construct a BMP Peer Up Notification message.
        construct_bmp_peer_down_message(peer_ip: str, peer_asn: int, timestamp: float, notification_message: dict) -> bytes:
            Construct a BMP Peer Down Notification message.
        encode_prefix(prefix: str) -> bytes:
            Encode a prefix into bytes as per BGP specification.
    """

    # BMP header lengths (not counting the version in the common hdr)
    BMP_HDRv3_LEN = 6             # BMP v3 header length, not counting the version
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

    @staticmethod
    def construct(collector: str, peer_ip: str, peer_asn: int, timestamp: float, msg_type: str, path=[], origin='IGP', community=[], announcements=[], withdrawals=[], state=None, med=None) -> List[bytes]:
        """
        Construct BMPv3 (RFC7854) messages.

        Args:
            collector (str): The collector name
            peer_ip (str): The peer IP address
            peer_asn (int): The peer AS number
            timestamp (float): The timestamp
            msg_type (str): The message type
            path (list): The AS path
            origin (str): The origin
            community (list): The community list
            announcements (list): The announcement list
            withdrawals (list): The withdrawal list
            state (str): The peer state
            med (int): The MED value

        Returns:
            List[bytes]: A list of BMP Route Monitoring, Keepalive, or Peer State messages in bytes
        """
        # Initialize the list of BMP messages
        bmp_messages = []

        # Handle UPDATE messages
        if msg_type.upper() == "UPDATE":
            # Common attributes
            common_attributes = {
                'origin': origin.lower(),
                'as-path': path,
                'community': community
            }

            if med is not None:
                common_attributes['med'] = med

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
                        nlri += BMPv3.encode_prefix(prefix)
                    attributes['nlri'] = nlri
                    update_message = {
                        'attribute': attributes,
                    }
                else:
                    # For IPv4, include NLRI in the update_message
                    nlri = b''
                    for prefix in prefixes:
                        nlri += BMPv3.encode_prefix(prefix)
                    update_message = {
                        'attribute': attributes,
                        'nlri': nlri,
                    }

                # Build BMP message
                bmp_message = BMPv3.construct_bmp_route_monitoring_message(
                    peer_ip=peer_ip,
                    peer_asn=peer_asn,
                    timestamp=timestamp,
                    update_message=update_message,
                    collector=collector
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
                        nlri += BMPv3.encode_prefix(prefix)
                    attributes['withdrawn_nlri'] = nlri
                    update_message = {
                        'attribute': attributes,
                    }
                else:
                    # For IPv4, withdrawals are in the BGP UPDATE message body
                    withdrawn_routes = b''
                    for prefix in withdrawals:
                        withdrawn_routes += BMPv3.encode_prefix(prefix)
                    update_message = {
                        'attribute': attributes,
                        'withdrawn_routes': withdrawn_routes,
                    }

                # Build BMP message
                bmp_message = BMPv3.construct_bmp_route_monitoring_message(
                    peer_ip=peer_ip,
                    peer_asn=peer_asn,
                    timestamp=timestamp,
                    update_message=update_message,
                    collector=collector
                )
                bmp_messages.append(bmp_message)

        # Handle KEEPALIVE messages
        elif msg_type.upper() == "KEEPALIVE":
            bmp_message = BMPv3.construct_bmp_keepalive_message(
                peer_ip=peer_ip,
                peer_asn=peer_asn,
                timestamp=timestamp,
                collector=collector
            )
            bmp_messages.append(bmp_message)

        # Handle RIS_PEER_STATE messages
        elif msg_type.upper() == "RIS_PEER_STATE":
            if state is None:
                raise ValueError("State must be provided for RIS_PEER_STATE messages")
            
            if state.lower() == 'connected':
                # Peer Up message
                bmp_message = BMPv3.construct_bmp_peer_up_message(
                    peer_ip=peer_ip,
                    peer_asn=peer_asn,
                    timestamp=timestamp,
                    collector=collector
                )
                bmp_messages.append(bmp_message)
            elif state.lower() == 'down':
                # Peer Down message
                bmp_message = BMPv3.construct_bmp_peer_down_message(
                    peer_ip=peer_ip,
                    peer_asn=peer_asn,
                    timestamp=timestamp,
                    notification_message={},
                    collector=collector
                )
                bmp_messages.append(bmp_message)

        return bmp_messages
        
    @staticmethod
    def construct_bmp_route_monitoring_message(peer_ip, peer_asn, timestamp, update_message, collector):
        """
        Construct a BMP Route Monitoring message containing a BGP UPDATE message.

        Args:
            peer_ip (str): The peer IP address
            peer_asn (int): The peer AS number
            timestamp (float): The timestamp
            update_message (dict): The BGP UPDATE message in dictionary form
            collector (str): The collector name

        Returns:
            bytes: The BMP message in bytes
        """
        # Build the BGP UPDATE message
        bgp_update = BMPv3.build_bgp_update_message(update_message)

        # Build the BMP Common Header
        bmp_msg_type = 0  # Route Monitoring
        per_peer_header = BMPv3.build_bmp_per_peer_header(peer_ip, peer_asn, timestamp, collector)

        total_length = BMPv3.BMP_HDRv3_LEN + len(per_peer_header) + len(bgp_update)

        bmp_common_header = struct.pack('!BIB', 3, total_length, bmp_msg_type)

        # Build the full BMP message
        bmp_message = bmp_common_header + per_peer_header + bgp_update

        return bmp_message

    @staticmethod
    def construct_bmp_keepalive_message(peer_ip, peer_asn, timestamp, collector):
        """
        Construct a BMP Route Monitoring message containing a BGP KEEPALIVE message.

        Args:
            peer_ip (str): The peer IP address.
            peer_asn (int): The peer AS number.
            timestamp (float): The timestamp.
            collector (str): The collector name.

        Returns:
            bytes: The BMP message in bytes.
        """
        # Build the BGP KEEPALIVE message
        bgp_keepalive = BMPv3.build_bgp_keepalive_message()

        # Build the BMP Common Header
        bmp_msg_type = 0  # Route Monitoring
        per_peer_header = BMPv3.build_bmp_per_peer_header(peer_ip, peer_asn, timestamp, collector)

        total_length = BMPv3.BMP_HDRv3_LEN + len(per_peer_header) + len(bgp_keepalive)

        bmp_common_header = struct.pack('!BIB', 3, total_length, bmp_msg_type)

        # Build the full BMP message
        bmp_message = bmp_common_header + per_peer_header + bgp_keepalive

        return bmp_message

    @staticmethod
    def construct_bmp_peer_up_message(peer_ip, peer_asn, timestamp, collector):
        """
        Construct a BMP Peer Up Notification message.

        Args:
            peer_ip (str): The peer IP address.
            peer_asn (int): The peer AS number.
            timestamp (float): The timestamp.
            collector (str): The collector name.

        Returns:
            bytes: The BMP message in bytes.
        """
        # For simplicity, we will not include all optional fields
        bmp_msg_type = 3  # Peer Up Notification
        per_peer_header = BMPv3.build_bmp_per_peer_header(peer_ip, peer_asn, timestamp, collector)

        # Local Address (16 bytes), Local Port (2 bytes), Remote Port (2 bytes), Sent OPEN Message, Received OPEN Message
        # For simplicity, we'll use placeholders
        local_address = b'\x00' * 16
        local_port = struct.pack('!H', 0)
        remote_port = struct.pack('!H', 179)
        sent_open_message = b''
        received_open_message = b''

        peer_up_msg = local_address + local_port + remote_port + sent_open_message + received_open_message

        total_length = BMPv3.BMP_HDRv3_LEN + len(per_peer_header) + len(peer_up_msg)

        bmp_common_header = struct.pack('!BIB', 3, total_length, bmp_msg_type)

        # Build the full BMP message
        bmp_message = bmp_common_header + per_peer_header + peer_up_msg

        return bmp_message

    @staticmethod
    def construct_bmp_peer_down_message(peer_ip, peer_asn, timestamp, notification_message, collector):
        """
        Construct a BMP Peer Down Notification message.

        Args:
            peer_ip (str): The peer IP address.
            peer_asn (int): The peer AS number.
            timestamp (float): The timestamp.
            notification_message (dict): The BGP Notification message in dictionary form.
            collector (str): The collector name.
            
        Returns:
            bytes: The BMP message in bytes.
        """
        # Build the BGP Notification message
        bgp_notification = BMPv3.build_bgp_notification_message(notification_message)

        # Build the BMP Common Header
        bmp_msg_type = 2  # Peer Down Notification
        per_peer_header = BMPv3.build_bmp_per_peer_header(peer_ip, peer_asn, timestamp, collector)

        # Reason: 1-byte code indicating the reason. For simplicity, use 1 (Local system closed the session)
        reason = struct.pack('!B', 1)  # Reason Code 1

        total_length = BMPv3.BMP_HDRv3_LEN + len(per_peer_header) + len(reason) + len(bgp_notification)

        bmp_common_header = struct.pack('!BIB', 3, total_length, bmp_msg_type)

        # Build the full BMP message
        bmp_message = bmp_common_header + per_peer_header + reason + bgp_notification

        return bmp_message

    @staticmethod
    def build_bgp_update_message(update_message):
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

        # Process 'withdrawn_routes'
        if 'withdrawn_routes' in update_message:
            withdrawn_routes = update_message['withdrawn_routes']
            withdrawn_routes_length = len(withdrawn_routes)

        # Process 'announce'
        if 'announce' in update_message:
            # NLRI
            announce = update_message['announce']
            # Prepare lists to hold IPv4 and IPv6 prefixes
            ipv4_prefixes = []
            ipv6_prefixes = []
            for afi_safi in announce:
                prefixes_dict = announce[afi_safi]
                for prefix in prefixes_dict:
                    prefix_bytes = BMPv3.encode_prefix(prefix)
                    if ':' in prefix:
                        ipv6_prefixes.append(prefix_bytes)
                    else:
                        ipv4_prefixes.append(prefix_bytes)

            # For IPv4, include prefixes in NLRI field
            if ipv4_prefixes:
                nlri += b''.join(ipv4_prefixes)

            # For IPv6, include prefixes in MP_REACH_NLRI attribute
            if ipv6_prefixes:
                # Build MP_REACH_NLRI attribute
                afi = 2  # IPv6
                safi = 1  # Unicast
                next_hop = update_message['attribute'].get('next-hop', ['::'])[0]
                next_hop_bytes = socket.inet_pton(socket.AF_INET6, next_hop)
                nh_length = len(next_hop_bytes)
                nlri_bytes = b''.join(ipv6_prefixes)
                mp_reach_nlri = struct.pack('!HBB', afi, safi, nh_length) + next_hop_bytes + b'\x00' + nlri_bytes

                # Add the MP_REACH_NLRI attribute to the path attributes
                attr_flags = 0x80  # Optional
                attr_type = 14  # MP_REACH_NLRI
                attr_length = len(mp_reach_nlri)
                if attr_length > 255:
                    attr_flags |= 0x10  # Extended Length
                    path_attributes += struct.pack('!BBH', attr_flags, attr_type, attr_length)
                else:
                    path_attributes += struct.pack('!BBB', attr_flags, attr_type, attr_length)
                path_attributes += mp_reach_nlri

        # Process 'attribute'
        if 'attribute' in update_message:
            attributes = update_message['attribute']
            # Now encode other path attributes
            path_attributes += BMPv3.encode_path_attributes(attributes)

        # Update total_path_attribute_length after adding all attributes
        total_path_attribute_length = len(path_attributes)

        # Build the UPDATE message
        # Withdrawn Routes Length (2 bytes)
        bgp_update = struct.pack('!H', withdrawn_routes_length)
        bgp_update += withdrawn_routes
        # Total Path Attribute Length (2 bytes)
        bgp_update += struct.pack('!H', total_path_attribute_length)
        bgp_update += path_attributes
        # NLRI (only for IPv4 prefixes)
        bgp_update += nlri

        # Now build the BGP Message Header
        # Marker: 16 bytes of 0xFF
        marker = b'\xFF' * 16
        length = 19 + len(bgp_update)  # Correct total length
        msg_type = 2  # UPDATE message

        # Correctly pack the header
        bgp_common_header = struct.pack('!HB', length, msg_type)

        # Build the full BGP message
        bgp_message = marker + bgp_common_header + bgp_update

        return bgp_message

    @staticmethod
    def encode_prefix(prefix):
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

    @staticmethod
    def encode_path_attributes(attributes):
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
            max_segment_length = 255  # Maximum length of a single AS_PATH segment in bytes

            def encode_as_segment(segment_type, as_numbers):
                """Helper function to encode an AS segment."""
                segment_length = len(as_numbers)
                segment_value = b''.join(struct.pack('!I', int(asn)) for asn in as_numbers)
                return struct.pack('!BB', segment_type, segment_length) + segment_value

            for element in as_path:
                if isinstance(element, list):
                    # AS_SET
                    if current_segment:
                        # Flush current AS_SEQUENCE segment if present
                        segments.append((2, current_segment))
                        current_segment = []
                    segments.append((1, element))  # AS_SET
                else:
                    # AS_SEQUENCE
                    current_segment.append(element)

                    # If the length of the current segment exceeds the byte limit, split it
                    if len(current_segment) * 4 + 2 > max_segment_length:
                        segments.append((2, current_segment[:max_segment_length // 4]))
                        current_segment = current_segment[max_segment_length // 4:]

            if current_segment:
                segments.append((2, current_segment))  # Flush remaining AS_SEQUENCE

            # Build the AS_PATH attribute with potential splitting
            for segment_type, as_numbers in segments:
                segment_encoded = encode_as_segment(segment_type, as_numbers)
                if len(segment_encoded) > max_segment_length:
                    # Split if the segment exceeds the maximum length
                    for i in range(0, len(as_numbers), max_segment_length // 4):
                        sub_segment = as_numbers[i:i + max_segment_length // 4]
                        as_path_value += encode_as_segment(segment_type, sub_segment)
                else:
                    as_path_value += segment_encoded

            attr_length = len(as_path_value)
            if attr_length > 255:
                # Use Extended Length if the overall length exceeds 255 bytes
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
                    if len(comm) == 2:
                        # Standard community: (ASN, sub-identifier)
                        asn, value = comm
                        community_value += struct.pack('!HH', int(asn), int(value))
                    elif len(comm) == 3:
                        # Large community: (ASN, sub-identifier1, sub-identifier2)
                        asn, value1, value2 = comm
                        community_value += struct.pack('!III', int(asn), int(value1), int(value2))
                        attr_type = 32  # Attribute type for large community

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

    @staticmethod
    def build_bgp_keepalive_message():
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

    @staticmethod
    def build_bgp_notification_message(notification_message):
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

    @staticmethod
    def build_bmp_per_peer_header(peer_ip, peer_asn, timestamp, collector):
        """
        Build the BMP Per-Peer Header.

        Args:
            peer_ip (str): The peer IP address.
            peer_asn (int): The peer AS number.
            timestamp (float): The timestamp.
            collector (str): The collector name.

        Returns:
            bytes: The Per-Peer Header in bytes.
        """
        peer_type = 0  # Global Instance Peer
        peer_flags = 0
        peer_distinguisher = hashlib.sha256(collector.encode('utf-8')).digest()[:8]

        # Peer Address (16 bytes): IPv4 mapped into IPv6
        if ':' in peer_ip:
            # IPv6 address
            peer_address = socket.inet_pton(socket.AF_INET6, peer_ip)
            peer_flags |= 0x80  # Set the 'IPv6 Peer' flag (bit 0)
        else:
            # IPv4 address
            peer_address = b'\x00' * 12 + socket.inet_pton(socket.AF_INET, peer_ip)
            # 'IPv6 Peer' flag remains unset (IPv4)

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
