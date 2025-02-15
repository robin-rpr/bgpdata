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
import hashlib
import struct
import socket

class BMPv3:
    """
    Turn structured data into BMPv3 (RFC7854) messages.
    https://datatracker.ietf.org/doc/html/rfc7854

    # Author: Robin Röper <rroeper@ripe.net>

    The BMPv3 class provides methods to build various BMP messages for
    Border Gateway Protocol (BGP) OPEN, UPDATE, NOTIFICATION, and KEEPALIVE messages.
    """

    # BMP header length (not counting the version in the common hdr)
    BMP_HDRv3_LEN = 6 # BMP v3 header length, not counting the version

    @staticmethod
    def monitoring_message(peer_ip, peer_asn, timestamp, bgp_update, collector):
        """
        Construct a BMP Route Monitoring message containing a BGP UPDATE message.

        Args:
            peer_ip (str): The peer IP address
            peer_asn (int): The peer AS number
            timestamp (float): The timestamp
            bgp_update (bytes): The BGP UPDATE message in bytes
            collector (str): The collector name

        Returns:
            bytes: The BMP message in bytes
        """
        # Build the BMP Common Header
        bmp_msg_type = 0  # Route Monitoring
        per_peer_header = BMPv3.per_peer_header(peer_ip, peer_asn, timestamp, collector)

        total_length = BMPv3.BMP_HDRv3_LEN + len(per_peer_header) + len(bgp_update)

        bmp_common_header = struct.pack('!BIB', 3, total_length, bmp_msg_type)

        # Build the full BMP message
        bmp_message = bmp_common_header + per_peer_header + bgp_update

        return bmp_message

    @staticmethod
    def keepalive_message(peer_ip, peer_asn, timestamp, bgp_keepalive, collector):
        """
        Construct a BMP Route Monitoring message containing a BGP KEEPALIVE message.

        Args:
            peer_ip (str): The peer IP address.
            peer_asn (int): The peer AS number.
            timestamp (float): The timestamp.
            bgp_keepalive (bytes): The BGP KEEPALIVE message in bytes.
            collector (str): The collector name.

        Returns:
            bytes: The BMP message in bytes.
        """
        # Build the BMP Common Header
        bmp_msg_type = 0  # Route Monitoring
        per_peer_header = BMPv3.per_peer_header(peer_ip, peer_asn, timestamp, collector)

        total_length = BMPv3.BMP_HDRv3_LEN + len(per_peer_header) + len(bgp_keepalive)

        bmp_common_header = struct.pack('!BIB', 3, total_length, bmp_msg_type)

        # Build the full BMP message
        bmp_message = bmp_common_header + per_peer_header + bgp_keepalive

        return bmp_message

    @staticmethod
    def init_message(router_name, router_descr):
        """
        Construct a BMP INIT message similar to Script 1's `getInitMessage`.
        
        Args:
            router_name (str): The router name being monitored.
            router_descr (str): The router description.

        Returns:
            bytes: The BMP INIT message in bytes.
        """
        # Encode router description and name
        router_descr_bytes = router_descr.encode('utf-8')
        router_name_bytes = router_name.encode('utf-8')

        # sysDescr TLV (Type=1)
        sysDescr_tlv = struct.pack('!HH', 1, len(router_descr_bytes)) + router_descr_bytes

        # sysName TLV (Type=2)
        sysName_tlv = struct.pack('!HH', 2, len(router_name_bytes)) + router_name_bytes

        # Calculate the total TLV length
        total_tlv_length = len(sysDescr_tlv) + len(sysName_tlv)

        # Create BMP Common Header
        version = 3  # BMP version
        msg_type = 4  # INIT Message
        # BMP Common Header: Version (1 byte) | Message Length (4 bytes) | Message Type (1 byte)
        bmp_common_header = struct.pack('!BIB', version, BMPv3.BMP_HDRv3_LEN + total_tlv_length, msg_type)

        # Build the full BMP INIT message
        init_message = bmp_common_header + sysDescr_tlv + sysName_tlv

        return init_message

    @staticmethod
    def term_message(reason_code=1):
        """
        Construct a BMP TERM message similar to Script 1's `getTerminationMessage`.
        
        Args:
            reason_code (int): The termination reason code (default: 1).

        Returns:
            bytes: The BMP TERM message in bytes.
        """
        # Reason TLV (Type=1)
        # According to BMP spec, Type=1 is 'Notification Reason'
        # The content is typically a 2-byte reason code
        # Here, reason_code is a 2-byte value
        reason_bytes = struct.pack('!H', reason_code)
        reason_tlv = struct.pack('!HH', 1, len(reason_bytes)) + reason_bytes

        # Calculate the total TLV length
        total_tlv_length = len(reason_tlv)

        # Create BMP Common Header
        version = 3  # BMP version
        msg_type = 5  # TERM Message
        # BMP Common Header: Version (1 byte) | Message Length (4 bytes) | Message Type (1 byte)
        bmp_common_header = struct.pack('!BIB', version, BMPv3.BMP_HDRv3_LEN + total_tlv_length, msg_type)

        # Build the full BMP TERM message
        term_message = bmp_common_header + reason_tlv

        return term_message

    @staticmethod
    def peer_up_message(peer_ip, peer_asn, timestamp, bgp_open, collector):
        """
        Construct a BMP Peer Up Notification message with BGP OPEN messages.
        
        Args:
            peer_ip (str): The peer IP address.
            peer_asn (int): The peer AS number.
            timestamp (float): The timestamp.
            collector (str): The collector name.
            bgp_open (bytes): The BGP OPEN message in bytes.
        
        Returns:
            bytes: The BMP message in bytes.
        """
        bmp_msg_type = 3  # Peer Up Notification
        per_peer_header = BMPv3.per_peer_header(peer_ip, peer_asn, timestamp, collector)
        
        peer_up_msg = (
            socket.inet_pton(socket.AF_INET6, '::') + # Local Address (IPv6)
            struct.pack('!HH', 0, 179) +              # Local Port, Remote Port
            bgp_open +                                # Sent OPEN
            bgp_open                                  # Received OPEN
        )
        
        total_length = BMPv3.BMP_HDRv3_LEN + len(per_peer_header) + len(peer_up_msg)
        
        bmp_common_header = struct.pack('!BIB', 3, total_length, bmp_msg_type)
        
        # Build the full BMP message
        bmp_message = bmp_common_header + per_peer_header + peer_up_msg
        
        return bmp_message

    @staticmethod
    def peer_down_message(peer_ip, peer_asn, timestamp, reason_code, bgp_notification, collector):
        """
        Construct a BMP Peer Down Notification message.

        Args:
            peer_ip (str): The peer IP address.
            peer_asn (int): The peer AS number.
            timestamp (float): The timestamp.
            reason_code (int): The reason code.
            bgp_notification (bytes): The BGP NOTIFICATION message in bytes.
            collector (str): The collector name.
            
        Returns:
            bytes: The BMP message in bytes.
        """
        # Build the BMP Common Header
        bmp_msg_type = 2  # Peer Down Notification
        per_peer_header = BMPv3.per_peer_header(peer_ip, peer_asn, timestamp, collector)

        # Reason: 1-byte code indicating the reason.
        #         Code 1: Local system closed with notification
        #         Code 2: Local system closed without notification
        #         Code 3: Remote system closed with notification
        #         Code 4: Remote system closed without notification
        #         Code 5: Peer monitoring stopped.
        reason = struct.pack('!B', reason_code)

        total_length = BMPv3.BMP_HDRv3_LEN + len(per_peer_header) + len(reason) + len(bgp_notification)

        bmp_common_header = struct.pack('!BIB', 3, total_length, bmp_msg_type)

        # Build the full BMP message
        bmp_message = bmp_common_header + per_peer_header + reason + bgp_notification

        return bmp_message

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
    def per_peer_header(peer_ip, peer_asn, timestamp, collector):
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
