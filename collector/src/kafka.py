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
from confluent_kafka import KafkaError, Consumer, TopicPartition, KafkaException
from confluent_kafka.admin import AdminClient
from datetime import datetime
import socket
import struct
import time
import re

def kafka_task(host, kafka, queue, db, logger, events, memory):
    """
    Task to poll a batch of messages from Kafka and add them to the queue.
    """

    try:
        # Await the injection event
        events['injection'].wait()

        # Broadcast the stage
        memory['task'] = "kafka"

        # Log the connection
        logger.info(f"Connecting to {kafka}")

        # Create Kafka Admin Client
        admin = AdminClient({
            'bootstrap.servers': kafka,
        })

        # Create Kafka Consumer
        consumer = Consumer({
            'bootstrap.servers': kafka,
            'group.id': f'bgpdata-{socket.gethostname()}',
            'partition.assignment.strategy': 'roundrobin',
            'enable.auto.commit': False,
            'security.protocol': 'PLAINTEXT',
            'fetch.max.bytes': 50 * 1024 * 1024, # 50 MB
            'session.timeout.ms': 30000,  # For stable group membership
        })

        # Define the on_assign callback
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
                    for tp in partitions:
                        last_offset = db.get(
                            f'offset_{tp.topic}_{tp.partition}'.encode('utf-8')
                        )

                        if last_offset is not None:
                            tp.offset = int.from_bytes(last_offset, byteorder='big')
                            # Start from the next message
                            if tp.offset > 0:
                                tp.offset += 1

                            # Check if offset is before low watermark
                            while True:
                                try:
                                    low, high = consumer.get_watermark_offsets(tp, timeout=15.0)
                                    if tp.offset < low:
                                        # If the high watermark is greater than the low watermark, the partition is not empty
                                        if high > low:
                                            raise RuntimeError(
                                                f"Continuity lost for partition {tp.partition} of {tp.topic}"
                                            )

                                        # The partition is empty, and it's safe to start from the beginning
                                        logger.warning(f"Exhausted partition {tp.partition} of {tp.topic} due to no messages")
                                        tp.offset = low
                                    break
                                except Exception as e:
                                    logger.warning(f"Watermark check is taking longer than 15 seconds. Delaying by 15000ms")
                                    time.sleep(15)

                        logger.debug(
                            f"Assigned offset for partition {tp.partition} of {tp.topic} to {tp.offset}"
                        )

                    # Assign the partitions to the consumer
                    consumer.assign(partitions)
            except Exception as e:
                logger.error(f"Error handling assignment: {e}", exc_info=True)
                raise  # Rethrow the exception to avoid silent failures

        # Get all topics
        all_topics = admin.list_topics(timeout=15).topics.keys()
        # Define regex for final filtering
        escaped_host = re.escape(host).replace(r'\-', '[-]?')
        pattern = re.compile(rf"^{escaped_host}\.(\d+)\.bmp_raw$")
        # Apply regex filtering on the pre-filtered list
        matching_topics = [(topic, pattern.match(topic).group(1)) for topic in all_topics if pattern.match(topic)]
        # All valid topics
        valid_topics = []

        # Loop through all matching topics
        combined_offsets = []
        for topic, peer in matching_topics:
            # Check if we know of this peer
            if db.get(f'timestamp_{peer}'.encode('utf-8')) is None:
                continue

            # Add to valid topics
            valid_topics.append(topic)

            # Check if already provisioned
            if db.get(f'provisioned_{peer}'.encode('utf-8')) == b'\x01':
                continue

            # Get the timestamp for this peer
            peer_timestamp = datetime.fromtimestamp(struct.unpack('>d', db.get(f'timestamp_{peer}'.encode('utf-8')))[0])

            # Convert to milliseconds
            peer_timestamp_ms = int(peer_timestamp.timestamp() * 1000)

            # Log the acquisition
            logger.info(f"Acquired new peer via {topic}")

            # Get metadata to retrieve all partitions for the topic
            metadata = consumer.list_topics(topic, timeout=10)
            partitions = metadata.topics[topic].partitions.keys()

            # Get offsets based on the timestamp
            offsets = consumer.offsets_for_times([TopicPartition(topic, p, peer_timestamp_ms) for p in partitions])

            # Store offsets in database
            for tp in offsets:
                db.set(
                    f'offset_{tp.topic}_{tp.partition}'.encode('utf-8'),
                    tp.offset.to_bytes(16, byteorder='big')
                )

            # Set the provisioned flag
            db.set(f'provisioned_{peer}'.encode('utf-8'), b'\x01')
            
            # Assign the offsets
            combined_offsets.extend(offsets)

        if len(valid_topics) == 0:
            raise RuntimeError("No valid topics found")

        # Subscribe to Kafka Consumer
        consumer.subscribe(
            valid_topics,
            on_assign=lambda c, p: on_assign(c, p, db),
            on_revoke=lambda c, p: logger.info(
                f"Partitions have been revoked: {[part.partition for part in p]}")
        )

        # Assign the offsets
        consumer.assign(combined_offsets)

        # Start Polling
        while True:
            # Consume a batch of messages
            msgs = consumer.consume(100000, timeout=0.1)

            if not msgs:
                continue

            for msg in msgs:
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # Kafka Broker tells us we are too fast, sleep a bit.
                        logger.info(f"End of partition reached: {msg.error()}")
                        time.sleep(1)
                        continue
                    elif msg.error().code() == KafkaError._OFFSET_OUT_OF_RANGE:
                        # Kafka Broker tells us we couldn't keep up, exit.
                        logger.critical("Offset out of range error encountered")
                        raise KafkaException(msg.error())
                    else:
                        # Some other error occurred
                        logger.error(f"Kafka error: {msg.error()}", exc_info=True)
                        raise KafkaException(msg.error())

                # Process the message
                value = msg.value()
                topic = msg.topic()
                offset = msg.offset()
                partition = msg.partition()
                # FIXME: Not sure if this is correct. Which tuple item is the right timestamp?!
                timestamp = msg.timestamp()[1] / 1000 

                # Update the bytes received counter
                memory['bytes_received'] += len(value)
                
                # Update the approximated time lag preceived by the consumer
                memory['time_lag'][f"{topic}_{partition}"] = datetime.now() - datetime.fromtimestamp(timestamp)

                # NOTE: In case of a OpenBMP message, we need to strip the OpenBMP
                #       raw binary header from the message. It's not needed and
                #       will cause the collector to fail.
                if (value[0] == 0x4f and value[1] == 0x42 and value[2] == 0x4d and value[3] == 0x50):
                    # Extract the OpenBMP raw binary header
                    i = 4
                    collector_version_major = struct.unpack("B", value[i:i + 1])[0]
                    i += 1
                    collector_version_minor = struct.unpack("B", value[i:i + 1])[0]
                    i += 1
                    header_length = struct.unpack("!H", value[i: i+2])[0]
                    i += 2
                    message_length = struct.unpack("!I", value[i: i+4])[0]
                    i += 4
                    flags = struct.unpack("B", value[i:i + 1])[0]
                    i += 1
                    obj_type = struct.unpack("B", value[i:i + 1])[0]
                    i += 1
                    ts = struct.unpack("!I", value[i:i + 4])[0]
                    i += 4
                    ts_msec = struct.unpack("!I", value[i:i + 4])[0]
                    i += 4
                    collector_hash = value[i:i + 16]
                    i += 16
                    admin_len = struct.unpack("!H", value[i:i + 2])[0]
                    i += 2
                    admin_id = value[i:i + admin_len]
                    i += admin_len
                    router_hash = value[i:i + 16]
                    i += 16
                    router_ip = value[i:i + 16]
                    i += 16
                    router_group_len = struct.unpack("!H", value[i:i + 2])[0]
                    i += 2
                    router_group = value[i:i + router_group_len]
                    i += router_group_len
                    row_count = struct.unpack("!I", value[i:i + 4])[0]
                    i += 4
                    # Strip the OpenBMP raw binary header
                    value = value[i:]

                # Add the message to the queue
                queue.put((value, offset, topic, partition, True))
    except Exception as e:
        logger.error(e, exc_info=True)
        events['shutdown'].set()
        raise e