from confluent_kafka import KafkaError, Consumer, TopicPartition, KafkaException
from datetime import datetime, timedelta
from protocols.bmp import BMPv3
import socket
import struct
import time

def kafka_task(source, target, router, queue, db, logger, events, memory):
    """
    Task to poll a batch of messages from Kafka and add them to the queue.
    """

    # Await the injection event
    events['injection'].wait()

    # Broadcast the stage
    memory['active_stage'] = "KAFKA_STREAM"

    # Create Kafka Consumer
    consumer = Consumer({
        'bootstrap.servers': 'stream.routeviews.org:9092',
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
                for partition in partitions:
                    last_offset = db.get(
                        f'offsets_{partition.topic}_{partition.partition}'.encode('utf-8')) or None
                    
                    # If the offset is stored, set it
                    if last_offset is not None:
                        # +1 because we start from the next message
                        partition.offset = int.from_bytes(last_offset, byteorder='big') + 1
                    
                    # Log the assigned offset
                    logger.info(
                        f"Assigned offset for partition {partition.partition} of {partition.topic} to {partition.offset}")
                    
                # Assign the partitions to the consumer
                consumer.assign(partitions)
        except Exception as e:
            logger.error(f"Error handling assignment: {e}", exc_info=True)

    # Subscribe to Kafka Consumer
    consumer.subscribe(
        memory['kafka_topics'],
        on_assign=lambda c, p: on_assign(c, p, db),
        on_revoke=lambda c, p: logger.info(
            f"Partitions have been revoked: {[part.partition for part in p]}")
    )

    # Provision the consumer
    if not events['provision'].is_set():
        for topic in memory['kafka_topics']:
            # Get the oldest timestamp for the topic
            timestamp = datetime.fromtimestamp(struct.unpack('>d', db.get(f'timestamp'.encode('utf-8')) or b'\x00\x00\x00\x00\x00\x00\x00\x00')[0])

            # Calculate the target time
            target_time = timestamp - timedelta(minutes=15)

            # Convert to milliseconds
            target_timestamp_ms = int(target_time.timestamp() * 1000)

            # Get metadata to retrieve all partitions for the topic
            metadata = consumer.list_topics(topic, timeout=10)
            partitions = metadata.topics[topic].partitions.keys()

            # Get offsets based on the timestamp
            offsets = consumer.offsets_for_times([TopicPartition(topic, p, target_timestamp_ms) for p in partitions])

            # Check if the offset is valid and not -1 (which means no valid offset was found for the given timestamp)
            valid_offsets = [tp for tp in offsets if tp.offset != -1]

            if valid_offsets:
                # Store offsets in database
                for tp in valid_offsets:
                    db.set(
                        f'offset_{tp.topic}_{tp.partition}'.encode('utf-8'),
                        tp.offset.to_bytes(16, byteorder='big')
                    )

                # Assign the offsets
                consumer.assign(valid_offsets)
            else:
                # Failed to find valid offsets
                raise Exception("No valid offsets found for the given timestamp")

            # Set the provision event
            events['provision'].set()

            # Set database as ready
            db.set(b'ready', b'\x01')

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

            # Initialize the messages list
            messages = []

            # Update the bytes received counter
            memory['bytes_received'] += len(value)

            # Skip the raw binary header (we don't need the fields)
            value = value[76 + struct.unpack("!H", value[54:56])[
                0] + struct.unpack("!H", value[72:74])[0]:]

            # TODO (1): Skip messages from unknown collectors.

            # TODO (2): Parse the message and replace the peer_distinguisher with our own hash representation
            #           Of the Route Views Collector name (SHA256) through the BMPv3.construct() function (e.g. the host).

            # TODO (3): We need to keep track of the timestamp of the message
            #           We do this to be able to show the time lag of the messages.

            # HACK: Using a mock timestamp for initial testing
            timestamp = time.time()

            # HACK: Dummy approximated time lag preceived by the consumer
            memory['time_lag'] = datetime.now() - datetime.fromtimestamp(timestamp)

            # HACK: Untempered message for now
            messages.append(value)
                
            for message in messages:
                queue.put((message, offset, topic, partition))
