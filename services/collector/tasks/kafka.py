from confluent_kafka import KafkaError, Consumer, TopicPartition, KafkaException
from datetime import datetime, timedelta
from protocols.bmp import BMPv3
from io import BytesIO
from config import *
from tasks import *
import fastavro
import struct
import time
import json

def kafka_task(configuration, collectors, topics, queue, db, status, batch_size, provider, events, logger):
    """
    Task to poll a batch of messages from Kafka and add them to the queue.

    Args:
        configuration (dict): The configuration of the Kafka consumer.
        collectors (list): A list of tuples containing the host and topic of the collectors.
        topics (list): A list of topics to subscribe to.
        queue (queue.Queue): The queue to add the messages to.
        db (rocksdbpy.DB): The RocksDB database.
        status (dict): A dictionary containing the following keys:
            - time_lag (dict): A dictionary containing the current time lag of messages for each host.
            - time_preceived (dict): A dictionary containing the latest read timestamp for each host.
            - bytes_sent (int): The number of bytes sent since the last log.
            - bytes_received (int): The number of bytes received since the last log.
            - activity (str): The current activity of the collector.
        batch_size (int): Number of messages to fetch at once.
        provider (str): The provider of the messages.
        events (dict): A dictionary containing the following keys:
            - route-views_injection (threading.Event): The event to wait for before starting.
            - route-views_provision (threading.Event): The event to wait for before starting.
            - ris_injection (threading.Event): The event to wait for before starting.
            - ris_provision (threading.Event): The event to wait for before starting.
        logger (logging.Logger): The logger to use.
    """

    # Wait for possible RIB injection to finish
    for key in events.keys():
        if key.endswith("_injection"):
            events[key].wait()

    # Set the activity
    status['activity'] = "BMP_STREAM"

    # Create Kafka Consumer
    consumer = Consumer(configuration)

    # Subscribe to Kafka Consumer
    consumer.subscribe(
        topics,
        on_assign=lambda c, p: on_assign(c, p, db),
        on_revoke=lambda c, p: logger.info(
            f"Revoked partitions: {[part.partition for part in p]}")
    )

    # If RIBs are injected but not yet provisioned
    if not events[f"{provider}_provision"].is_set():
        # Seek to desired offsets based on timestamps
        # Define a time delta (e.g., 15 minutes)
        time_delta = timedelta(minutes=15)

        # Keep track of the oldest timestamp for each topic
        # Why? In case multiple collectors stream to the same topic
        oldest_timestamps = {}

        for host, topic in collectors:
            # Assure the oldest timestamp for the topic (see comment above)
            oldest_timestamps[topic] = min(
                oldest_timestamps.get(
                    topic,
                    struct.unpack('>d', db.get(f'timestamps_{host}'.encode('utf-8')) or b'\x00\x00\x00\x00\x00\x00\x00\x00')[0]
                ),
                struct.unpack('>d', db.get(f'timestamps_{host}'.encode('utf-8')) or b'\x00\x00\x00\x00\x00\x00\x00\x00')[0]
            )

        for host, topic in collectors:
            # Get the oldest timestamp for the topic
            timestamp = datetime.fromtimestamp(oldest_timestamps[topic])

            # Calculate the target time
            target_time = timestamp - time_delta
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
                # Apply the offsets to RocksDB
                for tp in valid_offsets:
                    db.set(
                        f'offsets_{tp.topic}_{tp.partition}'.encode('utf-8'),
                        tp.offset.to_bytes(16, byteorder='big')
                    )

                # Inform the consumer about the assigned offsets
                consumer.assign(valid_offsets)
            else:
                # No valid offsets found for the given timestamp
                raise Exception("No valid offsets found for the given timestamp")

            # Mark Consumer as provisioned
            events[f"{provider}_provision"].set()

            # Wait for all consumers to be provisioned
            for key in events.keys():
                if key.endswith("_provision"):
                    events[key].wait()

            # Mark the RIBs injection as fulfilled
            db.set(b'ready', b'\x01')

    # Log the provision
    logger.info(f"Subscribed to {provider} Kafka Consumer")

    # Poll messages from Kafka
    while True:
        # Poll a batch of messages
        msgs = consumer.consume(batch_size, timeout=0.1)

        if not msgs:
            continue

        for msg in msgs:
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # We are too fast for Kafka, sleep a bit
                    logger.info(f"End of partition reached: {msg.error()}")
                    time.sleep(1)
                    continue
                elif msg.error().code() == KafkaError._OFFSET_OUT_OF_RANGE:
                    # We were too slow for Kafka, this needs to be fixed manually
                    logger.critical("Offset out of range error encountered")
                    raise KafkaException(msg.error())
                else:
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
            status['bytes_received'] += len(value)

            match provider:
                case 'route-views':
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
                    status['time_lag']["example.host"] = datetime.now() - datetime.fromtimestamp(timestamp)

                    # HACK: Untempered message for now
                    messages.append(value)
                case 'ris':
                    # Remove the first 5 bytes (we don't need them)
                    value = value[5:]

                    # Parse the Avro encoded exaBGP message
                    parsed = fastavro.schemaless_reader(
                        BytesIO(value), ris_avro_schema)
                    
                    # Cast from int to datetime float
                    timestamp = parsed['timestamp'] / 1000
                    host = parsed['host']  # Extract Host

                    # Skip messages from collectors that are not in our configured list
                    if db.get(f'timestamps_{host}'.encode('utf-8')) is None:
                        continue
                    
                    # Update the approximated time lag preceived by the consumer
                    status['time_lag'][host] = datetime.now() - datetime.fromtimestamp(timestamp)
                    status['time_preceived'][host] = datetime.fromtimestamp(timestamp)

                    # Skip messages before the ingested collector's RIB or before the collector was seen
                    latest_route = struct.unpack('>d', db.get(f'timestamps_{host}'.encode('utf-8')) or b'\x00\x00\x00\x00\x00\x00\x00\x00')[0]
                    if timestamp < latest_route:
                        # TODO: We are estimating the time gap between the message and the ingested RIB very statically,
                        #       but we should approach this more accurately, e.g. approximate the time gap through reverse graph analysis.
                        continue

                    # Parse to BMP messages and add to the queue
                    # JSON Schema: https://ris-live.ripe.net/manual/
                    marshal = json.loads(parsed['ris_live'])

                    messages.extend(BMPv3.construct(
                        host,
                        marshal['peer'],
                        int(marshal['peer_asn']), # Cast to int from string
                        marshal['timestamp'],
                        'PEER_STATE' if marshal['type'] == 'RIS_PEER_STATE' else marshal['type'],
                        marshal.get('path', []),
                        marshal.get('origin', 'INCOMPLETE'),
                        marshal.get('community', []),
                        marshal.get('announcements', []),
                        marshal.get('withdrawals', []),
                        marshal.get('state', None),
                        marshal.get('med', None)
                    ))

            for message in messages:
                queue.put((message, offset, topic, partition))
