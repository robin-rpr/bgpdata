from confluent_kafka import KafkaError, Consumer
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
from datetime import datetime, timedelta
from io import BytesIO
import asyncio
import traceback
import fastavro
import logging
import socket
import json
import os
import re

class DateTimeEncoder(json.JSONEncoder):
    """
    Custom JSON encoder that converts datetime objects to ISO 8601 strings.
    """
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

# Get the hostname and process ID
hostname = socket.gethostname()  # Get the machine's hostname
pid = os.getpid()  # Get the current process ID

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Create the engine and session for PostgreSQL
engine = create_async_engine(os.getenv("POSTGRESQL_DATABASE"), echo=False, future=True)

# Kafka Consumer configuration
KAFKA_BOOTSTRAP_SERVERS = 'node01.kafka-pub.ris.ripe.net:9094,node02.kafka-pub.ris.ripe.net:9094,node03.kafka-pub.ris.ripe.net:9094'
KAFKA_TOPIC = 'ris-live'
KAFKA_GROUP_ID = f"bgpdata-{hostname}:{pid}"

consumer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': KAFKA_GROUP_ID,
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

def decompress(data) -> list[dict]:
    """
    Decompresses BGP update data into individual messages.
    
    Args:
        data (dict): Raw BGP update data.
    
    Returns:
        tuple: A tuple containing the checkpoint (str) and a list of decompressed messages (list[dict]).
    """
    messages = []

    if len(data['announcements']) > 0:
        as_path = data['path'][:-1] if isinstance(data['path'][-1], list) else data['path']
        as_set = data['path'][-1] if isinstance(data['path'][-1], list) else None

        for announcement in data['announcements']:
            for prefix in announcement['prefixes']:
                messages.append({
                    "checkpoint": data['id'],
                    "timestamp": datetime.fromtimestamp(data['timestamp']),
                    "peer_ip": data['peer'],
                    "peer_as": int(data['peer_asn']),
                    "host": data['host'],
                    "type": "A",
                    "as_path": as_path,
                    "as_set": as_set,
                    "community": data['community'],
                    "origin": data.get('origin', None),
                    "med": data.get('med', None),
                    "aggregator": data.get('aggregator', None),
                    "next_hop": announcement['next_hop'].split(','),
                    "prefix": prefix,
                })

    if len(data['withdrawals']) > 0:
        for prefix in data['withdrawals']:
            messages.append({
                "checkpoint": data['id'],
                "timestamp": datetime.fromtimestamp(data['timestamp']),
                "peer_ip": data['peer'],
                "peer_as": int(data['peer_asn']),
                "host": data['host'],
                "type": "W",
                "as_path": data['path'],
                "community": [],
                "origin": data.get('origin', None),
                "med": data.get('med', None),
                "aggregator": data.get('aggregator', None),
                "next_hop": None,
                "prefix": prefix
            })

    return messages

def aggregate_ris(messages: list[dict]) -> dict:
    """
    Prepare RIS messages for the 'ris' table without modification (raw).

    Args:
        messages (list[dict]): A list of dictionaries, each containing a single RIS
                               message with its original attributes.

    Returns:
        list[tuple]: A list of tuples, where each tuple represents a single RIS
                     message in its raw form, ready for insertion into the 'ris' table.
    """
    data_tuples = [
        (msg['checkpoint'], msg['timestamp'], msg['peer_ip'], msg['peer_as'], msg['host'], msg['type'], msg['as_path'], msg['community'], msg['origin'], msg['med'], msg['aggregator'], msg['next_hop'], msg['prefix'])
        for msg in messages
    ]
    
    return data_tuples

def aggregate_ris_lite(messages: list[dict], two_hops=False) -> dict:
    """
    Aggregate RIS messages by origin and segment, possibly including a second hop.

    Args:
        messages (list[dict]): A list of dictionaries, each containing a single RIS
                               message with its original attributes.
        two_hops (bool): Whether to include a second hop in the aggregation.

    Returns:
        list[tuple]: A list of tuples, where each tuple represents a single RIS
                     message in its raw form, ready for insertion into the 'ris' table.
    """
    aggregation = {}

    # Aggregate by origin and segment (AS path)
    for msg in messages:
        if len(msg['as_path']) > 0:
            as_path = msg['as_path']

            # Origin AS is the last in the path
            origin = as_path[-1]
            # First hop is the second-to-last AS in the path (if present)
            first_hop = as_path[-2] if two_hops and len(as_path) > 1 else None

            path = (first_hop, origin) if first_hop else (origin,)

            key = (path, msg['prefix'], msg['host'])
            if key not in aggregation:
                aggregation[key] = {
                    'full_peer_count': 0,
                    'partial_peer_count': 0,
                    'timestamps': []
                }

            # Update full or partial peer counts based on message type
            if msg['type'] == 'F':
                aggregation[key]['full_peer_count'] += 1
            elif msg['type'] == 'U':
                aggregation[key]['partial_peer_count'] += 1

            # Store the timestamp
            aggregation[key]['timestamps'].append(msg['timestamp'])

    # Prepare data for bulk insert into ris_lite
    data_tuples = [
        (
            datetime.now(),  # Use current timestamp for now
            host,
            prefix,
            data['full_peer_count'],
            data['partial_peer_count'],
            path
        )
        for (path, prefix, host), data in aggregation.items()
    ]

    return data_tuples

# Callback to handle partition rebalancing
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

async def log_status(time_lag, poll_interval):
    """
    Periodically logs the time lag, number of inserted messages, and current poll interval.
    This coroutine runs concurrently with the main processing loop.
    
    Args:
        time_lag (timedelta): The current time lag of the messages.
        poll_interval (float): The current polling interval in seconds.
    """
    while True:
        await asyncio.sleep(5)  # Sleep for 5 seconds before logging
        
        logger.info(f"Time lag: {time_lag.total_seconds()} seconds, "
                    f"Poll interval: {poll_interval} seconds")

async def main():
    """
    Main function to consume messages from Kafka, process them, and insert into the database.

    This asynchronous function sets up a Kafka consumer, subscribes to the specified topic,
    and continuously polls for messages. It processes messages in batches, dynamically
    adjusts polling intervals based on message lag, and handles various error scenarios.

    The function performs the following key operations:
    1. Sets up a Kafka consumer with specified configuration and callbacks.
    2. Implements batch processing of messages with a configurable threshold.
    3. Dynamically adjusts polling intervals based on message time lag.
    4. Processes messages, including deserialization and filtering.
    5. Inserts processed data into the database in batches.
    6. Handles various error scenarios and implements retry logic.

    The function runs indefinitely until interrupted or an unhandled exception occurs.
    """
    consumer = Consumer(consumer_conf)
    consumer.subscribe([KAFKA_TOPIC])

    # Set up callbacks
    consumer.subscribe(
        [KAFKA_TOPIC],
        on_assign=on_rebalance,
        on_revoke=lambda c, p: logger.info(f"Revoked partitions: {[part.partition for part in p]}")
    )

    # Initialize settings for batch processing
    BATCH_THRESHOLD = 10000
    CATCHUP_POLL_INTERVAL = 1.0  # Fast poll when behind in time
    NORMAL_POLL_INTERVAL = 5.0   # Slow poll when caught up
    TIME_LAG_THRESHOLD = timedelta(minutes=5)  # Consider behind if messages are older than 5 minutes
    FAILURE_RETRY_DELAY = 5      # Delay in seconds before retrying a failed message

    messages_batch = []
    messages_size = 0

    poll_interval = NORMAL_POLL_INTERVAL  # Initialize with the normal poll interval
    time_lag = timedelta(0)               # Initialize time lag

    # Start logging task with time_lag and poll_interval being updated within the loop
    logging_task = asyncio.create_task(log_status(time_lag, poll_interval))

    try:
        while True:
            msg = consumer.poll(timeout=poll_interval)  # Use dynamically set poll_interval
            
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
                key = msg.key()
                value = msg.value()

                # Remove the first 5 bytes
                value = value[5:]
                
                # Deserialize the Avro message
                parsed = fastavro.schemaless_reader(BytesIO(value), avro_schema)

                # Skip if the message is not an UPDATE
                if parsed['type'] != "UPDATE":
                    continue

                messages = decompress(json.loads(parsed['ris_live']))
                
                # Check if the message is significantly behind the current time
                time_lag = datetime.now() - messages[0]['timestamp']

                # Adjust polling interval based on how far behind the messages are
                if time_lag > TIME_LAG_THRESHOLD:
                    poll_interval = CATCHUP_POLL_INTERVAL # Faster polling if we're behind
                else:
                    poll_interval = NORMAL_POLL_INTERVAL # Slow down if we're caught up

                # Add messages to the batch
                messages_batch.extend(messages)
                messages_size += 1

                # Process the batch when it reaches the threshold
                if messages_size >= BATCH_THRESHOLD:
                    # Aggregate messages
                    ris_data = aggregate_ris(messages_batch)
                    ris_lite_data = aggregate_ris_lite(messages_batch)

                    # Write to the database
                    async with engine.begin() as conn:
                        try:
                            # Start a transaction
                            await conn.execute(text("BEGIN"))
                            
                            raw_conn = await conn.get_raw_connection()
                            
                            # Insert into 'ris' table
                            await raw_conn._connection.copy_records_to_table(
                                'ris', 
                                records=ris_data, 
                                columns=['checkpoint', 'timestamp', 'peer_ip', 'peer_as', 'host', 'type', 'as_path', 'community', 'origin', 'med', 'aggregator', 'next_hop', 'prefix']
                            )
                            
                            # Insert into 'ris_lite' table
                            await raw_conn._connection.copy_records_to_table(
                                'ris_lite', 
                                records=ris_lite_data, 
                                columns=['timestamp', 'host', 'prefix', 'full_peer_count', 'partial_peer_count', 'segment']
                            )
                            
                            # If both operations succeed, commit the transaction
                            await conn.execute(text("COMMIT"))
                        
                        except SQLAlchemyError as e:
                            # If an exception occurs, explicitly roll back the transaction
                            await conn.execute(text("ROLLBACK"))
                            logger.error(f"Error during database insertion, transaction rolled back: {e}")
                            raise  # Re-raise the exception

                    logger.info(f"Processed {len(messages_batch)} messages.")

                    # Commit the offset after successful processing
                    consumer.commit()

                    # Reset the batch
                    messages_batch = []
                    messages_size = 0

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
