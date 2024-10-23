from datetime import datetime, timedelta
from confluent_kafka import KafkaError
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
import asyncio
import logging
import json
import re
import os

# Custom JSON encoder that converts datetime objects to ISO 8601 strings
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

# Initialize logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Create the engine and session for PostgreSQL
engine = create_async_engine(os.getenv("POSTGRESQL_DATABASE"), echo=False, future=True)

# Kafka Consumer configuration
KAFKA_BOOTSTRAP_SERVERS = 'node01.kafka-pub.ris.ripe.net:9094,node02.kafka-pub.ris.ripe.net:9094,node03.kafka-pub.ris.ripe.net:9094'
KAFKA_TOPIC = 'ris-live'
KAFKA_GROUP_ID = 'bgpdata'

consumer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': KAFKA_GROUP_ID,
    'auto.offset.reset': 'earliest',  # Start from the earliest offset if no previous offsets are found
    'enable.auto.commit': False,      # We will manually commit the offsets
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': os.getenv("SASL_KAFKA_USERNAME"),
    'sasl.password': os.getenv("SASL_KAFKA_PASSWORD"),
    'schema.registry.url': 'http://node01.kafka-pub.ris.ripe.net:8081',
    'specific.avro.reader': 'true'
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
        if msg['as_path']:
            as_path = msg['as_path'].split()

            # Origin AS is the last in the path
            origin = as_path[-1]
            # First hop is the second-to-last AS in the path (if present)
            first_hop = as_path[-2] if two_hops and len(as_path) > 1 else None

            path = (first_hop, origin) if first_hop else (origin,)

            key = (path, msg['prefix'], msg['collector'])
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
            collector,
            prefix,
            data['full_peer_count'],
            data['partial_peer_count'],
            ','.join(path)  # Store path as a string
        )
        for (path, prefix, collector), data in aggregation.items()
    ]

    return data_tuples

# Main function to consume messages from Kafka
async def main():
    consumer = AvroConsumer(consumer_conf)
    consumer.subscribe([KAFKA_TOPIC])

    # Initialize settings for batch processing
    BATCH_THRESHOLD = 10000
    CATCHUP_POLL_INTERVAL = 1.0  # Fast poll when behind in time
    NORMAL_POLL_INTERVAL = 5.0   # Slow poll when caught up
    TIME_LAG_THRESHOLD = timedelta(minutes=5)  # Consider behind if messages are older than 5 minutes
    FAILURE_RETRY_DELAY = 5      # Delay in seconds before retrying a failed message

    messages_batch = []
    messages_size = 0

    try:
        while True:
            msg = consumer.poll(NORMAL_POLL_INTERVAL)
            
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info(f"End of partition reached {msg.partition()}")
                elif msg.error():
                    logger.error(f"Kafka error: {msg.error()}")
                continue

            try:
                parsed = json.loads(msg.value())
                messages = decompress(parsed)

                logger.info(f"Received message from Kafka: {messages}")
                
                # Check if the message is significantly behind the current time
                message_time = datetime.fromtimestamp(parsed['data']['timestamp'])
                time_lag = datetime.now() - message_time

                # Adjust polling interval based on how far behind the messages are
                if time_lag > TIME_LAG_THRESHOLD:
                    poll_interval = CATCHUP_POLL_INTERVAL
                    logger.info(f"Processing older message, time lag: {time_lag}. Using fast poll interval.")
                else:
                    poll_interval = NORMAL_POLL_INTERVAL
                    logger.info(f"Processing recent message. Using normal poll interval.")

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
                            await conn.execute("BEGIN")
                            
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
                                columns=['timestamp', 'collector', 'prefix', 'full_peer_count', 'partial_peer_count', 'segment']
                            )
                            
                            # If both operations succeed, commit the transaction
                            await conn.execute("COMMIT")
                            logger.info(f"Successfully inserted {len(ris_data)} records into 'ris' and {len(ris_lite_data)} records into 'ris_lite'")
                        
                        except SQLAlchemyError as e:
                            # If an exception occurs, explicitly roll back the transaction
                            await conn.execute("ROLLBACK")
                            logger.error(f"Error during database insertion, transaction rolled back: {e}")
                            raise  # Re-raise the exception

                    logger.info(f"Processed {len(messages_batch)} messages.")

                    # Commit the offset after successful processing
                    consumer.commit()

                    # Reset the batch
                    messages_batch = []
                    messages_size = 0

            except Exception as e:
                logger.error(f"Failed to process message, retrying in {FAILURE_RETRY_DELAY} seconds... Error: {e}")
                # Wait before retrying the message to avoid overwhelming Kafka
                await asyncio.sleep(FAILURE_RETRY_DELAY)

    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        consumer.close()

if __name__ == "__main__":
    asyncio.run(main())
