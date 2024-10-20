import os
import json
import logging
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import MetaData, Table, Column, String, Integer, DateTime, Text
from datetime import datetime
from pybgpstream import BGPStream
import io
import asyncpg

# Initialize logging
logger = logging.getLogger(__name__)

# Create the engine and session for PostgreSQL
engine = create_async_engine(
    os.getenv("POSTGRESQL_DATABASE"), echo=False, future=True
)
Session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

# Define the 'bgp_updates' table schema using SQLAlchemy
metadata = MetaData()

bgp_updates = Table(
    "bgp_updates",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("type", String(1), nullable=False),  # F: Full (R)oute, U: (A)nnouncement, W: Withdrawal
    Column("timestamp", DateTime, nullable=False),
    Column("collector", String(50), nullable=False),
    Column("peer_as", Integer, nullable=False),
    Column("peer_ip", String(50), nullable=False),
    Column("prefix", String(50), nullable=False),
    Column("origins", Text, nullable=True),
    Column("as_path", Text, nullable=True),
)

ris_lite = Table(
    "ris_lite",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("timestamp", DateTime, nullable=False),
    Column("collector", String(50), nullable=False),
    Column("prefix", String(50), nullable=False),
    Column("full_peer_count", Integer, nullable=False),
    Column("partial_peer_count", Integer, nullable=False),
    Column("segment", Text, nullable=True),
)

# Function to format BGPStream elements into a dict format for TimescaleDB
def bgpstream_format(collector, elem):
    """Format BGPStream elements into a dict format for TimescaleDB."""
    if elem.type == "R" or elem.type == "A":
        as_path = elem.fields.get("as-path", "")
        if as_path:
            origins = elem.fields["as-path"].split()
            typ = "F" if elem.type == "R" else "U"
            yield {
                "type": typ,
                "timestamp": datetime.fromtimestamp(elem.time),
                "collector": collector,
                "peer_as": elem.peer_asn,
                "peer_ip": elem.peer_address,
                "prefix": elem.fields["prefix"],
                "origins": " ".join(origins),
                "as_path": as_path,
            }
    elif elem.type == "W":
        yield {
            "type": "W",
            "timestamp": datetime.fromtimestamp(elem.time),
            "collector": collector,
            "peer_as": elem.peer_asn,
            "peer_ip": elem.peer_address,
            "prefix": elem.fields["prefix"],
            "origins": None,
            "as_path": None,
        }

# Function to use COPY for bulk insert into TimescaleDB using asyncpg
async def copy_bgp_data(session, messages):
    """Use COPY to bulk insert data into TimescaleDB."""
    if not messages:
        return  # Return if no messages to insert

    # Convert messages to a list of tuples (prepare data for COPY)
    data_tuples = [
        (
            msg['type'],
            msg['timestamp'],
            msg['collector'],
            msg['peer_as'],
            msg['peer_ip'],
            msg['prefix'],
            msg['origins'],
            msg['as_path']
        )
        for msg in messages
    ]

    async with engine.connect() as conn:
        # Access the underlying asyncpg connection
        raw_conn = await conn.get_raw_connection()
        asyncpg_conn = raw_conn._connection  # This gets the underlying asyncpg connection

        # Use asyncpg's copy_records_to_table for efficient bulk insert
        await asyncpg_conn.copy_records_to_table(
            'bgp_updates',
            records=data_tuples,
            columns=[
                'type', 'timestamp', 'collector', 'peer_as', 'peer_ip', 'prefix', 'origins', 'as_path'
            ]
        )

async def aggregate_ris_lite(session, messages_batch):
    """Aggregate the data to create ris-lite entries."""
    aggregation = {}

    # Aggregate peer counts and path segments
    for msg in messages_batch:
        key = (msg['prefix'], msg['collector'])
        if key not in aggregation:
            aggregation[key] = {
                'full_peer_count': 0,
                'partial_peer_count': 0,
                'segment': set()  # Store segments
            }

        # Update full or partial peer counts based on message type
        if msg['type'] == 'F':
            aggregation[key]['full_peer_count'] += 1
        elif msg['type'] == 'U':
            aggregation[key]['partial_peer_count'] += 1

        # Store AS path segments
        if msg['as_path']:
            aggregation[key]['segment'].add(msg['as_path'])

    # Prepare data for bulk insert into ris_lite
    data_tuples = [
        (
            datetime.now(),  # Use current timestamp for now
            collector,
            prefix,
            data['full_peer_count'],
            data['partial_peer_count'],
            ','.join(data['segment'])
        )
        for (prefix, collector), data in aggregation.items()
    ]

    # Insert into the ris_lite table
    async with engine.connect() as conn:
        raw_conn = await conn.get_raw_connection()
        asyncpg_conn = raw_conn._connection
        await asyncpg_conn.copy_records_to_table(
            'ris_lite',
            records=data_tuples,
            columns=[
                'timestamp', 'collector', 'prefix', 'full_peer_count', 'partial_peer_count', 'segment'
            ]
        )

    logger.info(f"Inserted aggregated ris_lite data")

async def process_bgpstream():
    """Process BGPStream and store data in TimescaleDB."""
    collector = "ris-live"

    # Set up BGPStream
    stream = BGPStream(project=collector)
    
    count = 0
    batch_size = 10000
    messages_batch = []

    async with Session() as session:
        for elem in stream:
            for message in bgpstream_format(collector, elem):
                messages_batch.append(message)
                count += 1

                # When the batch is full, insert it using COPY
                if count % batch_size == 0:
                    await copy_bgp_data(session, messages_batch)
                    await aggregate_ris_lite(session, messages_batch)  # Aggregate ris_lite
                    messages_batch = []  # Reset the batch after inserting
                    logger.info(f"Processed {count} messages")

        # Insert any remaining messages in the batch
        if messages_batch:
            await copy_bgp_data(session, messages_batch)
            await aggregate_ris_lite(session, messages_batch)  # Aggregate ris_lite
            logger.info(f"Processed {count} messages")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Run the stream processing
    import asyncio
    asyncio.run(process_bgpstream())
