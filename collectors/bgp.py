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

async def process_bgpstream():
    """Process BGPStream and store data in TimescaleDB."""
    collector = os.getenv("BGP_COLLECTOR", "ris-live")

    # Set up BGPStream
    stream = BGPStream(project=collector)
    
    count = 0
    batch_size = 1000
    messages_batch = []

    async with Session() as session:
        for elem in stream:
            for message in bgpstream_format(collector, elem):
                messages_batch.append(message)
                count += 1

                # When the batch is full, insert it using COPY
                if count % batch_size == 0:
                    await copy_bgp_data(session, messages_batch)
                    messages_batch = []  # Reset the batch after inserting
                    logger.info(f"Processed {count} messages")

        # Insert any remaining messages in the batch
        if messages_batch:
            await copy_bgp_data(session, messages_batch)
            logger.info(f"Processed {count} messages")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Run the stream processing
    import asyncio
    asyncio.run(process_bgpstream())
