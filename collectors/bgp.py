import os
import json
import logging
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import MetaData, Table, Column, String, Integer, DateTime, Text
from datetime import datetime
from pybgpstream import BGPStream

# Define the database model
from sqlalchemy import MetaData, Table, Column, String, DateTime, Text

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

async def insert_bgp_data(session, messages):
    """Insert BGP data into TimescaleDB."""
    async with session.begin():
        await session.execute(bgp_updates.insert().values(messages))

async def process_bgpstream():
    """Process BGPStream and store data in TimescaleDB."""
    collector = os.getenv("BGP_COLLECTOR", "ris-live")

    # Set up BGPStream
    stream = BGPStream(project=collector)
    
    count = 0
    async with Session() as session:
        for elem in stream:
            for message in bgpstream_format(collector, elem):
                await insert_bgp_data(session, [message])

                # Log progress every 5000 messages
                count += 1
                if count % 5000 == 0:
                    logger.info(f"Processed {count} messages")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Run the stream processing
    import asyncio
    asyncio.run(process_bgpstream())
