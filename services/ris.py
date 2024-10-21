import os
import json
import logging
import hashlib
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import MetaData, Table, Column, String, Integer, DateTime, Text
from datetime import datetime
from pybgpstream import BGPStream
import io
import asyncpg
from mpi4py import MPI

# Initialize logging
logger = logging.getLogger(__name__)

# Create the engine and session for PostgreSQL
engine = create_async_engine(os.getenv("POSTGRESQL_DATABASE"), echo=False, future=True)
Session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

# Define the 'ris' and 'ris_lite' table schemas using SQLAlchemy
metadata = MetaData()

ris = Table(
    "ris", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("type", String(1), nullable=False), 
    Column("timestamp", DateTime, nullable=False),
    Column("collector", String(50), nullable=False),
    Column("peer_as", Integer, nullable=False),
    Column("peer_ip", String(50), nullable=False),
    Column("prefix", String(50), nullable=False),
    Column("origins", Text, nullable=True),
    Column("as_path", Text, nullable=True),
)

# MPI setup
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

# Define the leader process (rank 0 is the leader)
def is_leader():
    return rank == 0

# Function to format BGPStream elements into a dict format for TimescaleDB
def bgpstream_format(collector, elem):
    if elem.type in ["R", "A"]:
        as_path = elem.fields.get("as-path", "")
        if as_path:
            origins = elem.fields["as-path"].split()
            typ = "F" if elem.type == "R" else "U"
            return {
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
        return {
            "type": "W",
            "timestamp": datetime.fromtimestamp(elem.time),
            "collector": collector,
            "peer_as": elem.peer_asn,
            "peer_ip": elem.peer_address,
            "prefix": elem.fields["prefix"],
            "origins": None,
            "as_path": None,
        }
    return None

# Function to use COPY for bulk insert into TimescaleDB using asyncpg
async def copy_bgp_data(session, messages):
    if not messages:
        return
    data_tuples = [
        (msg['type'], msg['timestamp'], msg['collector'], msg['peer_as'], msg['peer_ip'], msg['prefix'], msg['origins'], msg['as_path'])
        for msg in messages
    ]
    async with engine.connect() as conn:
        raw_conn = await conn.get_raw_connection()
        asyncpg_conn = raw_conn._connection
        await asyncpg_conn.copy_records_to_table('ris', records=data_tuples, columns=['type', 'timestamp', 'collector', 'peer_as', 'peer_ip', 'prefix', 'origins', 'as_path'])

# Hashing function
def hash_batch(messages):
    hasher = hashlib.sha256()
    for message in messages:
        hasher.update(json.dumps(message, sort_keys=True).encode('utf-8'))
    return hasher.hexdigest()

# Leader election and failure handling logic
async def main():
    collector = "ris-live"
    stream = BGPStream(project=collector)
    batch_size = 10000
    cache_limit = 50000
    messages_batch = []
    cache = []

    async with Session() as session:
        for elem in stream:
            message = bgpstream_format(collector, elem)
            if message:
                messages_batch.append(message)
                cache.append(message)

                # Limit cache size to 50,000 messages
                if len(cache) > cache_limit:
                    cache.pop(0)

                # Only the leader writes to the database and broadcasts the batch
                if len(messages_batch) % batch_size == 0:
                    batch_hash = hash_batch(messages_batch)
                    if is_leader():
                        try:
                            await copy_bgp_data(session, messages_batch)
                            logger.info(f"Leader {rank} processed {len(messages_batch)} messages.")

                            # Broadcast the messages_batch and batch_hash
                            comm.bcast(json.dumps(messages_batch), root=rank)
                            comm.bcast(batch_hash, root=rank)
                        except Exception as e:
                            logger.error(f"Leader {rank} failed: {e}")
                            comm.bcast("error", root=rank)  # Notify failure
                            break  # Leader failure, exit and restart
                    else:
                        # Non-leaders cache and receive the broadcasted messages
                        broadcasted_batch = comm.bcast(None, root=0)
                        leader_hash = comm.bcast(None, root=0)
                        messages_batch = json.loads(broadcasted_batch)

                        # Insert into cache and process as a non-leader
                        cache.extend(messages_batch)

                        logger.info(f"Worker {rank} received batch of {len(messages_batch)} messages.")

                    messages_batch = []

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    import asyncio
    asyncio.run(main())
