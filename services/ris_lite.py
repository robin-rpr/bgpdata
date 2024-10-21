import os
import json
import logging
from mpi4py import MPI
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import MetaData, Table, Column, String, Integer, DateTime, Text
from datetime import datetime
import asyncpg
import asyncio

# Initialize logging
logger = logging.getLogger(__name__)

# Create the engine and session for PostgreSQL
engine = create_async_engine(os.getenv("POSTGRESQL_DATABASE"), echo=False, future=True)
Session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

# Define the 'ris_lite' table schema
metadata = MetaData()

ris_lite = Table(
    "ris_lite", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("timestamp", DateTime, nullable=False),
    Column("collector", String(50), nullable=False),
    Column("prefix", String(50), nullable=False),
    Column("full_peer_count", Integer, nullable=False),
    Column("partial_peer_count", Integer, nullable=False),
    Column("segment", Text, nullable=True),
)

# MPI setup
comm = MPI.COMM_WORLD
rank = comm.Get_rank()

# Function to insert received data into ris_lite
async def insert_into_ris_lite(session, messages_batch):
    data_tuples = [
        (msg['timestamp'], msg['collector'], msg['prefix'], 1, 0, msg['as_path'])
        for msg in messages_batch
        if msg['type'] == 'F'
    ]
    if data_tuples:
        async with engine.connect() as conn:
            raw_conn = await conn.get_raw_connection()
            asyncpg_conn = raw_conn._connection
            await asyncpg_conn.copy_records_to_table(
                'ris_lite',
                records=data_tuples,
                columns=['timestamp', 'collector', 'prefix', 'full_peer_count', 'partial_peer_count', 'segment']
            )

# Function to process received messages_batch
async def main():
    async with Session() as session:
        while True:
            # Receive the broadcasted batch from ris
            broadcasted_batch = comm.bcast(None, root=0)

            if broadcasted_batch is None:
                logger.info("No batch received. Sleeping for one second.")
                await asyncio.sleep(1)
                continue

            if broadcasted_batch == "error":
                logger.error("Received error notification from leader.")
                break

            messages_batch = json.loads(broadcasted_batch)
            await insert_into_ris_lite(session, messages_batch)
            logger.info(f"Worker {rank} inserted {len(messages_batch)} messages into ris_lite")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    import asyncio
    asyncio.run(main())
