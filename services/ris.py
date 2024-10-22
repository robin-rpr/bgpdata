import os
import json
import logging
import hashlib
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import MetaData, Table, Column, String, Integer, DateTime, Text
from datetime import datetime
from mpi4py import MPI
import websocket
import asyncpg
import random
import json

# Custom JSON encoder that converts datetime objects to ISO 8601 strings
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

# Initialize logging
logger = logging.getLogger(__name__)

# Create the engine and session for PostgreSQL
engine = create_async_engine(os.getenv("POSTGRESQL_DATABASE"), echo=False, future=True)

# MPI setup
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

# Leader setup
leader_rank = 0

# Check if current process is the leader
def is_leader():
    return rank == leader_rank

# New leadership election logic
def elect_new_leader():
    global leader_rank
    eligible_ranks = list(range(size))  # All processes except the current failed one can be re-elected
    if leader_rank in eligible_ranks:
        eligible_ranks.remove(leader_rank)  # Exclude the current leader who failed

    if eligible_ranks:
        leader_rank = random.choice(eligible_ranks)
    else:
        leader_rank = None  # No eligible leaders left
    return leader_rank

# Function to format message into a dict format for TimescaleDB
def extract_messages(data):
    messages = []
    global_id = data['id']

    if len(data['announcements']) > 0:
        as_path = data['path'][:-1] if isinstance(data['path'][-1], list) else data['path']
        as_set = data['path'][-1] if isinstance(data['path'][-1], list) else None

        for announcement in data['announcements']:
            for prefix in announcement['prefixes']:
                messages.append({
                    "global_id": data['id'],
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
        global_id = data['id']
        for prefix in data['withdrawals']:
            messages.append({
                "global_id": data['id'],
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

    return global_id, messages

# Main function
async def main():
    while True:
        THRESHOLD = 10000
        messages_batch = []
        messages_size = 0

        try:
            ws = websocket.WebSocket()
            ws.connect("wss://ris-live.ripe.net/v1/ws/?client=bgpdata")
            ws.send(json.dumps({"type": "ris_subscribe", "data": {"type": "UPDATE"}}))

            for data in ws:
                parsed = json.loads(data)
                global_id, messages = extract_messages(parsed['data'])

                # Add messages to the batch
                messages_batch.extend(messages)
                messages_size += 1
                    
                # Process the batch when it reaches the batch size
                if messages_size % THRESHOLD == 0:
                    if is_leader():
                        # Leader: Writes to the database
                        data_tuples = [
                            (msg['global_id'], msg['timestamp'], msg['peer_ip'], msg['peer_as'], msg['host'], msg['type'], msg['as_path'], msg['community'], msg['origin'], msg['med'], msg['aggregator'], msg['next_hop'], msg['prefix'])
                            for msg in messages_batch
                        ]

                        async with engine.connect() as conn:
                            raw_conn = await conn.get_raw_connection()
                            await raw_conn._connection.copy_records_to_table(
                                'ris', records=data_tuples, columns=['global_id', 'timestamp', 'peer_ip', 'peer_as', 'host', 'type', 'as_path', 'community', 'origin', 'med', 'aggregator', 'next_hop', 'prefix']
                            )

                        logger.info(f"Leader {rank} processed {len(messages_batch)} messages.")

                        # Broadcast the global_id for synchronization
                        comm.bcast(global_id, root=leader_rank)
                    else:
                        # Non-leaders: Receive the global_id for synchronization purposes
                        message = comm.bcast(None, root=leader_rank)

                        if message == "error":
                            logger.error(f"Leader failure detected by worker {rank}")
                            new_leader = comm.bcast(None, root=rank)
                            if new_leader is not None:
                                logger.info(f"New leader is now: {new_leader}")
                        else:
                            logger.info(f"Worker {rank} received global_id: {message}")

                    # Reset the batch
                    messages_batch = []

        except Exception as e:
            if is_leader():
                logger.error(f"Leader {rank} failed: {e}")
                # Broadcast failure notification
                comm.bcast("error", root=rank)

                # Elect a new leader and notify all processes
                new_leader = elect_new_leader()
                comm.bcast(new_leader, root=MPI.COMM_WORLD.Get_rank())
                logger.info(f"New leader elected: {new_leader}")
            else:
                logger.error(f"Worker {rank} failed: {e}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    import asyncio
    asyncio.run(main())
