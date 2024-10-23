from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
import aioredis
import asyncio
import logging
import os

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def main():
    """
    Main function to process RIS data and update peer information in the database.

    This asynchronous function performs the following key operations:
    1. Queries the RIS database to calculate prefix counts for each peer.
    2. Filters peers based on their prefix counts.
    3. Calculates median prefix counts for each IP version and dump.
    4. Identifies full and partial peers based on their prefix counts relative to the median.
    5. Inserts or updates peer information in the database.
    6. Handles various error scenarios and implements retry logic.

    The function runs indefinitely, periodically updating peer information until interrupted or an unhandled exception occurs.
    """

    # Create database clients
    postgres = create_async_engine(os.getenv("POSTGRESQL_DATABASE"), echo=False, future=True)
    redis = await aioredis.from_url(os.getenv("REDIS_DATABASE"))

    while True:
        try:
            query = """
                WITH prefix_counts AS (
                    SELECT 
                        peer_ip, 
                        peer_as, 
                        CASE 
                            WHEN prefix LIKE '%:%' THEN 6 
                            ELSE 4 
                        END AS prefix_ipversion, 
                        host,
                        FLOOR(EXTRACT(EPOCH FROM timestamp) * 1000 / 28800000) * 28800000 AS dump,
                        COUNT(prefix) AS prefix_count
                    FROM ris
                    GROUP BY peer_ip, peer_as, prefix_ipversion, host, FLOOR(EXTRACT(EPOCH FROM timestamp) * 1000 / 28800000) * 28800000
                ),
                filtered_peers AS (
                    SELECT *
                    FROM prefix_counts
                    WHERE prefix_count > CASE 
                                            WHEN prefix_ipversion = 4 THEN 100000
                                            ELSE 1000
                                        END
                ),
                ranked_counts AS (
                    SELECT 
                        peer_ip, 
                        peer_as, 
                        prefix_ipversion, 
                        host,
                        prefix_count,
                        dump,  
                        ROW_NUMBER() OVER (PARTITION BY dump, prefix_ipversion ORDER BY prefix_count) AS row_num,
                        COUNT(*) OVER (PARTITION BY dump, prefix_ipversion) AS total_count
                    FROM filtered_peers
                ),
                prefix_medians AS (
                    SELECT 
                        dump,
                        prefix_ipversion, 
                        AVG(prefix_count) AS median_prefix_count
                    FROM ranked_counts
                    WHERE row_num IN (total_count / 2, total_count / 2 + 1)
                    GROUP BY dump, prefix_ipversion
                )
                SELECT 
                    pc.peer_ip, 
                    pc.peer_as, 
                    pc.prefix_ipversion, 
                    pc.host,
                    pc.prefix_count,
                    pc.dump,
                    CASE 
                        WHEN pc.prefix_ipversion = 6 AND pc.dump < 1136073600000 THEN 'F'
                        WHEN pc.prefix_count > pm.median_prefix_count * 0.9 THEN 'F'
                        ELSE 'P'
                    END AS peer_tag
                FROM filtered_peers pc
                JOIN prefix_medians pm 
                ON pc.dump = pm.dump AND pc.prefix_ipversion = pm.prefix_ipversion;
            """

            # Query Postgres
            async with postgres.begin() as conn:
                try:
                    result = await conn.execute(text(query))
                    ris_peers = result.fetchall()
                
                except SQLAlchemyError as e:
                    # If an exception occurs, log the error
                    logger.error(f"Error during database query: {e}", exc_info=True)
                    raise  # Re-raise the exception

            # Insert into Redis
            try:
                # Start a pipeline
                pipe = redis.pipeline()

                # Check if the ris_peers table exists and delete it if it does
                exists = await redis.exists('ris_peers')
                if exists:
                    # Delete the old table contents if it exists
                    pipe.delete('ris_peers')

                # Prepare all peer data in a bulk insert
                for peer in ris_peers:
                    # Store each peer as a hash or list based on your need
                    peer_key = f"peer:{peer['peer_ip']}"
                    # Add multiple HSETs to the pipeline, to be executed in bulk
                    pipe.hset(peer_key, mapping={
                        'peer_ip': peer['peer_ip'],
                        'peer_as': peer['peer_as'],
                        'prefix_ipversion': peer['prefix_ipversion'],
                        'host': peer['host'],
                        'prefix_count': peer['prefix_count'],
                        'dump': peer['dump'],
                        'peer_tag': peer['peer_tag']
                    })

                # Execute all operations in the pipeline as a transaction
                await pipe.execute()
                print("Redis transaction committed successfully with bulk insert.")
            except Exception as e:
                print(f"Error during Redis transaction: {e}")
                # In case of an error, Redis will discard the transaction (DISCARD is automatic)
                raise  # Re-raise the exception

        except Exception as e:
            logger.error("Error", exc_info=True)
            # Wait before retrying the message to avoid overwhelming the system
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())
