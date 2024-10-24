from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
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

    # Create database client
    postgres = create_async_engine(os.getenv("POSTGRESQL_DATABASE"), echo=False, future=True)

    while True:
        try:
            query = """
                WITH prefix_counts AS (
                    SELECT 
                        peer_ip, 
                        peer_as, 
                        CASE 
                            WHEN prefix LIKE '%:%' THEN 6 -- IPv6 addresses contain colons
                            ELSE 4 -- IPv4 addresses do not contain colons
                        END AS prefix_ipversion, 
                        host,
                        -- Set dump to the current timestamp when the query is run, ensuring it is timezone-aware
                        CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS dump,
                        COUNT(prefix) AS prefix_count
                    FROM ris
                    -- No filter on timestamp, so it processes all data from the ris table
                    GROUP BY peer_ip, peer_as, prefix_ipversion, host
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
                        dump,  -- Include the current timestamp as the dump
                        ROW_NUMBER() OVER (PARTITION BY dump, prefix_ipversion ORDER BY prefix_count) AS row_num,
                        COUNT(*) OVER (PARTITION BY dump, prefix_ipversion) AS total_count
                    FROM filtered_peers
                ),
                prefix_medians AS (
                    -- Calculate median based on the entire dataset in this query run
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
                        -- Automatically tag older IPv6 peers as full if their dump timestamp is before a certain date
                        WHEN pc.prefix_ipversion = 6 AND pc.dump < '2006-01-01 00:00:00' THEN 'F'
                        -- Tag peers as full if their prefix count is greater than 90% of the median
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

            # Insert into PostgreSQL
            async with postgres.begin() as conn:
                try:
                    # Start a transaction
                    await conn.execute(text("BEGIN"))

                    raw_conn = await conn.get_raw_connection()

                    # Insert into 'ris_peers' table
                    await raw_conn._connection.copy_records_to_table(
                        'ris_peers',
                        records=ris_peers,
                        columns=['peer_ip', 'peer_as', 'prefix_ipversion', 'host', 'prefix_count', 'dump', 'peer_tag']
                    )

                    # Commit the transaction
                    await conn.execute(text("COMMIT"))

                    logger.info("Data inserted into ris_peers table successfully.")

                except SQLAlchemyError as e:
                    # Rollback the transaction in case of an error
                    await conn.execute(text("ROLLBACK"))
                    logger.error(f"Error during database insertion, transaction rolled back: {e}")
                    raise  # Re-raise the exception

        except Exception as e:
            logger.error("Error", exc_info=True)
            # Wait before retrying to avoid overwhelming the system
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())
