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
