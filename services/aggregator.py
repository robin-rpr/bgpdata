"""
BGPDATA - BGP Data Collection and Analytics Service

This software is part of the BGPDATA project, which is designed to collect, process, and analyze BGP data from various sources.
It helps researchers and network operators get insights into their network by providing a scalable and reliable way to collect and process BGP data.

Author: Robin Röper

© 2024 BGPDATA. All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
1. Redistributions of source code must retain the above copyright notice, this list of conditions, and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions, and the following disclaimer in the documentation and/or other materials provided with the distribution.
3. Neither the name of BGPDATA nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""
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
