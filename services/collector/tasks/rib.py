from protocols.bmp import BMPv3
from config import *
from tasks import *
import bgpkit
import struct
import time

def rib_task(queue, db, status, collectors, provider, events, logger):
    """
    Task to inject RIB messages from MRT Data Dumps into the queue.

    Args:
        queue (queue.Queue): The queue to add the messages to.
        db (rocksdbpy.DB): The RocksDB database.
        status (dict): A dictionary containing the following keys:
            - time_lag (dict): A dictionary containing the current time lag of messages for each host.
            - time_preceived (dict): A dictionary containing the latest read timestamp for each host.
            - bytes_sent (int): The number of bytes sent since the last log.
            - bytes_received (int): The number of bytes received since the last log.
            - activity (str): The current activity of the collector.
        collectors (list): A list of tuples containing the host and URL of the RIB Data Dumps.
        provider (str): The provider of the MRT Data Dumps.
        events (dict): A dictionary containing the following keys:
            - route-views_injection (threading.Event): The event to wait for before starting.
            - route-views_provision (threading.Event): The event to wait for before starting.
            - ris_injection (threading.Event): The event to wait for before starting.
            - ris_provision (threading.Event): The event to wait for before starting.
        logger (logging.Logger): The logger to use.
    """

    # If the event is set, the provider is already initialized, skip
    if events[f"{provider}_provision"].is_set():
        return

    # Set the activity
    status['activity'] = "RIB_DUMP"
    logger.info(f"Initiating RIB Injection from {provider} collectors...")

    try:
        for host, url in collectors:
            logger.info(f"Injecting RIB from {provider} of {host} via {url}")

            batch = []

            # Initialize the timestamp if it's not set
            if db.get(f'timestamps_{host}'.encode('utf-8')) is None:
                db.set(f'timestamps_{host}'.encode('utf-8'), b'\x00\x00\x00\x00\x00\x00\x00\x00')  # Store 0 as the initial value

            while True:
                try:
                    # Parse the RIB Data Dump via BGPKit
                    # Learn more at https://bgpkit.com/
                    parser = bgpkit.Parser(url=url, cache_dir='.cache')

                    for elem in parser:
                        # Decode the stored timestamp
                        stored_timestamp = struct.unpack('>d', db.get(f'timestamps_{host}'.encode('utf-8')))[0]

                        # Update the timestamp if it's the freshest
                        if float(elem['timestamp']) > stored_timestamp:
                            db.set(f'timestamps_{host}'.encode('utf-8'), struct.pack('>d', float(elem['timestamp'])))

                        # Construct the BMP message
                        messages = BMPv3.construct(
                            host,
                            elem['peer_ip'],
                            elem['peer_asn'],
                            elem['timestamp'],
                            "UPDATE",
                            [
                                [int(asn) for asn in part[1:-1].split(',')] if part.startswith('{') and part.endswith('}')
                                else int(part)
                                for part in elem['as_path'].split()
                            ],
                            elem['origin'],
                            [
                                # Only include compliant communities with 2 or 3 parts that are all valid integers
                                [int(part) for part in comm.split(
                                    ":")[1:] if part.isdigit()]
                                for comm in (elem.get("communities") or [])
                                if len(comm.split(":")) in {2, 3} and all(p.isdigit() for p in comm.split(":")[1:])
                            ],
                            [
                                {
                                    "next_hop": elem["next_hop"],
                                    "prefixes": [elem["prefix"]]
                                }
                            ],
                            [],
                            None,
                            0
                        )

                        # Update the bytes received counter
                        status['bytes_received'] += sum(len(message) for message in messages)

                        # Add the messages to the batch
                        batch.extend(messages)

                    break  # Exit retry loop when successful

                except Exception as e:
                    logger.warning(
                        f"Retrieving RIB from {provider} {host} via {url} failed, retrying...", exc_info=True)
                    time.sleep(10)  # Wait 10 seconds before retrying

            # Add the messages to the queue
            for message in batch:
                queue.put((message, 0, None, -1))

    except Exception as e:
        logger.error(
            f"Error injecting RIB from {provider} collectors: {e}", exc_info=True)
        raise e

    events[f"{provider}_injection"].set()
