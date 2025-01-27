from protocols.bmp import BMPv3
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import bgpkit
import struct
import time

def rib_task(target, router, queue, db, logger, events, memory):
    """
    Task to inject RIB messages from MRT Data Dumps into the queue.
    """

    # Return if already done
    if events['provision'].is_set():
        return

    # Broadcast the stage
    memory['active_stage'] = "RIB_DUMP"

    try:
        # Find the latest RIB file
        if router == "route-views2":
            index = f"https://archive.routeviews.org/bgpdata/{datetime.now().year}.{datetime.now().month:02d}/RIBS/"
        else:
            index = f"https://archive.routeviews.org/{router}/bgpdata/{datetime.now().year}.{datetime.now().month:02d}/RIBS/"

        response = requests.get(index, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        latest = soup.find_all('a')[-1].text
        url = f'{index}{latest}'

        # Store the timestamp (equal to 0)
        db.set(f'timestamp'.encode('utf-8'), b'\x00\x00\x00\x00\x00\x00\x00\x00')

        # Extracted messages
        messages = []

        while True:
            try:
                # Parse the RIB Data Dump via BGPKit
                # Learn more at https://bgpkit.com/
                parser = bgpkit.Parser(url=url, cache_dir='.cache')

                for elem in parser:
                    # Load the timestamp
                    stored = struct.unpack('>d', db.get(f'timestamp'.encode('utf-8')))[0]

                    # Update the timestamp if we find an earlier one
                    if float(elem['timestamp']) > stored:
                        db.set(f'timestamp'.encode('utf-8'), struct.pack('>d', float(elem['timestamp'])))

                    # Construct the BMP message
                    batch = BMPv3.construct(
                        collector=router,
                        peer_ip=elem['peer_ip'],
                        peer_asn=elem['peer_asn'],
                        timestamp=elem['timestamp'],
                        msg_type="UPDATE",
                        path=[
                            [int(asn) for asn in part[1:-1].split(',')] if part.startswith('{') and part.endswith('}')
                            else int(part)
                            for part in elem['as_path'].split()
                        ],
                        origin=elem['origin'],
                        community=[
                            # Only include compliant communities with 2 or 3 parts that are all valid integers
                            [int(part) for part in comm.split(
                                ":")[1:] if part.isdigit()]
                            for comm in (elem.get("communities") or [])
                            if len(comm.split(":")) in {2, 3} and all(p.isdigit() for p in comm.split(":")[1:])
                        ],
                        announcements=[
                            {
                                "next_hop": elem["next_hop"],
                                "prefixes": [elem["prefix"]]
                            }
                        ],
                        withdrawals=[],
                        med=0
                    )

                    # Track the bytes received metric
                    memory['bytes_received'] += sum(len(message) for message in messages)

                    # Add batch to messages
                    messages.extend(batch)

                # Exit retry loop
                break

            except Exception as e:
                logger.warning('Failed to download RIB, retrying in 10 seconds...', exc_info=True)
                time.sleep(10)  # Wait 10 seconds before retrying
                messages = [] # Reset messages

        # Finally, enqueue the messages
        for message in messages:
            queue.put((message, 0, None, -1))

    except Exception as e:
        logger.error(
            f"RIB Injection Failed: {e}", exc_info=True)
        raise e

    # Set the injection event
    events['injection'].set()
