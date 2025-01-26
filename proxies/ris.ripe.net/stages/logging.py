from datetime import datetime, timedelta
import struct
import time

def logging_task(status, queue, db, routeviews_hosts, ris_hosts, logger):
    """
    Task to periodically log the most recent timestamp, time lag, current poll interval, and consumption rate.
    This task runs within the main event loop.

    Args:
        status (dict): A dictionary containing the following keys:
            - time_lag (dict): A dictionary containing the current time lag of messages for each host.
            - time_preceived (dict): A dictionary containing the latest read timestamp for each host.
            - bytes_sent (int): The number of bytes sent since the last log.
            - bytes_received (int): The number of bytes received since the last log.
            - activity (str): The current activity of the collector.
        queue (queue.Queue): The queue containing the messages to send.
        db (rocksdbpy.DB): The RocksDB database.
        routeviews_hosts (list): A list of the Route Views hosts.
        ris_hosts (list): A list of the RIS hosts.
        logger (logging.Logger): The logger to use.
    """
    while True:
        seconds = 10
        time.sleep(seconds)  # Sleep for n seconds before logging

        # Compute kbit/s
        bytes_sent = status['bytes_sent']
        bytes_received = status['bytes_received']
        # Convert bytes to kilobits per second
        kbps_sent = (bytes_sent * 8) / seconds / 1000
        kbps_received = (bytes_received * 8) / seconds / 1000

        if status['activity'] == "RIB_DUMP":
            # RIB Dumping
            logger.info(f"{status['activity']}{(17 - len(status['activity'])) * ' '}| "
                        f"Receiving at ~{kbps_received:.2f} kbit/s, "
                        f"Sending at ~{kbps_sent:.2f} kbit/s, "
                        f"Queue size: ~{queue.qsize()}")
            
        elif status['activity'] == "BMP_STREAM":
            # BMP Streaming
            logger.info(f"{status['activity']}{(17 - len(status['activity'])) * ' '}| "
                        f"Receiving at ~{kbps_received:.2f} kbit/s, "
                        f"Sending at ~{kbps_sent:.2f} kbit/s, "
                        f"Queue size: ~{queue.qsize()}")
        
            for host in routeviews_hosts + ris_hosts:
                time_lag = status['time_lag'].get(host, timedelta(0))
                latest_route = struct.unpack('>d', db.get(f'timestamps_{host}'.encode('utf-8')) or b'\x00\x00\x00\x00\x00\x00\x00\x00')[0]
                hours, remainder = divmod(time_lag.total_seconds(), 3600)
                minutes, seconds = divmod(remainder, 60)
                logger.info(f"{status['activity']}{(17 - len(status['activity'])) * ' '}| "
                             f"{host}{(22 - len(host)) * ' '} | "
                             f"At: ~{status['time_preceived'].get(host, '(not measured yet)').strftime('%Y-%m-%d %H:%M:%S') if host in status['time_preceived'] else '(not measured yet)'}, "
                             f"Time lag: ~{int(hours)}h {int(minutes)}m {int(seconds)}s, "
                             f"Sends after: {datetime.fromtimestamp(latest_route).strftime('%Y-%m-%d %H:%M:%S')}")

        # Reset trackers
        status['bytes_sent'] = 0
        status['bytes_received'] = 0