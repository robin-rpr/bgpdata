from datetime import datetime
import struct
import time

def logging_task(openbmp, host, queue, db, logger, events, memory):
    """
    Task to periodically log the current state of the collector.
    """
    
    while True:
        timeout = 10

        # Compute kbit/s
        kbps_sent = (memory['bytes_sent'] * 8) / timeout / 1000
        kbps_received = (memory['bytes_received'] * 8) / timeout / 1000

        # Compute time lag
        h, remainder = divmod(memory['time_lag'].total_seconds(), 3600)
        m, s = divmod(remainder, 60)

        logger.info(f"host={host} openbmp={openbmp} source={memory['source']} at={memory['time_preceived'].strftime('%Y-%m-%d %H:%M:%S') if memory['time_preceived'] is not None else '(no data)'} lag={int(h)}h {int(m)}m {int(s)}s receive={kbps_received:.2f} kbps send={kbps_sent:.2f} kbps queued={queue.qsize()}")

        # Reset trackers
        memory['bytes_sent'] = 0
        memory['bytes_received'] = 0

        time.sleep(timeout)