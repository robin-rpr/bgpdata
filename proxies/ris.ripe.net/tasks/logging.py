from datetime import datetime
import struct
import time

def logging_task(source, target, router, queue, db, logger, events, memory):
    """
    Task to periodically log the most recent timestamp, time lag, current poll interval, and consumption rate.
    """
    
    while True:
        seconds = 10
        time.sleep(seconds)  # Sleep for n seconds before logging

        # Compute kbit/s
        bytes_sent = memory['bytes_sent']
        bytes_received = memory['bytes_received']
        # Convert bytes to kilobits per second
        kbps_sent = (bytes_sent * 8) / seconds / 1000
        kbps_received = (bytes_received * 8) / seconds / 1000

        if memory['active_stage'] == "RIB_DUMP":
            # RIB Dumping
            logger.info(f"{memory['active_stage']}{(17 - len(memory['active_stage'])) * ' '}| "
                        f"Receiving at ~{kbps_received:.2f} kbit/s, "
                        f"Sending at ~{kbps_sent:.2f} kbit/s, "
                        f"Queue size: ~{queue.qsize()}")
            
        elif memory['active_stage'] == "KAFKA_STREAM":
            # Kafka Streaming
            logger.info(f"{memory['active_stage']}{(17 - len(memory['active_stage'])) * ' '}| "
                        f"Receiving at ~{kbps_received:.2f} kbit/s, "
                        f"Sending at ~{kbps_sent:.2f} kbit/s, "
                        f"Queue size: ~{queue.qsize()}")
        
            earliest_message = struct.unpack('>d', db.get(f'timestamp'.encode('utf-8')) or b'\x00\x00\x00\x00\x00\x00\x00\x00')[0]
            hours, remainder = divmod(memory['time_lag'].total_seconds(), 3600)
            minutes, seconds = divmod(remainder, 60)
            logger.info(f"{memory['active_stage']}{(17 - len(memory['active_stage'])) * ' '}| "
                            f"{22 * ' '} | "
                            f"At: ~{memory['time_preceived'].strftime('%Y-%m-%d %H:%M:%S') if memory['time_preceived'] is not None else '(Not measured yet)'}, "
                            f"Time lag: ~{int(hours)}h {int(minutes)}m {int(seconds)}s, "
                            f"Sends after: {datetime.fromtimestamp(earliest_message).strftime('%Y-%m-%d %H:%M:%S')}")

        # Reset trackers
        memory['bytes_sent'] = 0
        memory['bytes_received'] = 0