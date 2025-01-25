import queue as queueio
from config import *
from tasks import *
import select
import socket
import time

def sender_task(queue, host, port, db, status, logger):
    """
    Task to transmit messages from the queue to the TCP socket.
    Only updates offset in RocksDB once message is successfully sent.

    Args:
        queue (queue.Queue): The queue containing the messages to send.
        host (str): The host of the OpenBMP collector.
        port (int): The port of the OpenBMP collector.
        db (rocksdbpy.DB): The RocksDB database to store the offset.
        status (dict): A dictionary containing the current status and statistics.
        logger (logging.Logger): The logger to use.
    """
    sent_message = False
    backpressure_threshold = 1.0  # Threshold in seconds to detect backpressure
    send_delay = 0.0  # Initial delay between sends

    with socket.create_connection((host, port), timeout=60) as sock:
        while True:
            try:
                # Ensure the connection is alive
                ready_to_read, _, _ = select.select([sock], [], [], 0)
                if ready_to_read and not sock.recv(1, socket.MSG_PEEK):
                    raise ConnectionError("TCP connection closed by the peer")

                start_time = time.time()  # Start timing the send operation

                # Get the message from the queue
                message, offset, topic, partition = queue.get()

                # Send the message
                sock.sendall(message)
                status['bytes_sent'] += len(message)

                # Measure the time taken for sending the message
                send_time = time.time() - start_time
                if send_time > backpressure_threshold:
                    logger.warning(f"Detected backpressure: sending took {send_time:.2f}s")
                    send_delay = min(send_delay + 0.1, 5.0)  # Increase delay up to 5 seconds
                else:
                    send_delay = max(send_delay - 0.05, 0.0)  # Gradually reduce delay if no backpressure

                # Apply the delay before the next send if needed
                if send_delay > 0:
                    time.sleep(send_delay)

                if not sent_message:
                    sent_message = True  # Mark that a message has been sent
                    db.set(b'started', b'\x01')

                if partition != -1 and topic is not None:
                    key = f'offsets_{topic}_{partition}'.encode('utf-8')
                    db.set(key, offset.to_bytes(16, byteorder='big'))

                queue.task_done()  # Mark the message as processed

            except queueio.Empty:
                time.sleep(0.1)  # Sleep a bit if the queue is empty
            except ConnectionError as e:
                logger.error("TCP connection lost", exc_info=True)
                raise e
            except Exception as e:
                logger.error("Error sending message over TCP", exc_info=True)
                raise e
