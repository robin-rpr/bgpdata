"""
BGPDATA - BGP Data Collection and Analytics Service

This software is part of the BGPDATA project, which is designed to collect, process, and analyze BGP data from various sources.
It helps researchers and network operators get insights into their network by providing a scalable and reliable way to analyze and inspect historical and live BGP data from Route Collectors around the world.

Author: Robin Röper

© 2024 BGPDATA. All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
1. Redistributions of source code must retain the above copyright notice, this list of conditions, and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions, and the following disclaimer in the documentation and/or other materials provided with the distribution.
3. Neither the name of BGPDATA nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""
import queue as queueio
import select
import socket
import time

def sender_task(openbmp, queue, db, logger, memory):
    """
    Task to transmit messages from the queue to the OpenBMP TCP socket.
    """

    started = False # If we have sent at least one message
    backpressure_threshold = 1.0  # Threshold in seconds to detect backpressure
    send_delay = 0.0  # Initial delay between sends

    # Create a connection to the openbmp
    logger.info(f"Connecting to {openbmp}")
    with socket.create_connection((openbmp.split(':')[0], int(openbmp.split(':')[1])), timeout=60) as sock:
        while True:
            try:
                # Ensure the connection is alive
                ready_to_read, _, _ = select.select([sock], [], [], 0)
                if ready_to_read and not sock.recv(1, socket.MSG_PEEK):
                    raise ConnectionError("TCP connection closed by the peer")

                # Get the message from the queue
                message, offset, topic, partition, update = queue.get()

                # Start timing the send operation
                start_time = time.time()

                # Send the message
                sock.sendall(message)
                memory['bytes_sent'] += len(message)
                
                # Measure the time taken for sending the message
                send_time = time.time() - start_time
                if send_time > backpressure_threshold:
                    logger.warning(f"Detected backpressure: sending took {send_time:.2f}s")
                    send_delay = min(send_delay + 0.1, 5.0) # Increase delay up to 5 seconds
                else:
                    send_delay = max(send_delay - 0.05, 0.0) # Gradually reduce delay if no backpressure

                # Apply the delay before the next send if needed
                if send_delay > 0:
                    time.sleep(send_delay)

                if not started and update:
                    started = True
                    # Set database as started
                    db.set(b'started', b'\x01')

                if partition != -1 and topic is not None:
                    key = f'offset_{topic}_{partition}'.encode('utf-8')
                    db.set(key, offset.to_bytes(16, byteorder='big'))

                # Message sent.
                queue.task_done()

            except queueio.Empty:
                # Sleep a bit if the queue is empty.
                time.sleep(0.1)
            except ConnectionError as e:
                # Connection lost, raise the error.
                logger.error("TCP connection lost", exc_info=True)
                raise e
            except Exception as e:
                # Some other error occurred
                logger.error("Error sending message over TCP", exc_info=True)
                raise e