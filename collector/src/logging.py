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
import time

def logging_task(host, queue, logger, events, memory):
    """
    Task to periodically log the current state of the collector.
    """

    try:
        while True:
            timeout = 10

            # Compute kbit/s
            kbps_sent = (memory['bytes_sent'] * 8) / timeout / 1000
            kbps_received = (memory['bytes_received'] * 8) / timeout / 1000

            # Compute time lag
            time_lag_values = memory['time_lag'].values()
            maximum_lag = max(time_lag_values, default=0)  # Default to 0 if empty
            h, remainder = divmod(maximum_lag.total_seconds() if maximum_lag else 0, 3600)
            m, s = divmod(remainder, 60)

            match memory['task']:
                case 'rib':
                    logger.info(f"host={host} task={memory['task']} processing={memory['rows_processed']} rps receive={kbps_received:.2f} kbps send={kbps_sent:.2f} kbps queued={queue.qsize()}")
                case 'kafka':
                    logger.info(f"host={host} task={memory['task']} lag={int(h)}h {int(m)}m {int(s)}s receive={kbps_received:.2f} kbps send={kbps_sent:.2f} kbps queued={queue.qsize()}")
                case _:
                    logger.info(f"host={host} task={memory['task']} receive={kbps_received:.2f} kbps send={kbps_sent:.2f} kbps queued={queue.qsize()}")
                
            # Reset trackers
            memory['bytes_sent'] = 0
            memory['bytes_received'] = 0
            memory['rows_processed'] = 0
            time.sleep(timeout)
    except Exception as e:
        logger.error(e, exc_info=True)
        events['shutdown'].set()
        raise e