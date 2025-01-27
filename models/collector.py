import logging
import asyncio
import queue as queueio
import rocksdbpy
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Callable

class Collector:
    futures = []
    events = {}
    stages = None
    queue = None
    executor = None
    source = None
    openbmp = None
    host = None
    loop = None
    memory = {}
    logger = None
    db = None

    def __init__(
            self, 
            before_start=None,
            after_start=None,
            before_stop=None,
            after_stop=None,
            openbmp=None,
            host=None,
            memory={}, 
            events=[], 
            tasks: list[Callable] = []
        ):
        """
        Initialize the Collector object with a list of tasks.

        Args:
            before_start (function): A function to call before the collector starts.
            after_start (function): A function to call after the collector starts.
            before_stop (function): A function to call before the collector stops.
            after_stop (function): A function to call after the collector stops.
            openbmp (str): The OpenBMP collector address.
            host (str): The selected collector host.
            memory (dict): A Dictionary for cross-task memory.
            events (list): A dictionary of events that can be awaited.
            tasks (list): A list of task functions to be executed.
        """

        self.logger = logging.getLogger(__name__)
        self.tasks = tasks
        self.memory = memory
        self.openbmp = openbmp
        self.host = host
        self.events = {e: threading.Event() for e in events}
        self.before_start = before_start # Function
        self.after_start = after_start # Function
        self.before_stop = before_stop # Function
        self.after_stop = after_stop # Function
        self.loop = asyncio.get_running_loop()
        self.queue = queueio.Queue(maxsize=10000000)
        self.executor = ThreadPoolExecutor()
        self.db = rocksdbpy.open_default("/var/lib/rocksdb")

    async def start(self):
        # Before Start
        if self.before_start:
            self.before_start(self)

        try:
            for _, func in enumerate(self.tasks, start=1):
                future = self.loop.run_in_executor(
                    self.executor,
                    func,
                    self.openbmp,
                    self.host,
                    self.queue,
                    self.db,
                    self.logger,
                    self.events,
                    self.memory
                )
                self.futures.append(asyncio.wrap_future(future))

            # Keep loop alive
            await asyncio.gather(*self.futures)
        except Exception as e:
            self.logger.critical(e, exc_info=True)
        finally:
            self.stop(1)

        # After Start
        if self.after_start:
            self.after_start(self)

    def stop(self, signum=0):
        # Before Stop
        if self.before_stop:
            self.before_stop(signum, self)

        self.db.close()
        self.executor.shutdown(wait=False)
        self.futures = []

        # After Stop
        if self.after_stop:
            self.after_stop(signum, self)
    
