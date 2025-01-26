import logging
import asyncio
import queue as queueio
import rocksdbpy
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Callable

class Proxy:
    futures = []
    events = {}
    stages = None
    queue = None
    executor = None
    source = None
    target = None
    router = None
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
            source=None,
            target=None,
            router=None,
            memory={}, 
            events=[], 
            tasks: list[Callable] = []
        ):
        """
        Initialize the Proxy object with a list of tasks.

        Args:
            before_start (function): A function to call before the proxy starts.
            after_start (function): A function to call after the proxy starts.
            before_stop (function): A function to call before the proxy stops.
            after_stop (function): A function to call after the proxy stops.
            source (str): The source of the proxy.
            target (str): The target of the proxy.
            router (str): The router of the proxy.
            memory (dict): A Dictionary for cross-task memory.
            events (list): A dictionary of events that can be awaited.
            tasks (list): A list of task functions to be executed.
        """

        self.logger = logging.getLogger(__name__)
        self.tasks = tasks
        self.memory = memory
        self.events = {e: threading.Event() for e in events}
        self.before_start = before_start # Function
        self.after_start = after_start # Function
        self.before_stop = before_stop # Function
        self.after_stop = after_stop # Function
        self.loop = asyncio.get_running_loop()
        self.queue = queueio.Queue(maxsize=10000000)
        self.executor = ThreadPoolExecutor()
        self.db = rocksdbpy.open_default("/var/lib/rocksdb")

        self.start()

    async def start(self):
        # Before Start
        if self.before_start:
            self.before_start(
                self.source,
                self.target,
                self.router,
                self.queue,
                self.db,
                self.logger,
                self.events,
                self.memory
            )

        try:
            for _, func in enumerate(self.tasks, start=1):
                future =self.loop.run_in_executor(
                    self.executor,
                    func,
                    self.source,
                    self.target,
                    self.router,
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
            self.after_start(
                self.source,
                self.target,
                self.router,
                self.queue,
                self.db,
                self.logger,
                self.events,
                self.memory
            )

    def stop(self, signum=0):
        # Before Stop
        if self.before_stop:
            self.before_stop(
                signum,
                self.source,
                self.target,
                self.router,
                self.queue,
                self.db,
                self.logger,
                self.events,
                self.memory
            )

        self.db.close()
        self.executor.shutdown(wait=False)
        self.futures = []

        # After Stop
        if self.after_stop:
            self.after_stop(
                signum,
                self.source,
                self.target,
                self.router,
                self.queue,
                self.db,
                self.logger,
                self.events,
                self.memory
            )
    
