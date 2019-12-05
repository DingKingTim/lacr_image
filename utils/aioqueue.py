import queue
import asyncio


class Queue:
    def __init__(self, max_size, timeout=.005):
        self._timeout = timeout
        self._queue = queue.Queue(maxsize=max_size)

    async def put(self, msg):
        while True:
            try:
                self._queue.put(msg, block=False)
                break
            except queue.Full:
                await asyncio.sleep(self._timeout)

    async def size(self):
        return self._queue.qsize()

    async def get(self):
        while True:
            try:
                return self._queue.get(block=False)
            except queue.Empty:
                await asyncio.sleep(self._timeout)

    def empty(self):
        return self._queue.empty()
