import asyncio
import aioredis
from synqueue.status.status import Status


class RedisStatus(Status):

    def __init__(self, **kwargs):
        self.prefix = kwargs.get("prefix", "offset")
        self.host = kwargs.get("host", "127.0.0.1")
        self.port = kwargs.get("port", 6379)
        self.password = kwargs.get("pwd", None)
        self.db = kwargs.get("db", 0)
        self.loop = kwargs.get("loop", asyncio.get_running_loop())
        self.conn = None

    async def open(self, *kwargs):
        self.conn = await aioredis.create_redis_pool(
            address=f"redis://{self.host}",
            db=self.db,
            password=self.password,
            maxsize=10,
            loop=self.loop)

    async def close(self):
        self.conn.close()
        await self.conn.wait_closed()

    async def update(self, topic: str, partition: int, offset: int) -> bool:
        try:
            res = await self.conn.execute(
                "hset", self.prefix, f"{topic}_{partition}", offset)
            return True
        except:
            return False

    async def read(self, topic: str, partition: int) -> int:
        try:
            res = await self.conn.execute(
                "hget", self.prefix, f"{topic}_{partition}")
            return 0 if res is None else int(res)
        except:
            return 0



