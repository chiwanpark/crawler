import asyncio
import aioredis
import msgpack
from aioredis.commands import StringCommandsMixin as cmds


class RedisManager(object):
    def __init__(self, host, port, worker, loop=None):
        self._host = host
        self._port = port
        self._loop = asyncio.get_running_loop() if not loop else loop
        self._worker = worker
        self._client = None

    @property
    def connected(self):
        return self._client is not None and not self._client.closed

    async def __aenter__(self):
        if not self.connected:
            redis_uri = 'redis://{}:{}'.format(self._host, self._port)
            self._client = await aioredis.create_redis(redis_uri, loop=self._loop)
            await self._elect_leader()

        return self

    async def __aexit__(self, *args):
        if self.connected:
            self._client.close()
            await self._client.wait_closed()
        self._client = None

    async def _elect_leader(self):
        if not self.connected:
            return
        await self._client.set(b'LEADER', self._worker, expire=1200, exist=cmds.SET_IF_NOT_EXIST)

    def get_leader(self):
        return self._client.get(b'LEADER')

    async def is_leader(self):
        return (await self.get_leader()) == self._worker

    def _pack(self, value, use_msgpack):
        return msgpack.packb(value, use_bin_type=True) if use_msgpack else str(value).encode('utf-8')

    def _create_value_callback(self, fut, use_msgpack):
        def _callback(value_fut):
            value = value_fut.result()
            if not value:
                fut.set_result(None)
            else:
                fut.set_result(msgpack.unpackb(value, raw=False) if use_msgpack else value)
        return _callback

    def _unpack(self, value_fut, use_msgpack):
        fut = value_fut.get_loop().create_future()
        value_fut.add_done_callback(self._create_value_callback(fut, use_msgpack))
        return fut

    def sadd(self, key, *values, use_msgpack=True):
        assert len(values) > 0
        packed_values = [self._pack(value, use_msgpack) for value in values]
        return self._client.sadd(key, *packed_values)

    def spick(self, key, use_msgpack=True):
        return self._unpack(self._client.srandmember(key), use_msgpack)

    def srem(self, key, value, use_msgpack=True):
        packed = self._pack(value, use_msgpack)
        return self._client.srem(key, packed)

    def slen(self, key):
        return self._client.scard(key)

    def qlen(self, key):
        return self._client.llen(key)

    def qpush(self, key, *values, use_msgpack=True):
        assert len(values) > 0
        packed_values = [self._pack(value, use_msgpack) for value in values]
        return self._client.lpush(key, *packed_values)

    def qpop(self, key, use_msgpack=True):
        tmp_key = '{}_{}'.format(key, self._worker)
        return self._unpack(self._client.rpoplpush(key, tmp_key), use_msgpack)

    def qfinish(self, key, value, use_msgpack=True):
        tmp_key = '{}_{}'.format(key, self._worker)
        packed = self._pack(value, use_msgpack)
        return self._client.lrem(tmp_key, 1, packed)
