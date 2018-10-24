import asynctest
import aioredis
import os
import time
from crawler.proxy import ProxyProvider
from crawler.redis import RedisManager


class ProxyProviderTest(asynctest.TestCase):
    async def setUp(self):
        self._host = os.environ.get('REDIS_HOST', 'localhost')
        self._port = os.environ.get('REDIS_PORT', 6379)
        self._redis = RedisManager(self._host, self._port, 'proxy-provider-test')

    async def tearDown(self):
        redis_uri = 'redis://{}:{}'.format(self._host, self._port)
        redis = await aioredis.create_redis(redis_uri)
        await redis.flushall()
        redis.close()
        await redis.wait_closed()

    async def test_proxy_update(self):
        proxies = ProxyProvider(self._redis)
        await proxies.update_once()
        picked = await proxies.pick()

        self.assertTrue(picked is not None)

    async def test_task_accept(self):
        task = {'task': 'proxy_update', 'update_time': time.time()}
        async with self._redis as redis:
            await redis.qpush(b'TASK_QUEUE', task)

        proxies = ProxyProvider(self._redis)
        async with self._redis as redis:
            task = await redis.qpop(b'TASK_QUEUE')
        self.assertTrue(proxies.accept(task))
