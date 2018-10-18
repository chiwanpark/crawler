import asyncio
import asynctest
import aioredis
import os
from crawler.redis import RedisManager


class RedisTest(asynctest.TestCase):
    def setUp(self):
        self._host = os.environ.get('REDIS_HOST', 'localhost')
        self._port = os.environ.get('REDIS_PORT', 6379)

    async def tearDown(self):
        redis_uri = 'redis://{}:{}'.format(self._host, self._port)
        redis = await aioredis.create_redis(redis_uri)
        await redis.flushall()
        redis.close()
        await redis.wait_closed()

    async def test_connection(self):
        async with RedisManager(self._host, self._port, b'workerA') as m:
            self.assertTrue(m.connected)

    async def test_leader_election(self):
        async with RedisManager(self._host, self._port, b'worker1') as m1, \
            RedisManager(self._host, self._port, b'worker2') as m2:
            l1, l2 = await asyncio.gather(m1.get_leader(), m2.get_leader())
            self.assertTrue(l1 == l2)

    async def test_set_methods(self):
        async with RedisManager(self._host, self._port, b'worker1') as m:
            m.sadd(b'set_test', b'1')
            self.assertEqual(b'1', await m.spick(b'set_test'))
            m.sadd(b'set_test', b'2')
            pick = await m.spick(b'set_test')
            self.assertTrue(b'1' == pick or b'2' == pick)

    async def test_queue_methods(self):
        async with RedisManager(self._host, self._port, b'worker1') as m:
            await m.qpush(b'q', 'task 1')
            await m.qpush(b'q', 'task 2')

            self.assertEqual('task 1', await m.qpop(b'q'))
            self.assertEqual(1, await m.qfinish(b'q', 'task 1'))
            self.assertEqual(0, await m.qfinish(b'q', 'task 1'))

            self.assertEqual('task 2', await m.qpop(b'q'))
            self.assertEqual(1, await m.qfinish(b'q', 'task 2'))
            self.assertEqual(0, await m.qfinish(b'q', 'task 2'))

            self.assertEqual(None, await m.qpop(b'q'))
