import asyncio
import functools
import signal
from crawler.redis import RedisManager
from crawler.proxy import ProxyProvider
from crawler.util import env


class Runner(object):
    def __init__(self, loop):
        self._worker = env('WORKER', 'WORKER_NAME_NOT_GIVEN')
        self._loop = loop

        self._loop.add_signal_handler(
            signal.SIGTERM,
            functools.partial(asyncio.ensure_future(self._shutdown(signal.SIGTERM, self._loop))))

        redis_host, redis_port = env('REDIS_HOST', 'localhost'), int(env('REDIS_PORT', 6379))
        self._redis = RedisManager(redis_host, redis_port, self._worker, self._loop)

    async def _shutdown(self, sig):
        all_tasks = asyncio.Task.all_tasks()
        current_task = asyncio.Task.current_task()
        remaining_tasks = [task for task in all_tasks if task is not current_task]
        for task in remaining_tasks:
            task.cancel()
        await asyncio.gather(*remaining_tasks, return_exceptions=True)
        self._loop.stop()

    def _register_proxy_provider(self):
        for app in self._apps:
            if isinstance(app, ProxyProvider):
                return

        self._apps.append(ProxyProvider(self._redis))

    def _init_apps(self):
        self._apps = []

    async def _dispatch(self, task):
        for app in self._apps:
            if app.accept(task):
                return app

        return None

    async def run(self):
        self._init_apps()

        queue_empty = False
        while True:
            try:
                if queue_empty:
                    await asyncio.sleep(int(env('SLEEP_QUEUE_EMPTY', 60)))
                    queue_empty = False

                async with self._redis as redis:
                    if await redis.is_leader():
                        self._register_proxy_provider()

                    task = await redis.qpop(b'TASK_QUEUE')
                    if not task:
                        queue_empty = True
                        continue

                    app = self._dispatch(task)
                    if app is None:
                        redis.qcancel(b'TASK_QUEUE', task)
                        continue

                children = await app.do(task)
                if len(children) > 0:
                    async with self._redis as redis:
                        await redis.qpush(b'TASK_QUEUE', children)
            except asyncio.CancelledError:
                break
