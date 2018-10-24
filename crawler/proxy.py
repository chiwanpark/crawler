import aiohttp
import time
import re


class ProxyProvider(object):
    def __init__(self, redis):
        self._proxy_list_url = 'http://spys.me/proxy.txt'
        self._proxy_pattern = re.compile(
            '(?P<addr>[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}:[0-9]{1,5}) (?P<option>.+)')
        self._last_updated = ''
        self._redis = redis

    def _parse_proxy_line(self, line):
        match = self._proxy_pattern.match(line)
        if not match:
            return None

        option = match.group('option')
        return {
            'addr': match.group('addr'),
            'country': option[:2],
            'anonymity': option[3],
            'support_https': '-S' in option[4:],
            'google_passed': '+' == option[-1]
        }

    async def _parse_proxy_list(self, content):
        lines = content.split('\n')
        if self._last_updated == lines[0]:
            return
        self._last_updated = lines[0]

        async with self._redis as redis:
            proxies = [self._parse_proxy_line(line.strip()) for line in lines[4:]]
            await redis.sadd(b'proxies', *proxies)

    async def _fetch_proxy_file(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(self._proxy_list_url) as res:
                if res.status != 200:
                    return None
                return await res.text()

    async def pick(self):
        async with self._redis as redis:
            return await redis.spick(b'proxies')

    async def update_once(self):
        content = await self._fetch_proxy_file()
        await self._parse_proxy_list(content)

    def accept(self, task):
        return task['task'] == 'proxy_update' and task['update_time'] <= time.time()

    async def do(self, task):
        assert self.accept(task)
        await self.update_once(self)

        return [{'task': 'proxy_update', 'update_time': time.time() + 1500}]
