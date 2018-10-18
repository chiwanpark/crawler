import aiohttp
import asyncio
import random
import re


class ProxyProvider(object):
    def __init__(self):
        self._proxy_list_url = 'http://spys.me/proxy.txt'
        self._proxy_pattern = re.compile(
            '(?P<addr>[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}:[0-9]{1,5}) (?P<option>.+)')
        self._last_updated = ''
        self._proxies = []

    def _parse_proxy_line(self, line):
        match = self._proxy_pattern.match(line)
        if not match:
            return None

        addr = match.group('addr')
        option = match.group('option')
        country = option[:2]
        anonymity = option[3]
        https = '-S' in option[4:]
        google_passed = '+' == option[-1]

        return {
            'addr': addr,
            'country': country,
            'anonymity': anonymity,
            'support_https': https,
            'google_passed': google_passed
        }

    def _parse_proxy_list(self, content):
        lines = content.split('\n')
        if self._last_updated == lines[0]:
            return
        self._last_updated = lines[0]

        self._proxies = []
        for line in lines[4:]:
            proxy = self._parse_proxy_line(line.strip())
            if not proxy:
                continue
            self._proxies.append(proxy)

    async def _fetch_proxy_file(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(self._proxy_list_url) as res:
                if res.status != 200:
                    return None
                return await res.text()

    def pick(self):
        return self._proxies[random.randrange(len(self._proxies))]

    async def update_periodically(self):
        while True:
            content = await self._fetch_proxy_file()
            self._parse_proxy_list(content)

            await asyncio.sleep(3600)


if __name__ == '__main__':
    provider = ProxyProvider()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(provider.update_periodically())
