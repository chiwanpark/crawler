import asyncio
from crawler.runner import Runner


def run():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(Runner(loop).run())
    loop.close()


if __name__ == '__main__':
    run()
