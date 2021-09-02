import logging
import asyncio
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../lib/site-packages")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from hbmqtt.broker import Broker

logger = logging.getLogger(__name__)

config = {
    'listeners': {
        'default': {
            'type': 'tcp',
            'bind': '0.0.0.0:1883',
        },
        'ws-mqtt': {
            'bind': '127.0.0.1:8080',
            'type': 'ws',
            'max_connections': 10,
        },
    },
    'sys_interval': 10,
    'auth': {
        'allow-anonymous': True,
        'password-file': os.path.join(os.path.dirname(os.path.realpath(__file__)), "passwd"),
        'plugins': [
            'auth_file', 'auth_anonymous'
        ]

    },
    'topic-check': {
        'enabled': True,
        'plugins': [
    #         'topic_taboo'
        ]
    }
}

broker = Broker(config)


@asyncio.coroutine
def test_coro():
    yield from broker.start()


if __name__ == '__main__':
    print("ok")
    formatter = "[%(asctime)s] :: %(levelname)s :: %(name)s :: %(message)s"
    logging.basicConfig(level=logging.ERROR, format=formatter)
    asyncio.get_event_loop().run_until_complete(test_coro())
    asyncio.get_event_loop().run_forever()
    # print("ok")
    # for i in range(100, 601):
    #     ss = "A123456789"
    #     print(ss + str(i))