# coding:utf-8
import logging
import asyncio
import os
import sys
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../lib/site-packages")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from hbmqtt.client import MQTTClient, ConnectException



#
# This sample shows how to publish messages to broker using different QOS
# Debug outputs shows the message flows
#

logger = logging.getLogger(__name__)

config = {
    'will': {
        'topic': '/will/client',
        'message': b'Dead or alive',
        'qos': 0x01,
        'retain': True
    }
}
#C = MQTTClient(config=config)
C = MQTTClient()


def disconnected(future):
    print("DISCONNECTED")
    asyncio.get_event_loop().stop()


@asyncio.coroutine
def test_coro():
    yield from C.connect('mqtt://172.30.81.246/')
    tasks = [
        asyncio.ensure_future(C.publish('a/b', b'TEST MESSAGE WITH QOS_0')),
        asyncio.ensure_future(C.publish('a/b', b'TEST MESSAGE WITH QOS_1', qos=0x01)),
        asyncio.ensure_future(C.publish('a/b', b'TEST MESSAGE WITH QOS_2', qos=0x02)),
    ]
    yield from asyncio.wait(tasks)
    logger.info("messages published")
    yield from C.disconnect()


@asyncio.coroutine
def test_coro2():
    try:
        yield from C.connect('mqtt://172.29.17.3/')
        # future = yield from C.connect('mqtt://127.0.0.1/')
        # future.add_done_callback(disconnected)
        i = 0
        while True:
            i += 1
            ssss = "AAAA" + str(i)
            message = yield from C.publish(ssss, b'TEST MESSAGE WITH QOS_0', qos=0x00)
            message = yield from C.publish(ssss, b'TEST MESSAGE WITH QOS_1', qos=0x01)
            message = yield from C.publish(ssss, b'TEST MESSAGE WITH QOS_2', qos=0x02)
            time.sleep(5)
        #print(message)
        logger.info("messages published")
        yield from C.disconnect()
    except ConnectException as ce:
        logger.error("Connection failed: %s" % ce)
        asyncio.get_event_loop().stop()


if __name__ == '__main__':
    formatter = "[%(asctime)s] %(name)s {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.DEBUG, format=formatter)
    asyncio.async(test_coro2())
    try:
        asyncio.get_event_loop().run_forever()
    finally:
        asyncio.get_event_loop().close()