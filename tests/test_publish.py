from twisted.internet import defer
from twisted.trial import unittest

from trex import redis

from .mixins import REDIS_HOST, REDIS_PORT


class TestRedisConnections(unittest.TestCase):
    @defer.inlineCallbacks
    def testRedisPublish(self):
        db = yield redis.Connection(REDIS_HOST, REDIS_PORT, reconnect=False)

        for value in ("foo", "bar"):
            yield db.publish("test_publish", value)

        yield db.disconnect()
