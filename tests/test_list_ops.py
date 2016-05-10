from twisted.internet import defer
from twisted.trial import unittest

from trex import redis

from .mixins import REDIS_HOST, REDIS_PORT


class TestRedisListOperations(unittest.TestCase):
    @defer.inlineCallbacks
    def testRedisLPUSHSingleValue(self):
        db = yield redis.Connection(REDIS_HOST, REDIS_PORT, reconnect=False)
        yield db.delete("trex:LPUSH")
        yield db.lpush("trex:LPUSH", "singlevalue")
        result = yield db.lpop("trex:LPUSH")
        self.assertEqual(result, "singlevalue")
        yield db.disconnect()

    @defer.inlineCallbacks
    def testRedisLPUSHListOfValues(self):
        db = yield redis.Connection(REDIS_HOST, REDIS_PORT, reconnect=False)
        yield db.delete("trex:LPUSH")
        yield db.lpush("trex:LPUSH", [1, 2, 3])
        result = yield db.lrange("trex:LPUSH", 0, -1)
        self.assertEqual(result, [3, 2, 1])
        yield db.disconnect()

    @defer.inlineCallbacks
    def testRedisRPUSHSingleValue(self):
        db = yield redis.Connection(REDIS_HOST, REDIS_PORT, reconnect=False)
        yield db.delete("trex:RPUSH")
        yield db.lpush("trex:RPUSH", "singlevalue")
        result = yield db.lpop("trex:RPUSH")
        self.assertEqual(result, "singlevalue")
        yield db.disconnect()

    @defer.inlineCallbacks
    def testRedisRPUSHListOfValues(self):
        db = yield redis.Connection(REDIS_HOST, REDIS_PORT, reconnect=False)
        yield db.delete("trex:RPUSH")
        yield db.lpush("trex:RPUSH", [1, 2, 3])
        result = yield db.lrange("trex:RPUSH", 0, -1)
        self.assertEqual(result, [3, 2, 1])
        yield db.disconnect()
