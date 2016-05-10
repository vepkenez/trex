from twisted.internet import defer
from twisted.trial import unittest

import trex
from trex import redis

from .mixins import REDIS_HOST, REDIS_PORT


class TestRedisSort(unittest.TestCase):

    @defer.inlineCallbacks
    def setUp(self):
        self.db = yield redis.Connection(REDIS_HOST, REDIS_PORT, reconnect=False)
        yield self.db.delete('trex:values')
        yield self.db.lpush('trex:values', [5, 3, 19, 2, 4, 34, 12])

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.db.disconnect()

    @defer.inlineCallbacks
    def testSort(self):
        r = yield self.db.sort('trex:values')
        self.assertEqual([2, 3, 4, 5, 12, 19, 34], r)

    @defer.inlineCallbacks
    def testSortWithEndOnly(self):
        try:
            yield self.db.sort('trex:values', end=3)
        except trex.exceptions.RedisError:
            pass
        else:
            self.fail('RedisError not raised: no start parameter given')

    @defer.inlineCallbacks
    def testSortWithStartOnly(self):
        try:
            yield self.db.sort('trex:values', start=3)
        except trex.exceptions.RedisError:
            pass
        else:
            self.fail('RedisError not raised: no end parameter given')

    @defer.inlineCallbacks
    def testSortWithLimits(self):
        r = yield self.db.sort('trex:values', start=2, end=4)
        self.assertEqual([4, 5, 12, 19], r)

    @defer.inlineCallbacks
    def testSortAlpha(self):
        yield self.db.delete('trex:alphavals')
        yield self.db.lpush('trex:alphavals', ['dog', 'cat', 'apple'])

        r = yield self.db.sort('trex:alphavals', alpha=True)
        self.assertEquals(['apple', 'cat', 'dog'], r)
