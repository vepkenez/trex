import sys

from twisted.trial import unittest
from twisted.internet import defer
from twisted.python import log

import trex
from trex import redis

from .mixins import REDIS_HOST, REDIS_PORT

# log.startLogging(sys.stdout)


class TestRedisConnections(unittest.TestCase):
    @defer.inlineCallbacks
    def testRedisConnection(self):
        rapi = yield redis.Connection(REDIS_HOST, REDIS_PORT)

        # test set() operation
        transaction = yield rapi.multi("trex:test_transaction")
        self.assertTrue(transaction.inTransaction)
        for key, value in (("trex:test_transaction", "foo"),
                           ("trex:test_transaction", "bar")):
            yield transaction.set(key, value)
        yield transaction.commit()
        self.assertFalse(transaction.inTransaction)
        result = yield rapi.get("trex:test_transaction")
        self.assertEqual(result, "bar")

        yield rapi.disconnect()

    @defer.inlineCallbacks
    def testRedisWithOnlyWatchUnwatch(self):
        rapi = yield redis.Connection(REDIS_HOST, REDIS_PORT)

        k = "trex:testRedisWithOnlyWatchAndUnwatch"
        tx = yield rapi.watch(k)
        self.assertTrue(tx.inTransaction)
        yield tx.set(k, "bar")
        v = yield tx.get(k)
        self.assertEqual("bar", v)
        yield tx.unwatch()
        self.assertFalse(tx.inTransaction)

        yield rapi.disconnect()

    @defer.inlineCallbacks
    def testRedisWithWatchAndMulti(self):
        rapi = yield redis.Connection(REDIS_HOST, REDIS_PORT)

        tx = yield rapi.watch("trex:testRedisWithWatchAndMulti")
        yield tx.multi()
        yield tx.unwatch()
        self.assertTrue(tx.inTransaction)
        yield tx.commit()
        self.assertFalse(tx.inTransaction)

        yield rapi.disconnect()

    # some sort of probabilistic test
    @defer.inlineCallbacks
    def testWatchAndPools_1(self):
        rapi = yield redis.ConnectionPool(REDIS_HOST, REDIS_PORT,
                                               poolsize=2, reconnect=False)
        tx1 = yield rapi.watch("foobar")
        tx2 = yield tx1.watch("foobaz")
        self.assertTrue(id(tx1) == id(tx2))
        yield rapi.disconnect()

    # some sort of probabilistic test
    @defer.inlineCallbacks
    def testWatchAndPools_2(self):
        rapi = yield redis.ConnectionPool(REDIS_HOST, REDIS_PORT,
                                               poolsize=2, reconnect=False)
        tx1 = yield rapi.watch("foobar")
        tx2 = yield rapi.watch("foobaz")
        self.assertTrue(id(tx1) != id(tx2))
        yield rapi.disconnect()

    @defer.inlineCallbacks
    def testWatchEdgeCase_1(self):
        rapi = yield redis.Connection(REDIS_HOST, REDIS_PORT)

        tx = yield rapi.multi("foobar")
        yield tx.unwatch()
        self.assertTrue(tx.inTransaction)
        yield tx.discard()
        self.assertFalse(tx.inTransaction)

        yield rapi.disconnect()

    @defer.inlineCallbacks
    def testWatchEdgeCase_2(self):
        rapi = yield redis.Connection(REDIS_HOST, REDIS_PORT)

        tx = yield rapi.multi()
        try:
            yield tx.watch("foobar")
        except trex.exceptions.ResponseError:
            pass
        yield tx.unwatch()
        self.assertTrue(tx.inTransaction)
        yield tx.discard()
        self.assertFalse(tx.inTransaction)
        yield rapi.disconnect()

    @defer.inlineCallbacks
    def testWatchEdgeCase_3(self):
        rapi = yield redis.Connection(REDIS_HOST, REDIS_PORT)

        tx = yield rapi.watch("foobar")
        tx = yield tx.multi("foobaz")
        yield tx.unwatch()
        self.assertTrue(tx.inTransaction)
        yield tx.discard()
        self.assertFalse(tx.inTransaction)

        yield rapi.disconnect()
