from twisted.internet import base
from twisted.internet import defer
from twisted.trial import unittest

from trex import redis

from .mixins import REDIS_HOST, REDIS_PORT

base.DelayedCall.debug = False


class TestConnectionMethods(unittest.TestCase):
    @defer.inlineCallbacks
    def test_Connection(self):
        db = yield redis.Connection(REDIS_HOST, REDIS_PORT, reconnect=False)
        self.assertEqual(isinstance(db, redis.ConnectionHandler), True)
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_ConnectionDB1(self):
        db = yield redis.Connection(REDIS_HOST, REDIS_PORT, dbid=1,
                                    reconnect=False)
        self.assertEqual(isinstance(db, redis.ConnectionHandler), True)
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_ConnectionPool(self):
        db = yield redis.ConnectionPool(REDIS_HOST, REDIS_PORT, poolsize=2,
                                        reconnect=False)
        self.assertEqual(isinstance(db, redis.ConnectionHandler), True)
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_lazyConnection(self):
        db = redis.lazyConnection(REDIS_HOST, REDIS_PORT, reconnect=False)
        self.assertEqual(isinstance(db._connected, defer.Deferred), True)
        db = yield db._connected
        self.assertEqual(isinstance(db, redis.ConnectionHandler), True)
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_lazyConnectionPool(self):
        db = redis.lazyConnectionPool(REDIS_HOST, REDIS_PORT, reconnect=False)
        self.assertEqual(isinstance(db._connected, defer.Deferred), True)
        db = yield db._connected
        self.assertEqual(isinstance(db, redis.ConnectionHandler), True)
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_ShardedConnection(self):
        hosts = ["%s:%s" % (REDIS_HOST, REDIS_PORT)]
        db = yield redis.ShardedConnection(hosts, reconnect=False)
        self.assertEqual(isinstance(db, redis.ShardedConnectionHandler), True)
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_ShardedConnectionPool(self):
        hosts = ["%s:%s" % (REDIS_HOST, REDIS_PORT)]
        db = yield redis.ShardedConnectionPool(hosts, reconnect=False)
        self.assertEqual(isinstance(db, redis.ShardedConnectionHandler), True)
        yield db.disconnect()
