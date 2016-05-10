# import sys

from twisted.trial import unittest
from twisted.internet import defer
# from twisted.python import log

import trex
from trex import redis

from .mixins import REDIS_HOST, REDIS_PORT

# log.startLogging(sys.stdout)


class InspectableTransport(object):

    def __init__(self, transport):
        self.original_transport = transport
        self.write_history = []

    def __getattr__(self, method):

        if method == "write":
            def write(data, *args, **kwargs):
                self.write_history.append(data)
                return self.original_transport.write(data, *args, **kwargs)
            return write
        return getattr(self.original_transport, method)


class TestRedisConnections(unittest.TestCase):

    @defer.inlineCallbacks
    def _assert_simple_sets_on_pipeline(self, db):

        pipeline = yield db.pipeline()
        self.assertTrue(pipeline.pipelining)

        # Hook into the transport so we can inspect what is happening
        # at the protocol level.
        pipeline.transport = InspectableTransport(pipeline.transport)

        pipeline.set("trex:test_pipeline", "foo")
        pipeline.set("trex:test_pipeline", "bar")
        pipeline.set("trex:test_pipeline2", "zip")

        yield pipeline.execute_pipeline()
        self.assertFalse(pipeline.pipelining)

        result = yield db.get("trex:test_pipeline")
        self.assertEqual(result, "bar")

        result = yield db.get("trex:test_pipeline2")
        self.assertEqual(result, "zip")

        # Make sure that all SET commands were sent in a single pipelined write.
        write_history = pipeline.transport.write_history
        lines_in_first_write = write_history[0].split("\n")
        sets_in_first_write = sum([1 for w in lines_in_first_write if "SET" in w])
        self.assertEqual(sets_in_first_write, 3)

    @defer.inlineCallbacks
    def _wait_for_lazy_connection(self, db):

        # For lazy connections, wait for the internal deferred to indicate
        # that the connection is established.
        yield db._connected

    @defer.inlineCallbacks
    def test_Connection(self):

        db = yield redis.Connection(REDIS_HOST, REDIS_PORT, reconnect=False)
        yield self._assert_simple_sets_on_pipeline(db=db)
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_ConnectionDB1(self):

        db = yield redis.Connection(REDIS_HOST, REDIS_PORT, dbid=1,
                                         reconnect=False)
        yield self._assert_simple_sets_on_pipeline(db=db)
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_ConnectionPool(self):

        db = yield redis.ConnectionPool(REDIS_HOST, REDIS_PORT, poolsize=2,
                                             reconnect=False)
        yield self._assert_simple_sets_on_pipeline(db=db)
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_lazyConnection(self):

        db = redis.lazyConnection(REDIS_HOST, REDIS_PORT, reconnect=False)
        yield self._wait_for_lazy_connection(db)
        yield self._assert_simple_sets_on_pipeline(db=db)
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_lazyConnectionPool(self):

        db = redis.lazyConnectionPool(REDIS_HOST, REDIS_PORT, reconnect=False)
        yield self._wait_for_lazy_connection(db)
        yield self._assert_simple_sets_on_pipeline(db=db)
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_ShardedConnection(self):

        hosts = ["%s:%s" % (REDIS_HOST, REDIS_PORT)]
        db = yield redis.ShardedConnection(hosts, reconnect=False)
        try:
            yield db.pipeline()
            raise self.failureException("Expected sharding to disallow pipelining")
        except NotImplementedError, e:
            self.assertTrue("not supported" in str(e).lower())
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_ShardedConnectionPool(self):

        hosts = ["%s:%s" % (REDIS_HOST, REDIS_PORT)]
        db = yield redis.ShardedConnectionPool(hosts, reconnect=False)
        try:
            yield db.pipeline()
            raise self.failureException("Expected sharding to disallow pipelining")
        except NotImplementedError, e:
            self.assertTrue("not supported" in str(e).lower())
        yield db.disconnect()
