import types

from .connections import ConnectionHandler
from .exceptions import ConnectionError
from .protocols import RedisProtocol, SubscriberProtocol, MonitorProtocol

from twisted.python import log
from twisted.internet.protocol import ReconnectingClientFactory
from twisted.internet.defer import (
    Deferred, DeferredQueue, CancelledError, inlineCallbacks, returnValue,
    succeed
)


class RedisFactory(ReconnectingClientFactory):
    maxDelay = 10
    protocol = RedisProtocol

    def __init__(
        self, uuid, dbid, poolsize, isLazy=False, handler=ConnectionHandler,
        charset="utf-8", password=None
    ):
        if not isinstance(poolsize, int):
            raise ValueError(
                "Redis poolsize must be an integer, not %s" % repr(poolsize)
            )

        if not isinstance(dbid, (int, types.NoneType)):
            raise ValueError(
                "Redis dbid must be an integer, not %s" % repr(dbid)
            )

        self.uuid = uuid
        self.dbid = dbid
        self.poolsize = poolsize
        self.isLazy = isLazy
        self.charset = charset
        self.password = password

        self.idx = 0
        self.size = 0
        self.pool = []
        self.deferred = Deferred()
        self.handler = handler(self)
        self.connectionQueue = DeferredQueue()
        self._waitingForEmptyPool = set()

    def buildProtocol(self, addr):
        if hasattr(self, 'charset'):
            p = self.protocol(self.charset)
        else:
            p = self.protocol()
        p.factory = self
        return p

    def addConnection(self, conn):
        self.connectionQueue.put(conn)
        self.pool.append(conn)
        self.size = len(self.pool)
        if self.deferred:
            if self.size == self.poolsize:
                self.deferred.callback(self.handler)
                self.deferred = None

    def delConnection(self, conn):
        try:
            self.pool.remove(conn)
        except Exception as e:
            log.msg("Could not remove connection from pool: %s" % str(e))

        self.size = len(self.pool)
        if not self.size and self._waitingForEmptyPool:
            deferreds = self._waitingForEmptyPool
            self._waitingForEmptyPool = set()
            for d in deferreds:
                d.callback(None)

    def _cancelWaitForEmptyPool(self, deferred):
        self._waitingForEmptyPool.discard(deferred)
        deferred.errback(CancelledError())

    def waitForEmptyPool(self):
        """
        Returns a Deferred which fires when the pool size has reached 0.
        """
        if not self.size:
            return succeed(None)
        d = Deferred(self._cancelWaitForEmptyPool)
        self._waitingForEmptyPool.add(d)
        return d

    def connectionError(self, why):
        if self.deferred:
            self.deferred.errback(ValueError(why))
            self.deferred = None

    @inlineCallbacks
    def getConnection(self, put_back=False):
        if not self.size and not self.isLazy:
            raise ConnectionError("Not connected")

        while True:
            conn = yield self.connectionQueue.get()
            if conn.connected == 0:
                log.msg('Discarding dead connection.')
            else:
                if put_back:
                    self.connectionQueue.put(conn)
                returnValue(conn)


class SubscriberFactory(RedisFactory):
    protocol = SubscriberProtocol

    def __init__(self, isLazy=False, handler=ConnectionHandler):
        RedisFactory.__init__(
            self, None, None, 1, isLazy=isLazy, handler=handler
        )


class MonitorFactory(RedisFactory):
    protocol = MonitorProtocol

    def __init__(self, isLazy=False, handler=ConnectionHandler):
        RedisFactory.__init__(
            self, None, None, 1, isLazy=isLazy, handler=handler
        )
