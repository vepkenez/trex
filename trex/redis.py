from .factories import RedisFactory
from .connections import (
    ConnectionHandler,
    ShardedConnectionHandler,
    UnixConnectionHandler,
    ShardedUnixConnectionHandler
)

from twisted.internet import defer
from twisted.internet import reactor


CONNECTION_OPTIONS = {
    'default': (
        ConnectionHandler,
        reactor.connectTCP,
        None
    ),
    'sharded': (
        ConnectionHandler,
        reactor.connectTCP,
        ShardedConnectionHandler
    ),
    'unix': (
        UnixConnectionHandler,
        reactor.connectUNIX,
        None
    ),
    'sharded-unix': (
        UnixConnectionHandler,
        reactor.connectUNIX,
        ShardedUnixConnectionHandler
    )
}


def connect(
    host='localhost',
    port=6379,
    dbid=None,
    poolsize=1,
    reconnect=True,
    isLazy=False,
    charset='utf-8',
    password=None,
    handler=None,
    hosts=None,
    path=None,
    paths=None
):

    handler = handler or 'default'
    handler = handler.lower()

    _IS_UNIX = 'unix' in handler or path is not None or paths is not None
    _IS_UNIX &= handler != 'sharded'

    # ensure that the right handler is being used for the parameters given
    if _IS_UNIX and paths:
        handler = 'sharded-unix'
    elif _IS_UNIX:
        handler = 'unix'

    _handler, endpoint, wrapper = CONNECTION_OPTIONS[handler]

    path = path or '/tmp/redis.sock'

    if not handler.startswith('sharded'):
        # non-sharded resource handling
        uri = '%s:%d' % (host, port) if not _IS_UNIX else path
        factory = RedisFactory(
            uri, dbid, poolsize, isLazy, _handler, charset, password
        )
        factory.continueTrying = reconnect
        args = (path, factory) if _IS_UNIX else (host, port, factory)
        for x in xrange(poolsize):
            endpoint(*args)

        if isLazy:
            return factory.handler
        else:
            return factory.deferred

    else:
        uris = hosts if not _IS_UNIX else paths
        connections = []
        for uri in uris:
            if not _IS_UNIX:
                host, port = uri.split(':')
                port = int(port)
            factory = RedisFactory(
                uri, dbid, poolsize, isLazy, _handler, charset, password
            )
            factory.continueTrying = reconnect
            args = (uri, factory) if _IS_UNIX else (host, port, factory)
            for x in xrange(poolsize):
                endpoint(*args)

            if isLazy:
                connections.append(factory.handler)
            else:
                connections.append(factory.deferred)

        if isLazy:
            return wrapper(connections)
        else:
            deferred = defer.DeferredList(connections)
            wrapper(deferred)
            return deferred


###############################################################################
#
# HELPER FUNCTIONS
# ----------------
# for backwards compatability with txredisapi
def Connection(
    host="localhost", port=6379, dbid=None, reconnect=True, charset="utf-8",
    password=None
):
    return connect(
        host=host, port=port, dbid=dbid, reconnect=reconnect,
        charset=charset, password=password
    )


def lazyConnection(
    host="localhost", port=6379, dbid=None, reconnect=True,
    charset="utf-8", password=None
):
    return connect(
        host=host, port=port, dbid=dbid, reconnect=reconnect,
        charset=charset, password=password, isLazy=True
    )


def ConnectionPool(
    host="localhost", port=6379, dbid=None, poolsize=10, reconnect=True,
    charset="utf-8", password=None
):
    return connect(
        host=host, port=port, dbid=dbid, reconnect=reconnect,
        charset=charset, password=password, poolsize=poolsize
    )


def lazyConnectionPool(
    host="localhost", port=6379, dbid=None, poolsize=10, reconnect=True,
    charset="utf-8", password=None
):
    return connect(
        host=host, port=port, dbid=dbid, reconnect=reconnect,
        charset=charset, password=password, poolsize=poolsize, isLazy=True
    )


def ShardedConnection(
    hosts, dbid=None, reconnect=True, charset="utf-8", password=None
):
    return connect(
        hosts=hosts, dbid=dbid, reconnect=reconnect,
        charset=charset, password=password, handler='sharded'
    )


def lazyShardedConnection(
    hosts, dbid=None, reconnect=True, charset="utf-8", password=None
):
    return connect(
        hosts=hosts, dbid=dbid, reconnect=reconnect, isLazy=True,
        charset=charset, password=password, handler='sharded'
    )


def ShardedConnectionPool(
    hosts, dbid=None, poolsize=10, reconnect=True, charset="utf-8",
    password=None
):
    return connect(
        hosts=hosts, dbid=dbid, reconnect=reconnect, poolsize=poolsize,
        charset=charset, password=password, handler='sharded'
    )


def lazyShardedConnectionPool(
    hosts, dbid=None, poolsize=10, reconnect=True, charset="utf-8",
    password=None
):
    return connect(
        hosts=hosts, dbid=dbid, reconnect=reconnect, isLazy=True,
        charset=charset, password=password, handler='sharded',
        poolsize=poolsize
    )


def UnixConnection(
    path="/tmp/redis.sock", dbid=None, reconnect=True, charset="utf-8",
    password=None
):
    return connect(
        path=path, dbid=dbid, reconnect=reconnect,
        charset=charset, password=password, handler='unix'
    )


def lazyUnixConnection(
    path="/tmp/redis.sock", dbid=None, reconnect=True, charset="utf-8",
    password=None
):
    return connect(
        path=path, dbid=dbid, reconnect=reconnect, isLazy=True,
        charset=charset, password=password, handler='unix'
    )


def UnixConnectionPool(
    path="/tmp/redis.sock", dbid=None, poolsize=10, reconnect=True,
    charset="utf-8", password=None
):
    return connect(
        path=path, dbid=dbid, reconnect=reconnect, poolsize=poolsize,
        charset=charset, password=password, handler='unix'
    )


def lazyUnixConnectionPool(
    path="/tmp/redis.sock", dbid=None, poolsize=10, reconnect=True,
    charset="utf-8", password=None
):
    return connect(
        path=path, dbid=dbid, reconnect=reconnect, poolsize=poolsize,
        charset=charset, password=password, handler='unix', isLazy=True
    )


def ShardedUnixConnection(
    paths, dbid=None, reconnect=True, charset="utf-8", password=None
):
    return connect(
        paths=paths, dbid=dbid, reconnect=reconnect,
        charset=charset, password=password, handler='sharded-unix'
    )


def lazyShardedUnixConnection(
    paths, dbid=None, reconnect=True, charset="utf-8", password=None
):
    return connect(
        paths=paths, dbid=dbid, reconnect=reconnect, isLazy=True,
        charset=charset, password=password, handler='sharded-unix'
    )


def ShardedUnixConnectionPool(
    paths, dbid=None, poolsize=10, reconnect=True, charset="utf-8",
    password=None
):
    return connect(
        paths=paths, dbid=dbid, reconnect=reconnect,
        charset=charset, password=password, handler='sharded-unix',
        poolsize=poolsize
    )


def lazyShardedUnixConnectionPool(
    paths, dbid=None, poolsize=10, reconnect=True, charset="utf-8",
    password=None
):
    return connect(
        paths=paths, dbid=dbid, reconnect=reconnect, isLazy=True,
        charset=charset, password=password, handler='sharded-unix',
        poolsize=poolsize
    )


__all__ = [
    connect,
    Connection, lazyConnection,
    ConnectionPool, lazyConnectionPool,
    ShardedConnection, lazyShardedConnection,
    ShardedConnectionPool, lazyShardedConnectionPool,
    UnixConnection, lazyUnixConnection,
    UnixConnectionPool, lazyUnixConnectionPool,
    ShardedUnixConnection, lazyShardedUnixConnection,
    ShardedUnixConnectionPool, lazyShardedUnixConnectionPool,
]
