import bisect
import collections
import functools
import operator
import re
import zlib

from .exceptions import ConnectionError
from .utils import list_or_args
from twisted.internet.defer import inlineCallbacks, returnValue, DeferredList


class ConnectionHandler(object):
    def __init__(self, factory):
        self._factory = factory
        self._connected = factory.deferred

    def disconnect(self):
        self._factory.continueTrying = 0
        for conn in self._factory.pool:
            try:
                conn.transport.loseConnection()
            except:
                pass

        return self._factory.waitForEmptyPool()

    def __getattr__(self, method):
        def wrapper(*args, **kwargs):
            d = self._factory.getConnection()

            def callback(connection):
                protocol_method = getattr(connection, method)
                try:
                    d = protocol_method(*args, **kwargs)
                except:
                    self._factory.connectionQueue.put(connection)
                    raise

                def put_back(reply):
                    if not connection.inTransaction:
                        self._factory.connectionQueue.put(connection)
                    return reply

                def switch_to_errback(reply):
                    if isinstance(reply, Exception):
                        raise reply
                    return reply

                d.addBoth(put_back)
                d.addCallback(switch_to_errback)
                return d
            d.addCallback(callback)
            return d
        return wrapper

    def __repr__(self):
        try:
            cli = self._factory.pool[0].transport.getPeer()
        except:
            return "<Redis Connection: Not connected>"
        else:
            return "<Redis Connection: %s:%s - %d connection(s)>" % (
                cli.host, cli.port, self._factory.size
            )


class UnixConnectionHandler(ConnectionHandler):
    def __repr__(self):
        try:
            cli = self._factory.pool[0].transport.getPeer()
        except:
            return "<Redis Connection: Not connected>"
        else:
            return "<Redis Unix Connection: %s - %d connection(s)>" % (
                cli.name, self._factory.size
            )


ShardedMethods = frozenset([
    "decr",
    "delete",
    "exists",
    "expire",
    "get",
    "get_type",
    "getset",
    "hdel",
    "hexists",
    "hget",
    "hgetall",
    "hincrby",
    "hkeys",
    "hlen",
    "hmget",
    "hmset",
    "hset",
    "hvals",
    "incr",
    "lindex",
    "llen",
    "lrange",
    "lrem",
    "lset",
    "ltrim",
    "pop",
    "publish",
    "push",
    "rename",
    "sadd",
    "set",
    "setex",
    "setnx",
    "sismember",
    "smembers",
    "srem",
    "ttl",
    "zadd",
    "zcard",
    "zcount",
    "zdecr",
    "zincr",
    "zincrby",
    "zrange",
    "zrangebyscore",
    "zrevrangebyscore",
    "zrevrank",
    "zrank",
    "zrem",
    "zremrangebyscore",
    "zremrangebyrank",
    "zrevrange",
    "zscore",
])

_findhash = re.compile(r'.+\{(.*)\}.*')


class HashRing(object):
    """Consistent hash for redis API"""
    def __init__(self, nodes=[], replicas=160):
        self.nodes = []
        self.replicas = replicas
        self.ring = {}
        self.sorted_keys = []

        for n in nodes:
            self.add_node(n)

    def add_node(self, node):
        self.nodes.append(node)
        for x in xrange(self.replicas):
            crckey = zlib.crc32("%s:%d" % (node._factory.uuid, x))
            self.ring[crckey] = node
            self.sorted_keys.append(crckey)

        self.sorted_keys.sort()

    def remove_node(self, node):
        self.nodes.remove(node)
        for x in xrange(self.replicas):
            crckey = zlib.crc32("%s:%d" % (node, x))
            self.ring.remove(crckey)
            self.sorted_keys.remove(crckey)

    def get_node(self, key):
        n, i = self.get_node_pos(key)
        return n

    def get_node_pos(self, key):
        if len(self.ring) == 0:
            return [None, None]
        crc = zlib.crc32(key)
        idx = bisect.bisect(self.sorted_keys, crc)
        # prevents out of range index
        idx = min(idx, (self.replicas * len(self.nodes)) - 1)
        return [self.ring[self.sorted_keys[idx]], idx]

    def iter_nodes(self, key):
        if len(self.ring) == 0:
            yield None, None
        node, pos = self.get_node_pos(key)
        for k in self.sorted_keys[pos:]:
            yield k, self.ring[k]

    def __call__(self, key):
        return self.get_node(key)


class ShardedConnectionHandler(object):
    def __init__(self, connections):
        if isinstance(connections, DeferredList):
            self._ring = None
            connections.addCallback(self._makeRing)
        else:
            self._ring = HashRing(connections)

    def _makeRing(self, connections):
        connections = map(operator.itemgetter(1), connections)
        self._ring = HashRing(connections)
        return self

    @inlineCallbacks
    def disconnect(self):
        if not self._ring:
            raise ConnectionError("Not connected")

        for conn in self._ring.nodes:
            yield conn.disconnect()
        returnValue(True)

    def _wrap(self, method, *args, **kwargs):
        try:
            key = args[0]
            assert isinstance(key, (str, unicode))
        except:
            raise ValueError(
                "Method '%s' requires a key as the first argument" % method)

        m = _findhash.match(key)
        if m is not None and len(m.groups()) >= 1:
            node = self._ring(m.groups()[0])
        else:
            node = self._ring(key)

        return getattr(node, method)(*args, **kwargs)

    def pipeline(self):
        raise NotImplementedError("Pipelining is not supported across shards")

    def __getattr__(self, method):
        if method in ShardedMethods:
            return functools.partial(self._wrap, method)
        else:
            raise NotImplementedError("Method '%s' cannot be sharded" % method)

    @inlineCallbacks
    def mget(self, keys, *args):
        """
        high-level mget, required because of the sharding support
        """

        keys = list_or_args("mget", keys, args)
        group = collections.defaultdict(lambda: [])
        for k in keys:
            node = self._ring(k)
            group[node].append(k)

        deferreds = []
        for node, keys in group.items():
            nd = node.mget(keys)
            deferreds.append(nd)

        result = []
        response = yield DeferredList(deferreds)
        for (success, values) in response:
            if success:
                result += values

        returnValue(result)

    def __repr__(self):
        nodes = []
        for conn in self._ring.nodes:
            try:
                cli = conn._factory.pool[0].transport.getPeer()
            except:
                pass
            else:
                nodes.append(
                    "%s:%s/%d" % (cli.host, cli.port, conn._factory.size)
                )
        return "<Redis Sharded Connection: %s>" % ", ".join(nodes)


class ShardedUnixConnectionHandler(ShardedConnectionHandler):
    def __repr__(self):
        nodes = []
        for conn in self._ring.nodes:
            try:
                cli = conn._factory.pool[0].transport.getPeer()
            except:
                pass
            else:
                nodes.append("%s/%d" %
                             (cli.name, conn._factory.size))
        return "<Redis Sharded Connection: %s>" % ", ".join(nodes)
