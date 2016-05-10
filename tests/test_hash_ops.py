from twisted.internet import defer
from twisted.trial import unittest

from trex import redis

from .mixins import REDIS_HOST, REDIS_PORT


class TestRedisHashOperations(unittest.TestCase):
    @defer.inlineCallbacks
    def testRedisHSetHGet(self):
        db = yield redis.Connection(REDIS_HOST, REDIS_PORT, reconnect=False)
        for hk in ("foo", "bar"):
            yield db.hset("trex:HSetHGet", hk, 1)
            result = yield db.hget("trex:HSetHGet", hk)
            self.assertEqual(result, 1)

        yield db.disconnect()

    @defer.inlineCallbacks
    def testRedisHMSetHMGet(self):
        db = yield redis.Connection(REDIS_HOST, REDIS_PORT, reconnect=False)
        t_dict = {}
        t_dict['key1'] = 'uno'
        t_dict['key2'] = 'dos'
        yield db.hmset("trex:HMSetHMGet", t_dict)
        ks = t_dict.keys()
        ks.reverse()
        vs = t_dict.values()
        vs.reverse()
        res = yield db.hmget("trex:HMSetHMGet", ks)
        self.assertEqual(vs, res)

        yield db.disconnect()

    @defer.inlineCallbacks
    def testRedisHKeysHVals(self):
        db = yield redis.Connection(REDIS_HOST, REDIS_PORT, reconnect=False)
        t_dict = {}
        t_dict['key1'] = 'uno'
        t_dict['key2'] = 'dos'
        yield db.hmset("trex:HKeysHVals", t_dict)

        vs_u = [unicode(v) for v in t_dict.values()]
        ks_u = [unicode(k) for k in t_dict.keys()]
        k_res = yield db.hkeys("trex:HKeysHVals")
        v_res = yield db.hvals("trex:HKeysHVals")
        self.assertEqual(ks_u, k_res)
        self.assertEqual(vs_u, v_res)

        yield db.disconnect()

    @defer.inlineCallbacks
    def testRedisHIncrBy(self):
        db = yield redis.Connection(REDIS_HOST, REDIS_PORT, reconnect=False)
        yield db.hset("trex:HIncrBy", "value", 1)
        yield db.hincr("trex:HIncrBy", "value")
        yield db.hincrby("trex:HIncrBy", "value", 2)
        result = yield db.hget("trex:HIncrBy", "value")
        self.assertEqual(result, 4)

        yield db.hincrby("trex:HIncrBy", "value", 10)
        yield db.hdecr("trex:HIncrBy", "value")
        result = yield db.hget("trex:HIncrBy", "value")
        self.assertEqual(result, 13)

        yield db.disconnect()

    @defer.inlineCallbacks
    def testRedisHLenHDelHExists(self):
        db = yield redis.Connection(REDIS_HOST, REDIS_PORT, reconnect=False)
        t_dict = {}
        t_dict['key1'] = 'uno'
        t_dict['key2'] = 'dos'

        s = yield db.hmset("trex:HDelHExists", t_dict)
        r_len = yield db.hlen("trex:HDelHExists")
        self.assertEqual(r_len, 2)

        s = yield db.hdel("trex:HDelHExists", "key2")
        r_len = yield db.hlen("trex:HDelHExists")
        self.assertEqual(r_len, 1)

        s = yield db.hexists("trex:HDelHExists", "key2")
        self.assertEqual(s, 0)

        yield db.disconnect()

    @defer.inlineCallbacks
    def testRedisHLenHDelMulti(self):
        db = yield redis.Connection(REDIS_HOST, REDIS_PORT, reconnect=False)
        t_dict = {}
        t_dict['key1'] = 'uno'
        t_dict['key2'] = 'dos'

        s = yield db.hmset("trex:HDelHExists", t_dict)
        r_len = yield db.hlen("trex:HDelHExists")
        self.assertEqual(r_len, 2)

        s = yield db.hdel("trex:HDelHExists", ["key1", "key2"])
        r_len = yield db.hlen("trex:HDelHExists")
        self.assertEqual(r_len, 0)

        s = yield db.hexists("trex:HDelHExists", ["key1", "key2"])
        self.assertEqual(s, 0)

        yield db.disconnect()

    @defer.inlineCallbacks
    def testRedisHGetAll(self):
        db = yield redis.Connection(REDIS_HOST, REDIS_PORT, reconnect=False)

        d = {u"key1": u"uno", u"key2": u"dos"}
        yield db.hmset("trex:HGetAll", d)
        s = yield db.hgetall("trex:HGetAll")

        self.assertEqual(d, s)
        yield db.disconnect()
