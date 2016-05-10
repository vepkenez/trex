import os

from twisted.internet import defer
from twisted.trial import unittest

from trex import redis

from .mixins import REDIS_HOST, REDIS_PORT


class LargeMultiBulk(unittest.TestCase):
    _KEY = 'trex:testlargemultibulk'

    @defer.inlineCallbacks
    def setUp(self):
        self.db = yield redis.Connection(
            REDIS_HOST, REDIS_PORT, reconnect=False)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.db.delete(self._KEY)
        yield self.db.disconnect()

    @defer.inlineCallbacks
    def _test_multibulk(self, data):
        yield defer.DeferredList([self.db.sadd(self._KEY, x) for x in data])
        res = yield self.db.smembers(self._KEY)
        self.assertEqual(set(res), data)

    def test_large_multibulk_int(self):
        data = set(range(1000))
        return self._test_multibulk(data)

    def test_large_multibulk_str(self):
        data = set([os.urandom(10).encode('base64') for x in range(100)])
        return self._test_multibulk(data)

    @defer.inlineCallbacks
    def test_bulk_numeric(self):
        test_values = [
            '', '.hello', '+world', '123test',
            +1, 0.1, 0.01, -0.1, 0, -10]
        for v in test_values:
            yield self.db.set(self._KEY, v)
            r = yield self.db.get(self._KEY)
            self.assertEqual(r, v)

    @defer.inlineCallbacks
    def test_bulk_corner_cases(self):
        '''
        Python's float() function consumes '+inf', '-inf' & 'nan' values.
        Currently, we only convert bulk strings floating point numbers
        if there's a '.' in the string.
        This test is to ensure this behavior isn't broken in the future.
        '''
        values = ['+inf', '-inf', 'NaN']
        for x in values:
            yield self.db.set(self._KEY, x)
            r = yield self.db.get(self._KEY)
            self.assertEqual(r, x)


class NestedMultiBulk(unittest.TestCase):
    @defer.inlineCallbacks
    def testNestedMultiBulkTransaction(self):
        db = yield redis.Connection(REDIS_HOST, REDIS_PORT, reconnect=False)

        test1 = {u"foo1": u"bar1", u"something": u"else"}
        test2 = {u"foo2": u"bar2", u"something": u"else"}

        t = yield db.multi()
        yield t.hmset("trex:nmb:test1", test1)
        yield t.hgetall("trex:nmb:test1")
        yield t.hmset("trex:nmb:test2", test2)
        yield t.hgetall("trex:nmb:test2")
        r = yield t.commit()

        self.assertEqual(r[0], "OK")
        self.assertEqual(sorted(r[1].keys()), sorted(test1.keys()))
        self.assertEqual(sorted(r[1].values()), sorted(test1.values()))
        self.assertEqual(r[2], "OK")
        self.assertEqual(sorted(r[3].keys()), sorted(test2.keys()))
        self.assertEqual(sorted(r[3].values()), sorted(test2.values()))

        yield db.disconnect()
