from trex.redis import Connection

from twisted.internet.defer import inlineCallbacks
from twisted.trial import unittest

from .mixins import RedisVersionCheckMixin, REDIS_HOST, REDIS_PORT


class TestScan(unittest.TestCase, RedisVersionCheckMixin):
    KEYS = ['_scan_test_' + str(v).zfill(4) for v in range(100)]
    SKEY = ['_scan_test_set']
    SUFFIX = '12'
    PATTERN = '_scan_test_*' + SUFFIX
    FILTERED_KEYS = [k for k in KEYS if k.endswith(SUFFIX)]

    @inlineCallbacks
    def setUp(self):
        self.db = yield Connection(REDIS_HOST, REDIS_PORT, reconnect=False)
        self.redis_2_8_0 = yield self.checkVersion(2, 8, 0)
        yield self.db.delete(*self.KEYS)
        yield self.db.delete(self.SKEY)

    @inlineCallbacks
    def tearDown(self):
        yield self.db.delete(*self.KEYS)
        yield self.db.delete(self.SKEY)
        yield self.db.disconnect()

    @inlineCallbacks
    def test_scan(self):
        self._skipCheck()
        yield self.db.mset(dict((k, 'value') for k in self.KEYS))

        cursor, result = yield self.db.scan(pattern=self.PATTERN)

        while cursor != 0:
            cursor, keys = yield self.db.scan(cursor, pattern=self.PATTERN)
            result.extend(keys)

        self.assertEqual(set(result), set(self.FILTERED_KEYS))

    @inlineCallbacks
    def test_sscan(self):
        self._skipCheck()
        yield self.db.sadd(self.SKEY, self.KEYS)

        cursor, result = yield self.db.sscan(self.SKEY, pattern=self.PATTERN)

        while cursor != 0:
            cursor, keys = yield self.db.sscan(self.SKEY, cursor,
                                               pattern=self.PATTERN)
            result.extend(keys)

        self.assertEqual(set(result), set(self.FILTERED_KEYS))

    def _skipCheck(self):
        if not self.redis_2_8_0:
            skipMsg = "Redis version < 2.8.0 (found version: %s)"
            raise unittest.SkipTest(skipMsg % self.redis_version)
