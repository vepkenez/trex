from trex import redis
from twisted.internet import defer
from twisted.trial import unittest


class TestConnectionCharset(unittest.TestCase):
    TEST_KEY = 'trex:test_key'
    TEST_VALUE_UNICODE = u'\u262d' * 3
    TEST_VALUE_BINARY = TEST_VALUE_UNICODE.encode('utf-8')

    @defer.inlineCallbacks
    def test_charset_None(self):
        db = yield redis.Connection(charset=None)

        yield db.set(self.TEST_KEY, self.TEST_VALUE_BINARY)
        result = yield db.get(self.TEST_KEY)
        self.assertTrue(type(result) == str)
        self.assertEqual(result, self.TEST_VALUE_BINARY)

        yield db.delete(self.TEST_KEY)
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_charset_default(self):
        db = yield redis.Connection()

        yield db.set(self.TEST_KEY, self.TEST_VALUE_UNICODE)
        result = yield db.get(self.TEST_KEY)
        self.assertTrue(type(result) == unicode)
        self.assertEqual(result, self.TEST_VALUE_UNICODE)

        yield db.delete(self.TEST_KEY)
        yield db.disconnect()
