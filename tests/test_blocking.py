from twisted.internet import defer
from twisted.trial import unittest

from trex import redis

from .mixins import REDIS_HOST, REDIS_PORT


class TestBlockingCommands(unittest.TestCase):
    QUEUE_KEY = 'trex:test_queue'
    TEST_KEY = 'trex:test_key'
    QUEUE_VALUE = 'queue_value'

    @defer.inlineCallbacks
    def testBlocking(self):
        db = yield redis.ConnectionPool(REDIS_HOST, REDIS_PORT, poolsize=2,
                                        reconnect=False)
        yield db.delete(self.QUEUE_KEY, self.TEST_KEY)

        # Block first connection.
        d = db.brpop(self.QUEUE_KEY, timeout=3)
        # Use second connection.
        yield db.set(self.TEST_KEY, 'somevalue')
        # Should use second connection again. Will block till end of
        # brpop otherwise.
        yield db.lpush('trex:test_queue', self.QUEUE_VALUE)

        brpop_result = yield d
        self.assertNotEqual(brpop_result, None)

        yield db.delete(self.QUEUE_KEY, self.TEST_KEY)
        yield db.disconnect()
