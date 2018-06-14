import hiredis
import string

from .exceptions import InvalidData, ResponseError, ConnectionError
from .api import RedisApiMixin

from twisted.protocols.basic import LineReceiver
from twisted.protocols import policies
from twisted.python import log
from twisted.internet.defer import inlineCallbacks, DeferredQueue, returnValue


# Possible first characters in a string containing an integer or a float.
_NUM_FIRST_CHARS = frozenset(string.digits + "+-.")


class RedisProtocol(LineReceiver, policies.TimeoutMixin, RedisApiMixin):
    """
    Redis client protocol.
    """
    delimiter = '\r\n'
    MAX_LENGTH = 16384

    def __init__(self, charset="utf-8", errors="strict"):
        self._reader = hiredis.Reader(
            protocolError=InvalidData, replyError=ResponseError
        )
        self.charset = charset
        self.errors = errors

        self.bulk_length = 0
        self.bulk_buffer = []

        self.post_proc = []

        self.replyQueue = DeferredQueue()

        self.transactions = 0
        self.inTransaction = False
        self.unwatch_cc = lambda: ()
        self.commit_cc = lambda: ()

        self.script_hashes = set()

        self.pipelining = False
        self.pipelined_commands = []
        self.pipelined_replies = []

    @inlineCallbacks
    def connectionMade(self):
        if self.factory.password is not None:
            try:
                response = yield self.auth(self.factory.password)
                if isinstance(response, ResponseError):
                    raise response
            except Exception as e:
                self.factory.continueTrying = False
                self.transport.loseConnection()

                msg = "Redis error: could not auth: %s" % (str(e))
                self.factory.connectionError(msg)
                if self.factory.isLazy:
                    log.msg(msg)
                returnValue(None)

        if self.factory.dbid is not None:
            try:
                response = yield self.select(self.factory.dbid)
                if isinstance(response, ResponseError):
                    raise response
            except Exception as e:
                self.factory.continueTrying = False
                self.transport.loseConnection()

                msg = "Redis error: could not set dbid=%s: %s" % (
                    self.factory.dbid, str(e)
                )
                self.factory.connectionError(msg)
                if self.factory.isLazy:
                    log.msg(msg)
                returnValue(None)

        self.connected = 1
        self.factory.addConnection(self)

    def connectionLost(self, why):
        self.connected = 0
        self.script_hashes.clear()
        self.factory.delConnection(self)
        LineReceiver.connectionLost(self, why)
        while self.replyQueue.waiting:
            self.replyReceived(ConnectionError("Lost connection"))

    def dataReceived(self, data, unpause=False):
        self.resetTimeout()
        if data:
            self._reader.feed(data)
        res = self._reader.gets()
        while res is not False:
            if isinstance(res, basestring):
                res = self.tryConvertData(res)
            elif isinstance(res, list):
                res = map(self.tryConvertData, res)
            if res == "QUEUED":
                self.transactions += 1
            else:
                res = self.handleTransactionData(res)

            self.replyReceived(res)
            res = self._reader.gets()

    def tryConvertData(self, data):
        if not isinstance(data, str):
            return data
        el = None
        if data and data[0] in _NUM_FIRST_CHARS:  # Most likely a number
            try:
                el = int(data) if data.find('.') == -1 else float(data)
            except ValueError:
                pass

        if el is None:
            el = data
            if self.charset is not None:
                try:
                    el = data.decode(self.charset)
                except UnicodeDecodeError:
                    pass
        return el

    def handleTransactionData(self, reply):
        # watch or multi has been called
        if self.inTransaction and isinstance(reply, list):
            if self.transactions > 0:
                # multi: this must be an exec [commit] reply
                self.transactions -= len(reply)
            if self.transactions == 0:
                self.commit_cc()
            # watch but no multi: process the reply as usual
            if self.inTransaction:
                f = self.post_proc[1:]
                if len(f) == 1 and callable(f[0]):
                    reply = f[0](reply)
            else:  # multi: this must be an exec reply
                tmp = []
                for f, v in zip(self.post_proc[1:], reply):
                    if callable(f):
                        tmp.append(f(v))
                    else:
                        tmp.append(v)
                    reply = tmp
            self.post_proc = []
        return reply

    def replyReceived(self, reply):
        """
        Complete reply received and ready to be pushed to the requesting
        function.
        """
        self.replyQueue.put(reply)

    @staticmethod
    def handle_reply(r):
        if isinstance(r, Exception):
            raise r
        return r


class MonitorProtocol(RedisProtocol):
    """
    monitor has the same behavior as subscribe: hold the connection until
    something happens.

    take care with the performance impact: http://redis.io/commands/monitor
    """

    def messageReceived(self, message):
        pass

    def replyReceived(self, reply):
        self.messageReceived(reply)

    def monitor(self):
        return self.execute_command("MONITOR")

    def stop(self):
        self.transport.loseConnection()


class SubscriberProtocol(RedisProtocol):
    def messageReceived(self, pattern, channel, message):
        pass

    def replyReceived(self, reply):
        if isinstance(reply, list):
            if reply[-3] == u"message":
                self.messageReceived(None, *reply[-2:])
            elif len(reply) > 3 and reply[-4] == u"pmessage":
                self.messageReceived(*reply[-3:])
            else:
                self.replyQueue.put(reply[-3:])
        elif isinstance(reply, Exception):
            self.replyQueue.put(reply)

    def subscribe(self, channels):
        if isinstance(channels, (str, unicode)):
            channels = [channels]
        return self.execute_command("SUBSCRIBE", *channels)

    def unsubscribe(self, channels):
        if isinstance(channels, (str, unicode)):
            channels = [channels]
        return self.execute_command("UNSUBSCRIBE", *channels)

    def psubscribe(self, patterns):
        if isinstance(patterns, (str, unicode)):
            patterns = [patterns]
        return self.execute_command("PSUBSCRIBE", *patterns)

    def punsubscribe(self, patterns):
        if isinstance(patterns, (str, unicode)):
            patterns = [patterns]
        return self.execute_command("PUNSUBSCRIBE", *patterns)
