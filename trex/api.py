import hashlib
import operator
import warnings

from .exceptions import (
    ConnectionError, InvalidData, RedisError, ResponseError, NoScriptRunning,
    ScriptDoesNotExist, WatchError
)
from .utils import list_or_args

from twisted.python.failure import Failure
from twisted.internet.defer import (
    fail, inlineCallbacks, returnValue, DeferredList, Deferred
)


class RedisApiMixin():

    def execute_command(self, *args, **kwargs):
        if self.connected == 0:
            raise ConnectionError("Not connected")
        else:

            # Build the redis command.
            cmds = []
            cmd_template = "$%s\r\n%s\r\n"
            for s in args:
                if isinstance(s, str):
                    cmd = s
                elif isinstance(s, unicode):
                    if self.charset is None:
                        raise InvalidData("Encoding charset was not specified")
                    try:
                        cmd = s.encode(self.charset, self.errors)
                    except UnicodeEncodeError as e:
                        raise InvalidData(
                            "Error encoding unicode value '%s': %s" %
                            (repr(s), e))
                elif isinstance(s, float):
                    try:
                        cmd = format(s, "f")
                    except NameError:
                        cmd = "%0.6f" % s
                else:
                    cmd = str(s)
                cmds.append(cmd_template % (len(cmd), cmd))
            command = "*%s\r\n%s" % (len(cmds), "".join(cmds))

            # When pipelining, buffer this command into our list of
            # pipelined commands. Otherwise, write the command immediately.
            if self.pipelining:
                self.pipelined_commands.append(command)
            else:
                self.transport.write(command)

            # Return deferred that will contain the result of this command.
            # Note: when using pipelining, this deferred will NOT return
            # until after execute_pipeline is called.
            r = self.replyQueue.get().addCallback(self.handle_reply)

            # When pipelining, we need to keep track of the deferred replies
            # so that we can wait for them in a DeferredList when
            # execute_pipeline is called.
            if self.pipelining:
                self.pipelined_replies.append(r)

            if self.inTransaction:
                self.post_proc.append(kwargs.get("post_proc"))
            else:
                if "post_proc" in kwargs:
                    f = kwargs["post_proc"]
                    if callable(f):
                        r.addCallback(f)

            return r

    # Connection handling
    def quit(self):
        """
        Close the connection
        """
        self.factory.continueTrying = False
        return self.execute_command("QUIT")

    def auth(self, password):
        """
        Simple password authentication if enabled
        """
        return self.execute_command("AUTH", password)

    def ping(self):
        """
        Ping the server
        """
        return self.execute_command("PING")

    # Commands operating on all value types
    def exists(self, key):
        """
        Test if a key exists
        """
        return self.execute_command("EXISTS", key)

    def delete(self, keys, *args):
        """
        Delete one or more keys
        """
        keys = list_or_args("delete", keys, args)
        return self.execute_command("DEL", *keys)

    def type(self, key):
        """
        Return the type of the value stored at key
        """
        return self.execute_command("TYPE", key)

    def keys(self, pattern="*"):
        """
        Return all the keys matching a given pattern
        """
        return self.execute_command("KEYS", pattern)

    @staticmethod
    def _build_scan_args(cursor, pattern, count):
        """
        Construct arguments list for SCAN, SSCAN, HSCAN, ZSCAN commands
        """
        args = [cursor]
        if pattern is not None:
            args.extend(("MATCH", pattern))
        if count is not None:
            args.extend(("COUNT", count))

        return args

    def scan(self, cursor=0, pattern=None, count=None):
        """
        Incrementally iterate the keys in database
        """
        args = self._build_scan_args(cursor, pattern, count)
        return self.execute_command("SCAN", *args)

    def randomkey(self):
        """
        Return a random key from the key space
        """
        return self.execute_command("RANDOMKEY")

    def rename(self, oldkey, newkey):
        """
        Rename the old key in the new one,
        destroying the newname key if it already exists
        """
        return self.execute_command("RENAME", oldkey, newkey)

    def renamenx(self, oldkey, newkey):
        """
        Rename the oldname key to newname,
        if the newname key does not already exist
        """
        return self.execute_command("RENAMENX", oldkey, newkey)

    def dbsize(self):
        """
        Return the number of keys in the current db
        """
        return self.execute_command("DBSIZE")

    def expire(self, key, time):
        """
        Set a time to live in seconds on a key
        """
        return self.execute_command("EXPIRE", key, time)

    def persist(self, key):
        """
        Remove the expire from a key
        """
        return self.execute_command("PERSIST", key)

    def ttl(self, key):
        """
        Get the time to live in seconds of a key
        """
        return self.execute_command("TTL", key)

    def select(self, index):
        """
        Select the DB with the specified index
        """
        return self.execute_command("SELECT", index)

    def move(self, key, dbindex):
        """
        Move the key from the currently selected DB to the dbindex DB
        """
        return self.execute_command("MOVE", key, dbindex)

    def flush(self, all_dbs=False):
        warnings.warn(DeprecationWarning(
            "redis.flush() has been deprecated, "
            "use redis.flushdb() or redis.flushall() instead"))
        return all_dbs and self.flushall() or self.flushdb()

    def flushdb(self):
        """
        Remove all the keys from the currently selected DB
        """
        return self.execute_command("FLUSHDB")

    def flushall(self):
        """
        Remove all the keys from all the databases
        """
        return self.execute_command("FLUSHALL")

    def time(self):
        """
        Returns the current server time as a two items lists: a Unix timestamp
        and the amount of microseconds already elapsed in the current second
        """
        return self.execute_command("TIME")

    # Commands operating on string values
    def set(self, key, value, expire=None, pexpire=None,
            only_if_not_exists=False, only_if_exists=False):
        """
        Set a key to a string value
        """
        args = []
        if expire is not None:
            args.extend(("EX", expire))
        if pexpire is not None:
            args.extend(("PX", pexpire))
        if only_if_not_exists and only_if_exists:
            raise RedisError("only_if_not_exists and only_if_exists "
                             "cannot be true simultaneously")
        if only_if_not_exists:
            args.append("NX")
        if only_if_exists:
            args.append("XX")
        return self.execute_command("SET", key, value, *args)

    def get(self, key):
        """
        Return the string value of the key
        """
        return self.execute_command("GET", key)

    def getbit(self, key, offset):
        """
        Return the bit value at offset in the string value stored at key
        """
        return self.execute_command("GETBIT", key, offset)

    def getset(self, key, value):
        """
        Set a key to a string returning the old value of the key
        """
        return self.execute_command("GETSET", key, value)

    def mget(self, keys, *args):
        """
        Multi-get, return the strings values of the keys
        """
        keys = list_or_args("mget", keys, args)
        return self.execute_command("MGET", *keys)

    def setbit(self, key, offset, value):
        """
        Sets or clears the bit at offset in the string value stored at key
        """
        if isinstance(value, bool):
            value = int(value)
        return self.execute_command("SETBIT", key, offset, value)

    def setnx(self, key, value):
        """
        Set a key to a string value if the key does not exist
        """
        return self.execute_command("SETNX", key, value)

    def setex(self, key, time, value):
        """
        Set+Expire combo command
        """
        return self.execute_command("SETEX", key, time, value)

    def mset(self, mapping):
        """
        Set the respective fields to the respective values.
        HMSET replaces old values with new values.
        """
        items = []
        for pair in mapping.iteritems():
            items.extend(pair)
        return self.execute_command("MSET", *items)

    def msetnx(self, mapping):
        """
        Set multiple keys to multiple values in a single atomic
        operation if none of the keys already exist
        """
        items = []
        for pair in mapping.iteritems():
            items.extend(pair)
        return self.execute_command("MSETNX", *items)

    def bitop(self, operation, destkey, *srckeys):
        """
        Perform a bitwise operation between multiple keys
        and store the result in the destination key.
        """
        srclen = len(srckeys)
        if srclen == 0:
            return fail(RedisError("no ``srckeys`` specified"))
        if isinstance(operation, (str, unicode)):
            operation = operation.upper()
        elif operation is operator.and_ or operation is operator.__and__:
            operation = 'AND'
        elif operation is operator.or_ or operation is operator.__or__:
            operation = 'OR'
        elif operation is operator.__xor__ or operation is operator.xor:
            operation = 'XOR'
        elif operation is operator.__not__ or operation is operator.not_:
            operation = 'NOT'
        if operation not in ('AND', 'OR', 'XOR', 'NOT'):
            return fail(InvalidData(
                "Invalid operation: %s" % operation))
        if operation == 'NOT' and srclen > 1:
            return fail(RedisError(
                "bitop NOT takes only one ``srckey``"))
        return self.execute_command('BITOP', operation, destkey, *srckeys)

    def bitcount(self, key, start=None, end=None):
        if (end is None and start is not None) or \
                (start is None and end is not None):
            raise RedisError("``start`` and ``end`` must both be specified")
        if start is not None:
            t = (start, end)
        else:
            t = ()
        return self.execute_command("BITCOUNT", key, *t)

    def incr(self, key, amount=1):
        """
        Increment the integer value of key
        """
        return self.execute_command("INCRBY", key, amount)

    def incrby(self, key, amount):
        """
        Increment the integer value of key by integer
        """
        return self.incr(key, amount)

    def decr(self, key, amount=1):
        """
        Decrement the integer value of key
        """
        return self.execute_command("DECRBY", key, amount)

    def decrby(self, key, amount):
        """
        Decrement the integer value of key by integer
        """
        return self.decr(key, amount)

    def append(self, key, value):
        """
        Append the specified string to the string stored at key
        """
        return self.execute_command("APPEND", key, value)

    def substr(self, key, start, end=-1):
        """
        Return a substring of a larger string
        """
        return self.execute_command("SUBSTR", key, start, end)

    # Commands operating on lists
    def push(self, key, value, tail=False):
        warnings.warn(DeprecationWarning(
            "redis.push() has been deprecated, "
            "use redis.lpush() or redis.rpush() instead"))

        return tail and self.rpush(key, value) or self.lpush(key, value)

    def rpush(self, key, value):
        """
        Append an element to the tail of the List value at key
        """
        if isinstance(value, tuple) or isinstance(value, list):
            return self.execute_command("RPUSH", key, *value)
        else:
            return self.execute_command("RPUSH", key, value)

    def lpush(self, key, value):
        """
        Append an element to the head of the List value at key
        """
        if isinstance(value, tuple) or isinstance(value, list):
            return self.execute_command("LPUSH", key, *value)
        else:
            return self.execute_command("LPUSH", key, value)

    def llen(self, key):
        """
        Return the length of the List value at key
        """
        return self.execute_command("LLEN", key)

    def lrange(self, key, start, end):
        """
        Return a range of elements from the List at key
        """
        return self.execute_command("LRANGE", key, start, end)

    def ltrim(self, key, start, end):
        """
        Trim the list at key to the specified range of elements
        """
        return self.execute_command("LTRIM", key, start, end)

    def lindex(self, key, index):
        """
        Return the element at index position from the List at key
        """
        return self.execute_command("LINDEX", key, index)

    def lset(self, key, index, value):
        """
        Set a new value as the element at index position of the List at key
        """
        return self.execute_command("LSET", key, index, value)

    def lrem(self, key, count, value):
        """
        Remove the first-N, last-N, or all the elements matching value
        from the List at key
        """
        return self.execute_command("LREM", key, count, value)

    def pop(self, key, tail=False):
        warnings.warn(DeprecationWarning(
            "redis.pop() has been deprecated, "
            "user redis.lpop() or redis.rpop() instead"))

        return tail and self.rpop(key) or self.lpop(key)

    def lpop(self, key):
        """
        Return and remove (atomically) the first element of the List at key
        """
        return self.execute_command("LPOP", key)

    def rpop(self, key):
        """
        Return and remove (atomically) the last element of the List at key
        """
        return self.execute_command("RPOP", key)

    def blpop(self, keys, timeout=0):
        """
        Blocking LPOP
        """
        if isinstance(keys, (str, unicode)):
            keys = [keys]
        else:
            keys = list(keys)

        keys.append(timeout)
        return self.execute_command("BLPOP", *keys)

    def brpop(self, keys, timeout=0):
        """
        Blocking RPOP
        """
        if isinstance(keys, (str, unicode)):
            keys = [keys]
        else:
            keys = list(keys)

        keys.append(timeout)
        return self.execute_command("BRPOP", *keys)

    def brpoplpush(self, source, destination, timeout=0):
        """
        Pop a value from a list, push it to another list and return
        it; or block until one is available.
        """
        return self.execute_command("BRPOPLPUSH", source, destination, timeout)

    def rpoplpush(self, srckey, dstkey):
        """
        Return and remove (atomically) the last element of the source
        List  stored at srckey and push the same element to the
        destination List stored at dstkey
        """
        return self.execute_command("RPOPLPUSH", srckey, dstkey)

    def _make_set(self, result):
        if isinstance(result, list):
            return set(result)
        return result

    # Commands operating on sets
    def sadd(self, key, members, *args):
        """
        Add the specified member to the Set value at key
        """
        members = list_or_args("sadd", members, args)
        return self.execute_command("SADD", key, *members)

    def srem(self, key, members, *args):
        """
        Remove the specified member from the Set value at key
        """
        members = list_or_args("srem", members, args)
        return self.execute_command("SREM", key, *members)

    def spop(self, key):
        """
        Remove and return (pop) a random element from the Set value at key
        """
        return self.execute_command("SPOP", key)

    def smove(self, srckey, dstkey, member):
        """
        Move the specified member from one Set to another atomically
        """
        return self.execute_command(
            "SMOVE", srckey, dstkey, member).addCallback(bool)

    def scard(self, key):
        """
        Return the number of elements (the cardinality) of the Set at key
        """
        return self.execute_command("SCARD", key)

    def sismember(self, key, value):
        """
        Test if the specified value is a member of the Set at key
        """
        return self.execute_command("SISMEMBER", key, value).addCallback(bool)

    def sinter(self, keys, *args):
        """
        Return the intersection between the Sets stored at key1, ..., keyN
        """
        keys = list_or_args("sinter", keys, args)
        return self.execute_command("SINTER", *keys).addCallback(
            self._make_set)

    def sinterstore(self, dstkey, keys, *args):
        """
        Compute the intersection between the Sets stored
        at key1, key2, ..., keyN, and store the resulting Set at dstkey
        """
        keys = list_or_args("sinterstore", keys, args)
        return self.execute_command("SINTERSTORE", dstkey, *keys)

    def sunion(self, keys, *args):
        """
        Return the union between the Sets stored at key1, key2, ..., keyN
        """
        keys = list_or_args("sunion", keys, args)
        return self.execute_command("SUNION", *keys).addCallback(
            self._make_set)

    def sunionstore(self, dstkey, keys, *args):
        """
        Compute the union between the Sets stored
        at key1, key2, ..., keyN, and store the resulting Set at dstkey
        """
        keys = list_or_args("sunionstore", keys, args)
        return self.execute_command("SUNIONSTORE", dstkey, *keys)

    def sdiff(self, keys, *args):
        """
        Return the difference between the Set stored at key1 and
        all the Sets key2, ..., keyN
        """
        keys = list_or_args("sdiff", keys, args)
        return self.execute_command("SDIFF", *keys).addCallback(
            self._make_set)

    def sdiffstore(self, dstkey, keys, *args):
        """
        Compute the difference between the Set key1 and all the
        Sets key2, ..., keyN, and store the resulting Set at dstkey
        """
        keys = list_or_args("sdiffstore", keys, args)
        return self.execute_command("SDIFFSTORE", dstkey, *keys)

    def smembers(self, key):
        """
        Return all the members of the Set value at key
        """
        return self.execute_command("SMEMBERS", key).addCallback(
            self._make_set)

    def srandmember(self, key):
        """
        Return a random member of the Set value at key
        """
        return self.execute_command("SRANDMEMBER", key)

    def sscan(self, key, cursor=0, pattern=None, count=None):
        args = self._build_scan_args(cursor, pattern, count)
        return self.execute_command("SSCAN", key, *args)

    # Commands operating on sorted zsets (sorted sets)
    def zadd(self, key, score, member, *args):
        """
        Add the specified member to the Sorted Set value at key
        or update the score if it already exist
        """
        if args:
            # Args should be pairs (have even number of elements)
            if len(args) % 2:
                return fail(InvalidData(
                    "Invalid number of arguments to ZADD"))
            else:
                l = [score, member]
                l.extend(args)
                args = l
        else:
            args = [score, member]
        return self.execute_command("ZADD", key, *args)

    def zrem(self, key, *args):
        """
        Remove the specified member from the Sorted Set value at key
        """
        return self.execute_command("ZREM", key, *args)

    def zincr(self, key, member):
        return self.zincrby(key, 1, member)

    def zdecr(self, key, member):
        return self.zincrby(key, -1, member)

    def zincrby(self, key, increment, member):
        """
        If the member already exists increment its score by increment,
        otherwise add the member setting increment as score
        """
        return self.execute_command("ZINCRBY", key, increment, member)

    def zrank(self, key, member):
        """
        Return the rank (or index) or member in the sorted set at key,
        with scores being ordered from low to high
        """
        return self.execute_command("ZRANK", key, member)

    def zrevrank(self, key, member):
        """
        Return the rank (or index) or member in the sorted set at key,
        with scores being ordered from high to low
        """
        return self.execute_command("ZREVRANK", key, member)

    def _handle_withscores(self, r):
        if isinstance(r, list):
            # Return a list tuples of form (value, score)
            return zip(r[::2], r[1::2])
        return r

    def _zrange(self, key, start, end, withscores, reverse):
        if reverse:
            cmd = "ZREVRANGE"
        else:
            cmd = "ZRANGE"
        if withscores:
            pieces = (cmd, key, start, end, "WITHSCORES")
        else:
            pieces = (cmd, key, start, end)
        r = self.execute_command(*pieces)
        if withscores:
            r.addCallback(self._handle_withscores)
        return r

    def zrange(self, key, start=0, end=-1, withscores=False):
        """
        Return a range of elements from the sorted set at key
        """
        return self._zrange(key, start, end, withscores, False)

    def zrevrange(self, key, start=0, end=-1, withscores=False):
        """
        Return a range of elements from the sorted set at key,
        exactly like ZRANGE, but the sorted set is ordered in
        traversed in reverse order, from the greatest to the smallest score
        """
        return self._zrange(key, start, end, withscores, True)

    def _zrangebyscore(self, key, min, max, withscores, offset, count, rev):
        if rev:
            cmd = "ZREVRANGEBYSCORE"
        else:
            cmd = "ZRANGEBYSCORE"
        if (offset is None) != (count is None):  # XNOR
            return fail(InvalidData(
                "Invalid count and offset arguments to %s" % cmd))
        if withscores:
            pieces = [cmd, key, min, max, "WITHSCORES"]
        else:
            pieces = [cmd, key, min, max]
        if offset is not None and count is not None:
            pieces.extend(("LIMIT", offset, count))
        r = self.execute_command(*pieces)
        if withscores:
            r.addCallback(self._handle_withscores)
        return r

    def zrangebyscore(self, key, min='-inf', max='+inf', withscores=False,
                      offset=None, count=None):
        """
        Return all the elements with score >= min and score <= max
        (a range query) from the sorted set
        """
        return self._zrangebyscore(key, min, max, withscores, offset,
                                   count, False)

    def zrevrangebyscore(self, key, max='+inf', min='-inf', withscores=False,
                         offset=None, count=None):
        """
        ZRANGEBYSCORE in reverse order
        """
        # ZREVRANGEBYSCORE takes max before min
        return self._zrangebyscore(key, max, min, withscores, offset,
                                   count, True)

    def zcount(self, key, min='-inf', max='+inf'):
        """
        Return the number of elements with score >= min and score <= max
        in the sorted set
        """
        if min == '-inf' and max == '+inf':
            return self.zcard(key)
        return self.execute_command("ZCOUNT", key, min, max)

    def zcard(self, key):
        """
        Return the cardinality (number of elements) of the sorted set at key
        """
        return self.execute_command("ZCARD", key)

    def zscore(self, key, element):
        """
        Return the score associated with the specified element of the sorted
        set at key
        """
        return self.execute_command("ZSCORE", key, element)

    def zremrangebyrank(self, key, min=0, max=-1):
        """
        Remove all the elements with rank >= min and rank <= max from
        the sorted set
        """
        return self.execute_command("ZREMRANGEBYRANK", key, min, max)

    def zremrangebyscore(self, key, min='-inf', max='+inf'):
        """
        Remove all the elements with score >= min and score <= max from
        the sorted set
        """
        return self.execute_command("ZREMRANGEBYSCORE", key, min, max)

    def zunionstore(self, dstkey, keys, aggregate=None):
        """
        Perform a union over a number of sorted sets with optional
        weight and aggregate
        """
        return self._zaggregate("ZUNIONSTORE", dstkey, keys, aggregate)

    def zinterstore(self, dstkey, keys, aggregate=None):
        """
        Perform an intersection over a number of sorted sets with optional
        weight and aggregate
        """
        return self._zaggregate("ZINTERSTORE", dstkey, keys, aggregate)

    def _zaggregate(self, command, dstkey, keys, aggregate):
        pieces = [command, dstkey, len(keys)]
        if isinstance(keys, dict):
            keys, weights = zip(*keys.items())
        else:
            weights = None

        pieces.extend(keys)
        if weights:
            pieces.append("WEIGHTS")
            pieces.extend(weights)

        if aggregate:
            if aggregate is min:
                aggregate = 'MIN'
            elif aggregate is max:
                aggregate = 'MAX'
            elif aggregate is sum:
                aggregate = 'SUM'
            else:
                err_flag = True
                if isinstance(aggregate, (str, unicode)):
                    aggregate_u = aggregate.upper()
                    if aggregate_u in ('MIN', 'MAX', 'SUM'):
                        aggregate = aggregate_u
                        err_flag = False
                if err_flag:
                    return fail(InvalidData(
                        "Invalid aggregate function: %s" % aggregate))
            pieces.extend(("AGGREGATE", aggregate))
        return self.execute_command(*pieces)

    def zscan(self, key, cursor=0, pattern=None, count=None):
        args = self._build_scan_args(cursor, pattern, count)
        return self.execute_command("ZSCAN", key, *args)

    # Commands operating on hashes
    def hset(self, key, field, value):
        """
        Set the hash field to the specified value. Creates the hash if needed
        """
        return self.execute_command("HSET", key, field, value)

    def hsetnx(self, key, field, value):
        """
        Set the hash field to the specified value if the field does not exist.
        Creates the hash if needed
        """
        return self.execute_command("HSETNX", key, field, value)

    def hget(self, key, field):
        """
        Retrieve the value of the specified hash field.
        """
        return self.execute_command("HGET", key, field)

    def hmget(self, key, fields):
        """
        Get the hash values associated to the specified fields.
        """
        return self.execute_command("HMGET", key, *fields)

    def hmset(self, key, mapping):
        """
        Set the hash fields to their respective values.
        """
        items = []
        for pair in mapping.iteritems():
            items.extend(pair)
        return self.execute_command("HMSET", key, *items)

    def hincr(self, key, field):
        return self.hincrby(key, field, 1)

    def hdecr(self, key, field):
        return self.hincrby(key, field, -1)

    def hincrby(self, key, field, integer):
        """
        Increment the integer value of the hash at key on field with integer.
        """
        return self.execute_command("HINCRBY", key, field, integer)

    def hexists(self, key, field):
        """
        Test for existence of a specified field in a hash
        """
        return self.execute_command("HEXISTS", key, field)

    def hdel(self, key, fields):
        """
        Remove the specified field or fields from a hash
        """
        if isinstance(fields, (str, unicode)):
            fields = [fields]
        else:
            fields = list(fields)
        return self.execute_command("HDEL", key, *fields)

    def hlen(self, key):
        """
        Return the number of items in a hash.
        """
        return self.execute_command("HLEN", key)

    def hkeys(self, key):
        """
        Return all the fields in a hash.
        """
        return self.execute_command("HKEYS", key)

    def hvals(self, key):
        """
        Return all the values in a hash.
        """
        return self.execute_command("HVALS", key)

    def hgetall(self, key):
        """
        Return all the fields and associated values in a hash.
        """
        f = lambda d: dict(zip(d[::2], d[1::2]))
        return self.execute_command("HGETALL", key, post_proc=f)

    def hscan(self, key, cursor=0, pattern=None, count=None):
        args = self._build_scan_args(cursor, pattern, count)
        return self.execute_command("HSCAN", key, *args)

    # Sorting
    def sort(self, key, start=None, end=None, by=None, get=None,
             desc=None, alpha=False, store=None):
        if (start is not None and end is None) or \
           (end is not None and start is None):
            raise RedisError("``start`` and ``end`` must both be specified")

        pieces = [key]
        if by is not None:
            pieces.append("BY")
            pieces.append(by)
        if start is not None and end is not None:
            pieces.append("LIMIT")
            pieces.append(start)
            pieces.append(end)
        if get is not None:
            pieces.append("GET")
            pieces.append(get)
        if desc:
            pieces.append("DESC")
        if alpha:
            pieces.append("ALPHA")
        if store is not None:
            pieces.append("STORE")
            pieces.append(store)

        return self.execute_command("SORT", *pieces)

    def _clear_txstate(self):
        if self.inTransaction:
            self.inTransaction = False
            self.factory.connectionQueue.put(self)

    def watch(self, keys):
        if not self.inTransaction:
            self.inTransaction = True
            self.unwatch_cc = self._clear_txstate
            self.commit_cc = lambda: ()
        if isinstance(keys, (str, unicode)):
            keys = [keys]
        d = self.execute_command("WATCH", *keys).addCallback(self._tx_started)
        return d

    def unwatch(self):
        self.unwatch_cc()
        return self.execute_command("UNWATCH")

    # Transactions
    # multi() will return a deferred with a "connection" object
    # That object must be used for further interactions within
    # the transaction. At the end, either exec() or discard()
    # must be executed.
    def multi(self, keys=None):
        self.inTransaction = True
        self.unwatch_cc = lambda: ()
        self.commit_cc = self._clear_txstate
        if keys is not None:
            d = self.watch(keys)
            d.addCallback(lambda _: self.execute_command("MULTI"))
        else:
            d = self.execute_command("MULTI")
        d.addCallback(self._tx_started)
        return d

    def _tx_started(self, response):
        if response != 'OK':
            raise RedisError('Invalid response: %s' % response)
        return self

    def _commit_check(self, response):
        if response is None:
            self.transactions = 0
            self._clear_txstate()
            raise WatchError("Transaction failed")
        else:
            return response

    def commit(self):
        if self.inTransaction is False:
            raise RedisError("Not in transaction")
        return self.execute_command("EXEC").addCallback(self._commit_check)

    def discard(self):
        if self.inTransaction is False:
            raise RedisError("Not in transaction")
        self.post_proc = []
        self.transactions = 0
        self._clear_txstate()
        return self.execute_command("DISCARD")

    # Returns a proxy that works just like .multi() except that commands
    # are simply buffered to be written all at once in a pipeline.
    # http://redis.io/topics/pipelining
    def pipeline(self):
        """
        Return a deferred that returns self (rather than simply self) to allow
        ConnectionHandler to wrap this method with async connection retrieval.
        """
        self.pipelining = True
        self.pipelined_commands = []
        self.pipelined_replies = []
        d = Deferred()
        d.addCallback(lambda x: x)
        d.callback(self)
        return d

    @inlineCallbacks
    def execute_pipeline(self):
        if not self.pipelining:
            raise RedisError(
                "Not currently pipelining commands, please use pipeline() "
                "first"
            )

        # Flush all the commands at once to redis. Wait for all replies
        # to come back using a deferred list.
        try:
            self.transport.write("".join(self.pipelined_commands))
            results = yield DeferredList(
                deferredList=self.pipelined_replies,
                fireOnOneErrback=True,
                consumeErrors=True,
                )
            returnValue([value for success, value in results])

        finally:
            self.pipelining = False
            self.pipelined_commands = []
            self.pipelined_replies = []

    # Publish/Subscribe
    # see the SubscriberProtocol for subscribing to channels
    def publish(self, channel, message):
        """
        Publish message to a channel
        """
        return self.execute_command("PUBLISH", channel, message)

    # Persistence control commands
    def save(self):
        """
        Synchronously save the DB on disk
        """
        return self.execute_command("SAVE")

    def bgsave(self):
        """
        Asynchronously save the DB on disk
        """
        return self.execute_command("BGSAVE")

    def lastsave(self):
        """
        Return the UNIX time stamp of the last successfully saving of the
        dataset on disk
        """
        return self.execute_command("LASTSAVE")

    def shutdown(self):
        """
        Synchronously save the DB on disk, then shutdown the server
        """
        self.factory.continueTrying = False
        return self.execute_command("SHUTDOWN")

    def bgrewriteaof(self):
        """
        Rewrite the append only file in background when it gets too big
        """
        return self.execute_command("BGREWRITEAOF")

    def _process_info(self, r):
        keypairs = [x for x in r.split('\r\n') if
                    u':' in x and not x.startswith(u'#')]
        d = {}
        for kv in keypairs:
            k, v = kv.split(u':')
            d[k] = v
        return d

    # Remote server control commands
    def info(self, type=None):
        """
        Provide information and statistics about the server
        """
        if type is None:
            return self.execute_command("INFO")
        else:
            r = self.execute_command("INFO", type)
            return r.addCallback(self._process_info)

    # slaveof is missing

    # Redis 2.6 scripting commands
    def _eval(self, script, script_hash, keys, args):
        n = len(keys)
        keys_and_args = tuple(keys) + tuple(args)
        r = self.execute_command("EVAL", script, n, *keys_and_args)
        if script_hash in self.script_hashes:
            return r
        return r.addCallback(self._eval_success, script_hash)

    def _eval_success(self, r, script_hash):
        self.script_hashes.add(script_hash)
        return r

    def _evalsha_failed(self, err, script, script_hash, keys, args):
        if err.check(ScriptDoesNotExist):
            return self._eval(script, script_hash, keys, args)
        return err

    def eval(self, script, keys=[], args=[]):
        h = hashlib.sha1(script).hexdigest()
        if h in self.script_hashes:
            return self.evalsha(h, keys, args).addErrback(
                self._evalsha_failed, script, h, keys, args)
        return self._eval(script, h, keys, args)

    def _evalsha_errback(self, err, script_hash):
        if err.check(ResponseError):
            if err.value.args[0].startswith(u'NOSCRIPT'):
                if script_hash in self.script_hashes:
                    self.script_hashes.remove(script_hash)
                raise ScriptDoesNotExist("No script matching hash: %s found" %
                                         script_hash)
        return err

    def evalsha(self, sha1_hash, keys=[], args=[]):
        n = len(keys)
        keys_and_args = tuple(keys) + tuple(args)
        r = self.execute_command(
            "EVALSHA", sha1_hash, n, *keys_and_args
        ).addErrback(self._evalsha_errback, sha1_hash)
        if sha1_hash not in self.script_hashes:
            r.addCallback(self._eval_success, sha1_hash)
        return r

    def _script_exists_success(self, r):
        l = [bool(x) for x in r]
        if len(l) == 1:
            return l[0]
        else:
            return l

    def script_exists(self, *hashes):
        return self.execute_command("SCRIPT", "EXISTS",
                                    post_proc=self._script_exists_success,
                                    *hashes)

    def _script_flush_success(self, r):
        self.script_hashes.clear()
        return r

    def script_flush(self):
        return self.execute_command("SCRIPT", "FLUSH").addCallback(
            self._script_flush_success)

    def _handle_script_kill(self, r):
        if isinstance(r, Failure):
            if r.check(ResponseError):
                if r.value.args[0].startswith(u'NOTBUSY'):
                    raise NoScriptRunning("No script running")
        else:
            pass
        return r

    def script_kill(self):
        return self.execute_command("SCRIPT",
                                    "KILL").addBoth(self._handle_script_kill)

    def script_load(self, script):
        return self.execute_command("SCRIPT",  "LOAD", script)

    # Redis 2.8.9 HyperLogLog commands
    def pfadd(self, key, elements, *args):
        elements = list_or_args("pfadd", elements, args)
        return self.execute_command("PFADD", key, *elements)

    def pfcount(self, keys, *args):
        keys = list_or_args("pfcount", keys, args)
        return self.execute_command("PFCOUNT", *keys)

    def pfmerge(self, destKey, sourceKeys, *args):
        sourceKeys = list_or_args("pfmerge", sourceKeys, args)
        return self.execute_command("PFMERGE", destKey, *sourceKeys)
