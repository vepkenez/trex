# trex

#### A simple redis client for twisted

**trex** inherits directly from *txredisapi*
but represents a fundamental shift away from both the user interface and
code structure as defined in *txredisapi*. The point is to have a more
maintainable codebase through:
- the use of twisted's endpoints API
- the use of modules for code organisation and documentation
- the maintenance of docs (RTD)

```python
from trex import redis
from twisted.internet.defer import returnValue, inlineCallbacks
from twisted.internet import reactor

conn = redis.connect()  # returns a Deferred connection

@inlineCallbacks
def pingItHard(conn):
    response = yield conn.ping()
    for idx in range(10):
        print 'ping', response, 'isn\'t hard... is it?'
    returnValue(response)

conn.addCallback(pingItHard)
conn.addCallback(lambda ign: reactor.stop())

reactor.run()
```

## Installation

`pip install trex`
