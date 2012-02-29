from sys import stdout

from twisted.python.log         import startLogging, err
from twisted.protocols.amp      import AMP
from twisted.internet           import reactor
from twisted.internet.protocol  import Factory
from twisted.internet.endpoints import TCP4ClientEndpoint

from server import WriteCommand

def connect():
    endpoint = TCP4ClientEndpoint(reactor, "127.0.0.1", 8751)
    factory = Factory()
    factory.protocol = AMP
    return endpoint.connect(factory)

def on_connect(p):
    return p.callRemote(WriteCommand, key="test_key", value="0")

def on_response(r):
    print r

startLogging(stdout)

d = connect()
d.addErrback(err, "connection failed")
d.addCallback(on_connect)
d.addCallback(on_response)

reactor.run()
