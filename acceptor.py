from sys import stdout

from twisted.python.log         import startLogging, err
from twisted.protocols          import amp
from twisted.internet           import reactor
from twisted.internet.protocol  import Factory
from twisted.internet.endpoints import TCP4ClientEndpoint

from server import PrepareCommand, LeaderProtocol

class AcceptorProtocol(amp.AMP):
    def prepare(self, proposal):
        print "prepare with proposal %d" % proposal
        return {"res" : "ok"}

    PrepareCommand.responder(prepare)


def connect():
    endpoint = TCP4ClientEndpoint(reactor, "127.0.0.1", 8750)
    factory = Factory()
    factory.protocol = AcceptorProtocol
    return endpoint.connect(factory)

def on_connect(p):
    pass
    #return p.callRemote(WriteCommand, key="test_key", value="0")

def on_response(r):
    print r

startLogging(stdout)

d = connect()
d.addErrback(err, "connection failed")
d.addCallback(on_connect)
d.addCallback(on_response)

reactor.run()
