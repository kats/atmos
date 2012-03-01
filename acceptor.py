from sys import stdout

from twisted.python.log         import startLogging, err
from twisted.protocols          import amp
from twisted.internet           import reactor
from twisted.internet.protocol  import Factory
from twisted.internet.endpoints import TCP4ClientEndpoint

import commands

class AcceptorProtocol(amp.AMP):
    @commands.PrepareCommand.responder
    def prepare(self, proposal):
        print "prepare with proposal %d" % proposal
        return {
            "has_higher" : False,
            "proposal"   : 0,
            "value"      : ""}


def connect():
    endpoint = TCP4ClientEndpoint(reactor, "127.0.0.1", 8750)
    factory = Factory()
    factory.protocol = AcceptorProtocol
    return endpoint.connect(factory)

def on_connect():
    pass

def on_response(r):
    print r

startLogging(stdout)

d = connect()
d.addErrback(err, "connection failed")
d.addCallback(on_connect)
d.addCallback(on_response)

reactor.run()
