from sys import stdout

from twisted.python.log         import startLogging, err
from twisted.protocols          import amp
from twisted.internet           import reactor
from twisted.internet.protocol  import Factory
from twisted.internet.endpoints import TCP4ServerEndpoint

import commands

class ControlProtocol(amp.AMP):

    @commands.WriteCommand.responder
    def write(self, key, value):
        a = acceptors[0]
        d = a.prepare()
        def formatResponse(p):
            return {"response" : str(p)}
        d.addCallback(formatResponse)
        return d

class LeaderProtocol(amp.AMP):
    def __init__(self, acceptors):
        super(LeaderProtocol, self).__init__()
        self._acceptors = acceptors

    def connectionMade(self):
        self._acceptors.append(self)

    def connectionLost(self, reason):
        self._acceptors.remove(self)

    def prepare(self):
        return self.callRemote(commands.PrepareCommand, proposal = 0)

class LeaderFactory(Factory):
    def __init__(self, acceptors):
        self._acceptors = acceptors

    def buildProtocol(self, addr):
        return LeaderProtocol(self._acceptors)

if __name__ == "__main__":
    acceptors = []

    startLogging(stdout)

    TCP4ServerEndpoint(reactor, 8750).listen(LeaderFactory(acceptors))
    
    ctrlf = Factory()
    ctrlf.protocol = ControlProtocol
    TCP4ServerEndpoint(reactor, 8751).listen(ctrlf)

    reactor.run()