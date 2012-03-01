from twisted.internet.protocol import Factory
from twisted.protocols         import amp
import commands

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
