from twisted.internet.protocol import Factory
from twisted.protocols         import amp
import commands

class ControlProtocol(amp.AMP):
    def __init__(self, acceptors):
        super(ControlProtocol, self).__init__()
        self._acceptors = acceptors

    @commands.WriteCommand.responder
    def write(self, key, value):
        a = self._acceptors[0]
        d = a.prepare()
        def formatResponse(p):
            return {"response" : str(p)}
        d.addCallback(formatResponse)
        return d

class ControlFactory(Factory):
    def __init__(self, acceptors):
        self._acceptors = acceptors

    def buildProtocol(self, addr):
        return ControlProtocol(self._acceptors)
