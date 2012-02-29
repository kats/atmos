from sys import stdout

from twisted.python.log         import startLogging, err
from twisted.protocols          import amp
from twisted.internet           import reactor
from twisted.internet.protocol  import Factory
from twisted.internet.endpoints import TCP4ServerEndpoint

class PrepareCommand(amp.Command):
    arguments = [("proposal", amp.Integer())]
    response  = [("res", amp.String())]

class WriteCommand(amp.Command):
    arguments = [("key", amp.String()), ("value", amp.String())]
    response  = [("response", amp.String())]

class ControlProtocol(amp.AMP):

    def write(self, key, value):
        for a in acceptors:
            a.prepare()

        return {"response" : "write request: " + key + " : " + value}
        
    WriteCommand.responder(write)

class LeaderProtocol(amp.AMP):
    def __init__(self, acceptors):
        super(LeaderProtocol, self).__init__()
        self._acceptors = acceptors

    def connectionMade(self):
        self._acceptors.append(self)

    def connectionLost(self, reason):
        self._acceptors.remove(self)

    def prepare(self):
        self.callRemote(PrepareCommand, proposal = 0)

class LeaderFactory(Factory):
    def __init__(self, acceptors):
        self._acceptors = acceptors

    def buildProtocol(self, addr):
        return LeaderProtocol(acceptors)

if __name__ == "__main__":
    startLogging(stdout)

    leaderf = Factory()
    leaderf.protocol = LeaderProtocol
    leadere = TCP4ServerEndpoint(reactor, 8750)
    leadere.listen(leaderf)
    
    ctrlf = Factory()
    ctrlf.protocol = ControlProtocol
    ctrle = TCP4ServerEndpoint(reactor, 8751)
    ctrle.listen(ctrlf)

    reactor.run()