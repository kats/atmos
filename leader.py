import control_protocol
import leader_protocol

class Leader(object):
    def __init__(self):
        self._acceptors = []

    def leaderProtocolFactory(self):
        return leader_protocol.LeaderFactory(self._acceptors)

    def controlProtocolFactory(self):
        return control_protocol.ControlFactory(self._acceptors)
