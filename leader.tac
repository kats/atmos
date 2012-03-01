from twisted.application import internet, service
from leader import Leader

application = service.Application("leader")
leader = Leader()

internet.TCPServer(8750, leader.leaderProtocolFactory()).setServiceParent(application)
internet.TCPServer(8751, leader.controlProtocolFactory()).setServiceParent(application)
