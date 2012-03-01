from twisted.protocols import amp

class PrepareCommand(amp.Command):
    arguments = [("proposal", amp.Integer())]
    response  = [
        ("has_higher", amp.Boolean()),
        ("proposal", amp.Integer()),
        ("value", amp.String())
    ]

class AcceptCommand(amp.Command):
    pass

class WriteCommand(amp.Command):
    arguments = [("key", amp.String()), ("value", amp.String())]
    response  = [("response", amp.String())]

