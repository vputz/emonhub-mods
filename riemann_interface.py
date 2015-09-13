import socket
import json


class HyperionInterface(object):

    def __init__(self, hyperion_address='127.0.0.1',
                 hyperion_port=19444, default_priority=1000):
        self.hyperion_address = hyperion_address
        self.hyperion_port = hyperion_port
        self.socket = None
        self.bufsize = 4096
        self.last_result = None
        self.default_priority = default_priority

    def connect(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.settimeout(1)
        self.socket.connect((self.hyperion_address, self.hyperion_port))

    def close(self):
        if (self.socket):
            self.socket.close()

    def sendCommand(self, command, data=None):
        """
        command: string
        data: additional dictionary of data for the requisite command
        """
        cmd = data.copy() if data else {}
        cmd['command'] = command
        self.socket.sendall((json.dumps(cmd)+"\n").encode("ASCII"))
        self.last_result = self.socket.recv(self.bufsize).decode("ASCII")

    def getServerInfo(self):
        self.sendCommand("serverinfo")

    def clearAll(self):
        self.sendCommand("clearall", {'priority' : self.default_priority})

    def clear(self):
        self.sendCommand("clear", {'priority' : self.default_priority})

    def setColor(self, color):
        self.sendCommand("color",
                         {'color': color, 'priority': self.default_priority})

    def effect(self, effect_name):
        """
        effect: string (must match that in list)
        """
        self.sendCommand("effect",
                         {'effect': {"name": effect_name},
                          'priority': self.default_priority})

    def effectList(self):
        """
        return list of effects callable
        """
        self.getServerInfo()
        info = json.loads(self.last_result)
        return [x['name'] for x in info['info']['effects']]
