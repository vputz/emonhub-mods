import riemann_client.client

class RiemannInterface(object):

    def __init__(self, riemann_address='127.0.0.1',
                 riemann_port=19444):
        self.riemann_address = hyperion_address
        self.riemann_port = hyperion_port
        self.riemann_client = riemann_client.client.Client()

    def 
