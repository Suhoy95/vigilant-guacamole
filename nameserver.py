import rpyc

class NameServerService(rpyc.Service):
    def on_connect(self, conn):
        print("get connection: ", repr(conn._channel.stream.sock))

    def on_disconnect(self, conn):
        print("disconnected: ", repr(conn._channel.stream.sock))

    def exposed_get(self):
        return 42

    exposed_number = 43

    def get(self):
        return "qwe"

    def put(self):
        return "asd"

if __name__ == "__main__":
    from rpyc.utils.server import ThreadedServer
    from rpyc.utils.authenticators import SSLAuthenticator
    auth = SSLAuthenticator(
        keyfile="certs/nameserver/nameserver.key",
        certfile="certs/nameserver/nameserver.crt",
        ca_certs="certs/rootCA.crt",
    )
    t = ThreadedServer(NameServerService, port=8081, authenticator=auth)
    t.start()
    # conn = rpyc.ssl_connect("localhost", port=8081, keyfile="certs/enemy/enemy.key", certfile="certs/enemy/enemy.crt")
    # conn = rpyc.ssl_connect("localhost", port=8081, keyfile="certs/qwe/qwe.key", certfile="certs/qwe/qwe.crt")