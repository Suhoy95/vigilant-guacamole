import logging
import rpyc


Clients = list()
Storages = list()


class NameServerService(rpyc.Service):
    def on_connect(self, conn):
        self.conn = conn
        print("get connection: ", repr(conn._channel.stream.sock))

    def on_disconnect(self, conn):
        if self.role == "client":
            Clients.remove(self)
        elif self.role == "storage":
            Storages.remove(self)

    def exposed_register(self, host, port, role):
        self.role = role
        self.addr = (host, port)
        if role == "client":
            Clients.append(self)
            logging.info("Register client %s:%d", host, port)
        elif role == "storage":
            Storages.append(self)
            logging.info("Register storage %s:%d", host, port)
        else:
            raise ValueError("Unknown role")

    def exposed_get_storages(self):
        return [s.addr for s in Storages if not s.conn.closed]


if __name__ == "__main__":
    from rpyc.utils.server import ThreadedServer
    logging.basicConfig(level=logging.DEBUG)
    # from rpyc.utils.authenticators import SSLAuthenticator
    # auth = SSLAuthenticator(
    #     keyfile="certs/nameserver/nameserver.key",
    #     certfile="certs/nameserver/nameserver.crt",
    #     ca_certs="certs/rootCA.crt",
    # )
    # t = ThreadedServer(NameServerService, port=8081, authenticator=auth)
    t = ThreadedServer(NameServerService, port=8081)
    t.start()
    # conn = rpyc.ssl_connect("localhost", port=8081, keyfile="certs/enemy/enemy.key", certfile="certs/enemy/enemy.crt")
    # conn = rpyc.ssl_connect("localhost", port=8081, keyfile="certs/qwe/qwe.key", certfile="certs/qwe/qwe.crt")
#