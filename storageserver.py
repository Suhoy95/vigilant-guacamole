import rpyc

Hostname = "localhost"
Port = 8084

class StorageService(rpyc.Service):
    pass


if __name__ == "__main__":
    from rpyc.utils.server import ThreadedServer
    Ns_conn = rpyc.connect("localhost", port=8081)
    Ns_conn.root.register(Hostname, Port, "storage")

    service = StorageService()
    Server = ThreadedServer(service, hostname=Hostname, port=Port)
    Server.start()