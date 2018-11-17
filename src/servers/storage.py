import rpyc
from rpyc.utils.server import ThreadedServer

from src.services.storage import StorageService


def startStorageServer(nsHost, nsPort, hostname, port, capacity, free, rootPath):

    Ns_conn = rpyc.connect(nsHost, port=nsPort)

    # check_files('/', Ns_conn)

    Ns_conn.root.register(hostname, port, capacity, free)
    Server = ThreadedServer(StorageService(rootPath),
                            hostname=hostname, port=port,
                            protocol_config={
                                'allow_public_attrs': True,
                            })
    Server.start()
