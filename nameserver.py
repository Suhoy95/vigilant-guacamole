import logging
import json
import rpyc
from rpyc.utils.server import ThreadedServer

from src.services.nameserver import NameServerService


def parse_args():
    import argparse
    parser = argparse.ArgumentParser(description="Starting nameserver")
    parser.add_argument('--tree', help="path to json-file, which contains tree information")
    parser.add_argument('--port', help="port of nameserver", type=int, defaul)

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    # TODO: rm
    from restoretree import restoretree
    restoretree('storage1/', 'tree.json')

    t = ThreadedServer(NameServerService, port=8081)
    t.start()

    # from rpyc.utils.authenticators import SSLAuthenticator
    # auth = SSLAuthenticator(
    #     keyfile="certs/nameserver/nameserver.key",
    #     certfile="certs/nameserver/nameserver.crt",
    #     ca_certs="certs/rootCA.crt",
    # )
    # t = ThreadedServer(NameServerService, port=8081, authenticator=auth)
    # conn = rpyc.ssl_connect("localhost", port=8081, keyfile="certs/enemy/enemy.key", certfile="certs/enemy/enemy.crt")
    # conn = rpyc.ssl_connect("localhost", port=8081, keyfile="certs/qwe/qwe.key", certfile="certs/qwe/qwe.crt")
