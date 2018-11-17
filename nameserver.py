import logging
import json
import rpyc

from os import path

import src.proto as proto
from src.exceptions import *


def Dir(path, files):
    return {
        'type': proto.DIRECTORY,
        'size': 0,
        'path': path,
        'files': files
    }


def File(path, size):
    return {
        'type': proto.FILE,
        'size': size,
        'path': path,
        'nodes': [
            ('localhost', 8084)
        ]
    }


Storages = list()

# TODO: loading from file
Tree = None


def dump_tree(tree):
    # TODO: write to tmpfile -> rename
    with open("tree.json", "w") as f:
        f.write(json.dumps(tree, indent=4))


class NameServerService(rpyc.Service):
    def on_connect(self, conn):
        self._conn = conn
        self._peer = conn._channel.stream.sock.getpeername()
        logging.info("on_connect: {}".format(self._peer))

        # for storage server
        self._role = None
        self._storage_addr = None

    def on_disconnect(self, conn):
        logging.info("on_disconnect: {}".format(self._peer))

        if self._role == "storage":
            Storages.remove(self)
            logging.info("Storage {} is disconnected".format(
                self._storage_addr))

    def exposed_register(self, host, port):
        self._role = "storage"
        self._storage_addr = (host, port)
        self._st = self._conn.root
        Storages.append(self)
        logging.info("Register storage %s:%d", host, port)

    def exposed_get_storages(self):
        return [s._storage_addr for s in Storages if not s._conn.closed]

    def exposed_stat(self, dfs_path):
        if not path.isabs(dfs_path):
            raise ValueError("Path '{}' is not absolute".format(dfs_path))

        stat = Tree.get(dfs_path, None)
        if not stat:
            raise DfsException("File '{}' does not exists".format(dfs_path))
        return stat

    def exposed_du(self):
        # TODO: state of storage server
        return [{
            'name': str(s._storage_addr),
            'free': 10000,
            'capacity': 10000,
        } for s in Storages if not s._conn.closed]

    def exposed_ls(self, dfs_dir):
        if not path.isabs(dfs_dir):
            raise ValueError("Path '{}' is not absolute".format(dfs_dir))

        stat = Tree.get(dfs_dir, None)
        if not stat:
            raise DfsException("File '{}' does not exist".format(dfs_dir))

        if stat['type'] != proto.DIRECTORY:
            raise DfsException("'{}' is not a directory".format(dfs_dir))

        return [Tree[path.join(dfs_dir, filename)] for filename in stat['files']]

    def exposed_mkdir(self, dfs_path):
        if not path.isabs(dfs_path):
            raise ValueError("Path '{}' is not absolute".format(dfs_path))

        stat = Tree.get(dfs_path, None)
        if stat:
            raise DfsException("File '{}' exists".format(dfs_path))

        dirname = path.dirname(dfs_path)
        stat = Tree.get(dirname, None)
        if not stat:
            raise DfsException("Directory '{}' does not exist".format(dirname))

        if stat['type'] == proto.FILE:
            raise DfsException("Can not create '{}' inside the file".format(dfs_path))

        for s in Storages:
            c = rpyc.connect(s._storage_addr[0], port=s._storage_addr[1])
            c.root.mkdir(dfs_path)
            c.close()
            # I don not know, but this call is blocked
            # did not find way how to serve remote call from client
            # s._conn.root.mkdir(dfs_path)

        Tree[dfs_path] = Dir(dfs_path, [])
        stat['files'].append(path.basename(dfs_path))
        dump_tree(Tree)

    def exposed_rm(self, dfs_path, recursive=False, fstat=None):
        if not fstat:
            fstat = self.exposed_stat(dfs_path)

        if fstat['type'] == proto.DIRECTORY and not recursive:
            raise DfsException("{} is a directory".format(dfs_path))

        if fstat['type'] == proto.DIRECTORY:
            for f in self.exposed_ls(dfs_path):
                self.exposed_rm(f['path'], recursive=recursive, fstat=f)

        if fstat['type'] == proto.DIRECTORY:
            for s in Storages:
                c = rpyc.connect(s._storage_addr[0], port=s._storage_addr[1])
                c.root.rmdir(dfs_path)
                c.close()
        elif fstat['type'] == proto.FILE:
            for s in fstat['nodes']:
                c = rpyc.connect(s[0], port=s[1])
                c.root.rm(dfs_path)
                c.close()
        else:
            raise DfsException("Unexpected type of file: {}".format(fstat['type']))

        Tree.pop(dfs_path)
        Tree[ path.dirname(dfs_path) ]['files'].remove(path.basename(dfs_path))
        dump_tree(Tree)


if __name__ == "__main__":
    from rpyc.utils.server import ThreadedServer
    logging.basicConfig(level=logging.DEBUG)

    # TODO: rm
    from restoretree import restoretree
    restoretree('storage1/', 'tree.json')

    with open('tree.json', 'r') as f:
        Tree = json.load(f)

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
