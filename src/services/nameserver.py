import os.path as path
import logging
import rpyc

import src.proto as proto
from src.exceptions import *
from src.utils import *

Storages = list()


class NameServerService(rpyc.Service):
    def __init__(self, tree, treefile):
        rpyc.Service.__init__(self)
        self._tree = tree
        self._treefile = treefile

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

    def exposed_register(self, host, port, capacity, free):
        self._role = "storage"
        self._storage_addr = (host, port)
        self._capacity = capacity
        self._free = free
        self._st = self._conn.root
        Storages.append(self)
        logging.info("Register storage %s:%d", host, port)

    def exposed_get_storages(self):
        return [s._storage_addr for s in Storages if not s._conn.closed]

    def exposed_stat(self, dfs_path):
        if not path.isabs(dfs_path):
            raise ValueError("Path '{}' is not absolute".format(dfs_path))

        stat = self._tree.get(dfs_path, None)
        if not stat:
            raise DfsException("File '{}' does not exists".format(dfs_path))
        return stat

    def exposed_du(self):
        # TODO: state of storage server
        return [{
            'name': str(s._storage_addr),
            'free': s._free,
            'capacity': s._capacity,
        } for s in Storages if not s._conn.closed]

    def exposed_ls(self, dfs_dir):
        if not path.isabs(dfs_dir):
            raise ValueError("Path '{}' is not absolute".format(dfs_dir))

        stat = self._tree.get(dfs_dir, None)
        if not stat:
            raise DfsException("File '{}' does not exist".format(dfs_dir))

        if stat['type'] != proto.DIRECTORY:
            raise DfsException("'{}' is not a directory".format(dfs_dir))

        return [self._tree[path.join(dfs_dir, filename)] for filename in stat['files']]

    def exposed_mkdir(self, dfs_path):
        if not path.isabs(dfs_path):
            raise ValueError("Path '{}' is not absolute".format(dfs_path))

        stat = self._tree.get(dfs_path, None)
        if stat:
            raise DfsException("File '{}' exists".format(dfs_path))

        dirname = path.dirname(dfs_path)
        stat = self._tree.get(dirname, None)
        if not stat:
            raise DfsException("Directory '{}' does not exist".format(dirname))

        if stat['type'] == proto.FILE:
            raise DfsException(
                "Can not create '{}' inside the file".format(dfs_path))

        for s in Storages:
            c = rpyc.connect(s._storage_addr[0], port=s._storage_addr[1])
            c.root.mkdir(dfs_path)
            c.close()
            # I don not know, but this call is blocked
            # did not find way how to serve remote call from client
            # s._conn.root.mkdir(dfs_path)

        self._tree[dfs_path] = Dir(dfs_path, [])
        stat['files'].append(path.basename(dfs_path))
        dump_tree(self._tree, self._treefile)

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
            raise DfsException(
                "Unexpected type of file: {}".format(fstat['type']))

        self._tree.pop(dfs_path)
        self._tree[path.dirname(dfs_path)]['files'].remove(
            path.basename(dfs_path))
        dump_tree(self._tree, self._treefile)
