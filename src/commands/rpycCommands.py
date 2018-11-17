import rpyc
import logging

from .commands import Commands
from ..exceptions import *
import src.proto as proto


class RpycCommands(Commands):
    def __init__(self, ns_host, ns_port):
        self._conn = rpyc.connect(ns_host, port=ns_port)
        self._ns = self._conn.root

    def stat(self, dfs_path):
        return self._ns.stat(dfs_path)

    def du(self):
        return self._ns.du()

    def get(self, src_dfs_path, dst_local_path, recursive=False):
        stat = self._ns.stat(src_dfs_path)
        if stat['type'] != proto.FILE:
            raise DfsException("'{}' is not a File".format(src_dfs_path))

        for n in stat['nodes']:
            try:
                logging.info("Trying receive %s from storage %s:%d", src_dfs_path, n[0], n[1])
                storage_conn = rpyc.connect(n[0], port=n[1])
                st = storage_conn.root

                src = st.open(src_dfs_path, "rb")
                # TODO: write to the tmp file then move
                with open(dst_local_path, "wb") as dst:
                    while True:
                        data = src.read(1024)

                        if data == b"":
                            break

                        dst.write(data)
                src.close()
                storage_conn.close()
                return
            except Exception as e:
                logging.warn("Fail receive %s from storage %s:%d", src_dfs_path, n[0], n[1])
                logging.exception(e)
        else:
            raise DfsException("get {} {} | Can not reach storage servers".format(
                src_dfs_path, dst_local_path))

    def ls(self, dfs_dir):
        return self._ns.ls(dfs_dir)

    def mkdir(self, dfs_path):
        self._ns.mkdir(dfs_path)

    def rm(self, dfs_path, recursive=False):
        self._ns.rm(dfs_path, recursive=recursive)

