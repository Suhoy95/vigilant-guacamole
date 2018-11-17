import logging
import os
import os.path as path

import rpyc


class StorageService(rpyc.Service):

    def __init__(self, root):
        self._root = root

    def exposed_open(self, dfs_path, mode):
        fullpath = path.join(self._root, '.' + dfs_path)
        logging.debug("open %s", fullpath)
        return open(fullpath, mode)

    def exposed_mkdir(self, dfs_path):
        fullpath = path.join(self._root, '.' + dfs_path)
        logging.debug("mkdir %s", fullpath)
        os.mkdir(fullpath)

    def exposed_rmdir(self, dfs_path):
        fullpath = path.join(self._root, '.' + dfs_path)
        logging.debug("rmdir %s", fullpath)
        os.rmdir(fullpath)

    def exposed_rm(self, dfs_path):
        fullpath = path.join(self._root, '.' + dfs_path)
        logging.debug("rm %s", fullpath)
        os.remove(fullpath)

    def exposed_write(self, dfs_path, infile):
        fullpath = path.join(self._root, '.' + dfs_path)
        logging.debug("write %s", fullpath)
        with open(fullpath, "bw") as outfile:
            while True:
                data = infile.read(1024)
                if data == b"":
                    break
                outfile.write(data)
            infile.close()
