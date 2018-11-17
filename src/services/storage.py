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

