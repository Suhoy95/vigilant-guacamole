from os import path

import src.proto as proto
from src.exceptions import *
from .commands import Commands


class MemoryCommands(Commands):
    def __init__(self):
        self.db = dict() # abs_path -> file_stat
        self._add_dir('/')
        self._add_dir('/test')
        self._add_file('/test/test.txt', 512)

    def _add_dir(self, path):
        self.db[path] = {
            'type': proto.DIRECTORY,
            'size': 0,
            'path': path,
        }

    def _add_file(self, path, size):
        self.db[path] = {
            'type': proto.FILE,
            'size': size,
            'path': path,
        }

    def stat(self, dfs_path):
        f = self.db.get(dfs_path, None)
        if not f:
            raise DfsException("'{}' is not found".format(dfs_path))

        return f

    def ls(self, dfs_dir):
        f = self.stat(dfs_dir)
        if f['type'] != proto.DIRECTORY:
            raise ValueError("'{}' is not a directory".format(dfs_dir))
        
        for p, f in self.db.items():
            d = path.dirname(p)
            if d == dfs_dir:
                yield f
        