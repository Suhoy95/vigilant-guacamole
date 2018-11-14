import requests

from .commands import Commands
from ..exceptions import *
from ..proto import (
    FILE,
    DIRECTORY
)

class HttpCommands(Commands):

    def __init__(self, ns_address):
        self.ns_address = ns_address

    def stat(self, path):
        resp = self._get('/properties', {'path': path})

        if resp.status_code == 200:
            return resp.json()

        if resp.status_code == 400:
            error = resp.json()
            raise DfsHttpException(error['message'])

        raise Exception("Unexpected status_code")

    def put(self, local_path, dfs_path, recursive=False):
        raise NotImplementedError()

    def rm(self, dfs_path, recursive=False):
        if dfs_path.endswith('/') and not recursive:
            raise ValueError("Can not remove directory without recursion")

        if dfs_path.endswith('/'):
            for f in self.ls(dfs_path):
                self.rm(f['path'], recursive=recursive)

        self._delete('/storage', {'path': dfs_path})

    def ls(self, dfs_dir):
        resp = self._get('/list', {'path': dfs_dir})

        if resp.status_code == 200:
            return resp.json()

        if resp.status_code == 400:
            error = resp.json()
            raise DfsHttpException(error['message'])

        raise Exception("Unexpected status_code")

    def mkdir(self, fds_dir):
        resp = self._post('/storage', {'type': DIRECTORY, 'path': fds_dir, 'size': 0})

        if resp.status_code == 200:
            return resp.json()

        if resp.status_code == 400:
            error = resp.json()
            raise DfsHttpException(error['message'])

        raise Exception("Unexpected status_code")

    def _get(self, apiPath, params):
        url = "http://%s%s" % (self.ns_address, apiPath)
        return requests.get(url, params=params)

    def _post(self, apiPath, params):
        url = "http://%s%s" % (self.ns_address, apiPath)
        return requests.post(url, json=params)

    def _delete(self, apiPath, params):
        url = "http://%s%s" % (self.ns_address, apiPath)
        return requests.delete(url, params=params)