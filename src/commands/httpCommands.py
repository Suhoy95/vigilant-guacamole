import os.path as path
import json
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
        return self._get('/properties', {'path': path})

    def du(self):
        return self._get('/nodes/status')

    def get(self, dfs_path, local_path, recursive=False):
        operation = self._get('/storage', {'path': dfs_path})

        if '_id' in operation:
            operation.pop('_id')

        if not operation['digest']:
            operation['digest'] = 'aasd2w854sdf9q64fa'

        # operation['nodeAddress'])
        url = "http://{}/storage/download".format('127.0.0.1:8081')
        resp = requests.post(url, json=operation)

        if resp.status_code == 200:
            with open(local_path, "wb") as f:
                f.write(resp.content)
            return

        if resp.status_code == 400:
            error = resp.json()
            raise DfsHttpException(error['message'])

        raise Exception("Unexpected status_code")

    def put(self, local_path, dfs_path, recursive=False):
        filesize = path.getsize(local_path)

        operation = self._post('/storage', {
            'path': dfs_path,
            'type': FILE,
            'size': filesize,
        })

        # operation['nodeAddress'])
        url = "http://{}/storage/upload".format('127.0.0.1:8081')
        files = {'file': open(local_path, 'rb')}

        if '_id' in operation:
            operation.pop('_id')

        if not operation['digest']:
            operation['digest'] = 'aasd2w854sdf9q64fa'

        resp = requests.post(
            url, params={'operation': json.dumps(operation)}, files=files)

        if resp.status_code == 200:
            return

        if resp.status_code == 400:
            error = resp.json()
            raise DfsHttpException(error['message'])

        raise Exception("Unexpected status_code")

    def rm(self, dfs_path, recursive=False):
        if dfs_path.endswith('/') and not recursive:
            raise ValueError("Can not remove directory without recursion")

        if dfs_path.endswith('/'):
            for f in self.ls(dfs_path):
                self.rm(f['path'], recursive=recursive)

        self._delete('/storage', {'path': dfs_path})

    def ls(self, dfs_dir):
        files = self._get('/list', {'path': dfs_dir})

        # todo: remove
        for f in files:
            if f['type'] == DIRECTORY:
                f['path'] = f['path'][:-1]
        return files

    def mkdir(self, fds_dir):
        return self._post('/storage', {
            'type': DIRECTORY,
            'path': fds_dir,
            'size': 0
        })

    def _get(self, apiPath, params=dict()):
        url = "http://%s%s" % (self.ns_address, apiPath)
        resp = requests.get(url, params=params)

        if resp.status_code == 200:
            return resp.json()

        if resp.status_code == 400:
            error = resp.json()
            raise DfsHttpException(error['message'])

        raise Exception("Unexpected status_code")

    def _post(self, apiPath, params=dict()):
        url = "http://%s%s" % (self.ns_address, apiPath)
        resp = requests.post(url, json=params)

        if resp.status_code == 200:
            return resp.json()

        if resp.status_code == 400:
            error = resp.json()
            raise DfsHttpException(error['message'])

        raise Exception("Unexpected status_code")

    def _delete(self, apiPath, params=dict()):
        url = "http://%s%s" % (self.ns_address, apiPath)
        return requests.delete(url, params=params)
