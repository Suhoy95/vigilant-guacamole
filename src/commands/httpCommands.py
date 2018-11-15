import os.path as path
import json
import requests
import logging

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
        url = "http://%s/properties" % (self.ns_address, )
        resp = requests.get(url, params={'path': path})

        if resp.status_code == 400:
            error = resp.json()
            raise DfsHttpException("""[ERROR] Could not perform
[ERROR] stat {}
[NAMESERVER][{}] {}""".format(path, self.ns_address, error['message']))

        if resp.status_code != 200:
            raise Exception(
                "Unexpected status_code {}".format(resp.status_code))

        return resp.json()

    def du(self):
        url = "http://%s/nodes/status" % (self.ns_address,)
        resp = requests.get(url)

        if resp.status_code == 400:
            error = resp.json()
            raise DfsHttpException("""[ERROR] Could not perform
[ERROR] du
[NAMESERVER][{}] {}""".format(self.ns_address, error['message']))

        if resp.status_code != 200:
            raise Exception(
                "Unexpected status_code {}".format(resp.status_code))

        return resp.json()

    def get(self, src_dfs_path, dst_local_path, recursive=False):

        # go to NameServer to get operation structure (a.k.a ticket for storage server)
        url = "http://%s/storage" % (self.ns_address, )
        resp = requests.get(url, params={'path': src_dfs_path})

        if resp.status_code == 400:
            error = resp.json()
            raise DfsHttpException("""[ERROR] Could not perform
[ERROR] get {} {}
[NAMESERVER][{}] {}""".format(src_dfs_path, dst_local_path, self.ns_address, error['message']))

        if resp.status_code != 200:
            raise Exception(
                "Unexpected status_code {}".format(resp.status_code))

        operation = resp.json()

        logging.debug(repr(operation))
        url = "http://{}/storage/download".format(operation['nodeAddress'])

        resp = requests.post(url, json=operation)
        if resp.status_code == 400:
            error = resp.json()
            raise DfsHttpException("""[ERROR] Could not perform
[ERROR] get {} {}
[STORAGE SERVER][{}] {}""".format(src_dfs_path, dst_local_path, operation['nodeAddress'], error['message']))

        if resp.status_code != 200:
            raise Exception(
                "Unexpected status_code {}".format(resp.status_code))

        with open(dst_local_path, "wb") as f:
            f.write(resp.content)

    def put(self, local_path, dfs_path, recursive=False):
        filesize = path.getsize(local_path)

        url = "http://%s/storage" % (self.ns_address, )
        resp = requests.post(url, json={
            'path': dfs_path,
            'type': FILE,
            'size': filesize,
        })

        if resp.status_code == 400:
            error = resp.json()
            raise DfsHttpException("""[ERROR] Could not perform
[ERROR] put {} {}
[NAMESERVER][{}] {}""".format(local_path, dfs_path, self.ns_address, error['message']))

        if resp.status_code != 200:
            raise Exception(
                "Unexpected status_code {}".format(resp.status_code))

        operation = resp.json()

        url = "http://{}/storage/upload".format(operation['nodeAddress'])
        files = {'file': open(local_path, 'rb')}

        resp = requests.post(
            url, params={'operation': json.dumps(operation)}, files=files)

        if resp.status_code == 400:
            error = resp.json()
            raise DfsHttpException(error['message'])

        if resp.status_code != 200:
            raise Exception(
                "Unexpected status_code {}".format(resp.status_code))

    def rm(self, dfs_path, recursive=False, fstat=None):
        if not fstat:
            fstat = self.stat(dfs_path)

        if fstat['type'] == DIRECTORY and not recursive:
            raise DfsException("{} is a directory".format(dfs_path))

        if fstat['type'] == DIRECTORY:
            for f in self.ls(dfs_path):
                self.rm(f['path'], recursive=recursive, fstat=f)

        url = "http://%s/storage" % (self.ns_address,)
        resp = requests.delete(url, params={'path': fstat['path']})

        if resp.status_code == 400:
            error = resp.json()
            raise DfsHttpException("""[ERROR] Could not perform
[ERROR] rm {}
[NAMESERVER][{}] {}""".format(dfs_path, self.ns_address, error['message']))

        if resp.status_code != 200:
            raise Exception(
                "Unexpected status_code {}".format(resp.status_code))

    def ls(self, dfs_dir):
        url = "http://%s/list" % (self.ns_address,)
        resp = requests.get(url, params={'path': dfs_dir})

        if resp.status_code == 400:
            error = resp.json()
            raise DfsHttpException("""[ERROR] Could not perform
[ERROR] ls {}
[NAMESERVER][{}] {}""".format(dfs_dir, self.ns_address, error['message']))

        if resp.status_code != 200:
            raise Exception(
                "Unexpected status_code {}".format(resp.status_code))

        files = resp.json()

        # TODO: remove crunch
        for f in files:
            if f['type'] == DIRECTORY and f['path'][-1] == '/':
                f['path'] = f['path'][:-1]

        return files

    def mkdir(self, dfs_dir):
        url = "http://%s/storage" % (self.ns_address, )
        resp = requests.post(url, json={
            'type': DIRECTORY,
            'path': dfs_dir,
            'size': 0
        })

        if resp.status_code == 400:
            error = resp.json()
            raise DfsHttpException("""[ERROR] Could not perform
[ERROR] mkir {}
[NAMESERVER][{}] {}""".format(dfs_dir, self.ns_address, error['message']))

        if resp.status_code != 200:
            raise Exception(
                "Unexpected status_code {}".format(resp.status_code))

        return resp.json()
