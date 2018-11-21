from ..proto import (
    FILE,
    DIRECTORY
)
from ..exceptions import *
from .commands import Commands
import os.path as path
import json
import requests
import logging

# suppress warning about verifying certificate
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class HttpCommands(Commands):

    def __init__(self, args):
        self.ns_address = args.nameserver

        url = "https://%s/auth" % (self.ns_address, )
        resp = requests.post(url, verify=False, json={
            'username': args.username,
            'password': args.password
        })

        if resp.status_code == 400:
            raise DfsHttpException("Bad username or password")

        if resp.status_code != 200:
            raise DfsHttpException(
                "Unexpected status code during authorization: {}\n {}".format(
                    resp.status_code, resp.text
                )
            )

        self.token = resp.json()['token']

    def stat(self, path):
        url = "https://%s/properties" % (self.ns_address, )
        resp = requests.get(url, params={'path': path}, verify=False)

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
        url = "https://%s/nodes/status" % (self.ns_address,)
        resp = requests.get(url, verify=False)

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
        url = "https://%s/storage" % (self.ns_address, )
        resp = requests.get(url, params={'path': src_dfs_path}, verify=False)

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
        url = "https://{}/storage/download".format(operation['nodeAddress'])

        resp = requests.post(url, json=operation, verify=False)
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

        url = "https://%s/storage" % (self.ns_address, )
        resp = requests.post(url, json={
            'path': dfs_path,
            'type': FILE,
            'size': filesize,
        }, verify=False)

        if resp.status_code == 400:
            error = resp.json()
            raise DfsHttpException("""[ERROR] Could not perform
[ERROR] put {} {}
[NAMESERVER][{}] {}""".format(local_path, dfs_path, self.ns_address, error['message']))

        if resp.status_code != 200:
            raise Exception(
                "Unexpected status_code {}".format(resp.status_code))

        operation = resp.json()

        url = "https://{}/storage/upload".format(operation['nodeAddress'])
        files = {'file': open(local_path, 'rb')}

        resp = requests.post(
            url, params={'operation': json.dumps(operation)},
            files=files,
            verify=False)

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

        url = "https://%s/storage" % (self.ns_address,)
        resp = requests.delete(url,
                               verify=False,
                               params={'path': fstat['path']},
                               headers={'Authorization': self.token}
                               )

        if resp.status_code == 400:
            error = resp.json()
            raise DfsHttpException("""[ERROR] Could not perform
[ERROR] rm {}
[NAMESERVER][{}] {}""".format(dfs_path, self.ns_address, error['message']))

        if resp.status_code != 200:
            raise Exception(
                "Unexpected status_code {}".format(resp.status_code))

    def ls(self, dfs_dir):
        url = "https://%s/list" % (self.ns_address,)
        resp = requests.get(url, params={'path': dfs_dir}, verify=False)

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
        url = "https://%s/storage" % (self.ns_address, )
        resp = requests.post(url, json={
            'type': DIRECTORY,
            'path': dfs_dir,
            'size': 0
        }, verify=False)

        if resp.status_code == 400:
            error = resp.json()
            raise DfsHttpException("""[ERROR] Could not perform
[ERROR] mkir {}
[NAMESERVER][{}] {}""".format(dfs_dir, self.ns_address, error['message']))

        if resp.status_code != 200:
            raise Exception(
                "Unexpected status_code {}".format(resp.status_code))

        return resp.json()
