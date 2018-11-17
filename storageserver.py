import rpyc
import logging
import threading

from os import path
import os

from os.path import (
    join,
    normpath,
    basename,
    dirname,
    isfile,
    isdir,
    getsize
)

RootPath = path.join(os.getcwd(), "storage1")

Hostname = "localhost"
Port = 8084

class StorageToNameserverService(rpyc.Service):
    def exposed_mkdir(self, dfs_path):
        fullpath = path.join(RootPath, '.' + dfs_path)
        logging.debug("mkdir %s", fullpath)
        os.mkdir(fullpath)

class StorageService(rpyc.Service):

    def exposed_open(self, dfs_path, mode):
        fullpath = path.join(RootPath, '.' + dfs_path)
        logging.debug("open %s", fullpath)
        return open(fullpath, mode)

    def exposed_mkdir(self, dfs_path):
        fullpath = path.join(RootPath, '.' + dfs_path)
        logging.debug("mkdir %s", fullpath)
        os.mkdir(fullpath)

    def exposed_rmdir(self, dfs_path):
        fullpath = path.join(RootPath, '.' + dfs_path)
        logging.debug("rmdir %s", fullpath)
        os.rmdir(fullpath)

    def exposed_rm(self, dfs_path):
        fullpath = path.join(RootPath, '.' + dfs_path)
        logging.debug("rm %s", fullpath)
        os.remove(fullpath)


# def check_files(dfs_path, ns):
#     abspath = normpath(join(RootPath, '.' + dfs_path ))
#     if isfile(abspath):
#         md5sum = md5(abspath)

#         if ns.stat(dfs_path)

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    from rpyc.utils.server import ThreadedServer

    Ns_conn = rpyc.connect("localhost", port=8081, service=StorageToNameserverService())

    # check_files('/', Ns_conn)

    Ns_conn.root.register(Hostname, Port)
    # rpyc.BgServingThread(Ns_conn)


    # def clock():
    #     while True:
    #         try:
    #             print("serve")
    #             Ns_conn.serve()
    #             time.sleep(1)
    #         except Exception as e:
    #             print(e)

    # t =threading.Thread(target=clock)
    # t.start()

    # while True:
    #     logging.debug("staing alive")
    #     import time
    #     time.sleep(1)
    Server = ThreadedServer(StorageService,
                            hostname=Hostname, port=Port, protocol_config={
                                'allow_public_attrs': True,
                            })
    Server.start()
