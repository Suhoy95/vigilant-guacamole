import time
from multiprocessing import Process
import os.path as path
import os

import rpyc
import pytest


from src.servers.nameserver import startNameServer
from src.servers.storage import startStorageServer
from src.utils import (dump_tree, Dir)

#
# nameserver fixtures
#


@pytest.fixture
def nsHostname():
    return "localhost"


@pytest.fixture
def nsPort():
    return 6000


@pytest.fixture
def treeJsonFilename(tmpdir):
    tree = {
        '/': Dir('/'),
    }
    jsonFilename = path.join(tmpdir, 'tree.json')
    dump_tree(tree, jsonFilename)
    yield jsonFilename
    os.remove(jsonFilename)


@pytest.fixture
def ns(treeJsonFilename, nsHostname, nsPort):
    p = Process(target=lambda: startNameServer(
        treeJsonFilename, nsHostname, nsPort))
    p.start()
    time.sleep(0.5)
    yield p
    p.terminate()


@pytest.fixture
def nsConn(ns, nsHostname, nsPort):
    con = rpyc.connect(nsHostname, port=nsPort)
    yield con.root
    con.close()

#
# storage1 fixtures
#


@pytest.fixture
def storage1dir(tmpdir):
    storagePath = path.join(tmpdir, 'storage1')
    os.mkdir(storagePath)
    return storagePath


@pytest.fixture
def storage1(ns, nsHostname, nsPort, storage1dir):
    p = Process(target=lambda: startStorageServer(nsHostname, nsPort,
                                                  "localhost", 6001,
                                                  capacity=11111, free=10000,
                                                  rootPath=storage1dir))
    p.start()
    time.sleep(0.5)
    yield p
    p.terminate()


@pytest.fixture
def st1Conn(storage1):
    conn = rpyc.connect("localhost", 6001, config={
        'allow_public_attrs': True,
    })
    yield conn.root
    conn.close()


#
# storage1 fixtures
#


@pytest.fixture
def storage2dir(tmpdir):
    storagePath = path.join(tmpdir, 'storage2')
    os.mkdir(storagePath)
    return storagePath


@pytest.fixture
def storage2(ns, nsHostname, nsPort, storage1dir):
    p = Process(target=lambda: startStorageServer(nsHostname, nsPort,
                                                  "localhost", 6002,
                                                  capacity=11111, free=6000,
                                                  rootPath=storage1dir))
    p.start()
    time.sleep(0.5)
    yield p
    p.terminate()


@pytest.fixture
def st2Conn(storage1):
    conn = rpyc.connect("localhost", 6002, config={
        'allow_public_attrs': True,
    })
    yield conn.root
    conn.close()


@pytest.fixture
def helloFile(tmpdir):
    filename = path.join(tmpdir, 'hello')
    with open(filename, "w") as f:
        f.write("Hello, world!")
    yield filename
    os.remove(filename)
