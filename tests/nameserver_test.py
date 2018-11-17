import os.path as path
import pytest
import rpyc.core.vinegar as vinegar

from src.utils import Dir
from src.exceptions import *
import src.proto as proto


def testDuWithoutStorages(nsConn):
    nodes = nsConn.du()
    assert isinstance(nodes, list)
    assert len(nodes) == 0


def testDuWithOneStorages(nsConn, storage1):
    nodes = nsConn.du()
    assert isinstance(nodes, list)
    assert len(nodes) == 1
    assert nodes[0]['name'] == "['localhost', 6001]"
    assert nodes[0]['free'] == 10000
    assert nodes[0]['capacity'] == 11111


def testMkdirLs(nsConn, storage1, storage1dir):
    nsConn.mkdir('/qwe')
    files = nsConn.ls('/')
    assert isinstance(files, list)
    assert len(files) == 1
    f = files[0]
    assert f['type'] == proto.DIRECTORY
    assert f['path'] == '/qwe'
    assert f['size'] == 0

    assert path.isdir(path.join(storage1dir, 'qwe'))


def testSelectStorage1(nsConn, storage1):
    st = nsConn.selectStorage(18)
    assert st[0] == 'localhost'
    assert st[1] == 6001


def testSelectStorage2(nsConn, storage2):
    st = nsConn.selectStorage(18)
    assert st[0] == 'localhost'
    assert st[1] == 6002

def testSelectStorage_FirstWithEnoughFreeSize(nsConn, storage2, storage1):
    st = nsConn.selectStorage(9000)
    assert st[0] == 'localhost'
    assert st[1] == 6001

def testSelectStorage_exceptionIfNoStorages(nsConn):
    try:
        st = nsConn.selectStorage(9000)
        assert False
    except DfsNoFreeStorages as e:
        assert True