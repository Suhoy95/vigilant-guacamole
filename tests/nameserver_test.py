import os.path as path

from src.utils import Dir
import src.proto as proto


def testDuWithoutStorages(nsConn):
    nodes = nsConn.du()
    assert isinstance(nodes, list)
    assert len(nodes) == 0


def testDuWithOneStorages(nsConn, storage1):
    nodes = nsConn.du()
    assert isinstance(nodes, list)
    assert len(nodes) == 1
    assert nodes[0]['name'] == "('localhost', 6001)"
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
