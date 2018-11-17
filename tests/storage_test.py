import os
import os.path as path


def test_open(st1Conn, storage1dir):
    f = st1Conn.open("/hello", "w")
    f.write("Hello, world!")
    f.close()

    with open(path.join(storage1dir, 'hello'), "r")as f:
        assert f.read() == "Hello, world!"


def test_mkdir(st1Conn, storage1dir):
    st1Conn.mkdir("/dir")
    assert path.isdir(path.join(storage1dir, 'dir'))


def test_rmdir(st1Conn, storage1dir):
    os.mkdir(path.join(storage1dir, 'dir'))
    st1Conn.rmdir('/dir')
    assert not path.isdir(path.join(storage1dir, 'dir'))


def test_rm(st1Conn, storage1dir):
    filename = path.join(storage1dir, 'hello')
    with open(filename, "w") as f:
        f.write("world")

    assert path.isfile(filename)

    st1Conn.rm("/hello")
    assert not path.isfile(filename)


def test_write(st1Conn, storage1dir, helloFile):
    st1Conn.write("/hello", open(helloFile, "br"))

    with open(helloFile, "r") as a, open(path.join(storage1dir, 'hello'), "r") as b:
        assert a.read() == b.read()
