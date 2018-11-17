import pytest

import src.proto as proto
from src.commands.httpCommands import HttpCommands
from src.exceptions import *


cmds = HttpCommands("localhost:8080")


@pytest.mark.skip
@pytest.mark.usefixtures("init_db")
class Test_httpCommands_stat:
    def test_Root(self):
        stats = cmds.stat('/')

        assert isinstance(stats, dict)
        assert stats == {
            'type': proto.DIRECTORY,
            'path': '/',
            'size': 0
        }

    def test_File(self):
        stats = cmds.stat('/test/test.txt')

        assert stats == {
            'type': proto.FILE,
            'path': '/test/test.txt',
            'size': 200
        }

    def test_FileNotExist(self):
        with pytest.raises(DfsHttpException):
            stats = cmds.stat('/qwe/qwe/qwe/qwe/qwe/qwe/qwe')


class Test_httpCommands_du:
    pass


class Test_httpCommands_get:
    pass


class Test_httpCommands_put:
    pass


@pytest.mark.skip
class Test_httpCommands_rm:
    def test_rmdir(self):
        try:
            cmds.mkdir('/test_delete/')
        except:
            pass

        cmds.rm('/test_delete/', recursive=True)

        with pytest.raises(DfsHttpException):
            cmds.stat('/test_delete/')


@pytest.mark.skip
class Test_httpCommands_ls:
    def test_RootDir(self):
        files = cmds.ls("/")

        assert files == [{
            'type': proto.DIRECTORY,
            'path': '/qwe/',
            'size': 0
        }, {
            'type': proto.DIRECTORY,
            'path': '/test/',
            'size': 0
        }]

    def test_EmptyDir(self):
        files = cmds.ls("/qwe/")
        assert files == []

    def test_nonExistedDir(self):
        with pytest.raises(DfsHttpException):
            cmds.ls("/qwe/qwe/")


@pytest.mark.skip
class Test_httpCommands_mkdir:
    def test_mkdir(self):
        try:
            cmds.rm('/qwe/', recursive=True)
        except:
            pass

        operation = cmds.mkdir('/qwe/')

        assert operation == {
            '_id': None,
            'digest': None,
            'nodeAddress': None,
            'path': '/qwe/',
            'operation': 'WRITE',
            'timestamp': operation['timestamp']
        }

        d = cmds.stat('/qwe/')
        assert d == {
            'type': proto.DIRECTORY,
            'size': 0,
            'path': '/qwe/'
        }

    @pytest.mark.skip()
    def test_mkdir_over_file(self):
        try:
            cmds.rm('/test/test.txt/', recursive=True)
        except:
            pass
        with pytest.raises(DfsHttpException):
            cmds.mkdir('/test/test.txt/')
