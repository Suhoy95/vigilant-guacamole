from src.cmd_parsers import *


class TestGetArgs:
    def test_one(self):
        "Retrun tuple with local_path and dfs_path"
        a = get_args("qwe qwe")
        assert isinstance(a, tuple)
        assert len(a) == 2

    