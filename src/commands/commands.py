
class Commands:
    def stat(self, dfs_path):
        raise NotImplementedError()

    def du(self):
        """
        return [
            {
                'name': str,
                'free': int,
                'capacity': int,
            }, ...
        ]
        """
        raise NotImplementedError()

    def get(self, src_dfs_path, dst_local_path, recursive=False):
        raise NotImplementedError()

    def put(self, local_path, dfs_path, recursive=False):
        raise NotImplementedError()

    def rm(self, dfs_path, recursive=False):
        raise NotImplementedError()

    def ls(self, dfs_dir):
        """
        return [
            {
                'type': str,
                'path': str,
                'size': int,
            }, ...
        ]
        """
        raise NotImplementedError()

    def mkdir(self, fds_dir):
        raise NotImplementedError()
