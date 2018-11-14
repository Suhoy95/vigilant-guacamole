
class Commands:
    def stat(self, dfs_path):
        raise NotImplementedError()

    def du(self):
        raise NotImplementedError()

    def get(self, dfs_path, local_path, recursive=False):
        raise NotImplementedError()

    def put(self, local_path, dfs_path, recursive=False):
        raise NotImplementedError()

    def rm(self, dfs_path, recursive=False):
        raise NotImplementedError()

    def ls(self, dfs_dir):
        raise NotImplementedError()

    def mkdir(self, fds_dir):
        raise NotImplementedError()
