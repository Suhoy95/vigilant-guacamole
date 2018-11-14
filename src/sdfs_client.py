import logging
import cmd

import os
from os import path

import src.proto as proto
import src.commands as commands

logger = logging.getLogger("sdfs-client")


class SdfsCmd(cmd.Cmd):
    prompt = "sdfs> "

    def __init__(self, storage, commands: commands.Commands):
        cmd.Cmd.__init__(self)
        os.chdir(storage)
        self._cwd = "/"
        self.cmd = commands

    def do_du(self, du):
        """
        syntax: du

        prints the common information about DFS:
            - list of nodes
            - capacity and avaliable space per each node
        """
        print("{:>20}\t{}\t{}\t".format("NODE", "FREE", "CAPACITY"))
        for node in self.cmd.du():
            fpath = path.join(line, f)
            ftype = "D" if os.path.isdir(fpath) else "F"
            print("{:4} {: >30}\t{}".format(
                ftype, f, os.path.getsize(fpath)))

        pass

    def do_get(self, line):
        """
        syntax: get SRC_DFS_PATH DST_LOCAL_PATH

        Download file SRC_DFS_PATH from nodes to the local storage as DST_LOCAL_PATH
        """
        # check characters, split into 2 parts
        # relative paths -> absolute paths
        # check that dir, which should contain DST_LOCAL_PATH exists
        # ask about overwrite if DST_LOCAL_PATH exists
        # check that file SRC_DFS_PATH exists
        # [advanced] if SRC_DFS_PATH is dir: ask about loading dir recursevly
        # GET /storage?path=DST_LOCAL_PATH -> operation
        # GET node/file?op=operation
        # write respanse to SRC_DFS_PATH
        pass

    def do_put(self, line):
        """
        syntax: put SRC_LOCAL_PATH DST_DFS_PATH

        Upload file SRC_LOCAL_PATH from local storage to DST_DFS_PATH
        """
        # check characters, split into 2 parts
        # relative paths -> absolute paths
        # check that SRC_LOCAL_PATH file exists
        # check that SRC_LOCAL_PATH is readable
        # get filesize of SRC_LOCAL_PATH
        # check the DST_DFS_PATH exists, ask about overwrite
        #   GET /properties?path=DST_DFS_PATH
        #     -> can be dir is not exist
        # open DST_DFS_PATH for writing
        #   POST /storage {path, filesize} -> operations
        # read file SRC_LOCAL_PATH
        # POST node/write?operations&filedata -> ok
        parts = line.split()
        if len(parts) != 2:
            print(
                "Wrong amount of arguments. Expect: 2, actual: {0}", len(parts))
            return

        file = parts[0]
        path = os.path.join(os.getcwd(), file)

        if not os.path.exists(path):
            print("File {0} does not exist".format(file))
            return

        if not os.path.isfile(path):
            print("{0} is not regular file".format(file))
            return

        tgt = parts[1] if parts[1].startswith('/') \
            else os.path.join(self._cwd, parts[1])

        print("coping from {0} to {1}".format(path, tgt))

    def do_pwd(self, line):
        """
        syntax: pwd

        Print Current Working Directory (CWD) and Storage path
        """
        print("CWD: %s\nSTORAGE: %s" % (self._cwd, os.getcwd()))

    def do_ls(self, line):
        if line == "":
            line = "/"

        if not path.isabs(line):
            line = path.normpath(path.join(self._cwd, line))

        f = self.cmd.stat(line)
        if f['type'] != proto.DIRECTORY:
            raise ValueError("'%s' is not a directory".format(line))

        print("{:4} {: >30}\t{}".format("TYPE", "FILENAME", "FILESIZE"))
        for f in self.cmd.ls(line):
            ftype = "D" if f['type'] == proto.DIRECTORY else "F"
            print("{:4} {: >30}\t{}".format(
                ftype, f['path'], f['size']))
        pass

    def do_cd(self, line):
        if line == "":
            self._cwd = "/"

        if not path.isabs(line):
            line = path.normpath(path.join(self._cwd, line))
        pass

    # working with files
    def do_mv(self, line):
        pass

    def do_cp(self, line):
        pass

    def do_rm(self, line):
        pass

# uploading / downloading
    def do_storage(self, line):
        """
        syntax: storage [LOCAL_DIR]

        List content of LOCAL_DIR
        """
        line = "." if line == "" else line

        if not path.isdir(line):
            raise ValueError("%s is not a local directory" % (line, ))

        print("{:4} {: >30}\t{}".format("TYPE", "FILENAME", "FILESIZE"))
        for f in os.listdir(line):
            fpath = path.join(line, f)
            ftype = "D" if os.path.isdir(fpath) else "F"
            print("{:4} {: >30}\t{}".format(
                ftype, f, os.path.getsize(fpath)))

    def do_quit(self, _):
        print("quiting...")
        return True

    do_EOF = do_quit


def parse_args():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--storage",
                        default=os.getcwd(),
                        help="""
                        local directory for exchanging file with DFS.
                        Current Working Directory by default.
                        """,
                        )
    parser.add_argument("--logfile",
                        default=None,
                        help="""Path to log file""",)

    args = parser.parse_args()

    if path.isdir(args.storage):
        args.storage = path.abspath(args.storage)
    else:
        print("""ERROR:
ERROR: storage is not a local directory
ERROR:""")
        parser.print_help()
        exit(-1)

    return args


def setup_logger(log_file):
    if log_file:
        fh = logging.FileHandler(log_file)
        fh.setLevel(logging.INFO)
        logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setLevel(getattr(logging, os.getenv('LOG_LEVEL', 'ERROR')))
    logger.addHandler(ch)


if __name__ == "__main__":
    args = parse_args()
    setup_logger(args.logfile)

    print("""Welcome to DFS interactive client!

        use "help" to print avaliable commands
        use "help command" to print description of command

        COMMON NOTES:
            all filenames can contains only letters [a-zA-Z], digits [0-9],
            hyphen [-], underscore [_] or dot [.] with maximum length 255

            *_DFS_PATH - path on the dfs system,
                absolute path starts with '/'
                relative path is resolved relative to Current Working Directory
                    (print with "pwd" command)
            *_LOCAL_PATH - path on the current machine,
                absolute path starts with '/'
                relative path is resolved relative to --storage parameter
        """)
    quit = False
    sdfsCmd = SdfsCmd(args.storage, commands.HttpCommands('localhost:8080'))
    while not quit:
        try:
            sdfsCmd.cmdloop()
            quit = True
        except KeyboardInterrupt:
            print("Use 'quit' or C^D to exiting")
        except Exception as e:
            print("[ERROR] ", e)
