import logging
import cmd

import os
from os import path

import src.proto as proto
import src.commands as commands

logger = logging.getLogger("sdfs-client")


class SdfsCmd(cmd.Cmd):
    prompt = "sdfs> "

    def __init__(self, local, cmds: commands.Commands):
        cmd.Cmd.__init__(self)
        self.cmd = cmds
        os.chdir(local)
        self.do_cd("/")
        self.do_usage("")

    def do_usage(self, line):
        """
        syntax: dfs

        print common recomendation how to use DFS
        """
        print("""
    GET HELP:
        help - print avaliable commands
        help command - print description of command
        usage - this help

    COMMANDS:
           du - print info about nodes, its capacity and free space
           ls - print content of DFS directory
        local - print content of local directory
           cd - change cirectory inside DFS tree
          pwd - print current DWS working directory and local path

        mkdir - create DFS dir
        rmdir - remove DFS dir

          get - download file to the local directory
          put - upload file to the DFS
           rm - remove file

          EOF - quit from client

    DFS NOTES:
        all filenames should contains only letters [a-zA-Z], digits [0-9],
        hyphen [-], underscore [_] or dot [.] with maximum length 255

        *_DFS_PATH - path on the dfs system,
            absolute path starts with '/'
            relative path is resolved relative to Current Working Directory
                (print with "pwd" command)
        *_LOCAL_PATH - path on the current machine,
            absolute path starts with '/'
            relative path is resolved relative to --local parameter
        """)

    def do_du(self, line):
        """
        syntax: du

        print the common information about DFS:
            - list of nodes
            - capacity and avaliable space per each node
        """
        raise NotImplementedError()
        # print("{:>20}\t{}\t{}\t".format("NODE", "FREE", "CAPACITY"))
        # for node in self.cmd.du():
        #     print("{:>20} {}\t{}".format(
        #         node['hostname'],
        #         node['free'],
        #         node['capacity']
        #     ))

    def do_get(self, line):
        """
        syntax: get SRC_DFS_PATH DST_LOCAL_PATH

        Download file SRC_DFS_PATH from nodes to the local DST_LOCAL_PATH
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

        Upload local SRC_LOCAL_PATH file to DST_DFS_PATH
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

        Print Current Working Directory (CWD) and local path
        """
        print("CWD: %s\nLOCAL: %s" % (self._cwd, os.getcwd()))

    def do_ls(self, line):
        """
        syntax: ls [DFS_DIR_PATH]

        list files of DFS_DIR_PATH.
        if DFS_DIR_PATH is not specified, current working DFS directory will be used
        """
        if line == "":
            line = self._cwd

        if not path.isabs(line):
            line = path.normpath(path.join(self._cwd, line))

        print("{:4} {: >30}\t{}".format("TYPE", "FILENAME", "FILESIZE"))
        for f in self.cmd.ls(line):
            ftype = "D" if f['type'] == proto.DIRECTORY else "F"
            print("{:4} {: >30}\t{}".format(
                ftype, path.basename(f['path']), f['size']))
        pass

    def do_cd(self, line):
        """
        syntax: cd [DFS_DIR_PATH]

        Change current working DFS directory to DFS_DIR_PATH.
        if DFS_DIR_PATH is not specified, current working directory will set to "/"
        """
        if line == "":
            self._cwd = "/"

        if not path.isabs(line):
            line = path.normpath(path.join(self._cwd, line))

        f = self.cmd.stat(line)
        if f['type'] != proto.DIRECTORY:
            raise ValueError("'{0}' is not a directory".format(line))

        self._cwd = line
        self.prompt = "sdfs:{0}> ".format(line)
        pass

    def do_rm(self, line):
        """
        syntax: rm DFS_FILE_PATH

        Remove file, which is blased according DFS_FILE_PATH
        """
        pass

# uploading / downloading
    def do_local(self, line):
        """
        syntax: local [LOCAL_DIR]

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

    def do_mkdir(self, line):
        """
        syntax: mkdir DFS_DIR_PATH

        Create DFS directory on DFS_DIR_PATH
        """
        pass

    def do_rmdir(self, line):
        """
        syntax: rmdir DFS_DIR_PATH

        Remove directory DFS_DIR_PATH from DFS
        """
        pass

    def do_EOF(self, _):
        """
        syntax: EOF

        Exit from session
        """

        print("quiting...")
        return True
