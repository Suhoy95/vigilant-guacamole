import logging
import cmd

import os
from os import path

import src.proto as proto
import src.commands as commands
from src.exceptions import *


class SdfsCmd(cmd.Cmd):
    # prompt is changing dynamically by self.cd()
    prompt = "sdfs> "

    def __init__(self, local, cmds: commands.Commands):
        cmd.Cmd.__init__(self)
        self.cmds = cmds
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
           cd - change directory inside DFS tree
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
        print("{:>40}\t{}\t{}\t".format("NODE_ID", "FREE", "CAPACITY"))
        for node in self.cmds.du():
            print("{:>40}\t{}\t{}".format(
                node['name'],
                node['free'],
                node['capacity']
            ))

    def do_get(self, line):
        """
        syntax: get SRC_DFS_PATH DST_LOCAL_PATH

        Download file SRC_DFS_PATH from nodes to the local DST_LOCAL_PATH
        """
        # [advanced] TODO: if SRC_DFS_PATH is dir: ask about loading dir recursevly
        parts = line.split()
        if len(parts) != 2:
            raise DfsException(
                "Wrong amount of arguments. Expect: 2, actual: {0}".format(len(parts)))

        src_dfs_path = self._to_dfs_abs_path(parts[0])

        file = parts[1]
        dst_local_path = self._to_local_abs_path(file)

        if path.exists(dst_local_path):
            answer = input(
                "local file '%s' exists. Try to Overwrite [y/N]? " % (dst_local_path,))
            if answer.lower() != 'y':
                return

        self.cmds.get(src_dfs_path, dst_local_path)

    def do_put(self, line):
        """
        syntax: put SRC_LOCAL_PATH DST_DFS_PATH

        Upload local SRC_LOCAL_PATH file to DST_DFS_PATH
        """
        # [advanced] TODO: if SRC_LOCAL_PATH is dir: ask about loading dir recursevly
        parts = line.split()
        if len(parts) != 2:
            raise DfsException(
                "Wrong amount of arguments. Expect: 2, actual: {0}".format(len(parts)))

        file = parts[0]
        src_local_path = self._to_local_abs_path(file)

        if not os.path.exists(src_local_path):
            raise DfsException("File {0} does not exist".format(file))

        if not os.path.isfile(src_local_path):
            raise DfsException("{0} is not regular file".format(file))

        dst_dfs_path = self._to_dfs_abs_path(parts[1])

        self.cmds.put(src_local_path, dst_dfs_path)

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
        line = self._to_dfs_abs_path(line, isdir=True)

        print("{:4} {: >30}\t{}\t{}".format(
            "TYPE", "FILENAME", "FILESIZE", "NODES"))
        for f in self.cmds.ls(line):
            ftype = "D" if f['type'] == proto.DIRECTORY else "F"
            print("{:4} {: >30}\t{}\t{}".format(
                ftype, path.basename(f['path']), f['size'], f['node'])
            )
        pass

    def do_cd(self, line):
        """
        syntax: cd [DFS_DIR_PATH]

        Change current working DFS directory to DFS_DIR_PATH.
        if DFS_DIR_PATH is not specified, current working directory will set to "/"
        """
        if line == '':
            line = '/'

        line = self._to_dfs_abs_path(line, isdir=True)

        f = self.cmds.stat(line)
        if f['type'] != proto.DIRECTORY:
            raise DfsException("'{0}' is not a directory".format(line))

        self._cwd = line
        self.prompt = "sdfs:{0}> ".format(line)

    def do_rm(self, line):
        """
        syntax: rm DFS_FILE_PATH

        Remove file, which is blased according DFS_FILE_PATH
        """
        line = self._to_dfs_abs_path(line)
        self.cmds.rm(line)

    def do_local(self, line):
        """
        syntax: local [LOCAL_DIR]

        List content of LOCAL_DIR
        """
        line = self._to_local_abs_path(line)

        if not path.isdir(line):
            raise DfsException("'{}' is not a local directory".format(line))

        print("{:4} {: >30}\t{}".format("TYPE", "FILENAME", "FILESIZE"))
        for f in os.listdir(line):
            fpath = path.join(line, f)
            ftype = "D" if os.path.isdir(fpath) else "F"
            print("{:4} {: >30}\t{}".format(
                ftype, path.basename(f), os.path.getsize(fpath)))

    def do_mkdir(self, line):
        """
        syntax: mkdir DFS_DIR_PATH

        Create DFS directory on DFS_DIR_PATH
        """
        line = self._to_dfs_abs_path(line, isdir=True)
        self.cmds.mkdir(line)

    def do_rmdir(self, line):
        """
        syntax: rmdir DFS_DIR_PATH

        Remove directory DFS_DIR_PATH from DFS
        """
        line = self._to_dfs_abs_path(line, isdir=True)
        if len(self.cmds.ls(line)) > 0:
            confirm = input(
                "'{}' is not empty. Remove recursively [y/N]?".format(line))
            if confirm.lower() != 'y':
                return

        self.cmds.rm(line, recursive=True)

    def do_EOF(self, _):
        """
        syntax: EOF

        Exit from session
        """

        print("quiting...")
        return True

    def _to_dfs_abs_path(self, line, isdir=False):
        if not path.isabs(line):
            line = path.join(self._cwd, line)

        line = path.normpath(line)

        return line

    def _to_local_abs_path(self, line):
        if not path.isabs(line):
            line = path.join(os.getcwd(), line)

        line = path.normpath(line)

        return line
