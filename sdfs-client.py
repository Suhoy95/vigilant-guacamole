#!/usr/bin/env python3

import os
import traceback
from os import path


from src.sdfsCmd import SdfsCmd
from src.commands.httpCommands import HttpCommands
from src.commands.memoryCommands import MemoryCommands


def parse_args():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--local",
                        default=os.getcwd(),
                        help="""
                        local directory for exchanging file with DFS.
                        Current Working Directory by default.
                        """,
                        )
    parser.add_argument("--logfile",
                        default=None,
                        help="""Path to log file""",
                        )

    args = parser.parse_args()

    if path.isdir(args.local):
        args.local = path.abspath(args.local)
    else:
        print("""ERROR:
ERROR: --local is not a local directory
ERROR:""")
        parser.print_help()
        exit(-1)

    return args


def setup_logger(log_file):
    # if log_file:
    #     fh = logging.FileHandler(log_file)
    #     fh.setLevel(logging.INFO)
    #     logger.addHandler(fh)

    # ch = logging.StreamHandler()
    # ch.setLevel(getattr(logging, os.getenv('LOG_LEVEL', 'ERROR')))
    # logger.addHandler(ch)
    pass


if __name__ == "__main__":
    args = parse_args()
    setup_logger(args.logfile)

    print()
    quit = False
    cmds = HttpCommands('localhost:8080')
    # cmds = MemoryCommands()
    sdfsCmd = SdfsCmd(args.local, cmds)
    while not quit:
        try:
            sdfsCmd.cmdloop()
            quit = True
        except KeyboardInterrupt:
            print("Use C^D to exiting")
        except NotImplementedError as e:
            traceback.print_tb(e.__traceback__)
        except Exception as e:
            traceback.print_tb(e.__traceback__)
            print("[ERROR] ", type(e), e)
