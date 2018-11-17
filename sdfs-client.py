#!/usr/bin/env python3
import argparse
import os
import traceback
import logging
from os import path

from src.sdfsCmd import SdfsCmd
from src.exceptions import *
from src.commands.httpCommands import HttpCommands
from src.commands.rpycCommands import RpycCommands


def parse_args():
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
    parser.add_argument("--loglevel",
                        default="WARNING",
                        help="Set logging level (DEBUG, INFO, WARNING, ERROR)",
                        )
    parser.add_argument("--nameserver",
                        default=None,
                        help="""address of nameserver in format [hostname]:[port]"""
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

    numeric_level = getattr(logging, args.loglevel, None)
    if not isinstance(numeric_level, int):
        print("""ERROR:
ERROR: Invalid logging level: {}
ERROR: """.format(args.loglevel))
        exit(-1)

    logging.basicConfig(
        level=numeric_level,
        filename=args.logfile,
        format='%(asctime)s %(levelname)s %(module)s | %(message)s')

    return args


if __name__ == "__main__":
    args = parse_args()

    logging.debug("Starting client:")
    logging.debug('--local %s', args.local)
    logging.debug('--logfile %s',
                  args.logfile if args.logfile else 'NULL')  # pay respect to C
    logging.debug('--loglevel %s', args.loglevel)
    logging.debug('--nameserver %s', args.nameserver)

    quit = False

    sdfsCmd = None
    try:
        # HttpCommands() will do cd("/") -> stat("/")
        # that will check the connection to the nameserver
        # cmds = HttpCommands(args.nameserver)
        cmds = RpycCommands("localhost", 8081)
        sdfsCmd = SdfsCmd(args.local, cmds)
    except:
        print("Faild to connect to {}".format(args.nameserver))
        exit(-1)

    while not quit:
        try:
            sdfsCmd.cmdloop()
            quit = True
        except KeyboardInterrupt:
            print("Use C^D to exiting")
        except DfsException as e:
            print(e)
        except Exception as e:
            print("[ERROR] ", type(e), e)
            logging.exception("[ERROR]")
