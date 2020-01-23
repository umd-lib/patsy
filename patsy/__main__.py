#!/user/bin/env python3

import argparse
from . import version
from .crud import create
from .utils import print_header

def main():
    # main parser for command line arguments
    parser = argparse.ArgumentParser(description='CLI for PATSy database')
    subparsers = parser.add_subparsers(
        title='subcommands',
        description='valid subcommands',
        help='-h additional help',
        metavar='{create}',
        dest='cmd'
    )
    parser.add_argument(
        '-v', '--version',
        action='version',
        help='Print version number and exit',
        version=version
    )
    subparsers.required = True

    # argument parser for the deposit sub-command
    create_parser = subparsers.add_parser(
        'create',
        help='Add records',
        description='Add records by table'
    )
    create_parser.add_argument(
        '-t', '--type',
        action='store',
        required=True,
        help=''
    )
    create_parser.add_argument(
        '-s', '--source',
        action='store',
        help='Source file to read from'
    )

    create_parser.set_defaults(func=create)

    # parse the args and call the default sub-command function
    args = parser.parse_args()
    print_header()
    args.func(args)

if __name__ == "__main__":
    main()
