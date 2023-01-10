#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import sys

from importlib import import_module
from patsy import commands, version
from pkgutil import iter_modules
from patsy.core.db_gateway import DbGateway
from patsy.database import DatabaseNotSetError
from sqlalchemy.exc import OperationalError


def print_header(subcommand: str) -> None:
    """Generate script header and display it in the console."""
    title = 'patsy {0}'.format(subcommand)
    sys.stderr.write(
        '\n{0}\n{1}\n'.format(title, '=' * len(title))
    )


def main() -> None:
    """Parse args and set the chosen sub-command as the default function."""

    # main parser for command line arguments
    parser = argparse.ArgumentParser(
        prog='patsy',
        description='Command-line client for preservation asset tracking system (PATSy)'
    )

    parser.add_argument(
        '-v', '--version',
        action='version',
        help='Print version number and exit',
        version=version
    )

    parser.add_argument(
        '-d', '--database',
        default=None,
        action='store',
        help='Path to db file (defaults to None)',
    )

    subparsers = parser.add_subparsers(title='commands')

    # load all defined subcommands from the patsy.commands package
    command_modules = {}
    for _finder, name, _ispkg in iter_modules(commands.__path__):  # type: ignore[attr-defined]
        module = import_module(commands.__name__ + '.' + name)
        if hasattr(module, 'configure_cli'):
            module.configure_cli(subparsers)  # type: ignore[attr-defined]
            command_modules[name] = module

    # parse command line args
    args = parser.parse_args()

    # if no subcommand was selected, display the help
    if not hasattr(args, 'cmd_name'):
        parser.print_help()
        sys.exit(0)

    # get the selected subcommand
    command = command_modules[args.cmd_name].Command()  # type: ignore[attr-defined]

    print_header(args.cmd_name)

    try:
        gateway = DbGateway(args)
        result = command(args, gateway)
        gateway.close()

        if result:
            sys.stderr.write(result)
            sys.stderr.write('\n\n')
    except DatabaseNotSetError:
        sys.stderr.write('The "-d" argument was not set nor was the "PATSY_DATABASE" environment variable.\n')
        # exit with a non-zero code to indicate to the shell that the command failed to run
        sys.exit(1)
    except OperationalError as e:
        error = str(e.orig)
        sys.stderr.write(f'SQLAlchemy OperationalError: {error}\n')
        # exit with a non-zero code to indicate to the shell that the command failed to run
        sys.exit(1)


if __name__ == "__main__":
    main()
