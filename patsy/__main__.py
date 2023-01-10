#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import sys

from importlib import import_module
from patsy import commands, version
from pkgutil import iter_modules
from patsy.core.db_gateway import DbGateway
from patsy.core.sync import MissingHeadersError, InvalidStatusCodeError, InvalidTimeError
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
        sys.exit(1)
    except OperationalError:
        sys.stderr.write('The URL did not work. Is the URL correct? Are you connected to the VPN?\n')
        sys.exit(1)
    except InvalidStatusCodeError:
        sys.stderr.write(
            'An error occured when using the API. This could be due to the servers, '
            'or the headers provided may be incorrect.\n'
        )
        sys.exit(1)
    except MissingHeadersError:
        sys.stderr.write('The headers to access the ApTrust API were not set. \
                          Provide them as an argument to the sync command or as environment variables in the shell.\n')
        sys.exit(1)
    except InvalidTimeError:
        sys.stderr.write('Both time arguments were provided. Only provide one of them.\n')
        sys.exit(1)


if __name__ == "__main__":
    main()
