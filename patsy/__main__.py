#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import sys

from . import version


def print_header():
    """Generate script header and display it in the console."""
    title = f'| PATSy |'
    border = '=' * len(title)
    spacer = '|' + ' '*(len(title)-2) + '|'
    sys.stdout.write(
        '\n'.join(['', border, spacer, title, spacer, border, '', ''])
    )


def main():
    """Parse args and set the chosen sub-command as the default function."""

    # main parser for command line arguments
    parser = argparse.ArgumentParser(description='Command-line client for preservation asset tracking system (PATSy)')

    parser.add_argument(
        '-v', '--version',
        action='version',
        help='Print version number and exit',
        version=version
    )

    # parse the args and call the default sub-command function
    args = parser.parse_args()
    print_header()
    result = args.func(args)
    if result:
        sys.stderr.write(result)
        sys.stderr.write('\n\n')


if __name__ == "__main__":
    main()
