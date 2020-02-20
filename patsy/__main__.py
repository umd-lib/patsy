#!/user/bin/env python3

import argparse

from . import version
from .accession import AccessionCsvLoader
from .database import create_schema
from .perfect_matches import find_perfect_matches_command
from .database import use_database_file
from .restore import RestoreCsvLoader
from .utils import print_header
from .progress_notifier import PrintProgressNotifier


def get_args():
    """
    Create parsers and return a namespace object
    """
    parser = argparse.ArgumentParser(
        description='CLI for PATSy database'
    )
    parser.add_argument(
        '-v', '--version',
        action='version',
        help='Print version number and exit',
        version=version
    )
    parser.add_argument(
         '-d', '--database',
         default=':memory:',
         action='store',
         help='Path to db file (defaults to in-memory db)',
    )

    # Subcommand interface
    subparsers = parser.add_subparsers(
        dest='cmd',
        help='sub-command help'
        )

    # create the parser for the "schema" command
    schema_subcommand = subparsers.add_parser(
        'schema', 
        help='Create schema from the declarative base'
        )

    # create the parser for the "load_accessions" command
    accessions_subcommand = subparsers.add_parser(
        'accessions', 
        help='Load accession records'
        )    
    accessions_subcommand.add_argument(
        '-s', '--source', 
        action='store',
        help='Source of accessions to load'
        )

    # create the parser for the "load_restores" command
    restores_subcommand = subparsers.add_parser(
        'restores', 
        help='Load restored files table'
        )    
    restores_subcommand.add_argument(
        '-s', '--source', 
        action='store',
        help='Source of restores to load'
        )

    # create the parser for the "load_accessions" command
    find_perfect_matches_subcommand = subparsers.add_parser(
        'find_perfect_matches',
        help='Scans accession and restore records looking for perfect matches'
        )
    find_perfect_matches_subcommand.add_argument(
        '-b', '--batch',
        action='store',
        default=None,
        help='Batchname to query'
        )

    return parser.parse_args()


def main():
    """
    Carry out the main actions as specified in the args.
    """
    args = get_args()
    print_header()

    #print(args)

    if args.cmd == 'schema':
        create_schema(args)
    elif args.cmd == 'accessions':
        use_database_file(args.database)
        accession_loader = AccessionCsvLoader()
        result = accession_loader.load(args.source, PrintProgressNotifier())
        print("----- Accession Load ----")
        print(result)

    elif args.cmd == 'restores':
        use_database_file(args.database)
        restore_loader = RestoreCsvLoader()
        result = restore_loader.load(args.source, PrintProgressNotifier())
        print("-----Restore Load ----")
        print(result)

    elif args.cmd == 'find_perfect_matches':
        use_database_file(args.database)
        matches_found = find_perfect_matches_command(args.batch, PrintProgressNotifier())
        print("-----Perfect Matches ----")
        for match in matches_found:
            print(match)
        print(f"{len(matches_found)} new matches found")

    print(f"Actions complete!")
    if args.database == ':memory:':
        print(f"Cannot query transient DB. Use -d to specify a database file.")
    else:
        print(f"Query the bootstrapped database at {args.database}.")


if __name__ == "__main__":
    main()
