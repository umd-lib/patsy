#!/user/bin/env python3

import argparse

from . import version
from .accession import AccessionCsvLoader
from .database import create_schema
from .perfect_matches import find_perfect_matches_command
from .altered_md5_matches import find_altered_md5_matches_command
from .filename_only_matches import find_filename_only_matches_command
from .transfer_matches import find_transfer_matches_command
from .database import use_database_file
from .restore import RestoreCsvLoader
from .transfer import TransferCsvLoader
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

    # create the parser for the "load_transfers" command
    transfers_subcommand = subparsers.add_parser(
        'transfers',
        help='Load transfer records'
        )
    transfers_subcommand.add_argument(
        '-s', '--source',
        action='store',
        help='Source of transfers to load'
        )

    # create the parser for the "find_perfect_matches" command
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

    # create the parser for the "find_altered_md5_matches" command
    find_altered_md5_matches_subcommand = subparsers.add_parser(
        'find_altered_md5_matches',
        help='Scans accession and restore records looking for altered MD5 matches'
        )
    find_altered_md5_matches_subcommand.add_argument(
        '-b', '--batch',
        action='store',
        default=None,
        help='Batchname to query'
        )

    # create the parser for the "find_filename_only_matches" command
    find_filename_only_matches_subcommand = subparsers.add_parser(
        'find_filename_only_matches',
        help='Scans accession and restore records looking for filename only matches'
        )
    find_filename_only_matches_subcommand.add_argument(
        '-b', '--batch',
        action='store',
        default=None,
        help='Batchname to query'
        )

    # create the parser for the "find_transfer_matches" command
    find_transfer_matches_subcommand = subparsers.add_parser(
        'find_transfer_matches',
        help='Scans unmatched transfer recoreds looking for matching restores'
        )

    return parser.parse_args()


def main():
    """
    Carry out the main actions as specified in the args.
    """
    args = get_args()
    print_header()

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

    elif args.cmd == 'transfers':
        use_database_file(args.database)
        transfer_loader = TransferCsvLoader()
        result = transfer_loader.load(args.source, PrintProgressNotifier())
        print("-----Transfer Load ----")
        print(result)

    elif args.cmd == 'find_perfect_matches':
        use_database_file(args.database)
        matches_found = find_perfect_matches_command(args.batch, PrintProgressNotifier())
        print("-----Perfect Matches ----")
        for match in matches_found:
            print(match)
        print(f"{len(matches_found)} new matches found")

    elif args.cmd == 'find_altered_md5_matches':
        use_database_file(args.database)
        matches_found = find_altered_md5_matches_command(args.batch, PrintProgressNotifier())
        print("-----Altered MD5 Matches ----")
        for match in matches_found:
            print(match)
        print(f"{len(matches_found)} new matches found")

    elif args.cmd == 'find_filename_only_matches':
        use_database_file(args.database)
        matches_found = find_filename_only_matches_command(args.batch, PrintProgressNotifier())
        print("-----Filename Only Matches ----")
        for match in matches_found:
            print(match)
        print(f"{len(matches_found)} new matches found")

    elif args.cmd == 'find_transfer_matches':
        use_database_file(args.database)
        matches_found = find_transfer_matches_command(PrintProgressNotifier())
        print("-----Transfer Matches ----")
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
