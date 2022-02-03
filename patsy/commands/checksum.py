import argparse
import csv
import sys
from typing import Dict, Iterable, Mapping, Optional, Tuple

import patsy.core.command
from patsy.core.db_gateway import DbGateway


def configure_cli(subparsers) -> None:  # type: ignore
    """
    Configures the CLI arguments for this command
    """
    parser = subparsers.add_parser(
        name='checksum',
        description='Look up accession checksum by storage location'
    )
    parser.add_argument(
        '-f', '--file',
        type=argparse.FileType(),
        dest='locations_file',
        help='CSV file containing locations to look up'
    )
    parser.add_argument(
        '-o', '--output-file',
        type=argparse.FileType(mode='w'),
        default=sys.stdout,
        help='file to write checksums to; defaults to STDOUT'
    )
    checksum_type_parser = parser.add_mutually_exclusive_group()
    checksum_type_parser.add_argument(
        '--md5',
        dest='output_type',
        action='store_const',
        const='md5',
        help='retrieve MD5 checksum'
    )
    checksum_type_parser.add_argument(
        '--sha1',
        dest='output_type',
        action='store_const',
        const='sha1',
        help='retrieve SHA1 checksum'
    )
    checksum_type_parser.add_argument(
        '--sha256',
        dest='output_type',
        action='store_const',
        const='sha256',
        help='retrieve SHA256 checksum'
    )
    parser.add_argument(
        'location',
        nargs='*',
        default=[]
    )
    parser.set_defaults(cmd_name='checksum')


def get_checksum(gateway: DbGateway, row: Mapping[str, str], checksum_type: str) -> Optional[Tuple[str, str]]:
    location = row['location']
    accession = gateway.get_accession_by_location(location)
    # default to using the location in the output if there is no separate destination value
    destination = row.get('destination', location)
    if accession is not None:
        if checksum_type == 'md5' and accession.md5:
            return accession.md5, destination
        elif checksum_type == 'sha1' and accession.sha1:
            return accession.sha1, destination
        elif checksum_type == 'sha256' and accession.sha256:
            return accession.sha256, destination
        else:
            sys.stderr.write(f'No {checksum_type.upper()} checksum found for "{row["location"]}"\n')
    else:
        sys.stderr.write(f'No accession record found for "{row["location"]}"\n')
    return None


class Command(patsy.core.command.Command):
    def __call__(self, args: argparse.Namespace, gateway: DbGateway) -> str:
        if args.output_type is None:
            args.output_type = 'md5'

        if getattr(args, 'locations_file', None) is not None:
            locations: Iterable[Dict[str, str]] = csv.DictReader(args.locations_file)
        else:
            locations = [{'location': location} for location in args.location]
        for row in locations:
            checksum_and_path = get_checksum(gateway=gateway, row=row, checksum_type=args.output_type)
            if checksum_and_path:
                print('  '.join(checksum_and_path), file=args.output_file)

        return ''
