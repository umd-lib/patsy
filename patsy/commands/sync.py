import os
import sys
import argparse
import patsy.core.command

from datetime import datetime, timedelta
from patsy.model import Accession
from patsy.core.db_gateway import DbGateway
from patsy.core.sync import Sync, MissingHeadersError, InvalidTimeError


def configure_cli(subparsers) -> None:  # type: ignore
    """
    Configures the CLI arguments for this command
    """
    parser = subparsers.add_parser(
        name='sync',
        description='Sync files from ApTrust with accessions in PATSy and add locations if not in PATSy'
    )
    parser.set_defaults(cmd_name='sync')

    parser.add_argument(
        '-n', '--name',
        action='store',
        default=None,
        help='The header name for the APTrust API'
    )

    parser.add_argument(
        '-k', '--key',
        action='store',
        default=None,
        help='The header key for the ApTrust API'
    )

    parser.add_argument(
        '-tb', '--timebefore',
        action='store',
        default=None,
        help='Checks for bags created before the given timestamp.'
    )

    parser.add_argument(
        '-ta', '--timeafter',
        action='store',
        default=None,
        help='Checks for bags created after the given timestamp.'
    )


class Command(patsy.core.command.Command):
    def __call__(self, args: argparse.Namespace, gateway: DbGateway) -> str:
        x_pharos_name = args.name
        x_pharos_key = args.key
        timebefore = args.timebefore
        timeafter = args.timeafter

        if x_pharos_name is None or x_pharos_key is None:
            x_pharos_name = os.getenv('X_PHAROS_NAME')
            x_pharos_key = os.getenv('X_PHAROS_KEY')

            if x_pharos_name is None or x_pharos_key is None:
                raise MissingHeadersError

        sys.stderr.write(
            f'Running sync command with the following options:\n\n'
            f'  - Header name: {x_pharos_name}\n'
            f'  - Bags created before the given timestamp: {timebefore}\n'
            f'  - Bags created after the given timestamp: {timeafter}\n'
            '======\n'
        )

        headers = {
            'X-Pharos-API-User': x_pharos_name,
            'X-Pharos-API-Key': x_pharos_key
        }

        sync = Sync(gateway=gateway, headers=headers)

        if timebefore and timeafter:
            tb = datetime.strptime(timebefore, '%Y-%m-%d').date()
            ta = datetime.strptime(timeafter, '%Y-%m-%d').date()

            if ta >= tb:
                raise InvalidTimeError

            sync_result = sync.process(created_at__lteq=timebefore, created_at__gteq=timeafter)

        elif timebefore:
            sync_result = sync.process(created_at__lteq=timebefore)
        elif timeafter:
            sync_result = sync.process(created_at__gteq=timeafter)
        else:
            prior_week = (datetime.now() - timedelta(days=7)).date()
            sync_result = sync.process(created_at__gteq=prior_week)

        splt_files_not_found = '\n'.join(sync_result.files_not_found)
        result_messages = [
            f"Total files processed: {sync_result.files_processed}",
            f"Total locations added: {sync_result.locations_added}",
            f"Total duplicate files found: {sync_result.duplicate_files}",
            f"FILES NOT FOUND:\n{splt_files_not_found}"
        ]

        result = '\n'.join(result_messages)

        return result
