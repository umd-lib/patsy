import patsy.core.command
import argparse
import logging
import sys
import os

from patsy.core.sync import Sync, MissingHeadersError, InvalidTimeError
from patsy.core.db_gateway import DbGateway
from datetime import datetime, timedelta
from patsy.model import Accession


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
    def __call__(self, args: argparse.Namespace, gateway: DbGateway) -> None:
        x_pharos_name = args.name
        x_pharos_key = args.key
        timebefore = args.timebefore
        timeafter = args.timeafter

        if x_pharos_name is None or x_pharos_key is None:
            x_pharos_name = os.getenv('X_PHAROS_NAME')
            x_pharos_key = os.getenv('X_PHAROS_KEY')

            if x_pharos_name is None or x_pharos_key is None:
                raise MissingHeadersError

        inputs = {"tb": timebefore, "ta": timeafter}
        logging.info(f"Running sync command with the following options: {inputs}")

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

        files_processed = sync_result.files_processed
        locations_added = sync_result.locations_added
        files_not_found = sync_result.files_not_found
        files_duplicated = sync_result.files_duplicated
        duplicate_amount = sync_result.duplicate_files
        batches_skipped = sync_result.batches_skipped
        skipped_batches = sync_result.skipped_batches

        if len(files_not_found) > 0:
            logging.warning(f"AMOUNT OF FILES NOT FOUND: {len(files_not_found)}")
            for f in files_not_found:
                logging.warning(f"FILE NOT FOUND: {f}")

        if duplicate_amount > 0:
            logging.warning(f"AMOUNT OF DUPLICATE FILES FOUND: {duplicate_amount}")
            for f in files_duplicated:
                logging.warning(f"FILE ALREADY IN DATABASE: {f}")

        if batches_skipped > 0:
            logging.warning(f"AMOUNT OF BATCHES SKIPPED: {batches_skipped}")
            for b in skipped_batches:
                logging.warning(f"BATCH SKIPPED: {b}")

        logging.info(f"Total files processed: {files_processed}")
        logging.info(f"Total locations added: {locations_added}")
