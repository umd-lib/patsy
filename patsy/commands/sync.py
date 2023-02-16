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

            logging.info(f"Accessing bags created before {timebefore}, but also after {timeafter}")
            sync_result = sync.process(created_at__lteq=timebefore, created_at__gteq=timeafter)

        elif timebefore:
            logging.info(f"Accessing bags created before {prior_week}")
            sync_result = sync.process(created_at__lteq=timebefore)
        elif timeafter:
            logging.info(f"Accessing bags created after {prior_week}")
            sync_result = sync.process(created_at__gteq=timeafter)
        else:
            prior_week = (datetime.now() - timedelta(days=7)).date()
            logging.info(f"Accessing bags created after {prior_week}")
            sync_result = sync.process(created_at__gteq=prior_week)

        files_processed = sync_result.files_processed
        locations_added = sync_result.locations_added
        files_not_found = sync_result.files_not_found
        files_duplicated = sync_result.files_duplicated
        duplicate_amount = sync_result.duplicate_files
        batches_skipped = sync_result.batches_skipped
        skipped_batches = sync_result.skipped_batches

        # I'll leave it in here because it could be useful to see one day, but as of now
        # it clogs up the log files so it's probably best to not log.
        # for f in files_duplicated:
        #     logging.debug(f"FILE ALREADY IN DATABASE: {f}")

        for f in files_not_found:
            logging.warning(f"FILE NOT FOUND: {f}")

        for b in skipped_batches:
            logging.warning(f"Batch was skipped: {b}")

        logging.info(f"Total files processed: {files_processed}")
        logging.info(f"Total locations added: {locations_added}")
        logging.info(f"Amount of files not found: {len(files_not_found)}")
        logging.info(f"Amount of files already in PATSy: {duplicate_amount}")
        logging.info(f"Amount of batches skipped: {batches_skipped}")
