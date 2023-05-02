import argparse
import logging
import patsy.core.command

from patsy.core.db_gateway import DbGateway
from patsy.core.update import Update, UpdateArgs


def configure_cli(subparsers) -> None:  # type: ignore
    """
    Configures the CLI arguments for this command
    """
    parser = subparsers.add_parser(
        name='update',
        description='Updates the "accessions" database table using values from the given CSV file.'
    )
    parser.set_defaults(cmd_name='update')

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Perform a "dry run" without actually updating the database.',
    )

    parser.add_argument(
        '--skip-existing',
        action='store_true',
        help='Do not perform update if database update field already contains a value',
    )

    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Provide additional information about individual updates',
    )

    parser.add_argument(
        '-b', '--batch',
        action='store',
        required=True,
        help='Name of the batch to update.'
    )

    parser.add_argument(
        '--db-compare-column',
        action='store',
        required=True,
        help='The accessions database column to use for comparison'
    )

    parser.add_argument(
        '--db-target-column',
        action='store',
        required=True,
        help='The accessions database column to update'
    )

    parser.add_argument(
        '--csv-compare-value',
        action='store',
        required=True,
        help='The CSV column specifying the comparison value'
    )

    parser.add_argument(
        '--csv-update-value',
        action='store',
        required=True,
        help='The CSV column specifying the updated value'
    )

    parser.add_argument(
        'file', action='store', help="The CSV file containing the updates"
    )


class Command(patsy.core.command.Command):
    def __call__(self, args: argparse.Namespace, gateway: DbGateway) -> None:
        update_args = UpdateArgs.from_cli_args(args)

        logging.info(f'Running update command with the following options. {update_args}')

        update_impl = Update(gateway)
        update_result = update_impl.update(update_args)

        logging.info(f"Total CSV rows processed: {update_result.csv_rows_processed}")
        logging.info(f"Database rows updated: {update_result.db_rows_updated}")

        errors = update_result.errors
        error_count = len(errors)

        if error_count > 0:
            logging.warning(f"Number of errors: {error_count}")
            for error in errors:
                logging.warning(f"{error}")

            logging.warning("UPDATE COMPLETE WITH ERRORS")

        else:
            logging.info("UPDATE SUCCESSFUL")
