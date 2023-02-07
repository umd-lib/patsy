import patsy.core.command
import argparse
import logging

from patsy.core.db_gateway import DbGateway
from patsy.core.load import Load


def configure_cli(subparsers) -> None:  # type: ignore
    """
    Configures the CLI arguments for this command
    """
    parser = subparsers.add_parser(
        name='load',
        description='Load an inventory CSV file into the database.'
    )
    parser.set_defaults(cmd_name='load')

    parser.add_argument(
        "file", action='store', help="The inventory CSV file to load"
    )


class Command(patsy.core.command.Command):
    def __call__(self, args: argparse.Namespace, gateway: DbGateway) -> None:
        file = args.file
        inputs = {"file": file}
        # Display batch configuration information to the user
        logging.info(f'Running load command with the following options: {inputs}')

        load_impl = Load(gateway)
        load_result = load_impl.process_file(file)

        logging.info(f"Total rows processed: {load_result.rows_processed}")
        logging.info(f"Batches added: {load_result.batches_added}")
        logging.info(f"Accessions added: {load_result.accessions_added}")
        logging.info(f"Locations added: {load_result.locations_added}")

        errors = load_result.errors
        error_amount = len(errors)

        if error_amount > 0:
            logging.warning(f"Amount of invalid rows: {error_amount}")
            for error in errors:
                logging.warning(f"Invalid row: {error}")

            logging.warning("LOAD COMPLETE WITH ERRORS")

        else:
            logging.info("LOAD SUCCESSFUL")
