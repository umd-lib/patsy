import argparse
import patsy.core.command
from patsy.core.gateway import Gateway
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
    def __call__(self, args: argparse.Namespace, gateway: Gateway) -> str:
        file = args.file
        # Display batch configuration information to the user
        print(
            f'Running load command with the following options:\n\n'
            f'  - File: {file}\n'
            '======\n'
        )

        # from unittest.mock import MagicMock
        # gateway = MagicMock(Gateway)

        load_impl = Load(gateway)
        load_impl.process_file(file)

        results = load_impl.results
        result_messages = [
            f"Total rows processed: {results['rows_processed']}",
            f"Batches added: {results['batches_added']}",
            f"Accessions added: {results['accessions_added']}",
            f"Locations added: {results['locations_added']}",
        ]
        has_errors = len(results['errors']) > 0
        if has_errors:
            result_messages.extend([
               "\n---Errors----",
               f"Invalid rows: {len(results['errors'])}",
            ])
            result_messages.extend(results['errors'])

        if not has_errors:
            result_messages.append("\nLOAD SUCCESSFUL")
        else:
            result_messages.append("\n---- LOAD FAILED ----")

        result = "\n".join(result_messages)

        return result
