import argparse
import patsy.core.command
from patsy.core.load import Load
from patsy.core.db_gateway import DbGateway
from typing import cast, List


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
    def __call__(self, args: argparse.Namespace, gateway: DbGateway) -> str:
        file = args.file
        # Display batch configuration information to the user
        print(
            f'Running load command with the following options:\n\n'
            f'  - File: {file}\n'
            '======\n'
        )

        load_impl = Load(gateway)
        load_impl.process_file(file)

        results = load_impl.results
        result_messages = [
            f"Total rows processed: {results['rows_processed']}",
            f"Batches added: {results['batches_added']}",
            f"Accessions added: {results['accessions_added']}",
            f"Locations added: {results['locations_added']}",
        ]
        errors = cast(List[str], results['errors'])
        has_errors = len(errors) > 0
        if has_errors:
            result_messages.extend([
               "\n---Errors----",
               f"Invalid rows: {len(errors)}",
            ])
            result_messages.extend(errors)

        if not has_errors:
            result_messages.append("\nLOAD SUCCESSFUL")
        else:
            result_messages.append("\n---- LOAD FAILED ----")

        result = "\n".join(result_messages)

        return result
