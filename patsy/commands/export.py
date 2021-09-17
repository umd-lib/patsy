import argparse
import patsy.core.command
import sys

from patsy.core.export import Export
from patsy.core.db_gateway import DbGateway


def configure_cli(subparsers) -> None:  # type: ignore
    """
    Configures the CLI arguments for this command
    """
    parser = subparsers.add_parser(
        name='export',
        description="Export all records as an 'inventory' manifest file"
    )
    parser.set_defaults(cmd_name='export')

    parser.add_argument(
        '-b', '--batch',
        action='store',
        default=None,
        help='Optional batch name to query. Defaults to exporting all batches'
    )

    parser.add_argument(
        '-o', '--output',
        action='store',
        default=None,
        help='The (optional) file to write output to. Defaults to standard out'
    )

    parser.add_argument(
        '-u', '--untransferred',
        action='store_true',
        default=False,
        help='Export only those records with an empty storage_location.'
    )


class Command(patsy.core.command.Command):
    def __call__(self, args: argparse.Namespace, gateway: DbGateway) -> str:
        batch = args.batch
        output = args.output
        filter = args.untransferred
        # Display batch configuration information to the user
        sys.stderr.write(
            f'Running export command with the following options:\n\n'
            f'  - batch: {batch}\n'
            f'  - output: {output}\n'
            f'  - untransferred: {filter}\n'
            '======\n'
        )

        export_impl = Export(gateway)
        export_result = export_impl.export(batch, output, filter)
        result_messages = [
            f"Total (non-empty) Batches exported: " +
            f"{export_result.batches_exported}",
            f"Total rows exported: {export_result.rows_exported}",
            "\nEXPORT COMPLETE"
        ]

        result = "\n".join(result_messages)

        return result
