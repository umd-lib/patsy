import patsy.core.command
import argparse
import logging

from patsy.core.db_gateway import DbGateway
from patsy.core.schema import Schema


def configure_cli(subparsers) -> None:  # type: ignore
    """
    Configures the CLI arguments for this command
    """
    parser = subparsers.add_parser(
        name='schema',
        description='Create schema from the declarative base.'
    )
    parser.set_defaults(cmd_name='schema')


class Command(patsy.core.command.Command):
    def __call__(self, args: argparse.Namespace, gateway: DbGateway) -> None:
        schema_impl = Schema(gateway)
        schema_impl.create_schema()
        logging.info("Schema created")
