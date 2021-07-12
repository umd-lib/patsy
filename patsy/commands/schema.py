import argparse
import patsy.core.command
from patsy.core.gateway import Gateway
from patsy.database import create_schema


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
    def __call__(self, args: argparse.Namespace, gateway: Gateway) -> str:
        create_schema(args)
