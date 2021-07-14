import abc
import argparse
from patsy.core.db_gateway import DbGateway


class Command(metaclass=abc.ABCMeta):
    """
    Interface for commands run via the CLI
    """
    @abc.abstractmethod
    def __call__(self, args: argparse.Namespace, gateway: DbGateway) -> str:
        """
        Executes the command, return true if successful, false otherwise.
        """
        raise NotImplementedError
