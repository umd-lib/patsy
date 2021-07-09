import abc
import argparse


class Command(metaclass=abc.ABCMeta):
    """
    Interface for commands run via the CLI
    """
    @abc.abstractmethod
    def __call__(self, args: argparse.Namespace) -> str:
        """
        Executes the command, return true if successful, false otherwise.
        """
        raise NotImplementedError
