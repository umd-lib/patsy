import abc
from patsy.core.patsy_entry import PatsyEntry


class Gateway(metaclass=abc.ABCMeta):
    """
    Inteface to persistent storage backend
    """
    @abc.abstractmethod
    def add(patsy_entry: PatsyEntry) -> bool:
        """
        Adds the given PatsyEntry to the backend, returning True if that
        addition was performed, False, if the entry already existed.
        """
        raise NotImplementedError
