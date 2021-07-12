import abc
from patsy.core.patsy_record import PatsyRecord


class Gateway(metaclass=abc.ABCMeta):
    """
    Inteface to persistent storage backend
    """
    @abc.abstractmethod
    def add(self, patsy_record: PatsyRecord) -> bool:
        """
        Adds the given PatsyRecord to the backend, returning True if that
        addition was performed, False, if the record already existed.
        """
        raise NotImplementedError
