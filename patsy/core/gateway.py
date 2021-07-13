import abc
from patsy.core.patsy_record import PatsyRecord


class AddResult():
    def __init__(self):
        self.batches_added = 0
        self.accessions_added = 0
        self.locations_added = 0


class Gateway(metaclass=abc.ABCMeta):
    """
    Inteface to persistent storage backend
    """
    @abc.abstractmethod
    def add(self, patsy_record: PatsyRecord) -> AddResult:
        """
        Adds the given PatsyRecord to the backend, returning True if that
        addition was performed, False, if the record already existed.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def close(self) -> None:
        pass
