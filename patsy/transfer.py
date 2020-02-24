import csv

from .model import Transfer
from .csv_loader import AbstractCsvLoader


class TransferCsvLoader(AbstractCsvLoader):
    def csv_to_object(self, row):
        """
        Returns a Transfer object created from the given Dictionary

        :param row: a Dictionary representing a row in the CSV file
        :return: a Transfer object containing the information from the Dictionary
        :raises: KeyError if an expected key is not found
        """
        return Transfer(
            filepath=row['filepath'],
            storagepath=row['storagepath'])

    def file_line_handler(self, file_handle):
        """
        Returns a dictionary representing a single row from the file_handle

        :param file_handle: the file_handler provided by "open"
        :return: a Dictionary of values representing a single row
        """
        return csv.DictReader(file_handle, delimiter=',')
