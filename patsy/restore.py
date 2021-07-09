import csv

from .model import Restore
from .csv_loader import AbstractCsvLoader


class RestoreCsvLoader(AbstractCsvLoader):
    def csv_to_object(self, row):
        """
        Returns an Restore object created from the given String array

        :param row: a String array representing a row in the CSV file
        :return: a Restore object containing the information from the array
        :raises: IndexError if the row does not contain at least the expected number of items
        """
        return Restore(
            md5=row[0],
            filepath=row[1],
            filename=row[2],
            bytes=row[3])

    def file_line_handler(self, file_handle):
        """
        Returns a String array representing a single row from the file_handle

        :param file_handle: the file_handler provided by "open"
        :return: a String array representing a single row
        """
        return csv.reader(file_handle)
