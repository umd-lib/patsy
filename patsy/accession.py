import csv

from .model import Accession
from .csv_loader import AbstractCsvLoader


class AccessionCsvLoader(AbstractCsvLoader):
    def csv_to_object(self, row):
        """
        Returns an Accession object created from the given Dictionary

        :param row: a Dictionary representing a row in the CSV file
        :return: an Accession object containing the information from the Dictionary
        :raises: KeyError if an expected key is not found
        """
        return Accession(
            batch=row['batch'],
            bytes=row['bytes'],
            filename=row['filename'],
            md5=row['md5'],
            relpath=row['relpath'],
            sourcefile=row['sourcefile'],
            sourceline=row['sourceline'],
            timestamp=row['timestamp'])

    def file_line_handler(self, file_handle):
        """
        Returns a dictionary representing a single row from the file_handle

        :param file_handle: the file_handler provided by "open"
        :return: a Dictionary of values representing a single row
        """
        return csv.DictReader(file_handle, delimiter=',')
