from collections import namedtuple
import csv
import os
from sqlalchemy.exc import IntegrityError

from .model import Restore
from .database import Session


def csv_to_restore(row):
    """
    Returns an Restore object created from the given String array

    :param row: a String array representing a row in the CSV file
    :return: an Accession object containing the information from the Dictionary
    :raises: IndexError if the row does not contain at least the expected number of items
    """
    return Restore(
        md5=row[0],
        filepath=row[1],
        filename=row[2],
        bytes=row[3])


def load_restores(source):
    """
    Load a set of restore records from CSV

    :param source: the full path to the CSV file, or directory containing the
    the CSV files to load
    :return: a LoadResult describing the load
    """

    LoadResult = namedtuple("LoadResult",
                            "files_processed total_rows_processed total_successful_rows total_failed_rows file_load_results_map")

    # Process single file or directory of files
    if os.path.isfile(source):
        filepaths = [source]
    elif os.path.isdir(source):
        filepaths = [
            os.path.join(source, f) for f in os.listdir(source)
            ]

    files_processed = 0
    total_rows_processed = 0
    total_successful_rows = 0
    total_failed_rows = 0
    file_load_results_map = {}

    for filepath in filepaths:
        files_processed = files_processed + 1
        result = load_restores_from_file(filepath)
        total_rows_processed = total_rows_processed + result.num_processed
        total_successful_rows = total_successful_rows + len(result.successes)
        total_failed_rows = total_failed_rows + len(result.failures)
        file_load_results_map[filepath] = result

    result = LoadResult(files_processed=files_processed, total_rows_processed=total_rows_processed,
                        total_successful_rows=total_successful_rows, total_failed_rows=total_failed_rows,
                        file_load_results_map=file_load_results_map)
    return result


def load_restores_from_file(restore_file):
    """
    Load a set of restore records from a single CSV

    :param restore_file: the full path to the CSV file
    :return: a FileLoadResult describing the load
    """
    FileLoadResult = namedtuple("FileLoadResult", "successes failures num_processed")

    def iter_restore_records_from(catalog_file):
        """
        Load the restore catalog into Restore objects
        """
        with open(catalog_file, 'r') as handle:
            reader = csv.reader(handle)
            for row in reader:
                yield csv_to_restore(row)

    # Create asset objects
    insertions_succeeded = []
    insertions_failed = []
    num_processed = 0
    for restore in iter_restore_records_from(restore_file):
        num_processed = num_processed + 1
        try:
            session = Session()
            session.add(restore)
            session.commit()
            insertions_succeeded.append(repr(restore))
        except IntegrityError as err:
            insertions_failed.append(f"ERROR: {err.code, err.args[0]} - restore: {repr(restore)}")

    result = FileLoadResult(successes=insertions_succeeded, failures=insertions_failed, num_processed=num_processed)
    return result

