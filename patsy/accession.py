from collections import namedtuple
import csv
import os
from sqlalchemy.exc import IntegrityError

from .model import Accession
from .database import Session


def csv_to_accession(row):
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


def load_accessions(source):
    """
    Load a set of accessions records from CSV

    :param source: the full path to the CSV file, or directory containing the
    the CSV files to load
    :return: an AccessionLoadResult describing the load
    """

    AccessionsLoadResult = namedtuple("AccessionsLoadResult",
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
        result = load_accessions_from_file(filepath)
        total_rows_processed = total_rows_processed + result.num_processed
        total_successful_rows = total_successful_rows + len(result.successes)
        total_failed_rows = total_failed_rows + len(result.failures)
        file_load_results_map[filepath] = result

    result = AccessionsLoadResult(files_processed=files_processed, total_rows_processed=total_rows_processed,
                                  total_successful_rows=total_successful_rows, total_failed_rows=total_failed_rows,
                                  file_load_results_map=file_load_results_map)
    return result


def load_accessions_from_file(accession_file):
    """
    Load a set of accessions records from a single CSV

    :param accession_file: the full path to the CSV file
    :return: an AccessionFileLoadResult describing the load
    """
    AccessionFileLoadResult = namedtuple("AccessionFileLoadResult", "successes failures num_processed")

    def iter_accession_records_from(catalog_file):
        """
        Load the accession catalog into Accession objects
        """
        with open(catalog_file, 'r') as handle:
            reader = csv.DictReader(handle, delimiter=',')
            for row in reader:
                yield csv_to_accession(row)

    # Create asset objects
    insertions_succeeded = []
    insertions_failed = []
    num_processed = 0
    for accession in iter_accession_records_from(accession_file):
        num_processed = num_processed + 1
        try:
            session = Session()
            session.add(accession)
            session.commit()
            insertions_succeeded.append(repr(accession))
        except IntegrityError as err:
            insertions_failed.append(f"ERROR: {err.code, err.args[0]} - accession: {repr(accession)}")

    result = AccessionFileLoadResult(successes=insertions_succeeded, failures=insertions_failed, num_processed=num_processed)
    return result

