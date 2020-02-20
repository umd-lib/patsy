import abc
import os
from sqlalchemy.exc import IntegrityError
from .database import Session

from .load_result import LoadResult, FileLoadResult
from .progress_notifier import ProgressNotifier


class AbstractCsvLoader(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def csv_to_object(self, row):
        """
        Converts the given row information into an implementation-specific object
        :param row: the object returned by file_line_handler
        :return: an implementation-specific object representing a single line in the CSV
        """
        pass

    @abc.abstractmethod
    def file_line_handler(self, file_handle):
        """
        Converts the next line provided by the file_handle into an object
        (such as an array or Dictionary) that can be handled by the
        csv_to_object method.
        :param file_handle: the file_handler provided by "open"
        :return: an implementation-specific object that can be processed by csv_to_object
        """
        pass

    def load(self, source, progress_notifier=ProgressNotifier()):
        """
        Load a set of records from one or more CSV files

        :param source: the full path to the CSV file, or directory containing the
        the CSV files to load
        :param progress_notifier: A ProgressNotifier to report individual file loads
        and results. Defaults to ProgressNotifier, which is a no-op
        :return: a LoadResult describing the load
        """

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
            progress_notifier.notify(f"Loading {filepath}")
            files_processed = files_processed + 1
            result = self.load_from_file(filepath)
            total_rows_processed = total_rows_processed + result.num_processed
            total_successful_rows = total_successful_rows + len(result.successes)
            total_failed_rows = total_failed_rows + len(result.failures)
            file_load_results_map[filepath] = result

        result = LoadResult(files_processed=files_processed, total_rows_processed=total_rows_processed,
                            total_successful_rows=total_successful_rows, total_failed_rows=total_failed_rows,
                            file_load_results_map=file_load_results_map)
        return result

    def load_from_file(self, filename):
        """
        Load a set of records from a single CSV file

        :param filename: the full path to the CSV file
        :return: a FileLoadResult describing the load
        """
        instance = self

        def iter_records_from(filename):
            """
            Convert the file lines into objects
            """
            with open(filename, 'r') as file_handle:
                reader = instance.file_line_handler(file_handle)
                for row in reader:
                    yield instance.csv_to_object(row)

        # Create asset objects
        insertions_succeeded = []
        insertions_failed = []
        num_processed = 0
        for obj in iter_records_from(filename):
            num_processed = num_processed + 1
            try:
                session = Session()
                session.add(obj)
                session.commit()
                insertions_succeeded.append(repr(obj))
            except IntegrityError as err:
                insertions_failed.append(f"ERROR: {err.code, err.args[0]} - obj: {repr(obj)}")

        result = FileLoadResult(successes=insertions_succeeded, failures=insertions_failed, num_processed=num_processed)
        return result
