class LoadResult:
    """
    Data class encapsulating the results of loading multiple files
    """
    def __init__(self, files_processed, total_rows_processed, total_successful_rows,
                 total_failed_rows, file_load_results_map):
        """
        Constructs a new LoadResult with the given parameters

        :param files_processed: the total number of file processed
        :param total_rows_processed: the total number of rows processed
        :param total_successful_rows: the total number of rows that were
               successfully processed
        :param total_failed_rows: the total number of rows where a processing
               failure occurred
        :param file_load_results_map: Map containing the FileLoadResults, keyed
               by filename
        """
        self.files_processed = files_processed
        self.total_rows_processed = total_rows_processed
        self.total_successful_rows = total_successful_rows
        self.total_failed_rows = total_failed_rows
        self.file_load_results_map = file_load_results_map

    def __repr__(self):
        summary_str = (
            f"Number of files processed: {self.files_processed}\n"
            f"Total number of rows processed: {self.total_rows_processed}\n"
            f"Total Successful rows: {self.total_successful_rows}\n"
            f"Total Failed rows {self.total_failed_rows}\n"
        )

        error_str = "All files loaded successfully.\n"
        if self.total_failed_rows > 0:
            error_str = f"Files with errors:\n"
            for file_load_key in self.file_load_results_map.keys():
                file_load_result = self.file_load_results_map[file_load_key]
                if len(file_load_result.failures) > 0:
                    error_str = error_str + f"\t{file_load_key}\n"
                    for failure in file_load_result.failures:
                        error_str = error_str + f"\t\t{str(failure)}\n"

        return summary_str + error_str


class FileLoadResult:
    """
    Data class encapsulating the results of loading a single file
    """
    def __init__(self, successes, failures, num_processed):
        """
        Constructs a new LoadResult with the given parameters

        :param successes: the list of successes
        :param failures: the list of failures
        :param num_processed: the total number of rows that were processed
        """
        self.successes = successes
        self.failures = failures
        self.num_processed = num_processed

    def __repr__(self):
        successes_str = '\n\t'.join(self.successes)
        failures_str = '\n\t'.join(self.failures)
        return f"num_processed = {self.num_processed}\nSuccesses:\n\t{successes_str}\nFailures:\n\t{failures_str}"
