import unittest
from patsy.load_result import FileLoadResult, LoadResult


class TestLoadResult(unittest.TestCase):
    def test_load_file_result(self):
        successes = ["abc", "def", "ghi"]
        failures = ['zyx123']
        num_processed = 4

        file_load_result = FileLoadResult(successes=successes, failures=failures, num_processed=num_processed)
        self.assertEqual(successes, file_load_result.successes)
        self.assertEqual(failures, file_load_result.failures)
        self.assertEqual(4, file_load_result.num_processed)

        expected_str = "num_processed = 4\nSuccesses:\n\tabc\n\tdef\n\tghi\nFailures:\n\tzyx123"
        self.assertEqual(expected_str, str(file_load_result))

    def test_load_result(self):
        files_processed = 2
        total_rows_processed = 100
        total_successful_rows = 95
        total_failed_rows = 5

        file_load_result_1 = FileLoadResult(
            num_processed=25,
            successes=["abc", "def", "ghi"],
            failures=[])

        file_load_result_2 = FileLoadResult(
            num_processed=75,
            successes=[],
            failures=['zyx123'])

        file_load_results_map = {'abc.txt': file_load_result_1, '123.csv': file_load_result_2}

        load_result = LoadResult(
            files_processed=files_processed,
            total_rows_processed=total_rows_processed,
            total_successful_rows=total_successful_rows,
            total_failed_rows=total_failed_rows,
            file_load_results_map=file_load_results_map
        )

        self.assertEqual(files_processed, load_result.files_processed)
        self.assertEqual(total_rows_processed, load_result.total_rows_processed)
        self.assertEqual(total_successful_rows, load_result.total_successful_rows)
        self.assertEqual(total_failed_rows, load_result.total_failed_rows)
        self.assertEqual(file_load_results_map, load_result.file_load_results_map)



