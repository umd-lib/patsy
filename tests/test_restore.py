import patsy.database
from patsy.restore import RestoreCsvLoader
from patsy.model import Base
import unittest
from patsy.model import Restore
from .utils import create_test_engine

Session = patsy.database.Session


class TestRestore(unittest.TestCase):
    def setUp(self):
        create_test_engine()
        engine = Session().get_bind()
        Base.metadata.create_all(engine)

    def test_load_single_file(self):
        restore_file = 'tests/data/restores/sample_restore_1.csv'
        restore_loader = RestoreCsvLoader()
        result = restore_loader.load_from_file(restore_file)

        self.assertEqual(5, result.num_processed)
        self.assertEqual(5, len(result.successes))
        self.assertEqual(0, len(result.failures))

        session = Session()
        num_rows = session.query(Restore).count()
        self.assertEqual(5, num_rows)

    def test_loading_same_file_multiple_times(self):
        restore_file = 'tests/data/restores/sample_restore_1.csv'

        restore_loader = RestoreCsvLoader()
        result = restore_loader.load_from_file(restore_file)

        self.assertEqual(5, result.num_processed)
        self.assertEqual(5, len(result.successes))
        self.assertEqual(0, len(result.failures))

        restore_loader = RestoreCsvLoader()
        result = restore_loader.load_from_file(restore_file)

        self.assertEqual(5, result.num_processed)
        self.assertEqual(0, len(result.successes))
        self.assertEqual(5, len(result.failures))

        session = Session()
        num_rows = session.query(Restore).count()
        self.assertEqual(5, num_rows)

    def test_load_multiple_files(self):
        restores_dir = 'tests/data/restores'

        restore_loader = RestoreCsvLoader()
        result = restore_loader.load(restores_dir)

        self.assertEqual(2, result.files_processed)
        self.assertEqual(10, result.total_rows_processed)
        self.assertEqual(10, result.total_successful_rows)
        self.assertEqual(0, result.total_failed_rows)
        self.assertEqual(2, len(result.file_load_results_map.keys()))

        session = Session()
        num_rows = session.query(Restore).count()
        self.assertEqual(10, num_rows)

    def test_csv_to_restore(self):
        # Row with all the elements
        row = [
            'ABC123',  # md5
            '/test_path/test_file',  # filepath
            'test_file',  # filename
            '456']  # bytes

        restore_loader = RestoreCsvLoader()
        restore = restore_loader.csv_to_object(row)
        self.assertEqual(row[0], restore.md5)
        self.assertEqual(row[1], restore.filepath)
        self.assertEqual(row[2], restore.filename)
        self.assertEqual(row[3], restore.bytes)

        # Rows with missing elements throws exception
        row = [
            'ABC123',  # md5
            '/test_path/test_file']  # filepath
        with self.assertRaises(IndexError):
            restore = restore_loader.csv_to_object(row)

