from collections import namedtuple
import patsy.database
import patsy.restore
from sqlalchemy import create_engine
from patsy.model import Base
import unittest
from patsy.model import Restore

Session = patsy.database.Session


class TestRestore(unittest.TestCase):
    def setUp(self):
        engine = create_engine('sqlite:///:memory:')
        Session.configure(bind=engine)
        Base.metadata.create_all(engine)

    def test_load_single_accession_file(self):
        restore_file = 'tests/data/restores/sample_restore_1.csv'
        result = patsy.restore.load_restores_from_file(restore_file)

        self.assertEqual(5, result.num_processed)
        self.assertEqual(5, len(result.successes))
        self.assertEqual(0, len(result.failures))

        session = Session()
        num_rows = session.query(Restore).count()
        self.assertEqual(5, num_rows)

    def test_loading_same_accession_file_multiple_times(self):
        restore_file = 'tests/data/restores/sample_restore_1.csv'

        result = patsy.restore.load_restores_from_file(restore_file)
        self.assertEqual(5, result.num_processed)
        self.assertEqual(5, len(result.successes))
        self.assertEqual(0, len(result.failures))

        result = patsy.restore.load_restores_from_file(restore_file)
        self.assertEqual(5, result.num_processed)
        self.assertEqual(0, len(result.successes))
        self.assertEqual(5, len(result.failures))

        session = Session()
        num_rows = session.query(Restore).count()
        self.assertEqual(5, num_rows)

    def test_load_multiple_accession_files(self):
        restores_dir = 'tests/data/restores'
        result = patsy.restore.load_restores(restores_dir)

        self.assertEqual(2, result.files_processed)
        self.assertEqual(10, result.total_rows_processed)
        self.assertEqual(10, result.total_successful_rows)
        self.assertEqual(0, result.total_failed_rows)
        self.assertEqual(2, len(result.file_load_results_map.keys()))

        session = Session()
        num_rows = session.query(Restore).count()
        self.assertEqual(10, num_rows)

    def test_csv_to_accession(self):
        # Row with all the elements
        row = [
            'ABC123',  # md5
            '/test_path/test_file',  # filepath
            'test_file',  # filename
            '456']  # bytes
        restore = patsy.restore.csv_to_restore(row)
        self.assertEqual(row[0], restore.md5)
        self.assertEqual(row[1], restore.filepath)
        self.assertEqual(row[2], restore.filename)
        self.assertEqual(row[3], restore.bytes)

        # Rows with missing elements throws exception
        row = [
            'ABC123',  # md5
            '/test_path/test_file']  # filepath
        with self.assertRaises(IndexError):
            accession = patsy.restore.csv_to_restore(row)

