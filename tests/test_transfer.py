import patsy.database
from patsy.transfer import TransferCsvLoader
from patsy.model import Base
import unittest
from patsy.model import Transfer
from .utils import create_test_engine

Session = patsy.database.Session


class TestTransfer(unittest.TestCase):
    def setUp(self):
        create_test_engine()
        engine = Session().get_bind()
        Base.metadata.create_all(engine)

    def test_load_single_file(self):
        transfer_file = 'tests/data/transfers/sample_transfer_1.csv'

        transfer_loader = TransferCsvLoader()
        result = transfer_loader.load_from_file(transfer_file)

        self.assertEqual(5, result.num_processed)
        self.assertEqual(5, len(result.successes))
        self.assertEqual(0, len(result.failures))

        session = Session()
        num_rows = session.query(Transfer).count()
        self.assertEqual(5, num_rows)

    def test_loading_same_file_multiple_times(self):
        transfer_file = 'tests/data/transfers/sample_transfer_1.csv'

        transfer_loader = TransferCsvLoader()
        result = transfer_loader.load_from_file(transfer_file)
        self.assertEqual(5, result.num_processed)
        self.assertEqual(5, len(result.successes))
        self.assertEqual(0, len(result.failures))

        transfer_loader = TransferCsvLoader()
        result = transfer_loader.load_from_file(transfer_file)
        self.assertEqual(5, result.num_processed)
        self.assertEqual(0, len(result.successes))
        self.assertEqual(5, len(result.failures))

        session = Session()
        num_rows = session.query(Transfer).count()
        self.assertEqual(5, num_rows)

    def test_load_multiple_files(self):
        transfer_dir = 'tests/data/transfers'

        transfer_loader = TransferCsvLoader()
        result = transfer_loader.load(transfer_dir)

        self.assertEqual(2, result.files_processed)
        self.assertEqual(10, result.total_rows_processed)
        self.assertEqual(10, result.total_successful_rows)
        self.assertEqual(0, result.total_failed_rows)
        self.assertEqual(2, len(result.file_load_results_map.keys()))

        session = Session()
        num_rows = session.query(Transfer).count()
        self.assertEqual(10, num_rows)

    def test_csv_to_transfer(self):
        # Row with all the elements
        row = dict(filepath='/test/file.jpg', storagepath='aws_storagepath1/file.jpg')

        transfer_loader = TransferCsvLoader()
        transfer = transfer_loader.csv_to_object(row)
        self.assertEqual(row['filepath'], transfer.filepath)
        self.assertEqual(row['storagepath'], transfer.storagepath)

        # Rows with missing elements throws exception
        row = dict(filepath='/test/file.jpg')
        with self.assertRaises(KeyError):
            transfer = transfer_loader.csv_to_object(row)

        # Rows with missing elements throws exception
        row = dict(storagepath='aws_storagepath1/file.jpg')
        with self.assertRaises(KeyError):
            transfer = transfer_loader.csv_to_object(row)
