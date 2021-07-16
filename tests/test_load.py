import unittest
from argparse import Namespace
from patsy.core.schema import Schema
from patsy.core.db_gateway import DbGateway
from patsy.core.load import Load
from typing import Dict


class TestLoad(unittest.TestCase):
    def setUp(self):
        self.valid_row_dict = {
            'BATCH': 'batch',
            'RELPATH': 'relpath',
            'FILENAME': 'filename',
            'EXTENSION': 'extension',
            'BYTES': 'bytes',
            'MTIME': 'mtime',
            'MODDATE': 'moddate',
            'MD5': 'md5',
            'SHA1': 'sha1',
            'SHA256': 'sha256',
            'storageprovider': 'storageprovider',
            'storagepath': 'storagepath'
        }

        args = Namespace()
        args.database = ":memory:"
        self.gateway = DbGateway(args)
        schema = Schema(self.gateway)
        schema.create_schema()
        self.load = Load(self.gateway)

    def test_process_csv_file(self):
        csv_file = 'tests/fixtures/load/colors_inventory-aws-archiver.csv'

        load_result = self.load.process_file(csv_file)
        self.assertEqual(3, load_result.rows_processed)

    def test_is_row_valid__empty_dict(self):
        csv_line_index = 2

        row_dict = {}
        self.assertFalse(self.load.is_row_valid(csv_line_index, row_dict))

        self.assertEqual(1, len(self.load.load_result.errors))

    def test_is_row_valid__missing_required_field(self):
        csv_line_index = 2

        row_dict = remove_key(self.valid_row_dict, 'BATCH')
        self.assertFalse(self.load.is_row_valid(csv_line_index, row_dict))
        self.assertEqual(1, len(self.load.load_result.errors))

    def test_is_row_valid__missing_allowed_empty_field(self):
        csv_line_index = 2

        row_dict = remove_key(self.valid_row_dict, 'SHA256')
        self.assertFalse(self.load.is_row_valid(csv_line_index, row_dict))
        self.assertEqual(1, len(self.load.load_result.errors))

    def test_is_row_valid__required_field_no_content(self):
        csv_line_index = 2

        row_dict = self.valid_row_dict.copy()
        row_dict['BATCH'] = ""
        self.assertFalse(self.load.is_row_valid(csv_line_index, row_dict))
        self.assertEqual(1, len(self.load.load_result.errors))

    def test_is_row_valid__allowed_missing_field_no_content(self):
        csv_line_index = 2

        row_dict = self.valid_row_dict.copy()
        row_dict['SHA256'] = ""
        self.assertTrue(self.load.is_row_valid(csv_line_index, row_dict))
        self.assertEqual(0, len(self.load.load_result.errors))

    def test_load__file_with_invalid_rows(self):
        load_result = self.load.process_file('tests/fixtures/load/invalid_inventory.csv')
        self.assertEqual(3, load_result.rows_processed)
        self.assertEqual(1, load_result.batches_added)
        self.assertEqual(2, load_result.accessions_added)
        self.assertEqual(2, load_result.locations_added)
        self.assertEqual(1, len(load_result.errors))

    def test_load__file_with_valid_rows(self):
        load_result = self.load.process_file('tests/fixtures/load/colors_inventory-aws-archiver.csv')
        self.assertEqual(3, load_result.rows_processed)
        self.assertEqual(1, load_result.batches_added)
        self.assertEqual(3, load_result.accessions_added)
        self.assertEqual(3, load_result.locations_added)
        self.assertEqual(0, len(load_result.errors))

    def test_load__file_with_multiple_accessions_one_location(self):
        load_result = self.load.process_file('tests/fixtures/load/multiple_accessions_one_location.csv')
        self.assertEqual(2, load_result.rows_processed)
        self.assertEqual(2, load_result.batches_added)
        self.assertEqual(2, load_result.accessions_added)
        self.assertEqual(1, load_result.locations_added)
        self.assertEqual(0, len(load_result.errors))

    def test_load__file_with_valid_rows_loaded_twice(self):
        # First load
        load_result = self.load.process_file('tests/fixtures/load/colors_inventory-aws-archiver.csv')
        self.assertEqual(3, load_result.rows_processed)
        self.assertEqual(1, load_result.batches_added)
        self.assertEqual(3, load_result.accessions_added)
        self.assertEqual(3, load_result.locations_added)
        self.assertEqual(0, len(load_result.errors))

        # Second load - nothing should be added
        self.load = Load(self.gateway)
        load_result = self.load.process_file('tests/fixtures/load/colors_inventory-aws-archiver.csv')
        self.assertEqual(3, load_result.rows_processed)
        self.assertEqual(0, load_result.batches_added)
        self.assertEqual(0, load_result.accessions_added)
        self.assertEqual(0, load_result.locations_added)
        self.assertEqual(0, len(load_result.errors))

    def test_load__file_from_preserve_tool(self):
        load_result = self.load.process_file('tests/fixtures/load/colors_inventory-preserve.csv')
        self.assertEqual(3, load_result.rows_processed)
        self.assertEqual(1, load_result.batches_added)
        self.assertEqual(3, load_result.accessions_added)
        self.assertEqual(0, load_result.locations_added)
        self.assertEqual(0, len(load_result.errors))

    def test_load__file_from_preserve_tool_then_archiver_update(self):
        # First load uses "preserve" file
        load_result = self.load.process_file('tests/fixtures/load/colors_inventory-preserve.csv')
        self.assertEqual(3, load_result.rows_processed)
        self.assertEqual(1, load_result.batches_added)
        self.assertEqual(3, load_result.accessions_added)
        self.assertEqual(0, load_result.locations_added)
        self.assertEqual(0, len(load_result.errors))

        # Second load updates the locations using the "asw-archiver" file,
        # only locations should be added.
        self.load = Load(self.gateway)
        load_result = self.load.process_file('tests/fixtures/load/colors_inventory-aws-archiver.csv')
        self.assertEqual(3, load_result.rows_processed)
        self.assertEqual(0, load_result.batches_added)
        self.assertEqual(0, load_result.accessions_added)
        self.assertEqual(3, load_result.locations_added)
        self.assertEqual(0, len(load_result.errors))

    def tearDown(self):
        self.gateway.close()


def remove_key(dict: Dict[str, str], key: str) -> Dict[str, str]:
    new_dict = dict.copy()
    new_dict.pop(key)
    return new_dict
