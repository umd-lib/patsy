import unittest
from unittest.mock import MagicMock
from patsy.core.gateway import Gateway
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

        self.mock_gateway = MagicMock(spec=Gateway)
        self.load = Load(self.mock_gateway)

    def test_process_csv_file(self):
        csv_file = 'tests/fixtures/load/colors_inventory.csv'

        self.load.process_file(csv_file)
        results = self.load.results
        self.assertEqual(3, results['rows_processed'])

    def test_is_row_valid__empty_dict(self):
        csv_line_index = 2

        row_dict = {}
        self.assertFalse(self.load.is_row_valid(csv_line_index, row_dict))

        self.assertEqual(1, len(self.load.results['errors']))

    def test_is_row_valid__missing_required_field(self):
        csv_line_index = 2

        row_dict = remove_key(self.valid_row_dict, 'BATCH')
        self.assertFalse(self.load.is_row_valid(csv_line_index, row_dict))
        self.assertEqual(1, len(self.load.results['errors']))

    def test_is_row_valid__missing_allowed_empty_field(self):
        csv_line_index = 2

        row_dict = remove_key(self.valid_row_dict, 'SHA256')
        self.assertFalse(self.load.is_row_valid(csv_line_index, row_dict))
        self.assertEqual(1, len(self.load.results['errors']))

    def test_is_row_valid__required_field_no_content(self):
        csv_line_index = 2

        row_dict = self.valid_row_dict.copy()
        row_dict['BATCH'] = ""
        self.assertFalse(self.load.is_row_valid(csv_line_index, row_dict))
        self.assertEqual(1, len(self.load.results['errors']))

    def test_is_row_valid__allowed_missing_field_no_content(self):
        csv_line_index = 2

        row_dict = self.valid_row_dict.copy()
        row_dict['SHA256'] = ""
        self.assertTrue(self.load.is_row_valid(csv_line_index, row_dict))
        self.assertEqual(0, len(self.load.results['errors']))


def remove_key(dict: Dict[str, str], key: str) -> Dict[str, str]:
    new_dict = dict.copy()
    new_dict.pop(key)
    return new_dict
