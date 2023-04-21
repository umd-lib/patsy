from patsy.core.load import Load
from tests import clear_database
from typing import Dict


def setUp(obj, gateway):
    obj.gateway = gateway

    obj.valid_row_dict = {
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

    obj.load = Load(obj.gateway)


def tearDown(obj):
    clear_database(obj)


class TestLoad():
    def test_process_csv_file(self, db_gateway):
        try:
            setUp(self, db_gateway)
            csv_file = 'tests/fixtures/load/colors_inventory-aws-archiver.csv'
            load_result = self.load.process_file(csv_file)
            assert load_result.rows_processed == 3
        finally:
            tearDown(self)

    def test_is_row_valid__empty_dict(self, db_gateway):
        try:
            setUp(self, db_gateway)
            csv_line_index = 2
            row_dict = {}
            assert self.load.is_row_valid(csv_line_index, row_dict) is False
            assert len(self.load.load_result.errors) == 1
        finally:
            tearDown(self)

    def test_is_row_valid__missing_required_field(self, db_gateway):
        try:
            setUp(self, db_gateway)
            csv_line_index = 2
            row_dict = remove_key(self.valid_row_dict, 'BATCH')
            assert self.load.is_row_valid(csv_line_index, row_dict) is False
            assert len(self.load.load_result.errors) == 1
        finally:
            tearDown(self)

    def test_is_row_valid__missing_allowed_empty_field(self, db_gateway):
        try:
            setUp(self, db_gateway)
            csv_line_index = 2
            row_dict = remove_key(self.valid_row_dict, 'SHA256')
            assert self.load.is_row_valid(csv_line_index, row_dict) is False
            assert len(self.load.load_result.errors) == 1
        finally:
            tearDown(self)

    def test_is_row_valid__required_field_no_content(self, db_gateway):
        try:
            setUp(self, db_gateway)
            csv_line_index = 2
            row_dict = self.valid_row_dict.copy()
            row_dict['BATCH'] = ""
            assert self.load.is_row_valid(csv_line_index, row_dict) is False
            assert len(self.load.load_result.errors) == 1
        finally:
            tearDown(self)

    def test_is_row_valid__allowed_missing_field_no_content(self, db_gateway):
        try:
            setUp(self, db_gateway)
            csv_line_index = 2
            row_dict = self.valid_row_dict.copy()
            row_dict['SHA256'] = ""
            assert self.load.is_row_valid(csv_line_index, row_dict) is True
            assert len(self.load.load_result.errors) == 0
        finally:
            tearDown(self)

    def test_load__file_with_invalid_rows(self, db_gateway):
        try:
            setUp(self, db_gateway)
            load_result = self.load.process_file('tests/fixtures/load/invalid_inventory.csv')
            assert load_result.rows_processed == 3
            assert load_result.batches_added == 1
            assert load_result.accessions_added == 2
            assert load_result.locations_added == 2
            assert len(load_result.errors) == 1
        finally:
            tearDown(self)

    def test_load__file_with_valid_rows(self, db_gateway):
        try:
            setUp(self, db_gateway)
            load_result = self.load.process_file('tests/fixtures/load/colors_inventory-aws-archiver.csv')
            assert load_result.rows_processed == 3
            assert load_result.batches_added == 1
            assert load_result.accessions_added == 3
            assert load_result.locations_added == 3
            assert len(load_result.errors) == 0
        finally:
            tearDown(self)

    def test_load__file_with_multiple_accessions_one_location(self, db_gateway):
        try:
            setUp(self, db_gateway)
            load_result = self.load.process_file('tests/fixtures/load/multiple_accessions_one_location.csv')
            assert load_result.rows_processed == 2
            assert load_result.batches_added == 2
            assert load_result.accessions_added == 2
            assert load_result.locations_added == 1
            assert len(load_result.errors) == 0
        finally:
            tearDown(self)

    def test_load__file_with_valid_rows_loaded_twice(self, db_gateway):
        try:
            setUp(self, db_gateway)

            # First load
            load_result = self.load.process_file('tests/fixtures/load/colors_inventory-aws-archiver.csv')
            assert load_result.rows_processed == 3
            assert load_result.batches_added == 1
            assert load_result.accessions_added == 3
            assert load_result.locations_added == 3
            assert len(load_result.errors) == 0

            # Second load - nothing should be added
            self.load = Load(self.gateway)
            load_result = self.load.process_file('tests/fixtures/load/colors_inventory-aws-archiver.csv')
            assert load_result.rows_processed == 3
            assert load_result.batches_added == 0
            assert load_result.accessions_added == 0
            assert load_result.locations_added == 0
            assert len(load_result.errors) == 0
        finally:
            tearDown(self)

    def test_load__file_from_preserve_tool(self, db_gateway):
        try:
            setUp(self, db_gateway)
            load_result = self.load.process_file('tests/fixtures/load/colors_inventory-preserve.csv')
            assert load_result.rows_processed == 3
            assert load_result.batches_added == 1
            assert load_result.accessions_added == 3
            assert load_result.locations_added == 0
            assert len(load_result.errors) == 0
        finally:
            tearDown(self)

    def test_load__file_from_preserve_tool_then_archiver_update(self, db_gateway):
        try:
            setUp(self, db_gateway)

            # First load uses "preserve" file
            load_result = self.load.process_file('tests/fixtures/load/colors_inventory-preserve.csv')
            assert load_result.rows_processed == 3
            assert load_result.batches_added == 1
            assert load_result.accessions_added == 3
            assert load_result.locations_added == 0
            assert len(load_result.errors) == 0

            # Second load updates the locations using the "asw-archiver" file,
            # only locations should be added.
            self.load = Load(self.gateway)
            load_result = self.load.process_file('tests/fixtures/load/colors_inventory-aws-archiver.csv')
            assert load_result.rows_processed == 3
            assert load_result.batches_added == 0
            assert load_result.accessions_added == 0
            assert load_result.locations_added == 3
            assert len(load_result.errors) == 0
        finally:
            tearDown(self)


def remove_key(dict: Dict[str, str], key: str) -> Dict[str, str]:
    new_dict = dict.copy()
    new_dict.pop(key)
    return new_dict
