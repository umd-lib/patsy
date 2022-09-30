import pytest

from argparse import Namespace
from patsy.commands.schema import Command
from patsy.core.db_gateway import DbGateway
from patsy.core.load import Load
from patsy.model import Base
from sqlalchemy.schema import DropTable
from sqlalchemy.ext.compiler import compiles
from typing import Dict


# pytestmark = pytest.mark.parametrize(
#     "addr", [":memory"]  # , "postgresql+psycopg2://postgres:password@localhost:5432/postgres"]
# )


@pytest.fixture
def addr(request):
    return request.config.getoption('--base-url')


@compiles(DropTable, "postgresql")
def _compile_drop_table(element, compiler, **kwargs):
    return compiler.visit_drop_table(element) + " CASCADE"


def setUp(obj, addr):
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

    args = Namespace()
    args.database = addr
    obj.gateway = DbGateway(args)
    # schema = Schema(obj.gateway)
    # schema.create_schema()
    Command.__call__(obj, obj.gateway)
    obj.load = Load(obj.gateway)


def tearDown(obj):
    obj.gateway.close()
    Base.metadata.drop_all(obj.gateway.session.get_bind())


class TestLoad():
    def test_process_csv_file(self, addr):
        setUp(self, addr)
        csv_file = 'tests/fixtures/load/colors_inventory-aws-archiver.csv'
        load_result = self.load.process_file(csv_file)
        assert load_result.rows_processed == 3
        tearDown(self)

    def test_is_row_valid__empty_dict(self, addr):
        setUp(self, addr)
        csv_line_index = 2
        row_dict = {}
        assert self.load.is_row_valid(csv_line_index, row_dict) is False
        assert len(self.load.load_result.errors) == 1
        tearDown(self)

    def test_is_row_valid__missing_required_field(self, addr):
        setUp(self, addr)
        csv_line_index = 2
        row_dict = remove_key(self.valid_row_dict, 'BATCH')
        assert self.load.is_row_valid(csv_line_index, row_dict) is False
        assert len(self.load.load_result.errors) == 1
        tearDown(self)

    def test_is_row_valid__missing_allowed_empty_field(self, addr):
        setUp(self, addr)
        csv_line_index = 2
        row_dict = remove_key(self.valid_row_dict, 'SHA256')
        assert self.load.is_row_valid(csv_line_index, row_dict) is False
        assert len(self.load.load_result.errors) == 1
        tearDown(self)

    def test_is_row_valid__required_field_no_content(self, addr):
        setUp(self, addr)
        csv_line_index = 2
        row_dict = self.valid_row_dict.copy()
        row_dict['BATCH'] = ""
        assert self.load.is_row_valid(csv_line_index, row_dict) is False
        assert len(self.load.load_result.errors) == 1
        tearDown(self)

    def test_is_row_valid__allowed_missing_field_no_content(self, addr):
        setUp(self, addr)
        csv_line_index = 2
        row_dict = self.valid_row_dict.copy()
        row_dict['SHA256'] = ""
        assert self.load.is_row_valid(csv_line_index, row_dict) is True
        assert len(self.load.load_result.errors) == 0
        tearDown(self)

    def test_load__file_with_invalid_rows(self, addr):
        setUp(self, addr)
        load_result = self.load.process_file('tests/fixtures/load/invalid_inventory.csv')
        assert load_result.rows_processed == 3
        assert load_result.batches_added == 1
        assert load_result.accessions_added == 2
        assert load_result.locations_added == 2
        assert len(load_result.errors) == 1
        tearDown(self)

    def test_load__file_with_valid_rows(self, addr):
        setUp(self, addr)
        load_result = self.load.process_file('tests/fixtures/load/colors_inventory-aws-archiver.csv')
        assert load_result.rows_processed == 3
        assert load_result.batches_added == 1
        assert load_result.accessions_added == 3
        assert load_result.locations_added == 3
        assert len(load_result.errors) == 0
        tearDown(self)

    def test_load__file_with_multiple_accessions_one_location(self, addr):
        setUp(self, addr)
        load_result = self.load.process_file('tests/fixtures/load/multiple_accessions_one_location.csv')
        assert load_result.rows_processed == 2
        assert load_result.batches_added == 2
        assert load_result.accessions_added == 2
        assert load_result.locations_added == 1
        assert len(load_result.errors) == 0
        tearDown(self)

    def test_load__file_with_valid_rows_loaded_twice(self, addr):
        setUp(self, addr)

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
        tearDown(self)

    def test_load__file_from_preserve_tool(self, addr):
        setUp(self, addr)
        load_result = self.load.process_file('tests/fixtures/load/colors_inventory-preserve.csv')
        assert load_result.rows_processed == 3
        assert load_result.batches_added == 1
        assert load_result.accessions_added == 3
        assert load_result.locations_added == 0
        assert len(load_result.errors) == 0
        tearDown(self)

    def test_load__file_from_preserve_tool_then_archiver_update(self, addr):
        setUp(self, addr)

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
        tearDown(self)


def remove_key(dict: Dict[str, str], key: str) -> Dict[str, str]:
    new_dict = dict.copy()
    new_dict.pop(key)
    return new_dict
