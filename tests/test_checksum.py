import io
import os
import re
import logging
import pytest
import tempfile


from argparse import Namespace
from patsy.commands.checksum import Command, get_checksum
from patsy.core.schema import Schema
from patsy.core.db_gateway import DbGateway
from patsy.core.load import Load
from patsy.model import Base
from sqlalchemy.schema import DropTable
from sqlalchemy.ext.compiler import compiles
from unittest.mock import patch


LOGGER = logging.getLogger('__name__')


pytestmark = pytest.mark.parametrize(
    "addr", [":memory"]  # , "postgresql+psycopg2://postgres:password@localhost:5432/postgres"]
)


@compiles(DropTable, "postgresql")
def _compile_drop_table(element, compiler, **kwargs):
    return compiler.visit_drop_table(element) + " CASCADE"


def tearDown(obj):
    obj.gateway.close()
    Base.metadata.drop_all(obj.gateway.session.get_bind())


def setUp(obj, addr):
    args = Namespace()
    args.database = addr
    obj.gateway = DbGateway(args)
    schema = Schema(obj.gateway)
    schema.create_schema()
    obj.load = Load(obj.gateway)
    csv_file = 'tests/fixtures/load/colors_inventory-aws-archiver.csv'
    obj.load.process_file(csv_file)

    # Arguments passed to checksum.Command
    obj.checksum_command = Command()
    obj.command_args = Namespace()
    obj.command_args.location = None
    obj.command_args.output_type = None
    obj.command_args.output_file = None
    csv_file = 'tests/fixtures/load/colors_inventory-aws-archiver.csv'
    obj.load.process_file(csv_file)


class TestChecksumCommand:
    @patch('sys.stdout', new_callable=io.StringIO)
    def test_location_arg(self, mock_out, addr):
        # The call command will write to a file
        # and compare the answer to what's written in file.

        setUp(self, addr)
        self.command_args.location = ['test_bucket/TEST_BATCH/colors/sample_blue.jpg']
        self.checksum_command.__call__(self.command_args, self.gateway)
        # LOGGER.info(f"{mock_out.getvalue()}")
        expected = '85a929103d2f58ddfa8c8768eb6339ad  test_bucket/TEST_BATCH/colors/sample_blue.jpg\n'
        assert mock_out.getvalue() == expected
        tearDown(self)

    @patch('sys.stdout', new_callable=io.StringIO)
    def test_output_type_arg(self, mock_out, addr):
        setUp(self, addr)
        self.command_args.location = ['test_bucket/TEST_BATCH/colors/sample_blue.jpg']
        self.command_args.output_type = 'sha1'
        self.checksum_command.__call__(self.command_args, self.gateway)
        # LOGGER.info(f"{mock_out.getvalue()}")
        expected = '2fa953a48600e1aef0486b4b3a17c6100cfeef80  test_bucket/TEST_BATCH/colors/sample_blue.jpg\n'
        assert mock_out.getvalue() == expected
        tearDown(self)

    @patch('sys.stdout', new_callable=io.StringIO)
    def test_locations_file_arg(self, mock_out, addr):
        setUp(self, addr)
        with open('tests/fixtures/checksum/locations_file.csv') as f:
            self.command_args.locations_file = f
            self.checksum_command.__call__(self.command_args, self.gateway)
            # LOGGER.info(f"{mock_out.getvalue()}")
            expected = '85a929103d2f58ddfa8c8768eb6339ad  test_bucket/TEST_BATCH/colors/sample_blue.jpg\n' \
                       '1041fd1cf84c71183db2d5d95942a41c  test_bucket/TEST_BATCH/colors/sample_red.jpg\n'
            assert mock_out.getvalue() == expected

        tearDown(self)

    def test_output_file_arg(self, addr):
        setUp(self, addr)
        with tempfile.TemporaryDirectory() as tmpdirname:
            output_filename = os.path.join(tmpdirname, 'test_output_file.csv')
            with open(output_filename, 'w') as output_file:
                self.command_args.location = ['test_bucket/TEST_BATCH/colors/sample_blue.jpg']
                self.command_args.output_file = output_file
                self.checksum_command.__call__(self.command_args, self.gateway)

            assert os.path.getsize(output_filename) == 80

        tearDown(self)


class TestGetChecksum:
    def test_valid_row_and_md5_checksum__returns_tuple(self, addr):
        setUp(self, addr)
        row = {'location': 'test_bucket/TEST_BATCH/colors/sample_blue.jpg'}
        expected = ('85a929103d2f58ddfa8c8768eb6339ad', 'test_bucket/TEST_BATCH/colors/sample_blue.jpg')
        checksum_and_path = get_checksum(self.gateway, row, 'md5')
        assert checksum_and_path == expected
        tearDown(self)

    def test_valid_row_and_sha1_checksum__returns_tuple(self, addr):
        setUp(self, addr)
        row = {'location': 'test_bucket/TEST_BATCH/colors/sample_blue.jpg'}
        expected = ('2fa953a48600e1aef0486b4b3a17c6100cfeef80', 'test_bucket/TEST_BATCH/colors/sample_blue.jpg')
        checksum_and_path = get_checksum(self.gateway, row, 'sha1')
        assert checksum_and_path == expected
        tearDown(self)

    def test_gvalid_row_and_sha256_checksum__returns_tuple(self, addr):
        setUp(self, addr)
        row = {'location': 'test_bucket/TEST_BATCH/colors/sample_blue.jpg'}
        expected = (
            'e80dd1c34dbdc98521138eacc0e921683d8c9970a1f7cfe75bbfff56d5638238',
            'test_bucket/TEST_BATCH/colors/sample_blue.jpg'
        )
        checksum_and_path = get_checksum(self.gateway, row, 'sha256')
        assert checksum_and_path == expected
        tearDown(self)

    @patch('sys.stderr', new_callable=io.StringIO)
    def test_location_not_found_returns__None_and_displays_error(self, mock_err, addr):
        setUp(self, addr)
        row = {'location': 'not_a_location_in_database'}
        checksum_and_path = get_checksum(self.gateway, row, 'md5')
        assert checksum_and_path is None
        assert re.search(r"No accession record found for .*", mock_err.getvalue()) is not None
        tearDown(self)

    @patch('sys.stderr', new_callable=io.StringIO)
    def test_checksum_type_not_found_returns__None_and_displays_error(self, mock_err, addr):
        setUp(self, addr)
        row = {'location': 'test_bucket/TEST_BATCH/colors/sample_blue.jpg'}
        checksum_type = 'invalid_type'
        checksum_and_path = get_checksum(self.gateway, row, checksum_type)
        assert checksum_and_path is None
        assert re.search(r"No INVALID_TYPE checksum found .*", mock_err.getvalue()) is not None
        tearDown(self)

    def test_tuple_contains_destination_if_provided(self, addr):
        setUp(self, addr)
        row = {'location': 'test_bucket/TEST_BATCH/colors/sample_blue.jpg', 'destination': 'DESTINATION'}
        expected = ('85a929103d2f58ddfa8c8768eb6339ad', 'DESTINATION')
        checksum_and_path = get_checksum(self.gateway, row, 'md5')
        assert checksum_and_path == expected
        tearDown(self)
