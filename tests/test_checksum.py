import io
import os
import tempfile
import unittest
import logging

from argparse import Namespace
from patsy.commands.checksum import Command, get_checksum
from patsy.core.schema import Schema
from patsy.core.db_gateway import DbGateway
from patsy.core.load import Load
from unittest.mock import patch
from patsy.model import Base

LOGGER = logging.getLogger('__name__')

from sqlalchemy.schema import DropTable
from sqlalchemy.ext.compiler import compiles

#https://stackoverflow.com/questions/38678336/sqlalchemy-how-to-implement-drop-table-cascade
#https://docs.sqlalchemy.org/en/14/core/compiler.html#changing-compilation-of-types
@compiles(DropTable, "postgresql")
def _compile_drop_table(element, compiler, **kwargs):
    return compiler.visit_drop_table(element) + " CASCADE"

class TestChecksumCommand(unittest.TestCase):
    def setUp(self):
        args = Namespace()
        #args.database = ":memory:"
        args.database = "postgresql+psycopg2://postgres:password@localhost:5432/postgres"
        self.gateway = DbGateway(args)
    
        #LOGGER.info("\n\n##########CREATING SCHEMA##########\n\n")
        schema = Schema(self.gateway)
        schema.create_schema()

        #LOGGER.info("\n\n##########LOADING##########\n\n")
        self.load = Load(self.gateway)

        #LOGGER.info("\n\n##########LOADING PROCESS FILE##########\n\n")
        csv_file = 'tests/fixtures/load/colors_inventory-aws-archiver.csv'
        self.load.process_file(csv_file)
        #LOGGER.info("\n\n##########FINISHED PROCESS FILE##########\n\n")

        self.checksum_command = Command()
        # Arguments passed to checksum.Command
        self.command_args = Namespace()
        self.command_args.location = None
        self.command_args.output_type = None
        self.command_args.output_file = None
        #LOGGER.info("\n\n##########FINISHED SETUP##########\n\n")

    @patch('sys.stdout', new_callable=io.StringIO)
    def test_location_arg(self, mock_out):
        self.command_args.location = ['test_bucket/TEST_BATCH/colors/sample_blue.jpg']

        # The call command will write to a file
        #LOGGER.info("\n\n##########CALLING CHECKSUM###########\n\n")
        self.checksum_command.__call__(self.command_args, self.gateway)
        
        # Compare the answer to what's written in file.
        #LOGGER.info("\n\n###########COMPARING###########\n\n")
        expected = '85a929103d2f58ddfa8c8768eb6339ad  test_bucket/TEST_BATCH/colors/sample_blue.jpg\n'
        self.assertEqual(expected, mock_out.getvalue())   

    @patch('sys.stdout', new_callable=io.StringIO)
    def test_output_type_arg(self, mock_out):
        self.command_args.location = ['test_bucket/TEST_BATCH/colors/sample_blue.jpg']
        self.command_args.output_type = 'sha1'
        
        #LOGGER.info("\n\n##########CALLING CHECKSUM###########\n\n")
        self.checksum_command.__call__(self.command_args, self.gateway)

        expected = '2fa953a48600e1aef0486b4b3a17c6100cfeef80  test_bucket/TEST_BATCH/colors/sample_blue.jpg\n'
        #LOGGER.info("\n\n###########COMPARING###########\n\n")
        self.assertEqual(expected, mock_out.getvalue())

    @patch('sys.stdout', new_callable=io.StringIO)
    def test_locations_file_arg(self, mock_out):
        with open('tests/fixtures/checksum/locations_file.csv') as f:
            self.command_args.locations_file = f

            #LOGGER.info("\n\n##########CALLING CHECKSUM###########\n\n")
            self.checksum_command.__call__(self.command_args, self.gateway)

            expected = '85a929103d2f58ddfa8c8768eb6339ad  test_bucket/TEST_BATCH/colors/sample_blue.jpg\n' \
                       '1041fd1cf84c71183db2d5d95942a41c  test_bucket/TEST_BATCH/colors/sample_red.jpg\n'
            #LOGGER.info("\n\n###########COMPARING###########\n\n")
            self.assertEqual(expected, mock_out.getvalue())

    def test_output_file_arg(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            output_filename = os.path.join(tmpdirname, 'test_output_file.csv')
            with open(output_filename, 'w') as output_file:
                self.command_args.location = ['test_bucket/TEST_BATCH/colors/sample_blue.jpg']
                self.command_args.output_file = output_file

                #LOGGER.info("\n\n##########CALLING CHECKSUM###########\n\n")
                self.checksum_command.__call__(self.command_args, self.gateway)

            #LOGGER.info("\n\n###########CHECKING SIZE###########\n\n")
            self.assertEqual(80, os.path.getsize(output_filename))
    
    def tearDown(self):
        self.gateway.close()
        Base.metadata.drop_all(self.gateway.session.get_bind())

class TestGetChecksum(unittest.TestCase):
    def setUp(self):
        args = Namespace()
        #args.database = ":memory:"
        args.database = "postgresql+psycopg2://postgres:password@localhost:5432/postgres"

        self.gateway = DbGateway(args)
        schema = Schema(self.gateway)
        schema.create_schema()
        self.load = Load(self.gateway)

        csv_file = 'tests/fixtures/load/colors_inventory-aws-archiver.csv'

        self.load.process_file(csv_file)

    def test_valid_row_and_md5_checksum__returns_tuple(self):
        row = {'location': 'test_bucket/TEST_BATCH/colors/sample_blue.jpg'}
        expected = ('85a929103d2f58ddfa8c8768eb6339ad', 'test_bucket/TEST_BATCH/colors/sample_blue.jpg')

        checksum_and_path = get_checksum(self.gateway, row, 'md5')
        self.assertEqual(expected, checksum_and_path)

    def test_valid_row_and_sha1_checksum__returns_tuple(self):
        row = {'location': 'test_bucket/TEST_BATCH/colors/sample_blue.jpg'}
        expected = ('2fa953a48600e1aef0486b4b3a17c6100cfeef80', 'test_bucket/TEST_BATCH/colors/sample_blue.jpg')

        checksum_and_path = get_checksum(self.gateway, row, 'sha1')
        self.assertEqual(expected, checksum_and_path)

    def test_gvalid_row_and_sha256_checksum__returns_tuple(self):
        row = {'location': 'test_bucket/TEST_BATCH/colors/sample_blue.jpg'}
        expected = (
            'e80dd1c34dbdc98521138eacc0e921683d8c9970a1f7cfe75bbfff56d5638238',
            'test_bucket/TEST_BATCH/colors/sample_blue.jpg'
        )

        checksum_and_path = get_checksum(self.gateway, row, 'sha256')
        self.assertEqual(expected, checksum_and_path)

    @patch('sys.stderr', new_callable=io.StringIO)
    def test_location_not_found_returns__None_and_displays_error(self, mock_err):
        row = {'location': 'not_a_location_in_database'}

        checksum_and_path = get_checksum(self.gateway, row, 'md5')
        self.assertIsNone(checksum_and_path)
        self.assertRegex(mock_err.getvalue(), r"No accession record found for .*")

    @patch('sys.stderr', new_callable=io.StringIO)
    def test_checksum_type_not_found_returns__None_and_displays_error(self, mock_err):
        row = {'location': 'test_bucket/TEST_BATCH/colors/sample_blue.jpg'}
        checksum_type = 'invalid_type'

        checksum_and_path = get_checksum(self.gateway, row, checksum_type)
        self.assertIsNone(checksum_and_path)
        self.assertRegex(mock_err.getvalue(), r"No INVALID_TYPE checksum found .*")

    def test_tuple_contains_destination_if_provided(self):
        row = {'location': 'test_bucket/TEST_BATCH/colors/sample_blue.jpg', 'destination': 'DESTINATION'}
        expected = ('85a929103d2f58ddfa8c8768eb6339ad', 'DESTINATION')

        checksum_and_path = get_checksum(self.gateway, row, 'md5')
        self.assertEqual(expected, checksum_and_path)

    def tearDown(self):
        self.gateway.close()
        Base.metadata.drop_all(self.gateway.session.get_bind())