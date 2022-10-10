import pytest

from argparse import Namespace
from patsy.commands.schema import Command
from patsy.commands.load import Load
from patsy.commands.export import Command as ExportCommand
from patsy.core.db_gateway import DbGateway
from patsy.model import Base
from sqlalchemy.schema import DropTable
from sqlalchemy.ext.compiler import compiles

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

    obj.args = Namespace()
    obj.args.database = addr
    obj.gateway = DbGateway(obj.args)
    Command.__call__(obj, obj.args, obj.gateway)
    obj.load = Load(obj.gateway)


def tearDown(obj):
    obj.gateway.close()
    Base.metadata.drop_all(obj.gateway.session.get_bind())


class TestExport:
    def test_export_aws_archiver(self, addr, tmpdir):
        # Load file into database
        try:
            setUp(self, addr)
            csv_file = 'tests/fixtures/load/colors_inventory-aws-archiver.csv'
            export_file = tmpdir.join("colors_inventory-preserve-export.csv")
            load_result = self.load.process_file(csv_file)
            assert load_result.rows_processed == 3
            assert load_result.batches_added == 1
            assert load_result.accessions_added == 3
            assert load_result.locations_added == 3
            assert len(load_result.errors) == 0

            self.gateway.session.commit()

            # Then export it
            self.args.batch = None
            self.args.output = export_file
            ExportCommand.__call__(self, self.args, self.gateway)

            # Then load it again and check for the same results
            self.load = Load(self.gateway)
            load_result = self.load.process_file(export_file)

            assert load_result.rows_processed == 3
            assert load_result.batches_added == 0
            assert load_result.accessions_added == 0
            assert load_result.locations_added == 0
            assert len(load_result.errors) == 0
        finally:
            tearDown(self)

    def test_export_preserve(self, addr, tmpdir):
        # Load
        try:
            setUp(self, addr)
            csv_file = 'tests/fixtures/load/colors_inventory-preserve.csv'
            export_file = tmpdir.join("colors_inventory-preserve-export.csv")
            load_result = self.load.process_file(csv_file)
            assert load_result.rows_processed == 3
            assert load_result.batches_added == 1
            assert load_result.accessions_added == 3
            assert load_result.locations_added == 0
            assert len(load_result.errors) == 0

            self.gateway.session.commit()

            # Export
            self.args.batch = None
            self.args.output = export_file
            ExportCommand.__call__(self, self.args, self.gateway)

            # Compare
            self.load = Load(self.gateway)
            load_result = self.load.process_file(export_file)
            assert load_result.rows_processed == 3
            assert load_result.batches_added == 0
            assert load_result.accessions_added == 0
            assert load_result.locations_added == 0
            assert len(load_result.errors) == 0
        finally:
            tearDown(self)
