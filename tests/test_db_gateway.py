import csv
import pytest

from argparse import Namespace
from patsy.commands.schema import Command
from patsy.core.db_gateway import DbGateway
from patsy.core.load import Load
from patsy.core.patsy_record import PatsyUtils
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
    test_db_files = [
        "tests/fixtures/db_gateway/colors_inventory.csv",
        "tests/fixtures/db_gateway/solar_system_inventory.csv"
    ]

    args = Namespace()
    args.database = addr
    obj.gateway = DbGateway(args)
    # schema = Schema(obj.gateway)
    # schema.create_schema()
    Command.__call__(obj, obj.gateway)
    for file in test_db_files:
        load = Load(obj.gateway)
        load.process_file(file)


def tearDown(obj):
    obj.gateway.close()
    Base.metadata.drop_all(obj.gateway.session.get_bind())


class TestDbGateway:
    def test_get_all_batches(self, addr):
        setUp(self, addr)
        batches = self.gateway.get_all_batches()
        assert len(batches) == 2
        batch_names = [batch.name for batch in batches]
        assert "TEST_COLORS" in batch_names
        assert "TEST_SOLAR_SYSTEM" in batch_names
        tearDown(self)

    def test_get_batch_by_name__batch_does_not_exist(self, addr):
        setUp(self, addr)
        batch = self.gateway.get_batch_by_name("NON_EXISTENT_BATCH")
        assert batch is None
        tearDown(self)

    def test_get_batch_by_name__batch_exists(self, addr):
        setUp(self, addr)
        batch = self.gateway.get_batch_by_name("TEST_COLORS")
        assert batch is not None
        assert batch.name == "TEST_COLORS"
        tearDown(self)

    def test_get_batch_records__batch_does_not_exist(self, addr):
        setUp(self, addr)
        patsy_records = self.gateway.get_batch_records("NON_EXISTENT_BATCH")
        assert len(patsy_records) == 0
        tearDown(self)

    def test_get_batch_records__batch_exists(self, addr):
        setUp(self, addr)
        patsy_records = self.gateway.get_batch_records("TEST_COLORS")
        expected_patsy_records = []
        with open("tests/fixtures/db_gateway/colors_inventory.csv") as f:
            reader = csv.DictReader(f, delimiter=',')
            for row in reader:
                expected_patsy_records.append(PatsyUtils.from_inventory_csv(row))

        assert len(expected_patsy_records) >= 0
        assert len(expected_patsy_records) == len(patsy_records)

        for p in patsy_records:
            assert p in expected_patsy_records

        tearDown(self)
