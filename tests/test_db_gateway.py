import csv

from argparse import Namespace
from patsy.commands.load import Command as LoadCommand
from patsy.core.patsy_record import PatsyRecord, PatsyUtils
from patsy.model import StorageProvider
from tests import clear_database


def setUp(obj, gateway):
    obj.gateway = gateway

    args = Namespace()

    test_db_files = [
        "tests/fixtures/db_gateway/colors_inventory.csv",
        "tests/fixtures/db_gateway/solar_system_inventory.csv"
    ]

    for file in test_db_files:
        args.file = file
        LoadCommand.__call__(obj, args, obj.gateway)


def tearDown(obj):
    clear_database(obj)


class TestDbGateway:
    def test_get_all_batches(self, db_gateway):
        try:
            setUp(self, db_gateway)
            batches = self.gateway.get_all_batches()
            assert len(batches) == 2
            batch_names = [batch.name for batch in batches]
            assert "TEST_COLORS" in batch_names
            assert "TEST_SOLAR_SYSTEM" in batch_names
        finally:
            tearDown(self)

    def test_get_batch_by_name__batch_does_not_exist(self, db_gateway):
        try:
            setUp(self, db_gateway)
            batch = self.gateway.get_batch_by_name("NON_EXISTENT_BATCH")
            assert batch is None
        finally:
            tearDown(self)

    def test_get_batch_by_name__batch_exists(self, db_gateway):
        try:
            setUp(self, db_gateway)
            batch = self.gateway.get_batch_by_name("TEST_COLORS")
            assert batch is not None
            assert batch.name == "TEST_COLORS"
        finally:
            tearDown(self)

    def test_get_batch_records__batch_does_not_exist(self, db_gateway):
        try:
            setUp(self, db_gateway)
            patsy_records = self.gateway.get_batch_records("NON_EXISTENT_BATCH")
            assert len(patsy_records) == 0
        finally:
            tearDown(self)

    def test_get_batch_records__batch_exists(self, db_gateway):
        try:
            setUp(self, db_gateway)
            patsy_records = self.gateway.get_batch_records("TEST_COLORS")
            expected_patsy_records = []
            expected_csv = []
            with open("tests/fixtures/db_gateway/colors_inventory.csv") as f:
                reader = csv.DictReader(f, delimiter=',')
                for row in reader:
                    row_record = PatsyUtils.from_inventory_csv(row)
                    expected_patsy_records.append(row_record)
                    expected_csv.append(PatsyUtils.to_csv(row_record))

            assert len(expected_patsy_records) >= 0
            assert len(expected_patsy_records) == len(patsy_records)

            for p in patsy_records:
                record_csv = PatsyUtils.to_csv(p)
                assert p in expected_patsy_records
                assert record_csv in expected_csv

        finally:
            tearDown(self)

    def test_find_or_create_storage_provider__returns_None_if_no_storage_provider_in_patsy_record(self, db_gateway):
        try:
            setUp(self, db_gateway)
            patsy_record = PatsyRecord()
            assert self.gateway.find_or_create_storage_provider(patsy_record) is None
        finally:
            tearDown(self)

    def test_find_or_create_storage_provider__creates_storage_provider_when_does_not_exists(self, db_gateway):
        try:
            setUp(self, db_gateway)
            patsy_record = PatsyRecord()
            patsy_record.storage_provider = 'TEST_STORAGE_PROVIDER'

            # Verify 'TEST_STORAGE_PROVIDER' doesn't exist
            storage_providers = db_gateway.session.query(StorageProvider).filter(
                StorageProvider.name == patsy_record.storage_provider
            ).all()
            assert len(storage_providers) == 0

            storage_provider = self.gateway.find_or_create_storage_provider(patsy_record)
            assert storage_provider.name == 'TEST_STORAGE_PROVIDER'
        finally:
            tearDown(self)

    def test_find_or_create_storage_provider__returns_storage_provider_when_exists(self, db_gateway):
        try:
            setUp(self, db_gateway)
            patsy_record = PatsyRecord()
            patsy_record.storage_provider = 'TEST_STORAGE_PROVIDER'

            self.gateway.find_or_create_storage_provider(patsy_record)

            # Verify 'TEST_STORAGE_PROVIDER' exists
            storage_providers = db_gateway.session.query(StorageProvider).filter(
                StorageProvider.name == patsy_record.storage_provider
            ).all()
            assert len(storage_providers) == 1

            storage_provider = self.gateway.find_or_create_storage_provider(patsy_record)
            assert storage_provider.name == 'TEST_STORAGE_PROVIDER'

            # Verify 'TEST_STORAGE_PROVIDER' exists
            storage_providers = db_gateway.session.query(StorageProvider).filter(
                StorageProvider.name == patsy_record.storage_provider
            ).all()
            assert len(storage_providers) == 1
        finally:
            tearDown(self)

    def test_find_or_create_location__returns_None_if_no_storage_location_in_patsy_record(self, db_gateway):
        try:
            setUp(self, db_gateway)
            patsy_record = PatsyRecord()
            patsy_record.storage_provider == 'TEST_STORAGE_PROVIDER'

            location = self.gateway.find_or_create_location(patsy_record)
            assert location is None
        finally:
            tearDown(self)

    def test_find_or_create_location__returns_None_if_no_storage_provider_in_patsy_record(self, db_gateway):
        try:
            setUp(self, db_gateway)
            patsy_record = PatsyRecord()
            patsy_record.storage_location == 'TEST_STORAGE_LOCATION'

            location = self.gateway.find_or_create_location(patsy_record)
            assert location is None
        finally:
            tearDown(self)

    def test_find_or_create_location__returns_location_when_storage_location_and_provider_are_set(self, db_gateway):
        try:
            setUp(self, db_gateway)
            patsy_record = PatsyRecord()
            patsy_record.storage_location = 'TEST_STORAGE_LOCATION'
            patsy_record.storage_provider = 'TEST_STORAGE_PROVIDER'

            location = self.gateway.find_or_create_location(patsy_record)
            assert location.storage_location == 'TEST_STORAGE_LOCATION'
            assert location.storage_provider.name == 'TEST_STORAGE_PROVIDER'
        finally:
            tearDown(self)

    def test_find_or_create_location__returns_location_when_storage_provider_exists(self, db_gateway):
        try:
            setUp(self, db_gateway)
            patsy_record = PatsyRecord()
            patsy_record.storage_location = 'TEST_STORAGE_LOCATION'
            patsy_record.storage_provider = 'TEST_STORAGE_PROVIDER'
            self.gateway.find_or_create_storage_provider(patsy_record)

            location = self.gateway.find_or_create_location(patsy_record)
            assert location.storage_location == 'TEST_STORAGE_LOCATION'
            assert location.storage_provider.name == 'TEST_STORAGE_PROVIDER'
        finally:
            tearDown(self)
