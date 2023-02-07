import os
import json
import pytest
import httpretty

from argparse import Namespace
from patsy.core.sync import Sync
from patsy.core.load import Load
from patsy.model import Base, Accession
from patsy.commands.schema import Command
from patsy.core.db_gateway import DbGateway

from sqlalchemy.schema import DropTable
from sqlalchemy.ext.compiler import compiles


@pytest.fixture
def addr(request):
    return request.config.getoption('--base-url')


@compiles(DropTable, "postgresql")
def _compile_drop_table(element, compiler, **kwargs):
    return compiler.visit_drop_table(element) + " CASCADE"


def setUp(obj, addr, csv_file: str = 'tests/fixtures/sync/Archive149.csv', load: bool = False, head: bool = False):
    if head:
        headers = {
            'X-Pharos-API-User': os.getenv('X_PHAROS_NAME'),  # os.getenv('X_PHAROS_NAME'),
            'X-Pharos-API-Key': os.getenv('X_PHAROS_KEY'),  # os.getenv('X_PHAROS_KEY')
        }
    else:
        headers = {
            'X-Pharos-API-User': "not being used",  # os.getenv('X_PHAROS_NAME'),
            'X-Pharos-API-Key': "not being used",  # os.getenv('X_PHAROS_KEY')
        }

    args = Namespace(database=addr)

    obj.gateway = DbGateway(args)
    obj.sync = Sync(obj.gateway, headers)

    if load:
        Base.metadata.drop_all(obj.gateway.session.get_bind())
        Command.__call__(obj, args, obj.gateway)
        obj.load = Load(obj.gateway)
        obj.load.process_file(csv_file)


def tearDown(obj, tear: bool = True):
    obj.gateway.close()
    if tear:
        Base.metadata.drop_all(obj.gateway.session.get_bind())


class TestSync:
    def test_parse_name(self, addr):
        try:
            setUp(self, addr)
            assert self.sync.parse_name('archive0001') == 'Archive001'
            assert self.sync.parse_name('archive0010') == 'Archive010'
            assert self.sync.parse_name('archive0100') == 'Archive100'
            assert self.sync.parse_name('archive1000') == 'Archive1000'

            assert self.sync.parse_name('archive0001a') == 'Archive001'
            assert self.sync.parse_name('archive0010a') == 'Archive010'
            assert self.sync.parse_name('archive0100a') == 'Archive100'
            assert self.sync.parse_name('archive1000a') == 'Archive1000'

        finally:
            tearDown(self)

    def test_check_path(self, addr):
        try:
            setUp(self, addr, load=True)
            with open('tests/fixtures/sync/archive0149.json') as f:
                files = json.load(f)

            accessions = self.gateway.session.query(Accession) \
                             .filter(Accession.batch_id == 1) \
                             .all()

            identifiers = list(map(lambda x: x.get('identifier'), files))

            for id in identifiers:
                assert self.sync.check_path(id, accessions) is not None
        finally:
            tearDown(self)

    # The baglist.json file has 30 bags
    # Only 1 should be in the database
    def test_batches(self, addr):
        try:
            setUp(self, addr, load=True)

            with open('tests/fixtures/sync/baglist.json') as f:
                bags = json.load(f)

            amount_in_db = len(list(filter(lambda x: self.sync.check_batch(x), bags)))
            amount_not_in_db = len(list(filter(lambda x: not self.sync.check_batch(x), bags)))
            assert amount_in_db == 1
            assert amount_not_in_db == 29

        finally:
            tearDown(self)

    def test_check_locations_archive149(self, addr):
        try:
            setUp(self, addr, load=True)

            with open('tests/fixtures/sync/archive0149.json') as f:
                files = json.load(f)

            accessions = self.gateway.session.query(Accession) \
                             .with_entities(Accession.relpath, Accession.id) \
                             .filter(Accession.batch_id == 1) \
                             .all()

            identifiers = list(map(lambda x: x.get('identifier'), files))
            self.sync.check_or_add_files(identifiers, accessions)

            files_processed = self.sync.sync_results.files_processed
            locations_added = self.sync.sync_results.locations_added

            assert files_processed == 12
            assert locations_added == 12
            assert not self.sync.sync_results.files_not_found

        finally:
            tearDown(self)

    def test_add_locations_archive149(self, addr):
        try:
            setUp(self, addr, load=True)

            with open('tests/fixtures/sync/archive0149.json') as f:
                files = json.load(f)

            accessions = self.gateway.session.query(Accession) \
                             .filter(Accession.batch_id == 1) \
                             .all()

            identifiers = list(map(lambda x: x.get('identifier'), files))
            self.sync.check_or_add_files(identifiers, accessions, True)

            files_processed = self.sync.sync_results.files_processed
            locations_added = self.sync.sync_results.locations_added

            assert files_processed == 12
            assert locations_added == 12
            assert not self.sync.sync_results.files_not_found

        finally:
            tearDown(self)

    def test_catch_duplicates_archive149(self, addr):
        try:
            setUp(self, addr, load=True)

            with open('tests/fixtures/sync/archive0149.json') as f:
                files = json.load(f)

            accessions = self.gateway.session.query(Accession) \
                             .filter(Accession.batch_id == 1) \
                             .all()

            identifiers = list(map(lambda x: x.get('identifier'), files))
            self.sync.check_or_add_files(identifiers, accessions, True)
            self.gateway.session.commit()
            self.sync.check_or_add_files(identifiers, accessions, True)

            files_processed = self.sync.sync_results.files_processed
            locations_added = self.sync.sync_results.locations_added
            duplicate_files = self.sync.sync_results.duplicate_files

            assert files_processed == 24
            assert locations_added == 12
            assert duplicate_files == 12
            assert not self.sync.sync_results.files_not_found

        finally:
            tearDown(self)

    # Using a modified CSV file which uses the same path but with the
    # Storage provider changed to AWS
    def test_add_second_locations_archive149(self, addr):
        try:
            setUp(self, addr, csv_file='tests/fixtures/sync/Archive149_Alternate.csv', load=True)

            with open('tests/fixtures/sync/archive0149.json') as f:
                files = json.load(f)

            accessions = self.gateway.session.query(Accession) \
                             .filter(Accession.batch_id == 1) \
                             .all()

            identifiers = list(map(lambda x: x.get('identifier'), files))
            self.sync.check_or_add_files(identifiers, accessions, True)

            files_processed = self.sync.sync_results.files_processed
            locations_added = self.sync.sync_results.locations_added

            assert files_processed - locations_added == 0
            assert not self.sync.sync_results.files_not_found

        finally:
            tearDown(self)

    @httpretty.activate
    def test_aptrust_without_time(self, addr):
        try:
            setUp(self, addr, csv_file='tests/fixtures/sync/Archive149_Alternate.csv', load=True)

            # Create mock urls for getting the bags
            # and getting the files from archive0149
            # since its in the database
            mock_object_url = 'https://repo.aptrust.org/member-api/v3/objects?per_page=10000'
            mock_file_url = 'https://repo.aptrust.org/member-api/v3/files?intellectual_object_id=246810&per_page=3986'

            with open('tests/fixtures/sync/bags.json') as f:
                body1 = f.read()

            with open('tests/fixtures/sync/archive149_request.json') as f:
                body2 = f.read()

            httpretty.register_uri(
                httpretty.GET,
                mock_object_url,
                body=body1,
                status=200,
                content_type="application/json"
            )

            httpretty.register_uri(
                httpretty.GET,
                mock_file_url,
                body=body2,
                status=200,
                content_type="application/json"
            )

            sync_result = self.sync.process()

            assert len(sync_result.files_not_found) == 0
            assert sync_result.files_processed == 12
            assert sync_result.locations_added == 12
            assert sync_result.duplicate_files == 0

        finally:
            tearDown(self)

    @httpretty.activate
    def test_aptrust_specify_timebefore(self, addr):
        try:
            setUp(self, addr, csv_file='tests/fixtures/sync/Archive149_Alternate.csv', load=True, head=True)

            # Create mock urls for getting the bags,
            # not for getting the files though, because
            # since the bag isn't in the database, the files
            # wont be processed.
            mock_url = 'https://repo.aptrust.org/member-api/v3/objects?per_page=10000&created_at__lteq=2022-08-05'

            with open('tests/fixtures/sync/bags_before_aug5.json') as f:
                body1 = f.read()

            httpretty.register_uri(
                httpretty.GET,
                mock_url,
                body=body1,
                status=200,
                content_type="application/json"
            )

            sync_result = self.sync.process(created_at__lteq='2022-08-05')

            # print(sync_result.files_not_found)
            # print(sync_result.files_processed)
            # print(sync_result.locations_added)
            # print(sync_result.duplicate_files)

            # Since archive 132 isn't in the test database,
            # none of the files in the batch should be processed,
            # so the results should all be 0
            assert len(sync_result.files_not_found) == 0
            assert sync_result.files_processed == 0
            assert sync_result.locations_added == 0
            assert sync_result.duplicate_files == 0

        finally:
            tearDown(self)

    @httpretty.activate
    def test_aptrust_specify_timeafter(self, addr):
        try:
            setUp(self, addr, csv_file='tests/fixtures/sync/Archive149_Alternate.csv', load=True, head=True)

            # Create mock urls for just getting the bags,
            # same as above
            mock_url = 'https://repo.aptrust.org/member-api/v3/objects?created_at__gteq=2022-10-01'

            with open('tests/fixtures/sync/bags_after_oct.json') as f:
                body1 = f.read()

            httpretty.register_uri(
                httpretty.GET,
                mock_url,
                body=body1,
                status=200,
                content_type="application/json"
            )

            sync_result = self.sync.process(created_at__gteq='2022-10-01')

            assert len(sync_result.files_not_found) == 0
            assert sync_result.files_processed == 0
            assert sync_result.locations_added == 0
            assert sync_result.duplicate_files == 0

        finally:
            tearDown(self)

    @httpretty.activate
    def test_aptrust_specify_both(self, addr):
        try:
            setUp(self, addr, csv_file='tests/fixtures/sync/Archive149_Alternate.csv', load=True, head=True)

            # Create mock urls for just getting the bags,
            # same as above
            mock_url = \
                'https://repo.aptrust.org/member-api/v3/objects?created_at__gteq=2022-10-01&created_at__lteq=2022-11-01'

            with open('tests/fixtures/sync/bags_between_oct_nov.json') as f:
                body1 = f.read()

            httpretty.register_uri(
                httpretty.GET,
                mock_url,
                body=body1,
                status=200,
                content_type="application/json"
            )

            sync_result = self.sync.process(created_at__gteq='2022-10-01', created_at__lteq='2022-11-01')

            assert len(sync_result.files_not_found) == 0
            assert sync_result.files_processed == 0
            assert sync_result.locations_added == 0
            assert sync_result.duplicate_files == 0

        finally:
            tearDown(self)
