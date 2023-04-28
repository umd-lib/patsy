from argparse import ArgumentParser, Namespace
from deepdiff import DeepDiff
from importlib import import_module
from patsy.commands.load import Command as LoadCommand
from patsy.core.db_gateway import DbGateway
from patsy.core.patsy_record import PatsyRecord
from patsy.core.update import Update, UpdateArgs, UpdateResult
from tests import clear_database


def create_valid_update_args() -> UpdateArgs:
    """
    Returns a valid UpdateArgs object, with references to the test fixtures.

    Individual tests can override the settings for their particular needs.
    """

    return UpdateArgs(
        dry_run=False, skip_existing=False, verbose=False, batch_name='TEST_BATCH1',
        db_compare_column='relpath', db_target_column='relpath',
        csv_compare_column='relpath_old', csv_update_column='relpath_new',
        file='tests/fixtures/update/relpath_updates.csv')


def setUp(obj, gateway: DbGateway):
    obj.gateway = gateway
    obj.update = Update(obj.gateway)

    test_db_files = [
        "tests/fixtures/update/database-test-entries.csv",
    ]

    args = Namespace()
    for file in test_db_files:
        args.file = file
        LoadCommand.__call__(obj, args, obj.gateway)

    gateway.session.commit()
    obj.update_args = create_valid_update_args()


def tearDown(obj):
    clear_database(obj)


class TestUpdateArgs():
    def test_from_cli_args(self):
        parser = ArgumentParser(prog='patsy')
        subparsers = parser.add_subparsers(title='commands')
        module = import_module('patsy.commands.update', 'update')
        module.configure_cli(subparsers)

        test_args = 'update --dry-run --skip-existing --batch BATCH ' \
                    '--db-compare-column DB_COMPARE_COLUMN --db-target-column DB_TARGET_COLUMN ' \
                    '--csv-compare-value CSV_COMPARE_VALUE --csv-update-value CSV_UPDATE_VALUE ' \
                    'FILE.CSV'.split(' ')

        args = parser.parse_args(test_args)

        update_args = UpdateArgs.from_cli_args(args)
        assert update_args.dry_run is True
        assert update_args.skip_existing is True
        assert update_args.db_compare_column == 'DB_COMPARE_COLUMN'
        assert update_args.db_target_column == 'DB_TARGET_COLUMN'
        assert update_args.csv_compare_column == 'CSV_COMPARE_VALUE'
        assert update_args.csv_update_column == 'CSV_UPDATE_VALUE'
        assert update_args.file == 'FILE.CSV'

    def test_validate__returns_error_when_batch_not_found(self, db_gateway):
        try:
            setUp(self, db_gateway)
            self.update_args.batch_name = 'NON-EXISTENT_BATCH'

            errors = self.update_args.validate(db_gateway)
            assert len(errors) == 1
            assert errors[0] == "Batch named 'NON-EXISTENT_BATCH' was not found."
        finally:
            tearDown(self)

    def test_validate__returns_error_when_db_compare_column_not_in_accessions(self, db_gateway):
        try:
            setUp(self, db_gateway)
            self.update_args.db_compare_column = 'not_a_column'

            errors = self.update_args.validate(db_gateway)
            assert len(errors) == 1
            assert errors[0] == "Database compare column 'not_a_column' does not exist for accessions."
        finally:
            tearDown(self)

    def test_validate__returns_error_when_db_target_column_not_in_accessions(self, db_gateway):
        try:
            setUp(self, db_gateway)
            self.update_args.db_target_column = 'not_a_column'

            errors = self.update_args.validate(db_gateway)
            assert len(errors) == 1
            assert errors[0] == "Database target column 'not_a_column' does not exist for accessions."
        finally:
            tearDown(self)

    def test_validate__returns_error_when_file_cannot_be_accessed(self, db_gateway):
        try:
            setUp(self, db_gateway)
            self.update_args.file = 'file_does_not_exist.csv'

            errors = self.update_args.validate(db_gateway)
            assert len(errors) == 1
            assert errors[0].startswith("Could not access 'file_does_not_exist.csv'.")
        finally:
            tearDown(self)

    def test_validate__returns_error_when_csv_compare_column_not_csv_file(self, db_gateway):
        try:
            setUp(self, db_gateway)
            self.update_args.csv_compare_column = 'not_a_column'

            errors = self.update_args.validate(db_gateway)
            assert len(errors) == 1
            assert errors[0] == \
                f"CSV compare column 'not_a_column' not found in '{self.update_args.file}'."
        finally:
            tearDown(self)

    def test_validate__returns_error_when_csv_update_column_not_csv_file(self, db_gateway):
        try:
            setUp(self, db_gateway)
            self.update_args.csv_update_column = 'not_a_column'

            errors = self.update_args.validate(db_gateway)
            assert len(errors) == 1
            assert errors[0] == \
                f"CSV update column 'not_a_column' not found in '{self.update_args.file}'."
        finally:
            tearDown(self)

    def test_validate__returns_no_errors_when_update_args_are_valid(self, db_gateway):
        try:
            setUp(self, db_gateway)

            errors = self.update_args.validate(db_gateway)
            assert len(errors) == 0
        finally:
            tearDown(self)


class TestUpdate():
    def test_update__returns_errors_update_args_are_invalid(self, db_gateway):
        try:
            setUp(self, db_gateway)

            self.update_args.batch_name = 'NON-EXISTENT_BATCH'
            self.update_args.db_compare_column = 'not_a_db_column'
            self.update_args.csv_update_column = 'not_a_csv_column'

            result = self.update.update(self.update_args)
            assert result.has_errors() is True
            assert len(result.errors) == 3
            assert "Batch named 'NON-EXISTENT_BATCH' was not found." in result.errors
            assert "Database compare column 'not_a_db_column' does not exist for accessions." in result.errors
            assert f"CSV update column 'not_a_csv_column' not found in '{self.update_args.file}'."\
                in result.errors
        finally:
            tearDown(self)

    def test_update__skips_update_when_dry_run(self, db_gateway: DbGateway):
        try:
            setUp(self, db_gateway)
            self.update_args.dry_run = True

            pre_test_patsy_records = DatabaseTestUtils.get_all_db_records(db_gateway)

            result = self.update.update(self.update_args)
            assert result.csv_rows_processed == 2
            assert result.db_rows_updated == 2

            post_test_patsy_records = DatabaseTestUtils.get_all_db_records(db_gateway)

            assert DatabaseTestUtils.has_no_differences(pre_test_patsy_records, post_test_patsy_records)
        finally:
            tearDown(self)

    def test_update(self, db_gateway: DbGateway):
        try:
            setUp(self, db_gateway)

            pre_test_patsy_records = DatabaseTestUtils.get_all_db_records(db_gateway)

            result = self.update.update(self.update_args)
            assert result.csv_rows_processed == 2
            assert result.db_rows_updated == 2

            post_test_patsy_records = DatabaseTestUtils.get_all_db_records(db_gateway)

            assert DatabaseTestUtils.differences_count(pre_test_patsy_records, post_test_patsy_records) == 2
        finally:
            tearDown(self)

    def test_update__makes_no_updates_when_run_a_second_time(self, db_gateway: DbGateway):
        try:
            setUp(self, db_gateway)

            # First run
            pre_run1_patsy_records = DatabaseTestUtils.get_all_db_records(db_gateway)

            result = self.update.update(self.update_args)
            assert result.csv_rows_processed == 2
            assert result.db_rows_updated == 2

            post_run1_patsy_records = DatabaseTestUtils.get_all_db_records(db_gateway)

            assert DatabaseTestUtils.differences_count(pre_run1_patsy_records, post_run1_patsy_records) == 2

            # Second run
            self.update = Update(db_gateway)
            result = self.update.update(self.update_args)
            assert result.csv_rows_processed == 2
            assert result.db_rows_updated == 0

            post_run2_patsy_records = DatabaseTestUtils.get_all_db_records(db_gateway)

            assert DatabaseTestUtils.has_no_differences(post_run1_patsy_records, post_run2_patsy_records)
        finally:
            tearDown(self)

    def test_update__does_not_update_existing_values_when_skip_existing_is_enabled(self, db_gateway: DbGateway):
        try:
            setUp(self, db_gateway)
            self.update_args.file = 'tests/fixtures/update/update_sha256_based_on_md5.csv'
            self.update_args.skip_existing = True
            self.update_args.db_compare_column = 'md5'
            self.update_args.db_target_column = 'sha256'
            self.update_args.csv_compare_column = 'md5sum'
            self.update_args.csv_update_column = 'sha256sum'

            pre_test_patsy_records = DatabaseTestUtils.get_all_db_records(db_gateway)

            result = self.update.update(self.update_args)
            assert result.csv_rows_processed == 2
            assert result.db_rows_updated == 0

            post_test_patsy_records = DatabaseTestUtils.get_all_db_records(db_gateway)

            assert DatabaseTestUtils.has_no_differences(pre_test_patsy_records, post_test_patsy_records)
        finally:
            tearDown(self)

    def test_update__does_not_update_field_if_current_db_value_matches_new_csv_value(self, db_gateway: DbGateway):
        # This is mainly a test for a "generic" update, where the comparison
        # column is different from the target column.
        #
        # The difference betweeen this test, and tests for the "skip existing"
        # flag is that we don't want to count as an update the situation where
        # the target database field already has the updated value from the
        # CSV fields.
        try:
            setUp(self, db_gateway)
            self.update_args.file = 'tests/fixtures/update/update_sha256_based_on_md5.csv'
            self.update_args.db_compare_column = 'md5'
            self.update_args.db_target_column = 'sha256'
            self.update_args.csv_compare_column = 'md5sum'
            self.update_args.csv_update_column = 'sha256sum'

            # Edit first accession so "sha256" field matches the CSV value we are updating to.
            batch = db_gateway.get_batch_by_name('TEST_BATCH1')
            patsy_records = db_gateway.get_batch_records('TEST_BATCH1')
            accession0 = db_gateway.find_or_create_accession(batch_id=batch.id, patsy_record=patsy_records[0])
            accession0.sha256 = 'UPDATED_SHA256_sample_blue.jpg'
            db_gateway.session.commit()

            result = self.update.update(self.update_args)
            assert result.csv_rows_processed == 2
            assert result.db_rows_updated == 1
        finally:
            tearDown(self)

    def test_update__skip_existing_updates_null_or_empty_values(self, db_gateway: DbGateway):
        try:
            setUp(self, db_gateway)
            self.update_args.file = 'tests/fixtures/update/update_sha256_based_on_md5.csv'
            self.update_args.skip_existing = True
            self.update_args.db_compare_column = 'md5'
            self.update_args.db_target_column = 'sha256'
            self.update_args.csv_compare_column = 'md5sum'
            self.update_args.csv_update_column = 'sha256sum'

            # Set up batch records with null/empty "sha256" field
            batch = db_gateway.get_batch_by_name('TEST_BATCH1')
            patsy_records = db_gateway.get_batch_records('TEST_BATCH1')
            accession0 = db_gateway.find_or_create_accession(batch_id=batch.id, patsy_record=patsy_records[0])
            accession0.sha256 = None
            accession1 = db_gateway.find_or_create_accession(batch_id=batch.id, patsy_record=patsy_records[1])
            accession1.sha256 = ''
            db_gateway.session.commit()

            pre_test_patsy_records = DatabaseTestUtils.get_all_db_records(db_gateway)

            result = self.update.update(self.update_args)
            assert result.csv_rows_processed == 2
            assert result.db_rows_updated == 2

            post_test_patsy_records = DatabaseTestUtils.get_all_db_records(db_gateway)

            assert DatabaseTestUtils.differences_count(pre_test_patsy_records, post_test_patsy_records) == 2
        finally:
            tearDown(self)

    def test_no_verbose_logging(self, db_gateway, caplog):
        try:
            setUp(self, db_gateway)
            self.update_args.verbose = False

            result = self.update.update(self.update_args)
            assert result.csv_rows_processed == 2
            assert result.db_rows_updated == 2

            assert 'Updating accession id: 1' not in caplog.text
            assert 'Updating accession id: 2' not in caplog.text
        finally:
            tearDown(self)

    def test_verbose_logging(self, db_gateway, caplog):
        try:
            setUp(self, db_gateway)
            self.update_args.verbose = True

            result = self.update.update(self.update_args)
            assert result.csv_rows_processed == 2
            assert result.db_rows_updated == 2

            assert 'Updating accession id: 1, old_value: colors/sample_blue.jpg, new_value: new_dir/colors/UPDATED_blue.jpg' in caplog.text  # noqa
            assert 'Updating accession id: 2, old_value: colors/sample_red.jpg, new_value: new_dir/colors/UPDATED_red.jpg' in caplog.text  # noqa
        finally:
            tearDown(self)


class TestUpdateResult:
    def test_repr(self):
        update_result = UpdateResult()
        update_result.csv_rows_processed = 2
        update_result.db_rows_updated = 1
        assert str(update_result) == "<UpdateResult(csv_rows_processed='2',db_rows_updated='1',errors='[]')>"


class DatabaseTestUtils:
    @classmethod
    def has_no_differences(cls, db_records1: list[PatsyRecord], db_records2: list[PatsyRecord]) -> bool:
        """Returns True if there are not differences between the two lists, False otherwise"""
        return len(DeepDiff(db_records1, db_records2)) == 0

    @classmethod
    def differences_count(cls, db_records1: list[PatsyRecord], db_records2: list[PatsyRecord]) -> int:
        """Returns the number of records that differ between the two lists"""
        # A field changing from null recorded by DeepDiff as a type change, not
        # a value change, so need to count both type and value changes
        diff = DeepDiff(db_records1, db_records2)
        num_type_changes = len(diff.get('type_changes', {}))
        num_values_changed = len(diff.get('values_changed', {}))
        return num_type_changes + num_values_changed

    @classmethod
    def get_all_db_records(cls, db_gateway: DbGateway) -> list[PatsyRecord]:
        batches = db_gateway.get_all_batches()
        patsy_records = []
        for batch in batches:
            patsy_records.extend(db_gateway.get_batch_records(batch.name))

        return patsy_records
