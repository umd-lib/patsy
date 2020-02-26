import io
import patsy.database
from sqlalchemy import create_engine
from patsy.model import Base
from patsy.perfect_matches import find_perfect_matches
from patsy.transfer_matches import find_transfer_matches
from patsy.altered_md5_matches import find_altered_md5_matches
import unittest
from patsy.model import Accession
from .utils import AccessionBuilder, RestoreBuilder, TransferBuilder, create_perfect_match
from patsy.aws_manifest import find_untransferred_accessions, generate_manifest_entries, output_manifest_entries

Session = patsy.database.Session


class TestAwsManifest(unittest.TestCase):
    def setUp(self):
        engine = create_engine('sqlite:///:memory:')
        Session.configure(bind=engine)
        Base.metadata.create_all(engine)

        session = Session()

        self.transferred_accession = AccessionBuilder().set_batch("transferred_batch").build()
        self.transferred_restore = create_perfect_match(self.transferred_accession)
        self.transfer = TransferBuilder().set_filepath(self.transferred_restore.filepath).build()

        session.add(self.transferred_accession)
        session.add(self.transferred_restore)
        session.add(self.transfer)

        self.untransferred_accession = AccessionBuilder().set_batch("untransferred_batch").build()
        self.untransferred_restore = create_perfect_match(self.untransferred_accession)

        session.add(self.untransferred_accession)
        session.add(self.untransferred_restore)

        find_perfect_matches(session, [self.transferred_accession, self.untransferred_accession])
        find_transfer_matches(session, [self.transfer])
        session.commit()

        self.assertEqual(1, len(self.transferred_accession.perfect_matches))
        self.assertEqual(1, len(self.transferred_restore.transfers))
        self.assertEqual(1, len(self.untransferred_accession.perfect_matches))
        self.assertEqual(0, len(self.untransferred_restore.transfers))

    def test_find_untransferred_accessions_with_None_batch(self):
        session = Session()

        untransferred_accessions = find_untransferred_accessions(session, None)
        self.assertEqual([], untransferred_accessions)

    def test_find_untransferred_accessions_with_invalid_batch(self):
        session = Session()

        batch = "invalid_batch"
        untransferred_accessions = find_untransferred_accessions(session, batch)
        self.assertEqual([], untransferred_accessions)

    def test_find_untransferred_accessions_with_batch_all_accessions_transferred(self):
        session = Session()

        batch = "transferred_batch"
        untransferred_accessions = find_untransferred_accessions(session, batch)
        self.assertEqual([], untransferred_accessions)

    def test_find_untransferred_accessions_with_batch_accessions_not_transferred(self):
        session = Session()

        batch = "untransferred_batch"
        untransferred_accessions = find_untransferred_accessions(session, batch)
        self.assertEqual(1, len(untransferred_accessions))
        self.assertEqual(self.untransferred_accession.id, untransferred_accessions[0].id)

    def test_find_untransferred_accessions_with_mixed_batch(self):
        session = Session()

        # Set up a batch with some accessions transferred, and others untransferred

        mixed_transferred_accession = AccessionBuilder().set_batch("mixed_batch").build()
        mixed_transferred_restore = create_perfect_match(mixed_transferred_accession)
        mixed_transfer = TransferBuilder().set_filepath(mixed_transferred_restore.filepath).build()

        session.add(mixed_transferred_accession)
        session.add(mixed_transferred_restore)
        session.add(mixed_transfer)

        mixed_untransferred_accession = AccessionBuilder().set_batch("mixed_batch").build()
        mixed_untransferred_restore = create_perfect_match(mixed_untransferred_accession)

        session.add(mixed_untransferred_accession)
        session.add(mixed_untransferred_restore)

        find_perfect_matches(session, [mixed_transferred_accession, mixed_untransferred_accession])
        find_transfer_matches(session, [mixed_transfer])
        session.commit()

        # Verify setup
        self.assertEqual(1, len(mixed_transferred_accession.perfect_matches))
        self.assertEqual(1, len(mixed_transferred_restore.transfers))
        self.assertEqual(1, len(mixed_untransferred_accession.perfect_matches))
        self.assertEqual(0, len(mixed_untransferred_restore.transfers))

        batch = "mixed_batch"
        untransferred_accessions = find_untransferred_accessions(session, batch)
        self.assertEqual(1, len(untransferred_accessions))
        self.assertEqual(mixed_untransferred_accession.id, untransferred_accessions[0].id)

    def test_generate_manifest_entries_invalid_parameters(self):
        # None as accessions
        manifest_entries = generate_manifest_entries(None)
        self.assertEqual([], manifest_entries)

        # Empty list as accessions
        manifest_entries = generate_manifest_entries([])
        self.assertEqual([], manifest_entries)

    def test_generate_manifest_entries_single_accession(self):
        # Single accession (in a list)
        accession = AccessionBuilder().set_md5('SINGLE_MD5').set_relpath('single/test.txt').build()
        restore = create_perfect_match(accession)
        restore.filepath = '/single/restore/filepath/test.txt'
        accession.perfect_matches.append(restore)

        accessions = [accession]
        manifest_entries = generate_manifest_entries(accessions)
        self.assertEqual(1, len(manifest_entries))
        expected_entry = {'md5': restore.md5, 'filepath': restore.filepath, 'relpath': accession.relpath}
        self.assertIn(expected_entry, manifest_entries)

    def test_generate_manifest_entries_multiple_accessions(self):
        # Multiple accession (in a list)
        accession1 = AccessionBuilder().set_md5('MULTIPLE_MD5_1').set_relpath('multiple/test1.txt').build()
        restore1 = create_perfect_match(accession1)
        restore1.filepath = '/multiple/restore/filepath/test1.txt'
        accession1.perfect_matches.append(restore1)

        accession2 = AccessionBuilder().set_md5('MULTIPLE_MD5_2').set_relpath('multiple/test2.txt').build()
        restore2 = create_perfect_match(accession2)
        restore2.filepath = '/multiple/restore/filepath/test2.txt'
        accession2.perfect_matches.append(restore2)

        accessions = [accession1, accession2]
        manifest_entries = generate_manifest_entries(accessions)
        self.assertEqual(2, len(manifest_entries))

        expected_entry1 = {'md5': restore1.md5, 'filepath': restore1.filepath, 'relpath': accession1.relpath}
        expected_entry2 = {'md5': restore2.md5, 'filepath': restore2.filepath, 'relpath': accession2.relpath}
        self.assertIn(expected_entry1, manifest_entries)
        self.assertIn(expected_entry2, manifest_entries)

    def test_generate_manifest_entries_one_accessions_multiple_perfect_matches(self):
        accession = AccessionBuilder().set_md5('MULTI_PERFECT_MD5_1').set_relpath('multi_perfect/test.txt').build()
        restore1 = create_perfect_match(accession)
        restore1.filepath = '/multi_perfect/restore/filepath/test.txt'
        accession.perfect_matches.append(restore1)

        restore2 = create_perfect_match(accession)
        restore2.filepath = '/multi_perfect/restore/filepath/test.txt'
        accession.perfect_matches.append(restore2)

        accessions = [accession]
        manifest_entries = generate_manifest_entries(accessions)
        self.assertEqual(1, len(manifest_entries))

        # Note: Because of the way this test is constructed, the values for
        # restore1 will always be returned, but when using an actual database,
        # the actual restore used will be indeterminate
        expected_entry1 = {'md5': restore1.md5, 'filepath': restore1.filepath, 'relpath': accession.relpath}
        self.assertIn(expected_entry1, manifest_entries)

    def test_output_manifest_entries(self):
        manifest_file_stream = io.StringIO()

        manifest_entries = [
            {'md5': 'MD5_1', 'filepath': '/filepath/file_1', 'relpath': 'relpath/file_1'},
            {'md5': 'MD5_2', 'filepath': '/filepath/file_2', 'relpath': 'relpath/file_2'}
        ]

        output_manifest_entries(manifest_file_stream, manifest_entries)

        output_lines = manifest_file_stream.getvalue().split('\n')
        self.assertEqual("md5,filepath,relpath", output_lines[0].strip())
        self.assertEqual("MD5_1,/filepath/file_1,relpath/file_1", output_lines[1].strip())
        self.assertEqual("MD5_2,/filepath/file_2,relpath/file_2", output_lines[2].strip())

