import patsy.database
from patsy.model import Base, Accession, perfect_matches_table, filename_only_matches_table, altered_md5_matches_table
from patsy.unmatched_accessions import unmatched_accessions, unmatched_accessions_output, delete_accessions
from patsy.perfect_matches import find_perfect_matches
import unittest
from .utils import AccessionBuilder, create_perfect_match, create_test_engine
import io

Session = patsy.database.Session


class TestTransferMatches(unittest.TestCase):
    def setUp(self):
        create_test_engine()
        engine = Session().get_bind()
        Base.metadata.create_all(engine)

    def test_unmatched_accessions_throws_error_if_batch_not_provided(self):
        session = Session()

        with self.assertRaises(ValueError):
            unmatched_accessions_found = unmatched_accessions(session)

    def test_no_accessions(self):
        session = Session()

        unmatched_accessions_found = unmatched_accessions(session, "TestBatch")
        self.assertEqual(0, len(unmatched_accessions_found))

    def test_one_unmatched_accession(self):
        session = Session()

        batch_name = "TestBatch"
        accession = AccessionBuilder().set_batch(batch_name).build()

        session.add(accession)
        session.commit()

        unmatched_accessions_found = unmatched_accessions(session, batch_name)
        self.assertEqual(1, len(unmatched_accessions_found))
        self.assertIn(accession, unmatched_accessions_found)

    def test_one_matched_and_one_unmatched_accession(self):
        session = Session()

        batch_name = "TestBatch"
        no_match_accession = AccessionBuilder().set_batch(batch_name).build()
        session.add(no_match_accession)

        match_accession = AccessionBuilder().set_batch(batch_name).build()
        match_restore = create_perfect_match(match_accession)
        session.add(match_accession)
        session.add(match_restore)

        accessions = session.query(Accession)
        new_matches_found = find_perfect_matches(session, accessions)
        self.assertEqual(1, len(new_matches_found))
        session.commit()

        unmatched_accessions_found = unmatched_accessions(session, batch_name)
        self.assertEqual(1, len(unmatched_accessions_found))
        self.assertIn(no_match_accession, unmatched_accessions_found)

    def test_one_unmatched_accession_in_batch(self):
        session = Session()

        batch_name = "TestBatch"
        accession = AccessionBuilder().set_batch(batch_name).build()
        session.add(accession)
        session.commit()

        unmatched_accessions_found = unmatched_accessions(session, batch_name)
        self.assertEqual(1, len(unmatched_accessions_found))
        self.assertIn(accession, unmatched_accessions_found)

    def test_one_unmatched_accession_in_batch_with_other_batches(self):
        session = Session()

        batch_name = "TestBatch"
        unmatched_accession = AccessionBuilder().set_batch(batch_name).build()
        session.add(unmatched_accession)

        matched_batch_name = "MatchedBatch"
        matched_accession = AccessionBuilder().set_batch(matched_batch_name).build()
        matched_restore = create_perfect_match(matched_accession)
        session.add(matched_accession)
        session.add(matched_restore)

        accessions = session.query(Accession)
        new_matches_found = find_perfect_matches(session, accessions)
        self.assertEqual(1, len(new_matches_found))
        session.commit()

        unmatched_accessions_found = unmatched_accessions(session, batch_name)
        self.assertEqual(1, len(unmatched_accessions_found))
        self.assertIn(unmatched_accession, unmatched_accessions_found)

    def test_one_unmatched_accessions_in_different_batches(self):
        session = Session()

        batch_name1 = "TestBatch1"
        unmatched_accession1 = AccessionBuilder().set_batch(batch_name1).build()
        session.add(unmatched_accession1)

        batch_name2 = "TestBatch2"
        unmatched_accession2 = AccessionBuilder().set_batch(batch_name2).build()
        session.add(unmatched_accession2)
        session.commit()

        unmatched_accessions_found = unmatched_accessions(session, batch_name1)
        self.assertEqual(1, len(unmatched_accessions_found))
        self.assertIn(unmatched_accession1, unmatched_accessions_found)

    def test_output_batch_stats_entries_no_unmatched_accessions(self):
        file_stream = io.StringIO()

        unmatched_accessions_found = []
        unmatched_accessions_output(file_stream, unmatched_accessions_found)

        output_lines = file_stream.getvalue().split('\n')
        self.assertEqual("No unmatched accessions found!", output_lines[0].strip())

    def test_output_batch_stats_entries_single_unmatched_accessions(self):
        file_stream = io.StringIO()

        unmatched_accession = AccessionBuilder().build()

        unmatched_accessions_found = [unmatched_accession]
        unmatched_accessions_output(file_stream, unmatched_accessions_found)

        output_lines = file_stream.getvalue().split('\n')
        self.assertEqual("batch,relpath", output_lines[0].strip())
        self.assertEqual(f"{unmatched_accession.batch},{unmatched_accession.relpath}", output_lines[1].strip())

    def test_delete_accessions_unmatched_accession(self):
        file_stream = io.StringIO()

        session = Session()
        accession = AccessionBuilder().build()
        session.add(accession)
        session.commit()
        accession_id = accession.id

        unmatched_accessions_found = [accession]
        delete_accessions(session, unmatched_accessions_found, file_stream)
        self.assertEqual(0, session.query(Accession).filter(Accession.id == accession_id).count())

        output_lines = file_stream.getvalue().split('\n')
        self.assertEqual("", output_lines[0].strip())
        self.assertEqual("---- Deleting accessions ----", output_lines[1].strip())
        self.assertEqual(f"{accession.batch},{accession.relpath}", output_lines[2].strip())

    def test_delete_accessions_matched_accession(self):
        # This should never happen - but we can delete _any_ accession,
        # even if it has a perfect match.
        file_stream = io.StringIO()

        session = Session()
        accession = AccessionBuilder().build()
        restore = create_perfect_match(accession)
        session.add(accession)
        session.add(restore)

        accessions = session.query(Accession)
        new_matches_found = find_perfect_matches(session, accessions)
        self.assertEqual(1, len(new_matches_found))
        perfect_matches_count = session.query(perfect_matches_table).count()
        self.assertEqual(1, perfect_matches_count)
        session.commit()
        accession_id = accession.id

        unmatched_accessions_found = [accession]
        delete_accessions(session, unmatched_accessions_found, file_stream)
        perfect_matches_count = session.query(perfect_matches_table).count()
        self.assertEqual(0, perfect_matches_count)
        self.assertEqual(0, session.query(Accession).filter(Accession.id == accession_id).count())

        output_lines = file_stream.getvalue().split('\n')
        self.assertEqual("", output_lines[0].strip())
        self.assertEqual("---- Deleting accessions ----", output_lines[1].strip())
        self.assertEqual(f"{accession.batch},{accession.relpath}", output_lines[2].strip())

    def test_delete_accession_with_filename_only_match(self):
        file_stream = io.StringIO()

        session = Session()
        accession = AccessionBuilder().build()
        restore = create_perfect_match(accession)
        accession.filename_only_matches.append(restore)
        session.add(accession)
        session.add(restore)

        session.commit()

        accession_id = accession.id
        # Verify that there is one filename_only match
        filename_only_count = session.query(filename_only_matches_table).count()
        self.assertEqual(1, filename_only_count)
        unmatched_accessions_found = [accession]
        delete_accessions(session, unmatched_accessions_found, file_stream)

        filename_only_count = session.query(filename_only_matches_table).count()

        # Verify that the accession and filename_only_match have been deleted.
        self.assertEqual(0, session.query(Accession).filter(Accession.id == accession_id).count())
        self.assertEqual(0, filename_only_count)

    def test_delete_accession_with_altered_md5_match(self):
        file_stream = io.StringIO()

        session = Session()
        accession = AccessionBuilder().build()
        restore = create_perfect_match(accession)
        accession.altered_md5_matches.append(restore)
        session.add(accession)
        session.add(restore)

        session.commit()

        accession_id = accession.id
        # Verify that there is one altered_md5 match
        altered_md5_count = session.query(altered_md5_matches_table).count()
        self.assertEqual(1, altered_md5_count)
        unmatched_accessions_found = [accession]
        delete_accessions(session, unmatched_accessions_found, file_stream)

        altered_md5_count = session.query(altered_md5_matches_table).count()

        # Verify that the accession and filename_only_match have been deleted.
        self.assertEqual(0, session.query(Accession).filter(Accession.id == accession_id).count())
        self.assertEqual(0, altered_md5_count)
