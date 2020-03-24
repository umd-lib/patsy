import io
import patsy.database
from patsy.model import Base, Transfer
from patsy.batch_stats import get_stats_for_batch, output_batch_stats_entries, batch_stats
import unittest
from .utils import AccessionBuilder, TransferBuilder, create_perfect_match, create_test_engine
from patsy.perfect_matches import find_perfect_matches
from patsy.transfer_matches import find_transfer_matches
from patsy.utils import get_accessions, get_batch_names

Session = patsy.database.Session


class TestBatchStats(unittest.TestCase):
    def setUp(self):
        create_test_engine()
        engine = Session().get_bind()
        Base.metadata.create_all(engine)

    def test_get_stats_for_batch_no_data(self):
        session = Session()

        result = get_stats_for_batch(session, 'no_data')

        self.assertEqual(0, result['num_accessions'])
        self.assertEqual(0, result['num_accessions_with_perfect_matches'])
        self.assertEqual(0, result['num_accessions_transferred'])

    def test_batch_stats_no_entries_for_batch(self):
        session = Session()

        accession = AccessionBuilder().set_batch("batch").build()
        restore = create_perfect_match(accession)

        result = get_stats_for_batch(session, "some_other_batch")

        self.assertEqual(0, result['num_accessions'])
        self.assertEqual(0, result['num_accessions_with_perfect_matches'])
        self.assertEqual(0, result['num_accessions_transferred'])

    def test_get_stats_for_batch(self):
        session = Session()

        batch_name = "test_batch_stats"
        TestBatchStats.setup_perfect_matches_and_transfers(session, batch_name)

        result = get_stats_for_batch(session, batch_name)

        self.assertEqual(4, result['num_accessions'])
        self.assertEqual(3, result['num_accessions_with_perfect_matches'])
        self.assertEqual(1, result['num_accessions_transferred'])

    def test_batch_stats_with_multiple_batches(self):
        session = Session()

        batch_name = "test_batch_stats"
        TestBatchStats.setup_perfect_matches_and_transfers(session, batch_name)
        TestBatchStats.setup_perfect_matches_and_transfers(session, "different_batch")

        result = get_stats_for_batch(session, batch_name)

        self.assertEqual(4, result['num_accessions'])
        self.assertEqual(3, result['num_accessions_with_perfect_matches'])
        self.assertEqual(1, result['num_accessions_transferred'])

    def test_output_batch_stats_entries(self):
        file_stream = io.StringIO()
        session = Session()

        batch_name = "test_batch_stats"
        TestBatchStats.setup_perfect_matches_and_transfers(session, batch_name)

        batch_stat_entries = batch_stats(session, get_batch_names(session))
        output_batch_stats_entries(file_stream, batch_stat_entries)

        output_lines = file_stream.getvalue().split('\n')
        self.assertEqual("batch,num_accessions,num_accessions_with_perfect_matches,num_accessions_transferred",
                         output_lines[0].strip())
        self.assertEqual("test_batch_stats,4,3,1", output_lines[1].strip())


    @staticmethod
    def setup_perfect_matches_and_transfers(session, batch_name):
        # Sets up the database with 4 accessions where:
        #    2 of the accessions has a single perfect match
        #    1 of the accessions has two perfect matches
        #    1 of the accessions has no perfect match
        # and one accession has been transferred.

        accession_single_perfect_match = AccessionBuilder().set_batch(batch_name).build()
        restore_single_perfect_match = create_perfect_match(accession_single_perfect_match)
        session.add(accession_single_perfect_match)
        session.add(restore_single_perfect_match)

        accession_multiple_perfect_matches = AccessionBuilder().set_batch(batch_name).build()
        restore_multiple_perfect_match1 = create_perfect_match(accession_multiple_perfect_matches)
        restore_multiple_perfect_match2 = create_perfect_match(accession_multiple_perfect_matches)
        session.add(accession_multiple_perfect_matches)
        session.add(restore_multiple_perfect_match1)
        session.add(restore_multiple_perfect_match2)

        accession_transfer = AccessionBuilder().set_batch(batch_name).build()
        restore_transfer = create_perfect_match(accession_transfer)
        transfer = TransferBuilder().set_filepath(restore_transfer.filepath).build()
        session.add(accession_transfer)
        session.add(restore_transfer)
        session.add(transfer)

        accession_without_match = AccessionBuilder().set_batch(batch_name).build()
        session.add(accession_without_match)

        find_perfect_matches(session, get_accessions(session, batch_name))
        transfers = session.query(Transfer).all()
        find_transfer_matches(session, transfers)
        session.commit()

