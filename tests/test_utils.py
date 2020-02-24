import patsy.database
from sqlalchemy import create_engine
from patsy.model import Base, Transfer
import unittest
from patsy.utils import get_accessions, get_unmatched_transfers
from .utils import AccessionBuilder, RestoreBuilder, TransferBuilder


Session = patsy.database.Session


class TestUtils(unittest.TestCase):
    def setUp(self):
        engine = create_engine('sqlite:///:memory:')
        Session.configure(bind=engine)
        Base.metadata.create_all(engine)

    def test_get_accessions(self):
        session = Session()

        accession_batch1 = AccessionBuilder().set_batch('Batch1').build()
        accession_batch2 = AccessionBuilder().set_batch('Batch2').build()

        session.add(accession_batch1)
        session.add(accession_batch2)
        session.commit()

        # Calling with only sessions queries all accessions
        accessions = get_accessions(session)
        self.assertEqual(2, accessions.count())

        # Calling with a batch only gives accessions in that batch
        accessions = get_accessions(session, 'Batch1')
        self.assertEqual(1, accessions.count())
        self.assertEqual(accession_batch1, accessions[0])

        accessions = get_accessions(session, 'Batch2')
        self.assertEqual(1, accessions.count())
        self.assertEqual(accession_batch2, accessions[0])

    def test_get_unmatched_transfers(self):
        session = Session()

        unmatched_transfer1 = TransferBuilder().build()
        unmatched_transfer2 = TransferBuilder().build()

        session.add(unmatched_transfer1)
        session.add(unmatched_transfer2)

        restore = RestoreBuilder().build()
        transfer_with_restore = TransferBuilder().build()
        transfer_with_restore.restore = restore
        session.add(restore)
        session.add(transfer_with_restore)

        session.commit()

        # Verify that there are a total of 3 transfers
        all_transfers = session.query(Transfer)
        self.assertEqual(3, all_transfers.count())

        # Only transfers without a "restore" record are returned
        unmatched_transfers = get_unmatched_transfers(session)
        self.assertEqual(2, unmatched_transfers.count())
        self.assertIn(unmatched_transfer1, unmatched_transfers)
        self.assertIn(unmatched_transfer2, unmatched_transfers)
