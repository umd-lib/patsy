import patsy.database
from patsy.model import Base
from patsy.transfer_matches import find_transfer_matches
import unittest
from patsy.model import Transfer
from .utils import RestoreBuilder, TransferBuilder, create_test_engine

Session = patsy.database.Session


class TestTransferMatches(unittest.TestCase):
    def setUp(self):
        create_test_engine()
        engine = Session().get_bind()
        Base.metadata.create_all(engine)

    def test_no_transfer_match(self):
        session = Session()

        restore = RestoreBuilder().build()
        transfer = TransferBuilder().build()

        session.add(restore)
        session.add(transfer)
        session.commit()

        # Verify that filepaths are not equal
        self.assertNotEqual(restore.filepath, transfer.filepath)

        transfers = session.query(Transfer)
        new_matches_found = find_transfer_matches(session, transfers)

        # No transfer matches should be found
        self.assertEqual(0, len(new_matches_found))
        self.assertEqual(0, len(restore.transfers))

    def test_one_transfer_match(self):
        session = Session()

        restore = RestoreBuilder().build()
        transfer = TransferBuilder().set_filepath(restore.filepath).build()

        session.add(restore)
        session.add(transfer)
        session.commit()

        self.assertEqual(0, len(restore.transfers))
        self.assertEqual(None, transfer.restore)

        transfers = session.query(Transfer)
        new_matches_found = find_transfer_matches(session, transfers)

        self.assertEqual(1, len(new_matches_found))
        self.assertEqual(1, len(restore.transfers))
        self.assertEqual(restore, transfer.restore)

    def test_multiple_transfer_matches_to_one_restore(self):
        session = Session()

        restore = RestoreBuilder().build()
        transfer1 = TransferBuilder().set_filepath(restore.filepath).build()
        transfer2 = TransferBuilder().set_filepath(restore.filepath).build()

        session.add(restore)
        session.add(transfer1)
        session.add(transfer2)
        session.commit()

        transfers = session.query(Transfer)
        new_matches_found = find_transfer_matches(session, transfers)

        self.assertEqual(2, len(new_matches_found))
        self.assertEqual(2, len(restore.transfers))
        self.assertEqual(restore, transfer1.restore)
        self.assertEqual(restore, transfer2.restore)
        self.assertIn(transfer1, restore.transfers)
        self.assertIn(transfer2, restore.transfers)

    def test_finding_transfer_matches_more_than_once(self):
        session = Session()

        restore = RestoreBuilder().build()
        transfer = TransferBuilder().set_filepath(restore.filepath).build()

        session.add(restore)
        session.add(transfer)
        session.commit()

        transfers = session.query(Transfer)
        new_matches_found = find_transfer_matches(session, transfers)

        self.assertEqual(1, len(new_matches_found))
        self.assertEqual(1, len(restore.transfers))
        self.assertIn(transfer, restore.transfers)
        self.assertEqual(restore, restore)

        transfers = session.query(Transfer)
        new_matches_found = find_transfer_matches(session, transfers)
        self.assertEqual(0, len(new_matches_found))
        self.assertEqual(1, len(restore.transfers))
        self.assertIn(transfer, restore.transfers)
        self.assertEqual(restore, restore)
