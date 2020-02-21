import patsy.database
import unittest
from patsy.utils import get_accessions
from .utils import AccessionBuilder


Session = patsy.database.Session


class TestUtils(unittest.TestCase):
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
