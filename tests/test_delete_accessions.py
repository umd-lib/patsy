import patsy.database
from patsy.delete_accessions import delete_accessions
from patsy.model import Base
import unittest
from patsy.model import Accession
from .utils import create_test_engine, AccessionBuilder, create_perfect_match
from patsy.perfect_matches import find_perfect_matches

Session = patsy.database.Session


class TestDeleteAccession(unittest.TestCase):
    def setUp(self):
        create_test_engine()
        engine = Session().get_bind()
        Base.metadata.create_all(engine)

    def test_batch_with_one_accession(self):
        session = Session()

        accession = AccessionBuilder().set_batch("batch_to_delete").build()
        session.add(accession)
        session.commit()

        accessions = session.query(Accession)
        self.assertEqual(1, accessions.count())

        delete_accessions(session, "batch_to_delete")
        session.commit()

        accessions = session.query(Accession)
        self.assertEqual(0, accessions.count())

    def test_batch_with_two_accessions_in_different_batches(self):
        session = Session()

        accession1 = AccessionBuilder().set_batch("batch_to_delete").build()
        accession2 = AccessionBuilder().set_batch("batch_to_preserve").build()

        session.add(accession1)
        session.add(accession2)
        session.commit()

        accessions = session.query(Accession)
        self.assertEqual(2, accessions.count())

        delete_accessions(session, "batch_to_delete")
        session.commit()

        accessions = session.query(Accession)
        self.assertEqual(1, accessions.count())
        self.assertEqual(accession2, accessions.first())

    def test_batch_with_accession_with_perfect_match(self):
        session = Session()

        accession = AccessionBuilder().set_batch("batch_to_delete").build()
        restore = create_perfect_match(accession)

        session.add(accession)
        session.add(restore)
        session.commit()

        accessions = session.query(Accession)
        new_matches_found = find_perfect_matches(session, accessions)

        self.assertEqual(1, len(new_matches_found))
        self.assertEqual(1, len(accession.perfect_matches))
        self.assertEqual(1, len(restore.perfect_matches))
        self.assertEqual(accession, restore.perfect_matches[0])
        self.assertEqual(restore, accession.perfect_matches[0])

        delete_accessions(session, "batch_to_delete")
        session.commit()

        accessions = session.query(Accession)
        self.assertEqual(0, accessions.count())
        self.assertEqual([], restore.perfect_matches)
