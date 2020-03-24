import patsy.database
from patsy.model import Base
from patsy.perfect_matches import find_perfect_matches
import unittest
from patsy.model import Accession, Restore, perfect_matches_table
from .utils import AccessionBuilder, RestoreBuilder, create_perfect_match, create_test_engine

Session = patsy.database.Session


class TestPerfectMatches(unittest.TestCase):
    def setUp(self):
        create_test_engine()
        engine = Session().get_bind()
        Base.metadata.create_all(engine)

    def test_no_perfect_match(self):
        session = Session()

        accession = AccessionBuilder().build()
        restore = RestoreBuilder().build()

        session.add(accession)
        session.add(restore)
        session.commit()

        # Verify that at least MD5 checksums are not equal
        self.assertNotEqual(accession.md5, restore.md5)

        accessions = session.query(Accession)
        new_matches_found = find_perfect_matches(session, accessions)

        # No perfect match should be found
        self.assertEqual(0, len(new_matches_found))
        self.assertEqual(0, len(accession.perfect_matches))
        self.assertEqual(0, len(restore.perfect_matches))

    def test_one_perfect_match(self):
        session = Session()

        accession = AccessionBuilder().build()
        restore = create_perfect_match(accession)

        session.add(accession)
        session.add(restore)
        session.commit()

        self.assertEqual(0, len(accession.perfect_matches))

        accessions = session.query(Accession)
        new_matches_found = find_perfect_matches(session, accessions)

        self.assertEqual(1, len(new_matches_found))
        self.assertEqual(1, len(accession.perfect_matches))

    def test_multiple_perfect_matches_to_one_accession(self):
        session = Session()

        accession = AccessionBuilder().build()
        restore1 = create_perfect_match(accession)
        restore2 = create_perfect_match(accession)

        session.add(accession)
        session.add(restore1)
        session.add(restore2)
        session.commit()

        accessions = session.query(Accession)
        new_matches_found = find_perfect_matches(session, accessions)

        self.assertEqual(2, len(new_matches_found))
        self.assertEqual(2, len(accession.perfect_matches))
        self.assertEqual(1, len(restore1.perfect_matches))
        self.assertEqual(accession, restore1.perfect_matches[0])
        self.assertEqual(1, len(restore2.perfect_matches))
        self.assertEqual(accession, restore2.perfect_matches[0])

    def test_multiple_perfect_matches_to_multiple_accession(self):
        # Tests that there can be two accessions that both match two restores
        session = Session()

        accession1 = AccessionBuilder().build()
        accession2 = AccessionBuilder().set_md5(accession1.md5)\
                                       .set_bytes(accession1.bytes)\
                                       .set_filename(accession1.filename)\
                                       .build()
        restore1 = create_perfect_match(accession1)
        restore2 = create_perfect_match(accession2)

        session.add(accession1)
        session.add(accession2)
        session.add(restore1)
        session.add(restore2)
        session.commit()

        accessions = session.query(Accession)
        new_matches_found = find_perfect_matches(session, accessions)

        self.assertEqual(4, len(new_matches_found))
        self.assertEqual(2, len(accession1.perfect_matches))
        self.assertEqual(2, len(accession2.perfect_matches))
        self.assertEqual(2, len(restore1.perfect_matches))
        self.assertIn(accession1, restore1.perfect_matches)
        self.assertIn(accession2, restore1.perfect_matches)
        self.assertIn(accession1, restore2.perfect_matches)
        self.assertIn(accession2, restore2.perfect_matches)

    def test_same_md5_but_not_same_filename(self):
        session = Session()

        accession = AccessionBuilder().set_filename('foo.txt').build()
        restore = create_perfect_match(accession)
        restore.filename = 'bar.txt'
        session.add(accession)
        session.add(restore)
        session.commit()

        # Verify that MD5 checksums are the same, but filenames differ
        self.assertEqual(accession.md5, restore.md5)
        self.assertNotEqual(accession.filename, restore.filename)

        accessions = session.query(Accession)
        new_matches_found = find_perfect_matches(session, accessions)

        self.assertEqual(0, len(new_matches_found))
        self.assertEqual(0, len(accession.perfect_matches))
        self.assertEqual(0, len(restore.perfect_matches))

    def test_same_md5_and_filename_but_not_same_bytes(self):
        session = Session()

        accession = AccessionBuilder().set_bytes(123).build()
        restore = create_perfect_match(accession)
        restore.bytes = 456
        session.add(accession)
        session.add(restore)
        session.commit()

        # Verify that MD5 checksums are the same, but filenames differ
        self.assertEqual(accession.md5, restore.md5)
        self.assertEqual(accession.filename, restore.filename)
        self.assertNotEqual(accession.bytes, restore.bytes)

        accessions = session.query(Accession)
        new_matches_found = find_perfect_matches(session, accessions)

        self.assertEqual(0, len(new_matches_found))
        self.assertEqual(0, len(accession.perfect_matches))
        self.assertEqual(0, len(restore.perfect_matches))

    def test_finding_perfect_matches_more_than_once(self):
        session = Session()

        accession = AccessionBuilder().build()
        restore = create_perfect_match(accession)

        session.add(accession)
        session.add(restore)
        session.commit()

        accessions = session.query(Accession)
        new_matches_found = find_perfect_matches(session, accessions)

        self.assertEqual(1, len(new_matches_found))
        self.assertEqual(1, len(accession.perfect_matches))
        self.assertEqual(1, len(restore.perfect_matches))

        accessions = session.query(Accession)
        new_matches_found = find_perfect_matches(session, accessions)
        self.assertEqual(0, len(new_matches_found))
        self.assertEqual(1, len(accession.perfect_matches))
        self.assertEqual(1, len(restore.perfect_matches))
