import unittest
from patsy.export_inventory import handle_timestamp


class TestExportInventory(unittest.TestCase):
    def setUp(self):
        pass

    def test_handle_timestamp__none(self):
        timestamp = None

        timestamp_dict = handle_timestamp(timestamp)

        self.assertEqual("", timestamp_dict["MTIME"])
        self.assertEqual("", timestamp_dict["MODDATE"])

    def test_handle_timestamp__empty_string(self):
        timestamp = None

        timestamp_dict = handle_timestamp(timestamp)

        self.assertEqual("", timestamp_dict["MTIME"])
        self.assertEqual("", timestamp_dict["MODDATE"])

    def test_handle_timestamp__various_formats(self):
        timestamp_tests = []
        timestamp_tests.append({
            "timestamp": "Tue Aug 18 15:20:38 EDT 2015",
            "expected_mtime": 1439925638.0,
            "expected_moddate": "2015-08-18T15:20:38"
        })
        timestamp_tests.append({
            "timestamp": "2011-02-09T14:09:00",
            "expected_mtime": 1297278540.0,
            "expected_moddate": '2011-02-09T14:09:00'
        })
        timestamp_tests.append({
            "timestamp": "2013-04-16 02:22:33",
            "expected_mtime": 1366093353.0,
            "expected_moddate": '2013-04-16T02:22:33'
        })
        timestamp_tests.append({
            "timestamp": "11/11/2009 17:33",
            "expected_mtime": 1257978780.0,
            "expected_moddate": '2009-11-11T17:33:00'
        })

        for test in timestamp_tests:
            timestamp_dict = handle_timestamp(test["timestamp"])
            self.assertEqual(test["expected_mtime"], timestamp_dict["MTIME"])
            self.assertEqual(test["expected_moddate"], timestamp_dict["MODDATE"])
