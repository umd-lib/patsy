import csv
import sys
import unittest

"""
Each row of the "fedora_av.csv" (or its cleaned-up equivalent) has one or
more parts, denoted by the "hasPart" field. Each entry in the "hasPart"
field has a corresponding entry in the "identifier" and "dpExtRef" fields.

This script "expands" the rows, by creating one row per entry in the "hasPart"
field, matched to the corresponding entry in the "identifier" and "dpExtRef"
fields.

For example, the following row with two parts:

|  pid     | displayTitle                                             | hasPart           | dmDate               | isMemberOfCollection | identifier                | dpExtRef                                                                                                            |
| -------- | -------------------------------------------------------- | ----------------- | -------------------- | -------------------- | ------------------------- | ------------------------------------------------------------------------------------------------------------------- |
|umd:2100  | The great step forward : China women in the 20th century | umd:2101,umd:2102 | 2003-01-01T05:00:00Z | umd:1158             | FHgreatste02,FHgreatste01 | http://streamer.lib.umd.edu/ssdcms/i.do?u=d0c16b26570c476,http://streamer.lib.umd.edu/ssdcms/i.do?u=a74cdc7b162b40b |

is converted into two rows, one for each part, with its corresponding
identifier:

|  pid     | displayTitle                                             | hasPart  | dmDate               | isMemberOfCollection | identifier   | dpExtRef                                                  |
| -------- | -------------------------------------------------------- | -------- | -------------------- | -------------------- | ------------ | --------------------------------------------------------- |
|umd:2100  | The great step forward : China women in the 20th century | umd:2101 | 2003-01-01T05:00:00Z | umd:1158             | FHgreatste02 | http://streamer.lib.umd.edu/ssdcms/i.do?u=d0c16b26570c476 |
|umd:2100  | The great step forward : China women in the 20th century | umd:2102 | 2003-01-01T05:00:00Z | umd:1158             | FHgreatste01 | http://streamer.lib.umd.edu/ssdcms/i.do?u=a74cdc7b162b40b |

Note: Some "dpExtRef" fields may have fewer entries than the "hasPart" field.
In these cases, the expanded rows will have a blank "dpExtRef" field once the
"dpExtRef" entries run out. 
"""


def expand_parts(row):
    """
    Expands the given row into multiple rows, if the "hasPart" field has
    mulitple comma-separated values. The new rows will have a single
    entry in the "hasPart" field, the corresponding entry in the "identifier"
    and "dpExtRef" fields.

    :param row: a Dictionary representing a row from the CSV file.
    :return:
    """
    expanded_rows = []

    has_part = row["hasPart"]
    identifier = row["identifier"]
    dpExtRef = row["dpExtRef"]

    parts = has_part.split(",")
    ids = identifier.split(",")
    refs = dpExtRef.split(",")

    if len(parts) != len(ids):
        raise ValueError(f"Parts mismatch for pid: {row['pid']}: has_part: {len(parts)}, identifier {len(ids)}), dpExtRef: {len(refs)}")

    # Pad out refs with blanks, if it is less than parts
    # See PID: umd:722610
    while len(parts) > len(refs):
        refs.append("")

    for part, id, ref in zip(parts, ids, refs):
        new_row = row.copy()
        new_row["hasPart"] = part.strip()
        new_row["identifier"] = id.strip()
        new_row["dpExtRef"] = ref.strip()

        expanded_rows.append(new_row)

    return expanded_rows


if __name__ == '__main__':
    '''
    main function for running script from the command line
    
    To run:
    
        > python3 fedora_av_expand_parts.py <INPUT_CSV_FILE>
    
    where <INPUT_CSV_FILE> is the name of the CSV file. The output will be
    printed to standard out.
    
    To run the tests:
    
        > pytest fedora_av_expand_parts.py
    '''
    arguments = sys.argv
    csv_filename = arguments[1]

    output_headers = [
        "pid", "displayTitle", "hasPart", "dmDate", "isMemberOfCollection", "identifier", "dpExtRef"
    ]

    writer = csv.DictWriter(sys.stdout, fieldnames=output_headers)
    writer.writeheader()

    with open(csv_filename, 'r') as file_handle:
        reader = csv.DictReader(file_handle, delimiter=',')
        for row in reader:
            expanded_rows = expand_parts(row)
            for expanded_row in expanded_rows:
                writer.writerow(expanded_row)


class FedoraAvExpandPartsTest(unittest.TestCase):
    def test_expand_parts_one_part(self):
        row = {"pid": "123", "field1": "one", "hasPart": "part1", "identifier": "id1", "dpExtRef": "ref1"}

        expected_result = [
            {"pid": "123", "field1": "one", "hasPart": "part1", "identifier": "id1", "dpExtRef": "ref1"}
        ]

        result = expand_parts(row)
        self.assertEqual(expected_result, result)

    def test_unequal_parts_and_identifier_lengths(self):
        # The number of items in "has_parts" is not the same as the number of
        # items in "identifier"
        row = {"pid": "123", "field1": "one", "hasPart": "part1,part2", "identifier": "id1", "dpExtRef": "ref1,ref2"}

        with self.assertRaises(ValueError):
            expand_parts(row)

    def test_unequal_parts_and_dpExtRef_lengths_should_pad_with_blanks(self):
        # The number of items in "has_parts" is not the same as the number of
        # items in "dpExtRef"
        row = {"pid": "123", "field1": "one", "hasPart": "part1,part2", "identifier": "id1,id2", "dpExtRef": "ref1"}

        expected_result = [
            {"pid": "123", "field1": "one", "hasPart": "part1", "identifier": "id1", "dpExtRef": "ref1"},
            {"pid": "123", "field1": "one", "hasPart": "part2", "identifier": "id2", "dpExtRef": ""}
        ]

        result = expand_parts(row)

        self.assertEqual(2, len(result))
        self.assertIn(result[0], expected_result)
        self.assertIn(result[1], expected_result)

    def test_expand_parts_two_parts(self):
        row = {"pid": "123", "field1": "one", "hasPart": "part1,part2", "identifier": "id1,id2", "dpExtRef": "ref1,ref2"}

        expected_result = [
            {"pid": "123", "field1": "one", "hasPart": "part1", "identifier": "id1", "dpExtRef": "ref1"},
            {"pid": "123", "field1": "one", "hasPart": "part2", "identifier": "id2", "dpExtRef": "ref2"}
        ]

        result = expand_parts(row)

        self.assertEqual(2, len(result))
        self.assertIn(result[0], expected_result)
        self.assertIn(result[1], expected_result)

    def test_expand_parts_three_parts(self):
        row = {"pid": "123", "field1": "one", "hasPart": "part1,part2,part3", "identifier": "id1,id2,id3", "dpExtRef": "ref1,ref2,ref3"}

        expected_result = [
            {"pid": "123", "field1": "one", "hasPart": "part1", "identifier": "id1", "dpExtRef": "ref1"},
            {"pid": "123", "field1": "one", "hasPart": "part2", "identifier": "id2", "dpExtRef": "ref2"},
            {"pid": "123", "field1": "one", "hasPart": "part3", "identifier": "id3", "dpExtRef": "ref3"}
        ]

        result = expand_parts(row)

        self.assertEqual(3, len(result))
        self.assertIn(result[0], expected_result)
        self.assertIn(result[1], expected_result)
        self.assertIn(result[2], expected_result)
