import csv
import sys
import patsy.database
from patsy.model import Restore

Session = patsy.database.Session

"""
Processes each row from the "fedora_av.csv" (or its cleaned-up equivalent)
and attempts to match against the "filename_lowercase" field in the "restores"
table.

Note: This script requires a specially modified instance of the "patsy" database
that includes a lower-case filename field (which is also trimmed) in the
"restores" table.

This script creates two file, one for "matched" rows, the other for "unmatched"
rows.

Note: The SQL "LIKE" operator was not used, because using it required SQLite to
do a full "restores" table scan for _every_ row, which takes about 30 seconds
per row.

This script takes each row and attempts a match using a provided list of
extensions. This allows an exact match against the "filename_lowercase" field
to be used.

Matched rows are returned with the information from the "restores" table, and
an additional "file_extension" field in CSV format.

Unmatched rows are returned as read in from the input CSV file.
"""

def process_matches(row, restore_matches, file_extension):
    if restore_matches:
        rows = []
        for restore in restore_matches:
            new_row = row.copy()
            new_row["restore_id"] = restore.id
            new_row["restore_md5"] = restore.md5
            new_row["restore_filename"] = restore.filename
            new_row["restore_filepath"] = restore.filepath
            new_row["restore_bytes"] = restore.bytes
            new_row["file_extension"] = file_extension.lower()
            rows.append(new_row)
        return rows
    else:
        return None


def match_base_filename_exact_match(session, row):
    base_filename = row["identifier"]

    extensions = ["", "wav", "mov", "mpg", "tif", "mp3"]
    suffixes = ["", " "]
    base_filename_lowercase = base_filename.lower()

    matched_rows = []
    for extension in extensions:
        for suffix in suffixes:
            exact_filename = base_filename_lowercase + suffix + "." + extension
            restore_matches = session.query(Restore).filter(Restore.filename_lowercase == exact_filename).all()
            processed_rows = process_matches(row, restore_matches, extension)

            if processed_rows:
                matched_rows.extend(processed_rows)

    if not matched_rows:
        return None

    return matched_rows


if __name__ == '__main__':
    arguments = sys.argv

    db_filename = arguments[1]
    csv_filename = arguments[2]
    matched_output_filename = arguments[3]
    unmatched_output_filename = arguments[4]

    patsy.database.use_database_file(db_filename)

    matched_output_headers = [
        "pid", "displayTitle", "hasPart", "dmDate", "isMemberOfCollection", "identifier", "dpExtRef",
        "restore_id", "restore_md5", "restore_filename", "restore_filepath", "restore_bytes", "file_extension"
    ]

    unmatched_output_headers = [
        "pid", "displayTitle", "hasPart", "dmDate", "isMemberOfCollection", "identifier", "dpExtRef",
    ]

    session = Session()

    with open(csv_filename, 'r') as file_handle:
        with open(unmatched_output_filename, "w") as unmatched_file_handle:
            with open(matched_output_filename, "w") as matched_file_handle:
                reader = csv.DictReader(file_handle, delimiter=',')

                unmatched_writer = csv.DictWriter(unmatched_file_handle, fieldnames=unmatched_output_headers)
                unmatched_writer.writeheader()

                matched_writer = csv.DictWriter(matched_file_handle, fieldnames=matched_output_headers)
                matched_writer.writeheader()

                for row in reader:
                    expanded_rows = match_base_filename_exact_match(session, row)
                    if expanded_rows is None:
                        unmatched_writer.writerow(row)
                        continue

                    for expanded_row in expanded_rows:
                        matched_writer.writerow(expanded_row)
