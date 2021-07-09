import csv
from patsy.core.gateway import Gateway
from patsy.core.patsy_entry import PatsyEntryFactory
from typing import Dict
import logging

logger = logging.getLogger(__name__)


class Load:
    # Fields that must be present in the CSV, with non-empty content
    REQUIRED_CSV_FIELDS = [
        'BATCH', 'RELPATH', 'FILENAME', 'EXTENSION', 'BYTES', 'MD5',
        'storageprovider', 'storagepath'
    ]

    # Fields that must be present, but may be empty
    ALLOWED_EMPTY_CSV_FIELDS = ['MTIME', 'SHA1', 'SHA256']

    ALL_CSV_FIELDS = REQUIRED_CSV_FIELDS + ALLOWED_EMPTY_CSV_FIELDS

    def __init__(self, gateway: Gateway) -> None:
        self.gateway = gateway
        self.results = {
            'rows_processed': 0,  # The total number of rows that were processed
            'rows_loaded': 0,  # The number of rows loaded into the database
            'rows_skipped': 0,  # The number of rows that were skipped (already existed in database)
            'errors': []  # Errors in rows (one entry in list for each row)
        }

    def process_file(self, file: str) -> None:
        csv_line_index = 2  # Starting at two to account for CSV header
        with open(file) as f:
            reader = csv.DictReader(f, delimiter=',')
            for row in reader:
                self.process_csv_row(csv_line_index, row)
                self.results['rows_processed'] += 1
                csv_line_index += 1

    def process_csv_row(self, csv_line_index: int, row: Dict[str, str]) -> None:
        if not self.is_row_valid(csv_line_index, row):
            return

        patsy_entry = PatsyEntryFactory.from_inventory_csv(row)
        if self.gateway.add(patsy_entry):
            self.results['rows_loaded'] += 1
        else:
            self.results['rows_skipped'] += 1

    def is_row_valid(self, csv_line_index: int, row_dict: Dict[str, str]) -> bool:
        """
        Returns True if the given row is valid, False otherwise.
        """
        missing_fields = []
        missing_values = []

        for key in Load.ALL_CSV_FIELDS:
            if key not in row_dict:
                missing_fields.append(key)
                continue
            if key in Load.REQUIRED_CSV_FIELDS:
                if not row_dict[key]:
                    missing_values.append(key)
                    continue

        if missing_fields or missing_values:
            self.results['errors'].append(
                f"Line {csv_line_index}, missing_fields: {missing_fields}, missing_values = {missing_values}"
            )
            return False
        return True