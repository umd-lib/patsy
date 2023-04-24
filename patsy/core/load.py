import csv
from patsy.core.db_gateway import DbGateway, AddResult
from patsy.core.patsy_record import PatsyUtils
from typing import Dict, List, Optional


class LoadResult():
    def __init__(self) -> None:
        self.rows_processed = 0
        self.batches_added = 0
        self.accessions_added = 0
        self.storage_providers_added = 0
        self.locations_added = 0
        self.errors: List[str] = []

    def __repr__(self) -> str:
        lines = [
            f"rows_processed='{self.rows_processed}'",
            f"batches_added='{self.batches_added}'",
            f"accessions_added='{self.accessions_added}'",
            f"storage_providers_added='{self.storage_providers_added}'",
            f"locations_added='{self.locations_added}'",
            f"errors='{self.errors}'"
        ]

        return f"<LoadResult({','.join(lines)})>"


class Load:
    ALL_CSV_FIELDS = [
        'BATCH', 'PATH', 'DIRECTORY', 'RELPATH', 'FILENAME', 'EXTENSION',
        'BYTES', 'MTIME', 'MODDATE', 'MD5', 'SHA1', 'SHA256',
        'storageprovider', 'storagepath'
    ]

    # Fields that must be present in the CSV, with non-empty content
    REQUIRED_CONTENT_CSV_FIELDS = [
        'BATCH', 'RELPATH', 'FILENAME', 'BYTES', 'MD5',

    ]

    # Fields that must be present, but may be empty
    ALLOWED_EMPTY_CSV_FIELDS = [
        'EXTENSION', 'MTIME', 'MODDATE', 'SHA1', 'SHA256'
    ]

    REQUIRED_CSV_FIELDS = REQUIRED_CONTENT_CSV_FIELDS + ALLOWED_EMPTY_CSV_FIELDS

    # The following fields are not required in the CSV file
    ALLOWED_MISSING_FIELDS = [
        'storageprovider', 'storagepath'
    ]

    def __init__(self, gateway: DbGateway) -> None:
        self.gateway = gateway
        self.load_result = LoadResult()

    def process_file(self, file: str) -> LoadResult:
        csv_line_index = 2  # Starting at two to account for CSV header
        with open(file) as f:
            reader = csv.DictReader(f, delimiter=',')
            add_result = None
            for row in reader:
                add_result = self.process_csv_row(csv_line_index, row)
                self.load_result.rows_processed += 1
                csv_line_index += 1

                if add_result:
                    self.load_result.batches_added += add_result.batches_added
                    self.load_result.accessions_added += add_result.accessions_added
                    self.load_result.storage_providers_added += add_result.storage_providers_added
                    self.load_result.locations_added += add_result.locations_added

        return self.load_result

    def process_csv_row(self, csv_line_index: int, row: Dict[str, str]) -> Optional[AddResult]:
        if not self.is_row_valid(csv_line_index, row):
            return None

        patsy_record = PatsyUtils.from_inventory_csv(row)
        return self.gateway.add(patsy_record)

    def is_row_valid(self, csv_line_index: int, row_dict: Dict[str, str]) -> bool:
        """
        Returns True if the given row is valid, False otherwise.
        """
        missing_fields = []
        missing_values = []

        for key in Load.REQUIRED_CSV_FIELDS:
            if key not in row_dict:
                missing_fields.append(key)
                continue
            if key in Load.REQUIRED_CONTENT_CSV_FIELDS:
                if not row_dict[key]:
                    missing_values.append(key)
                    continue

        if missing_fields or missing_values:
            self.load_result.errors.append(
                f"Line {csv_line_index}, missing_fields: {missing_fields}, missing_values = {missing_values}"
            )
            return False
        return True
