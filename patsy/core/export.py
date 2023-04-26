import csv
import sys
from patsy.core.db_gateway import DbGateway
from patsy.core.patsy_record import PatsyUtils
from patsy.core.load import Load
from typing import List, TextIO


class ExportResult():
    """
    Holds the information about the results of an export
    """
    def __init__(self) -> None:
        self.batches_exported = 0
        self.rows_exported = 0

    def __repr__(self) -> str:
        lines = []
        lines = [
            f"batches_exported='{self.batches_exported}'",
            f"rows_exported='{self.rows_exported}'",
        ]

        return f"<ExportResult({','.join(lines)})>"


class Export:
    def __init__(self, gateway: DbGateway) -> None:
        self.gateway = gateway
        self.export_result = ExportResult()

    def export(self, batch: str, output: str) -> ExportResult:
        batch_list = []

        if batch is None:
            batches = self.gateway.get_all_batches()
            batch_list = [batch.name for batch in batches]
        else:
            batch_list = [batch]

        if output is None:
            out = sys.stdout
            self.export_entries(batch_list, out)
            return self.export_result
        else:
            with open(output, mode='w') as file_stream:
                self.export_entries(batch_list, file_stream)
            return self.export_result

    def export_entries(self, batch_list: List[str], file_stream: TextIO) -> None:
        writer = csv.DictWriter(file_stream, fieldnames=Load.ALL_CSV_FIELDS, extrasaction='raise')

        writer.writeheader()
        for b in batch_list:
            batch_records = self.gateway.get_batch_records(b)
            if len(batch_records) > 0:
                self.export_result.batches_exported += 1
            for patsy_record in batch_records:
                csv_dict = PatsyUtils.to_csv(patsy_record)
                writer.writerow(csv_dict)
                self.export_result.rows_exported += 1
