import argparse
import csv
import logging
import sqlalchemy

from dataclasses import dataclass
from patsy.core.db_gateway import DbGateway
from patsy.model import Accession
from typing import List, TypeVar


@dataclass
class UpdateArgs:
    """Class holding command-line arguments for the "update" command"""
    dry_run: bool
    skip_existing: bool
    verbose: bool
    batch_name: str
    db_compare_column: str
    db_target_column: str
    csv_compare_column: str
    csv_update_column: str
    file: str

    # Following is needed to satisfy mypy until Python v3.11
    # See https://peps.python.org/pep-0673/
    Self = TypeVar("Self", bound="UpdateArgs")

    @classmethod
    def from_cli_args(cls: type[Self], args: argparse.Namespace) -> Self:
        return cls(
          dry_run=args.dry_run,
          skip_existing=args.skip_existing,
          verbose=args.verbose,
          batch_name=args.batch,
          db_compare_column=args.db_compare_column,
          db_target_column=args.db_target_column,
          csv_compare_column=args.csv_compare_value,
          csv_update_column=args.csv_update_value,
          file=args.file
        )

    def validate(self, gateway: DbGateway) -> list[str]:
        errors = []

        # Verify that batch exists
        batch = gateway.get_batch_by_name(self.batch_name)
        if batch is None:
            errors.append(f"Batch named '{self.batch_name}' was not found.")

        # Verify that db_compare_column exists
        if not hasattr(Accession, self.db_compare_column):
            errors.append(f"Database compare column '{self.db_compare_column}' does not exist for accessions.")

        # Verify that db_target_column exists
        if not hasattr(Accession, self.db_target_column):
            errors.append(f"Database target column '{self.db_target_column}' does not exist for accessions.")

        try:
            with open(self.file) as f:
                reader = csv.DictReader(f, delimiter=',')
                fieldnames = reader.fieldnames

                if fieldnames is None or self.csv_compare_column not in fieldnames:
                    errors.append(f"CSV compare column '{self.csv_compare_column}' not found in '{self.file}'.")

                if fieldnames is None or self.csv_update_column not in fieldnames:
                    errors.append(f"CSV update column '{self.csv_update_column}' not found in '{self.file}'.")
        except OSError as os:
            errors.append(f"Could not access '{self.file}'. {os}")

        return errors


class UpdateResult():
    def __init__(self) -> None:
        self.csv_rows_processed = 0
        self.db_rows_updated = 0
        self.errors: List[str] = []

    def has_errors(self) -> bool:
        return len(self.errors) > 0

    def add_errors(self, messages: list[str]) -> None:
        self.errors.extend(messages)

    def __repr__(self) -> str:
        lines = [
            f"csv_rows_processed='{self.csv_rows_processed}'",
            f"db_rows_updated='{self.db_rows_updated}'",
            f"errors='{self.errors}'"
        ]

        return f"<UpdateResult({','.join(lines)})>"


class Update:
    def __init__(self, gateway: DbGateway) -> None:
        self.gateway = gateway
        self.update_result = UpdateResult()

    def update(self, args: UpdateArgs) -> UpdateResult:
        arg_errors = args.validate(self.gateway)
        if len(arg_errors) > 0:
            self.update_result.add_errors(arg_errors)
            return self.update_result

        dry_run = args.dry_run
        skip_existing = args.skip_existing
        batch_name = args.batch_name
        db_compare_column = args.db_compare_column
        db_target_column = args.db_target_column
        csv_compare_column = args.csv_compare_column
        csv_update_column = args.csv_update_column
        csv_file = args.file

        session = self.gateway.session
        with open(csv_file) as f:
            reader = csv.DictReader(f, delimiter=',')
            for row in reader:
                self.update_result.csv_rows_processed += 1
                csv_compare_value = row[csv_compare_column]
                csv_update_value = row[csv_update_column]

                filter_criteria = [
                    Accession.batch.has(name=batch_name),
                    getattr(Accession, db_compare_column) == csv_compare_value
                ]

                if skip_existing:
                    filter_criteria.append(
                        sqlalchemy.or_(
                            getattr(Accession, db_target_column).is_(None),
                            getattr(Accession, db_target_column) == ''
                        )
                    )

                accession_query = session.query(Accession).filter(*filter_criteria)

                matched_accessions = accession_query.all()

                for accession in matched_accessions:
                    self._perform_update(accession, csv_update_value, args)

                if not dry_run:
                    session.commit()

        return self.update_result

    def _perform_update(self, accession: Accession, csv_update_value: str, args: UpdateArgs) -> None:
        """
        Actually performs the update.

        The database will _not_ be updated if:

        * The "dry_run" argument is set (but the "rows updated" will be
          incremented in the UpdateResults object.
        * The existing value in the database matches the update value from
          the CSV,
        """
        db_target_column = args.db_target_column
        dry_run = args.dry_run
        verbose = args.verbose

        accession_existing_value = getattr(accession, db_target_column)
        if (accession_existing_value == csv_update_value):
            # Skip update if database already has the updated value
            return

        if (verbose):
            logging.info(
                f"Updating accession id: {accession.id}, "
                f"old_value: {accession_existing_value}, new_value: {csv_update_value}"
            )
        if not dry_run:
            setattr(accession, db_target_column, csv_update_value)

        self.update_result.db_rows_updated += 1
