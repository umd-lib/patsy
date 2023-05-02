from argparse import Namespace
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql import text
from patsy.database import Session
from patsy.core.patsy_record import PatsyRecord
from patsy.model import Batch, Accession, Location, StorageProvider
from patsy.database import use_database_file
from typing import cast, Dict, List, Optional


class AddResult():
    def __init__(self) -> None:
        self.batches_added = 0
        self.accessions_added = 0
        self.storage_providers_added = 0
        self.locations_added = 0


class DbGateway():
    def __init__(self, args: Namespace) -> None:
        use_database_file(args.database)
        self.session = Session()
        self.batch_ids: Dict[str, int] = {}

    def add(self, patsy_record: PatsyRecord) -> AddResult:
        self.add_result = AddResult()
        batch_name = patsy_record.batch
        batch_id = self.batch_ids.get(batch_name, None)

        if batch_id is None:
            batch = self.find_or_create_batch(patsy_record)
            batch_id = batch.id
            self.batch_ids[batch_name] = batch_id

        accession = self.find_or_create_accession(batch_id, patsy_record)
        location = self.find_or_create_location(patsy_record)
        if location:
            accession.locations.append(location)
        return self.add_result

    def find_or_create_batch(self, patsy_record: PatsyRecord) -> Batch:
        batch_name = patsy_record.batch
        batch: Batch = self.session.query(Batch).filter(Batch.name == batch_name).first()

        if batch is None:
            batch = Batch(name=batch_name)
            self.session.add(batch)
            self.session.commit()
            self.add_result.batches_added += 1

        return batch

    def find_or_create_accession(self, batch_id: int, patsy_record: PatsyRecord) -> Accession:
        accession: Accession = self.session.query(Accession).filter(
            Accession.batch_id == batch_id,
            Accession.relpath == patsy_record.relpath
        ).first()

        if accession is None:
            accession = Accession(
                relpath=patsy_record.relpath,
                filename=patsy_record.filename,
                extension=patsy_record.extension,
                bytes=patsy_record.bytes,
                timestamp=patsy_record.moddate,
                md5=patsy_record.md5,
                sha1=patsy_record.sha1,
                sha256=patsy_record.sha256
            )
            accession.batch_id = batch_id
            self.session.add(accession)
            self.add_result.accessions_added += 1

        return accession

    def find_or_create_storage_provider(self, patsy_record: PatsyRecord) -> Optional[StorageProvider]:
        if not patsy_record.storage_provider:
            return None

        storage_provider: StorageProvider = self.session.query(StorageProvider).filter(
            StorageProvider.name == patsy_record.storage_provider
        ).first()

        if storage_provider is None:
            storage_provider = StorageProvider(
                name=patsy_record.storage_provider
            )
            self.session.add(storage_provider)
            self.add_result.storage_providers_added += 1

        return storage_provider

    def find_or_create_location(
          self, patsy_record: PatsyRecord) -> Optional[Location]:
        storage_location = patsy_record.storage_location
        if not storage_location:
            return None

        storage_provider = self.find_or_create_storage_provider(patsy_record)
        if not storage_provider:
            return None

        location: Location = self.session.query(Location).filter(
            Location.storage_location == patsy_record.storage_location,
            Location.storage_provider_id == storage_provider.id
        ).first()

        if location is None:
            location = Location(
                storage_location=patsy_record.storage_location,
                storage_provider=storage_provider
            )
            self.session.add(location)
            self.add_result.locations_added += 1

        return location

    def get_accession_by_location(self, location: str) -> Optional[Accession]:
        """
        Returns the Accession with the given location.
        """
        result = self.session.query(Accession).join(Location.accessions).filter(Location.storage_location == location)
        return cast(Optional[Accession], result.first())

    def get_all_batches(self) -> List[Batch]:
        """
        Returns a list of all the batches in the database.
        """
        result = self.session.query(Batch).order_by(Batch.name.asc()).all()
        return cast(List[Batch], result)

    def get_batch_by_name(self, name: str) -> Optional[Batch]:
        """
        Returns the batch with the given name, or None if a batch with that
        name is not found.
        """
        return cast(Optional[Batch], self.session.query(Batch).filter(Batch.name == name).first())

    def get_batch_records(self, batch_name: str) -> List[PatsyRecord]:
        """
        Returns a (possibly empty) List of PatsyRecord objects representing the
        data from the given batch.

        If a batch does not exist with the given batch_name, an empty list is
        returned.
        """
        SQL_PATSY_RECORD_BY_NAME = \
            "SELECT * FROM patsy_records WHERE batch_name=:batch_name"
        sql_stmt = text(SQL_PATSY_RECORD_BY_NAME)
        sql_stmt = sql_stmt.bindparams(batch_name=batch_name)

        patsy_records: List[PatsyRecord] = []

        if not batch_name:
            return patsy_records

        engine = self.session.get_bind()
        with engine.connect() as con:
            rs = con.execute(sql_stmt).fetchall()

            for row in rs:
                item = row.items()
                db_values = {}
                for (field, value) in item:
                    db_values[field] = value
                patsy_record = DbGateway.db_view_to_patsy_record(db_values)
                patsy_records.append(patsy_record)

        return patsy_records

    def close(self) -> None:
        try:
            self.session.commit()
        except IntegrityError as err:
            self.session.rollback()

    @staticmethod
    def db_view_to_patsy_record(db_values: Dict[str, str]) -> PatsyRecord:
        """
        Converts a Dictionary of "patsy_records" View values into a PatsyRecord
        """
        patsy_record = PatsyRecord()
        patsy_record.batch = db_values.get('batch_name', "")
        patsy_record.relpath = db_values.get('relpath', "")
        patsy_record.filename = db_values.get('filename', "")
        patsy_record.extension = db_values.get('extension', "")
        patsy_record.bytes = str(db_values.get('bytes', 0))
        patsy_record.moddate = db_values.get('timestamp', "")
        patsy_record.md5 = db_values.get('md5', "")
        patsy_record.sha1 = db_values.get('sha1', "")
        patsy_record.sha256 = db_values.get('sha256', "")
        patsy_record.storage_provider = db_values.get('storage_provider', None)
        patsy_record.storage_location = db_values.get('storage_location', None)

        return patsy_record
