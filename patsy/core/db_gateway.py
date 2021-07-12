import abc
import os
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql.operators import endswith_op
from sqlalchemy.sql.sqltypes import TIMESTAMP
from patsy.database import Session
from patsy.core.patsy_record import PatsyRecord
from patsy.model import Batch, Accession, Location
from patsy.core.gateway import Gateway
# from .load_result import LoadResult, FileLoadResult
# from .progress_notifier import ProgressNotifier
from patsy.database import use_database_file


class DbGateway(Gateway):
    def __init__(self, args):
        use_database_file(args.database)
        self.session = Session()
        engine = self.session.get_bind()

    def add(self, patsy_record: PatsyRecord) -> bool:
        # try:
        #     session = Session()

        batch_name = patsy_record.batch
        try:
            batch = self.find_or_create_batch(patsy_record)

            accession = self.find_or_create_accession(batch, patsy_record)

            location = self.find_or_create_location(accession, patsy_record)

            accession.locations.append(location)
            location.accessions.append(accession)
            self.session.commit()
        except IntegrityError as err:
            self.session.rollback()

        # finally:
        #     self.session.close()

    def find_or_create_batch(self, patsy_record: PatsyRecord) -> Batch:
        batch_name = patsy_record.batch
        batch = self.session.query(Batch).filter(Batch.name == batch_name).first()

        if batch is None:
            batch = Batch(name=batch_name)
            self.session.add(batch)

        return batch

    def find_or_create_accession(self, batch: Batch, patsy_record: PatsyRecord) -> Accession:
        accession = self.session.query(Accession).filter(
            Accession.batch == batch,
            Accession.relpath == patsy_record.relpath
            ).first()

        if accession is None:
            accession = Accession(
                relpath=patsy_record.relpath,
                filename=patsy_record.filename,
                extension=patsy_record.extension,
                bytes=patsy_record.bytes,
                timestamp=patsy_record.mtime,
                md5=patsy_record.md5,
                sha1=patsy_record.sha1,
                sha256=patsy_record.sha256
            )
            accession.batch = batch

        return accession

    def find_or_create_location(self, accession: Accession, patsy_record: PatsyRecord) -> Accession:

        location = self.session.query(Location).filter(
            Location.storage_location == patsy_record.storage_location,
            Location.storage_provider == patsy_record.storage_provider
        ).first()

        if location is None:
            location = Location(
                storage_location=patsy_record.storage_location,
                storage_provider=patsy_record.storage_provider
            )
            self.session.add(location)

        return location
