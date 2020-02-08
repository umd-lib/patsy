import sqlite3
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from .model import Batch
from .model import Asset
from .model import Dirlist
from .model import Instance
from .model import Base


def iter_accession_records_from(catalog_file, batch):
    """
    Load the accession catalog into batch, dirlist, & asset objects
    """
    with open(catalog_file, 'r') as handle:
        reader = csv.reader(handle, delimiter=',')
        for row in reader:
            if row[0] != batch:
                continue
            else:
                yield AccessionRecord(*row)


def create_schema(db_path):
    print("Setting up the database session...")
    engine = create_engine(db_path)
    Session = sessionmaker(bind=engine)
    session = Session()

    print("Creating the schema using the declarative base...")
    Base.metadata.create_all(engine)


def load_accessions():
    print("Loading the accessions file...")
    accessions_file = "/Users/westgard/Desktop/accession_catalog.csv"
    accessions_filter = args.batch

    # Create all batch objects
    print("Adding batches...", end="")
    all_batches = set()
    for rec in iter_accession_records_from(accessions_file,
                                           accessions_filter):
        batchname = rec.batch
        if batchname not in all_batches:
            all_batches.add(batchname)
            session.add(Batch(name=batchname))
            session.commit()
    batch_count = session.query(Batch).count()
    print(f"added {batch_count} batches")

    # Create all dirlist objects
    print("Adding dirlists...", end="")
    all_dirlists = set()
    for rec in iter_accession_records_from(accessions_file,
                                           accessions_filter):
        dirlistname = rec.sourcefile
        batchname = rec.batch
        if dirlistname not in all_dirlists:
            all_dirlists.add(dirlistname)
            r, = session.query(Batch.id).filter(
                Batch.name == batchname).one()
            session.add(Dirlist(filename=dirlistname, batch_id=int(r)))
            session.commit()
    dirlist_count = session.query(Dirlist).count()
    print(f"added {dirlist_count} dirlists")

    # Create asset objects
    for rec in iter_accession_records_from(accessions_file,
                                           accessions_filter):
        sourcefile_id, = session.query(Dirlist.id).filter(
            Dirlist.filename == rec.sourcefile).one()
        asset = Asset(md5=rec.md5,
                      timestamp=rec.timestamp,
                      filename=rec.filename,
                      bytes=rec.bytes,
                      dirlist_id=sourcefile_id,
                      dirlist_line=rec.sourceline
                      )
        session.add(asset)
        session.commit()
        added_count = session.query(Asset).count()
        print(f"Adding assets...added {added_count} assets", end="\r")


def load_restores():
    pass

