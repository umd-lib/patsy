#!/user/bin/env python3

import argparse
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from . import version
from .utils import print_header
from .model import Batch
from .model import Asset
from .model import Dirlist
from .model import Instance
from .model import Base

from .populate import iter_accession_records_from


def get_args():
    """
    Create parsers and return args namespace object
    """
    parser = argparse.ArgumentParser(
        description='CLI for PATSy database'
    )
    parser.add_argument(
        '-v', '--version',
        action='version',
        help='Print version number and exit',
        version=version
    )
    parser.add_argument(
        '-d', '--database',
        action='store',
        help='Path to database file',
    )
    return parser.parse_args()    


def main():
    args = get_args()
    print_header()

    # Set up database file or use in-memory db
    if args.database:
        db_file = args.database
        print(f"Using database at {db_file}...") 
    else:
        db_file = ":memory:"
        print(f"Using a transient in-memory database...")
    db_path = f"sqlite:///{db_file}"

    # Create the mapper and session
    print("Setting up the database session...")
    engine = create_engine(db_path)
    Session = sessionmaker(bind=engine)
    session = Session()

    print("Creating the schema using the declarative base...")
    Base.metadata.create_all(engine)
    
    accessions_file = "/Users/westgard/Desktop/accession_catalog.csv"
    
    # Create all batch objects
    print("Adding batches...", end="")
    all_batches = set()
    for rec in iter_accession_records_from(accessions_file):
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
    for rec in iter_accession_records_from(accessions_file):
        dirlistname = rec.sourcefile
        batchname = rec.batch
        if dirlistname not in all_dirlists:
            all_dirlists.add(dirlistname)
            r, = session.query(Batch.id).filter(Batch.name == batchname).one()
            session.add(Dirlist(filename=dirlistname, batch_id=int(r)))
            session.commit()
    dirlist_count = session.query(Dirlist).count()
    print(f"added {dirlist_count} dirlists")

    # Create asset objects
    for rec in iter_accession_records_from(accessions_file):
        asset = Asset(md5=rec.md5,
                      timestamp=rec.timestamp,
                      filename=rec.filename,
                      bytes=rec.bytes
                      )
        session.add(asset)
        session.commit()
        added_count = session.query(Asset).count()
        print(f"Adding assets ... {added_count}", end="\r")
    

if __name__ == "__main__":
    main()
