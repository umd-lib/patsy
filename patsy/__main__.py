#!/user/bin/env python3

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .cli import get_args
from .model import Batch
from .model import Asset
from .model import Dirlist
from .model import Instance
from .model import Base
from .populate import iter_accession_records_from
from .utils import print_header


def main():
    args = get_args()
    print_header()

    # Set up database file or use in-memory db
    if args.database == ":memory:":
        print(f"Using a transient in-memory database...")
    else:
        print(f"Using database at {args.database}...")         
    db_path = f"sqlite:///{args.database}"

    # Create the mapper and session
    print("Setting up the database session...")
    engine = create_engine(db_path)
    Session = sessionmaker(bind=engine)
    session = Session()

    """
    if 'bootstrap' in args.action:
        print("Bootstrapping the database...")
        print("Creating the schema using the declarative base...")
        Base.metadata.create_all(engine)
    
    if 'load_accessions' in args.action:
        print("Loading the accessions file..."
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
        
    print(f"\nActions complete!")
    if args.database == ':memory:':
        print(f"Cannot query transient DB. Use -d to specify a database file.")
    else:
        print(f"Query the bootstrapped database at {args.database}.")
    """

if __name__ == "__main__":
    main()
