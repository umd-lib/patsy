from collections import namedtuple
import csv
import os
import sqlite3
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from .model import Batch
from .model import Asset
from .model import Dirlist
from .model import Instance
from .model import RestoredFile
from .model import RestoredFileList
from .model import Base
from .utils import calculate_md5
from .utils import get_common_root

Session = sessionmaker()


def use_database_file(database):
    # Set up database file or use in-memory db
    if database == ":memory:":
        print(f"Using a transient in-memory database...")
    else:
        print(f"Using database at {database}...")         
    db_path = f"sqlite:///{database}"
    print("Binding the database session...")
    engine = create_engine(db_path)
    Session.configure(bind=engine)


def create_schema(args):
    use_database_file(args.database)
    session = Session()
    engine = session.get_bind()
    print("Creating the schema using the declarative base...")
    Base.metadata.create_all(engine)


def load_accessions(args):
    """
    Process a set of accession records and load to the database
    """

    AccessionRecord = namedtuple(
        "AccessionRecord", 
        "batch sourcefile sourceline filename bytes timestamp md5 relpath"
        )

    def iter_accession_records_from(catalog_file):
        """
        Load the accession catalog into batch, dirlist, & asset objects
        """
        with open(catalog_file, 'r') as handle:
            reader = csv.DictReader(handle, delimiter=',')
            for row in reader:
                yield AccessionRecord(*row)

    use_database_file(args.database)
    session = Session()
    
    # Process single file or directory of files
    print(f"Loading accessions from {args.source}")
    if os.path.isfile(args.source):
        filepaths = [args.source]
    elif os.path.isdir(args.source):
        filepaths = [
            os.path.join(args.source, f) for f in os.listdir(args.source)
            ]

    # Check whether batch exists and if not create it
    for sourcefile in filepaths:
        base = os.path.basename(sourcefile)
        batchname = os.path.splitext(base)[0]
        print(batchname)
        session.add(Batch(name=batchname))
        session.commit()
        batch_count = session.query(Batch).count()
        print(f"total {batch_count} batches")

    # Create all dirlist objects
    print("Adding dirlists...", end="")
    all_dirlists = set()
    for rec in iter_accession_records_from(args.source):
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
    for rec in iter_accession_records_from(args.source,
                                           args.filter):
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
    print("")


def load_restores(args):
    """
    Process a set of restored file lists and load to the database
    """
    use_database_file(args.database)
    session = Session()
    RestoredFileRecord = namedtuple(
        'RestoredFile', 
        "md5 path filename bytes"
        )

    # Process single file or directory of files
    if os.path.isfile(args.source):
        filepaths = [args.source]
    elif os.path.isdir(args.source):
        filepaths = [
            os.path.join(args.source, f) for f in os.listdir(args.source)
            ]

    for filepath in filepaths:
        print(f"Loading {filepath}...")
        md5 = calculate_md5(filepath)
        print("Checking database for this filelist...")
        results = session.query(RestoredFileList).filter(
            RestoredFileList.md5 == md5).all()
        if not results:
            filename = os.path.basename(filepath)
            commonroot = get_common_root(filepath)
            bytes = os.path.getsize(filepath)
            filelist = RestoredFileList(filename=filename, 
                                        md5=md5, 
                                        commonroot=commonroot, 
                                        bytes=bytes
                                        )
            session.add(filelist)
            session.commit()

        # Read each file list and add restored file records
        with open(filepath, 'r') as handle:
            reader = csv.reader(handle)
            for row in reader:
                fullpath = row[1]
                if fullpath.startswith(commonroot):
                    relpath = fullpath[len(commonroot):].lstrip('/')
                restore = RestoredFile(filename=row[3], 
                                       md5=row[0], 
                                       bytes=row[2], 
                                       path=row[1],
                                       relpath=relpath
                                       )
                session.add(restore)
                session.commit()
                added_count = session.query(RestoredFile).count()
                print(f"Adding restored files...added {added_count} files",
                        end="\r")
                
                '''
                print(restore)
                matches = session.query(Asset)\
                    .filter(Asset.md5 == restore.md5)\
                    .filter(Asset.filename == restore.filename)\
                    .filter(Asset.bytes == restore.bytes).all()
                print(f"Found {len(matches)} matches:")
                for match in matches:
                    print(match)
                '''


