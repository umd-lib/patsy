#!/user/bin/env python3

import argparse
import os

from . import version
from .utils import print_header
from .database import Db
from .model import Batch
from .populate import load_accession_records

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker



def main():
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

    # Parse args and initialize DB
    args = parser.parse_args()
    print_header()
    
    if args.database:
        path = args.database
        print(f"Using database at {path}...")
    else:
        path = ":memory:"
        print(f"Using a transient in-memory database...")


    dbpath = f'sqlite:///{path}'
    engine = create_engine(dbpath)
    Session = sessionmaker(bind=engine)
    session = Session()
    print(session)
    #session.configure(bind=engine, autoflush=False, expire_on_commit=False)
    Base = declarative_base()
    Batch.__table__.create(bind=engine, checkfirst=True)
    #Base.metadata.create_all(engine)

    batch = Batch(name="Archive001")    #print(Batch.__table__)
    session.add(batch)
    session.commit()
    my_batch = session.query(Batch).first()
    print(my_batch)


if __name__ == "__main__":
    main()
