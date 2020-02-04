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

from .populate import load_accession_records


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
    engine = create_engine(db_path)
    Session = sessionmaker(bind=engine)
    session = Session()
    print(session)
    for rec in load_accession_records("/home/jwestgard/Desktop/accession_catalog.csv"):
        batchname = rec.batch
        session.add(Batch(name=batchname))
    session.commit()


if __name__ == "__main__":
    main()
