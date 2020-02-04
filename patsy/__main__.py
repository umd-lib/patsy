#!/user/bin/env python3

import argparse
import os

from . import version
from .utils import print_header
from .database import Db
from .model import Batch
from .populate import load_accession_records


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

    db = Db(path)
    session = db.session()
    print(session)
    batch = Batch()

    """
    # Load schema if required
    if not db.has_schema():
        print(f"Loading database schema...")
        with open('patsy/patsy.schema', 'r') as handle:
            db.connection.executescript(handle.read())
    print(f"PATSy database has the following tables:")
    for table in db.tables():
        print(f"  - {table}")

    # Run the chosen function
    source = ("/Users/westgard/Box Sync/AWSMigration" + \
              "/aws-migration-data/AccessionInventories/dcrprojects")
    load_accession_records(source, db)
    """

if __name__ == "__main__":
    main()
