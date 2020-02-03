#!/user/bin/env python3

import argparse
import os

from . import version
from .crud import create
from .utils import print_header
from .database import Db


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

    # parse args and initialize or connect to DB
    args = parser.parse_args()
    print_header()
    
    if args.database:
        path = args.database
        print(f"Using database at {path}...")
    else:
        path = ":memory:"
        print(f"Using a transient in-memory database...")
        
    
    db = Db(path)
    print(db)

    '''
    if not db.has_schema():
        print(f"Loading database schema...")
        with open('patsy/patsy.schema', 'r') as handle:
            schema_script = handle.read()
            db.connection.executescript(schema_script)
    '''

if __name__ == "__main__":
    main()
