#!/user/bin/env python3

import argparse
import os

from . import version
from .utils import print_header
from .database import Db
from .model import Batch


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
    session = db.session()
    print(session)
    batch = Batch('Archive001')
    
if __name__ == "__main__":
    main()
