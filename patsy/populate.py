#!/usr/bin/env python3

import os
import sqlite3
import sys
import yaml

from .model import Dirlist
from .model import Batch
from .model import Asset
from .database import Db


def load_restored_files():
    pass


def load_accession_records(source_dir, db):

    batches = {}

    for file in os.listdir(source_dir):
        if file.startswith('.'):
            continue
        try:
            batchname, date, extra = file.split('_', 2)
            dirlist = DirList(os.path.join(source_dir, file))
            batches.setdefault(batchname, []).append(dirlist)
        except ValueError:
            sys.stdout.write(f"Could not parse file: {file}\n")
            sys.exit(1)

    for batchname in sorted(batches.keys()):
        print(f'\n{batchname.upper()}\n{"=" * len(batchname)}')

        '''
        batch = Batch(batchname, *batches[batchname])
        exists = db.lookup_batch(batch)
        if len(exists) == 1:
            id = exists[0][0]
            print(f"  Using existing batch {batch.identifier}... id = {id}")
        elif len(exists) > 1:
            sys.exit('too many matches')
        else:
            id = db.create_batch(batch)
            print(f"  Creating {batch.identifier}... id = {id}")
        print(f"  Source Files: {len(batch.dirlists)}")
        for n, dirlist in enumerate(batch.dirlists, 1):
            print(f"    ({n}) {dirlist.filename}: {len(dirlist.lines)} lines")
            dirlist_id = db.create_dirlist(dirlist, id)
        print(f"  Total Assets: {len(batch.assets)}")
        print(f"  Processing batch assets...")
        for n, asset in enumerate(batch.assets, 1):
            dirlist_id = db.lookup_dirlist_by_name(asset.dirlist_id)
            asset_id = db.create_asset(asset, dirlist_id)
        '''

