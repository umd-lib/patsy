import requests
import re

from patsy.model import Accession, Batch, Location
from patsy.core.db_gateway import DbGateway

from dataclasses import dataclass, field
from typing import Dict, List, Optional
from pathlib import Path


class InvalidStatusCodeError(Exception):
    pass


class MissingHeadersError(Exception):
    pass


class InvalidTimeError(Exception):
    pass


@dataclass
class SyncResult():
    files_not_found: List[str] = field(default_factory=list)
    files_processed: int = 0
    locations_added: int = 0
    duplicate_files: int = 0

    def __repr__(self) -> str:
        lines = [
            f"files_processed: '{self.files_processed}'",
            f"locations_added: '{self.locations_added}'",
            f"Identifiers not in PATSy: '{self.files_not_found}'"
        ]

        return f"<LoadResult({','.join(lines)})>"


class Sync:
    # The URL and endpoints
    APTRUST_URL = 'https://repo.aptrust.org'
    FILE_REQUEST = '/member-api/v3/files'
    OBJECT_REQUEST = '/member-api/v3/objects'

    def __init__(self, gateway: DbGateway, headers: Dict) -> None:
        # Headers will be an enviroment variable that will be obtained and passed in
        self.headers = headers
        self.gateway = gateway
        self.sync_results = SyncResult()

    def get_request(self, endpoint: str, **params) -> list:
        results = []
        r = requests.get(url=Sync.APTRUST_URL + endpoint, params=params, headers=self.headers)
        # print(r.url)
        next_page = r.json().get('next')

        while next_page != '' and r.status_code == 200:
            results += r.json().get('results')
            r = requests.get(url=Sync.APTRUST_URL + next_page, headers=self.headers)
            next_page = r.json().get('next')

        if r.status_code == 200:
            results += r.json().get('results')
            return results
        else:
            raise InvalidStatusCodeError

    def parse_name(self, batchname: str) -> str:
        if batchname.startswith('archive'):
            batch_number = batchname[7:]
            return 'Archive' + re.sub(r'^0(\d\d\d)', r'\1', batch_number)
        elif batchname.startswith('pca'):
            return batchname
        elif batchname.startswith('pcb'):
            return batchname
        elif batchname.startswith('pgb'):
            return batchname
        else:
            return batchname

    def check_path(self, id: str, relpaths: list) -> Optional[Accession]:
        p = Path(id)

        # Initially try removing the first 3 paths
        # Then increment down to 3 if it doesn't match
        for i in range(3, 6):
            id = '/'.join(map(str, p.parts[i:]))
            match = list(filter(lambda x: x.relpath == id, relpaths))
            if match:
                return match[0]

        # match will be None if no match was found
        return None

    def check_or_add_files(self, identifiers: list, accessions: list, add: bool = False) -> None:
        # Go through the identifiers
        for id in identifiers:
            # Add processed file and check the path
            self.sync_results.files_processed += 1
            match = self.check_path(id, accessions)

            if match is None:
                # Add the identifier to the list of not found files
                self.sync_results.files_not_found.append(id)
                continue

            # Add and update
            if not add:
                self.sync_results.locations_added += 1
                continue

            location = self.gateway.session.query(Location) \
                           .filter(Location.storage_location == id,
                                   Location.storage_provider == "ApTrust") \
                           .first()

            if location is None:
                location = Location(
                    storage_location=id,
                    storage_provider="ApTrust",
                )
                self.gateway.session.add(location)
                match.locations.append(location)
                self.sync_results.locations_added += 1
            else:
                self.sync_results.duplicate_files += 1

    def check_batch(self, bag: dict) -> tuple:
        # Check if archive in PATSy
        batch_name = self.parse_name(bag.get('bag_name'))
        query = self.gateway.session.query(Batch) \
                    .filter(Batch.name == batch_name) \
                    .first()

        # If so create tuple from the query
        if query is not None:
            return query.id, query.name

        return ()

    def process(self, **params) -> SyncResult:
        if 'created_at__lteq' in params:
            bags = self.get_request(Sync.OBJECT_REQUEST, per_page=10000, created_at__lteq=params['created_at__lteq'])
        elif 'created_at__gteq' in params:
            bags = self.get_request(Sync.OBJECT_REQUEST, per_page=10000, created_at__gteq=params['created_at__gteq'])
        else:
            bags = self.get_request(Sync.OBJECT_REQUEST, per_page=10000)

        # Get all the objects and loop over them
        for bag in bags:
            in_patsy = self.check_batch(bag)

            # If bag was found in PATSy, go through the files in the bag
            if in_patsy:
                batch_id, batch_name = in_patsy
                print(f'Querying files from {batch_name}')

                accessions = self.gateway.session.query(Accession) \
                                 .filter(Accession.batch_id == batch_id) \
                                 .all()

                per_page = bag.get('payload_file_count')
                object_id = bag.get('id')
                files = self.get_request(Sync.FILE_REQUEST, intellectual_object_id=object_id, per_page=per_page)

                identifiers = list(map(lambda x: x.get('identifier'), files))
                self.check_or_add_files(identifiers, accessions)

        return self.sync_results
