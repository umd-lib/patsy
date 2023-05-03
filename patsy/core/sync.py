import requests
import logging
import re

from patsy.model import Accession, Batch, Location, StorageProvider
from patsy.core.db_gateway import DbGateway

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from sqlalchemy import text
from pathlib import Path


class InvalidStatusCodeError(Exception):
    pass


class MissingHeadersError(Exception):
    pass


class InvalidTimeError(Exception):
    pass


@dataclass
class SyncResult():
    files_duplicated: List[str] = field(default_factory=list)
    files_not_found: List[str] = field(default_factory=list)
    skipped_batches: List[str] = field(default_factory=list)
    batches_processed: int = 0
    batches_skipped: int = 0
    files_processed: int = 0
    locations_added: int = 0
    duplicate_files: int = 0

    def __repr__(self) -> str:
        lines = [
            f"files_processed: {self.files_processed}",
            f"locations_added: {self.locations_added}",
            f"duplicate_files: {self.duplicate_files}",
            f"batches_skipped: {self.batches_skipped}",
            f"Identifiers not in PATSy: '{self.files_not_found}'"
            f"Files already in PATSy: '{self.files_duplicated}'"
            f"Batches that were skipped: '{self.skipped_batches}"
        ]

        return f"<LoadResult({','.join(lines)})>"


class Sync:
    # The URL and endpoints
    APTRUST_URL = 'https://repo.aptrust.org'
    FILE_REQUEST = '/member-api/v3/files'
    OBJECT_REQUEST = '/member-api/v3/objects'

    def __init__(self, gateway: DbGateway, headers: Dict[str, Any]) -> None:
        # Headers will be an enviroment variable that will be obtained and passed in
        self.headers = headers
        self.gateway = gateway
        self.sync_results = SyncResult()

    def get_request(self, endpoint: str, **params: Any) -> list[Dict[str, Any]]:
        results = []
        r = requests.get(url=self.APTRUST_URL + endpoint, params=params, headers=self.headers)

        while r.status_code == 200:
            get_results = r.json().get('results')
            if get_results is None:
                logging.info("There was no results to retrieve from the get request.")
                return []

            results.extend(get_results)
            next_page = r.json().get('next')
            if next_page == '':
                return results

            r = requests.get(url=self.APTRUST_URL + next_page, headers=self.headers)

        logging.warning(f"Got a {r.status_code} status code, skipping this get request.")
        return []

    def parse_name(self, batchname: str) -> str:
        if batchname.startswith('archive'):
            batch_number = batchname[7:]
            remove_first_digit = re.sub(r'^0(\d\d\d)', r'\1', batch_number)
            remove_letters = re.sub(r'\D', '', remove_first_digit)
            return 'Archive' + remove_letters
        elif batchname.startswith('pca'):
            return batchname
        elif batchname.startswith('pcb'):
            return batchname
        elif batchname.startswith('pgb'):
            return batchname
        else:
            return batchname

    def check_path(self, id: str, relpaths: list[Accession]) -> Optional[Accession]:
        p = Path(id)

        # Initially try removing the first 3 paths
        # Then increment down to 3 if it doesn't match
        for i in range(3, 6):
            id = '/'.join(map(str, p.parts[i:]))
            match = [x for x in relpaths if x.relpath == id]
            if match:
                return match[0]

        # match will be None if no match was found
        return None

    def check_or_add_files(self, batch: str, identifiers: list[str],
                           accessions: list[Accession], add: bool = False) -> None:
        amount_files_added: int = 0
        amount_not_found: int = 0
        amount_already_exists: int = 0
        amount_files_processed: int = 0

        ap_trust_storage_provider = self.gateway.session.query(StorageProvider).filter(
            StorageProvider.name == "APTrust"
            ).first()
        if ap_trust_storage_provider is None:
            return None

        # Go through the identifiers
        for id in identifiers:
            # Add processed file and check the path
            self.sync_results.files_processed += 1
            amount_files_processed += 1
            match = self.check_path(id, accessions)

            if match is None:
                # Add the identifier to the list of not found files
                self.sync_results.files_not_found.append(id)
                amount_not_found += 1
                continue

            location = self.gateway.session.query(Location) \
                           .filter(Location.storage_location == id,
                                   Location.storage_provider == ap_trust_storage_provider) \
                           .first()

            if location is None:
                if add:
                    location = Location(
                        storage_location=id,
                        storage_provider=ap_trust_storage_provider,
                    )
                    self.gateway.session.add(location)
                    match.locations.append(location)

                self.sync_results.locations_added += 1
                amount_files_added += 1
            else:
                self.sync_results.duplicate_files += 1
                self.sync_results.files_duplicated.append(id)
                amount_already_exists += 1

        if amount_files_added > 0:
            logging.info(f"Batch {batch}: {amount_files_added}/{amount_files_processed} files matched")

        if amount_not_found > 2:
            logging.info(f"Batch {batch}: {amount_not_found}/{amount_files_processed} not matched")

        if amount_already_exists > 0:
            logging.info(f"Batch {batch}: {amount_already_exists}/{amount_files_processed} preexisting matches")

    def check_batch(self, bag: Dict[str, Any]) -> Optional[tuple[str, str]]:
        # Check if archive in PATSy
        batch_name = self.parse_name(bag.get('bag_name'))  # type: ignore
        logging.debug(f"Checking if {batch_name} in database.")
        query = self.gateway.session.query(Batch) \
                    .filter(Batch.name == batch_name) \
                    .first()

        # If so create tuple from the query
        if query is not None:
            return query.id, query.name

        return None

    def check_new_locations(self, name: str) -> bool:
        session = self.gateway.session
        engine = session.get_bind()
        with engine.connect() as con:
            t = text("SELECT * FROM patsy_records WHERE batch_name=:name and storage_provider = 'APTrust'")
            rs = con.execute(t, name=name)

            if not rs:
                return True

        return False

    def process(self, **params: Any) -> SyncResult:
        bags = self.get_request(self.OBJECT_REQUEST, per_page=1000, **params)
        # Get all the objects and loop over them
        for bag in bags:
            in_patsy = self.check_batch(bag)
            self.sync_results.batches_processed += 1

            # If bag was found in PATSy, go through the files in the bag
            if in_patsy is not None:
                batch_id, batch_name = in_patsy
                accessions = self.gateway.session.query(Accession) \
                                 .filter(Accession.batch_id == batch_id) \
                                 .all()

                if self.check_new_locations(batch_name):
                    logging.info(f"Found a batch that didn't have APTrust locations in PATSy: {bag.get('title')}")
                    logging.info(f"There are {bag.get('file_count')} files in the batch")

                logging.debug(f'Attempting to check files from {batch_name}')
                object_id = bag.get('id')
                files = self.get_request(self.FILE_REQUEST, intellectual_object_id=object_id, per_page=1000)

                if files:
                    logging.debug("Successfully retrieved files!")
                    identifiers = [f.get('identifier') for f in files]
                    self.check_or_add_files(batch_name, identifiers, accessions, add=True)  # type: ignore

                else:
                    logging.warning("Batch was skipped!")
                    self.sync_results.batches_skipped += 1
                    self.sync_results.skipped_batches.append(bag.get('bag_name'))  # type: ignore

            else:
                logging.warning("Batch was not found in database! Skipping this batch!")
                self.sync_results.batches_skipped += 1
                self.sync_results.skipped_batches.append(bag.get('bag_name'))  # type: ignore

        logging.debug("FINISHED PROCESS")
        return self.sync_results
