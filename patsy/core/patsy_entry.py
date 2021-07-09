from typing import Dict


class PatsyEntry:
    """
    Represents a single accession/location in the database
    """
    def __init__(self):
        pass

    def __repr__(self) -> str:
        return f"{self.__class__}: {self.batch},{self.relpath},{self.md5},{self.storageprovider},{self.storagepath}"


class PatsyEntryFactory:
    @staticmethod
    def from_inventory_csv(csv_row: Dict[str, str]) -> PatsyEntry:
        patsy_entry = PatsyEntry()
        patsy_entry.batch = csv_row['BATCH']
        patsy_entry.relpath = csv_row['RELPATH']
        patsy_entry.filename = csv_row['FILENAME']
        patsy_entry.extension = csv_row['EXTENSION']
        patsy_entry.bytes = csv_row['BYTES']
        patsy_entry.mtime = csv_row['MTIME']
        patsy_entry.md5 = csv_row['MD5']
        patsy_entry.sha1 = csv_row['SHA1']
        patsy_entry.sha256 = csv_row['SHA256']
        patsy_entry.storageprovider = csv_row['storageprovider']
        patsy_entry.storagepath = csv_row['storagepath']

        return patsy_entry
