from typing import Dict


class PatsyRecord:
    """
    Represents a single accession/location in the database
    """
    def __init__(self):
        pass

    def __repr__(self) -> str:
        return f"{self.__class__}: {self.batch},{self.relpath},{self.md5},{self.storageprovider},{self.storagepath}"


class PatsyRecordFactory:
    @staticmethod
    def from_inventory_csv(csv_row: Dict[str, str]) -> PatsyRecord:
        patsy_record = PatsyRecord()
        patsy_record.batch = csv_row['BATCH']
        patsy_record.relpath = csv_row['RELPATH']
        patsy_record.filename = csv_row['FILENAME']
        patsy_record.extension = csv_row['EXTENSION']
        patsy_record.bytes = csv_row['BYTES']
        patsy_record.mtime = csv_row['MTIME']
        patsy_record.md5 = csv_row['MD5']
        patsy_record.sha1 = csv_row['SHA1']
        patsy_record.sha256 = csv_row['SHA256']
        patsy_record.storage_provider = csv_row['storageprovider']
        patsy_record.storage_location = csv_row['storagepath']

        return patsy_record
