from typing import Any, Dict, Optional


class PatsyRecord:
    """
    Represents a single accession/location in the database
    """
    def __init__(self) -> None:
        self.batch: str = ""
        self.relpath: str = ""
        self.filename: str = ""
        self.extension: str = ""
        self.bytes: str = ""
        self.moddate: str = ""
        self.md5: str = ""
        self.sha1: str = ""
        self.sha256: str = ""
        self.storage_provider: Optional[str] = None
        self.storage_location: Optional[str] = None

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, PatsyRecord):
            return self.batch == other.batch and \
                   self.relpath == other.relpath and \
                   self.filename == other.filename and \
                   self.extension == other.extension and \
                   self.bytes == other.bytes and \
                   self.moddate == other.moddate and \
                   self.md5 == other.md5 and \
                   self.sha1 == other.sha1 and \
                   self.sha256 == other.sha256 and \
                   self.storage_provider == other.storage_provider and \
                   self.storage_location == other.storage_location
        return False

    def __repr__(self) -> str:
        return f"{self.__class__}: {self.batch},{self.relpath},{self.md5}," \
               f"{self.storage_provider},{self.storage_location}"


class PatsyUtils:
    @staticmethod
    def from_inventory_csv(csv_row: Dict[str, str]) -> PatsyRecord:
        patsy_record = PatsyRecord()
        patsy_record.batch = csv_row['BATCH']
        patsy_record.relpath = csv_row['RELPATH']
        patsy_record.filename = csv_row['FILENAME']
        patsy_record.extension = csv_row['EXTENSION']
        patsy_record.bytes = csv_row['BYTES']
        patsy_record.moddate = csv_row['MODDATE']
        patsy_record.md5 = csv_row['MD5']
        patsy_record.sha1 = csv_row['SHA1']
        patsy_record.sha256 = csv_row['SHA256']
        patsy_record.storage_provider = csv_row.get('storageprovider', None)
        patsy_record.storage_location = csv_row.get('storagepath', None)

        return patsy_record

    @staticmethod
    def to_csv(patsy_record: PatsyRecord) -> Dict[str, str]:
        csv_row = {}

        csv_row['BATCH'] = patsy_record.batch
        csv_row['RELPATH'] = patsy_record.relpath
        csv_row['FILENAME'] = patsy_record.filename
        csv_row['EXTENSION'] = patsy_record.extension
        csv_row['BYTES'] = patsy_record.bytes
        csv_row['MODDATE'] = patsy_record.moddate
        csv_row['MD5'] = patsy_record.md5
        csv_row['SHA1'] = patsy_record.sha1
        csv_row['SHA256'] = patsy_record.sha256
        csv_row['storageprovider'] = patsy_record.storage_provider or ""
        csv_row['storagepath'] = patsy_record.storage_location or ""

        return csv_row
