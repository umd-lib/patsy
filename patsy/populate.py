from .model import Dirlist
from .model import Batch
from .model import Asset


def load_restored_files():
    pass


def load_accession_records(catalog_file):

    with open(catalog_file, 'r') as handle:
        