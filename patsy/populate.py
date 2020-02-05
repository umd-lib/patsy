import csv

from .model import AccessionRecord
from .model import Dirlist
from .model import Batch
from .model import Asset


def load_restored_files():
    pass


def iter_accession_records_from(catalog_file):
    """
    Load the accession catalog into batch, dirlist, & asset objects
    """
    with open(catalog_file, 'r') as handle:
        reader = csv.reader(handle, delimiter=',')
        for row in reader:
            yield AccessionRecord(*row)
