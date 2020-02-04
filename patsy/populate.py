from .model import Dirlist
from .model import Batch
from .model import Asset


def load_restored_files():
    pass


def load_accession_records(catalog_file):

    with open(catalog_file, 'r') as handle:
        for line in handle:
            cols = line.split(',')
            sourcefile = cols[0]
            sourceline = int(cols[1])
            filename = cols[2]
            if cols[3] != '':
                bytes = int(cols[3])
            else:
                bytes = None
            timestamp = cols[4]
            md5 = cols[5]
            asset = Asset(filename=filename, md5=md5, bytes=bytes,
                            dirlist_line=sourceline)
            print(asset)

