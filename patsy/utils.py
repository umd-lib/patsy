import csv
import hashlib
import os
import sys
from .model import Accession


def get_accessions(session, batch=None):
    """
    Queries the database for a list of accessions

    :param session: the Session in which to perform the query
    :param batch: The name of the batch to limit the search to. Defaults to None,
                  which means all accessions will be returned.
    :return: a Query object representing the list of accessions
    """
    if batch is None:
        accessions = session.query(Accession)
    else:
        accessions = session.query(Accession).filter(Accession.batch == batch)

    return accessions

# def calculate_md5(path):
#     """
#     Calclulate and return the object's md5 hash.
#     """
#     hash = hashlib.md5()
#     with open(path, 'rb') as f:
#         while True:
#             data = f.read(8192)
#             if not data:
#                 break
#             else:
#                 hash.update(data)
#     return hash.hexdigest()
#
#
# def get_common_root(path):
#     """
#     Return the root path common to all files in the dirlist.
#     Assumes that the dirlist is a csv with paths in the 2nd column.
#     """
#     with open(path) as handle:
#         reader = csv.reader(handle)
#         all_paths = [row[1] for row in reader]
#         if all_paths:
#             return os.path.commonpath(all_paths)
#         else:
#             return None
#
#
# def human_readable(bytes):
#     """
#     Return a human-readable representation of the provided number of bytes.
#     """
#     for n, label in enumerate(['bytes', 'KiB', 'MiB', 'GiB', 'TiB']):
#         value = bytes / (1024 ** n)
#         if value < 1024:
#             return f'{round(value, 2)} {label}'
#         else:
#             continue


def print_header():
    """
    Generate the script header and display it in the console.
    """
    title = f'| PATSy CLI |'
    border = '=' * len(title)
    spacer = '|' + ' '*(len(title)-2) + '|'
    sys.stdout.write(
        '\n'.join(['', border, spacer, title, spacer, border, '', ''])
    )
