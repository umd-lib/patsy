import sys
from .model import Accession, Transfer


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


def get_unmatched_transfers(session):
    """
    Queries the database for a list of all transfers without a matching restore

    :param session: the Session in which to perform the query
    :return: a Query object representing the list of transfers without a
             matching restore
    """
    transfers = session.query(Transfer).filter(Transfer.restore == None)

    return transfers


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
