import sys
from .model import Accession, Transfer
from sqlalchemy import asc


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


def get_batch_names(session):
    """
    Returns a list of all the batch names, i.e. the "batch" field in the
    Accession table, in alphabetical order

    :param session: the Session in which to perform the query
    :return: a list of all the batch names, in alphabetical order
    """

    batch_names = []
    query_result = session.query(Accession.batch).distinct().order_by(asc(Accession.batch)).all()

    for row in query_result:
        batch_names.append(row[0])

    return batch_names


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
