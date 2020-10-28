import csv
import io
from .database import Session
from .model import Accession, Restore, Transfer, perfect_matches_table
from .utils import get_accessions, get_batch_names
from sqlalchemy.orm import joinedload


def batch_stats_command(batch, output=None):
    """
    Called by the CLI to perform the "batch_stats" command.
    :param batch: The name of the batch to limit the search to.
    :param output: The filepath to write the manifest entries to.
    """
    session = Session()

    if batch is None:
        batch_list = get_batch_names(session)
    else:
        batch_list = [batch]

    batch_stats_entries = batch_stats(session, batch_list)

    if output is None:
        out = io.StringIO()
        output_batch_stats_entries(out, batch_stats_entries)
        return out.getvalue()
    else:
        with open(output, mode='w') as file_stream:
            output_batch_stats_entries(file_stream, batch_stats_entries)
        return f"Stats output to file: {output}"


def output_batch_stats_entries(file_stream, batch_stats_entries):
    """
    Writes out the batch stats entries to the given stream. The caller is
    responsible for opening and closing the stream.

    :param file_stream: the I/O stream to write the entries to
    :param batch_stats_entries: an array of Dictionary objects representing the
           batch stats entries
    """
    batch_stats_fields = ['batch', 'num_accessions', 'num_accessions_with_perfect_matches', 'num_accessions_transferred']

    writer = csv.DictWriter(file_stream, fieldnames=batch_stats_fields)
    writer.writeheader()
    for entry in batch_stats_entries:
        writer.writerow(entry)


def batch_stats(session, batch_list):
    """
    Queries each of the batches in the batch_list, returning an arrray of
    results.

    :param session: the Session in which to perform the query
    :param batch_list: the list of batch names to query
    :return: an array of results from get_stats_for_batch
    """
    results = []
    for batch in batch_list:
        result = get_stats_for_batch(session, batch)
        results.append(result)

    return results


def get_stats_for_batch(session, batch):
    """
    Returns a Dictionary with the following keys:
    - batch: the name of the batch
    - num_accessions: The total number of accessions records for the batch
    - num_accessions_with_perfect_matches: The number of accession records
      in the batch with at least one perfect match. If an accession has
      multiple perfect matches, it is only counted once.
    - num_accessions_transferred: The number of accession records in the
      batch that have been transferred. If an accession has multiple
      perfect matches that have been transferred, it is only counted once.

    :param session: the Session in which to perform the query
    :param batch: the name of the batch to limit the search to. The batch name
                  is required.
    :return: a Dictionary containing the statistics for the given batch.
    """
    num_accessions = get_num_accessions(session, batch)
    num_accessions_with_perfect_matches = get_num_accessions_with_perfect_matches(session, batch)
    num_accessions_transferred = get_num_accessions_transferred(session, batch)

    return {
            "batch": batch,
            "num_accessions": num_accessions,
            "num_accessions_with_perfect_matches": num_accessions_with_perfect_matches,
            "num_accessions_transferred": num_accessions_transferred
           }


def get_num_accessions(session, batch):
    """
    The total number of accession records in the batch

    :param session: the Session in which to perform the query
    :param batch: the name of the batch to limit the search to
    :return: the total number of accession records in the batch
    """
    return get_accessions(session, batch).count()


def get_num_accessions_with_perfect_matches(session, batch):
    """
    The number of accession records in the batch that have at least one
    perfect match. If an accession has multiple perfect matches, it will
    only be counted once.

    :param session: the Session in which to perform the query
    :param batch: the name of the batch to limit the search to
    :return: the number of accession records in the batch that have at least one
             perfect match.
    """
    # If an accession has multiple perfect matches, we only want to count it
    # once
    accessions_with_a_perfect_match = session.query(Accession)\
                             .join(perfect_matches_table)\
                             .group_by(Accession.id)\
                             .filter(Accession.batch == batch)
    return accessions_with_a_perfect_match.count()


def get_num_accessions_transferred(session, batch):
    """
    The number of accession records in the batch that have at least one
    transfer. If an accession has multiple restores that have been transferred,
    it will only be counted once.

    :param session: the Session in which to perform the query
    :param batch: the name of the batch to limit the search to
    :return: the number of accession records in the batch that have at least one
             transfer.
    """
    query = session.query(Accession)\
        .options(
            joinedload(Accession.perfect_matches, innerjoin=True).joinedload(Restore.transfers, innerjoin=True)
        ).group_by(Accession.id).filter(Accession.batch == batch)

    # Using len(query.all) instead of query.count() because of deduplication,
    # see https://docs.sqlalchemy.org/en/13/faq/sessions.html#my-query-does-not-return-the-same-number-of-objects-as-query-count-tells-me-why
    return len(query.all())
