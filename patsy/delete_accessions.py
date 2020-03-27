from .database import Session
from .model import Accession


def delete_accessions_command(batch=None):
    """
    Called by the CLI to perform the "delete_accessions" command.

    :param batch: The name of the batch to delete
    :return: a String containing information about the deleted accessions
    """
    session = Session()

    num_deleted = delete_accessions(session, batch)

    session.commit()
    session.close()

    return f"Deleted {num_deleted} accessions from batch '{batch}'."


def delete_accessions(session, batch=None):
    """
    Deletes all accessions in the given batch.

    Due to the way the database is constructed, this will also delete all the
    related entries in the "perfect_matches", "altered_md5_matches", and
    "filename_only_matches" tables.

    :param session: the Session in which to perform the query
    :param batch: The name of the batch delete. A batch name
                  must be provided.
    :return: a (possibly empty) array of unmatched accessions.
    """
    if batch is None:
        raise ValueError("Batch is required.")

    num_deleted = session.query(Accession).filter(Accession.batch == batch).delete(synchronize_session=False)
    return num_deleted
