from .database import Session
from .model import Restore
from .progress_notifier import ProgressNotifier
from .utils import get_accessions


def find_filename_only_matches_command(batch=None, progress_notifier=ProgressNotifier()):
    """
    Called by the CLI to perform the "find_filename_only_matches" command.
    :param batch: The name of the batch to limit the search to. Defaults to None,
                  which means all accessions will be searched.
    :param progress_notifier: A ProgressNotifier to report individual file loads
                              and results. Defaults to ProgressNotifier, which
                              is a no-op
    :return: an array containing a description of the new matches that were
             found
    """
    session = Session()

    accessions = get_accessions(session, batch)
    progress_notifier.notify(f"Querying {accessions.count()} accession records.")

    new_matches_found = find_filename_only_matches(session, accessions)
    session.commit()
    return new_matches_found


def find_filename_only_matches(session, accessions):
    """
    Queries the database and adds new filename only matches for the given
    accessions.

    Note: The method will update the database if new matches are found

    :param session: the Session in which to perform the query
    :param accessions: the list of accessions use for the search
    :return: an array containing a description of the new matches that were
             found
    """
    new_matches_found = []
    for accession in accessions:
        restores = session.query(Restore)\
                          .filter(Restore.filename == accession.filename)

        for restore in restores:
            if restore.md5 != accession.md5 and\
               restore.bytes != accession.bytes and\
               restore not in accession.filename_only_matches:
                accession.filename_only_matches.append(restore)
                new_matches_found.append(f"{accession}:{restore}")

    return new_matches_found
