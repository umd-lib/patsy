from .database import Session
from .model import Accession, Restore
from .progress_notifier import ProgressNotifier
from .utils import get_unmatched_transfers


def find_transfer_matches_command(progress_notifier=ProgressNotifier()):
    """
    Called by the CLI to perform the "find_transfer_matches" command.
    :param progress_notifier: A ProgressNotifier to report individual file loads
                              and results. Defaults to ProgressNotifier, which
                              is a no-op
    :return: an array containing a description of the new matches that were
             found
    """
    session = Session()

    unmatched_transfers = get_unmatched_transfers(session)
    progress_notifier.notify(f"Querying {unmatched_transfers.count()} unmatched transfer records.")

    new_matches_found = find_transfer_matches(session, unmatched_transfers)
    session.commit()
    return new_matches_found


def find_transfer_matches(session, transfers):
    """
    Queries the database and adds new transfer matches for the given transfers.

    Note: The method will update the database if new matches are found

    :param session: the Session in which to perform the query
    :param transfers: the list of transfers to search
    :return: an array containing a description of the new matches that were
             found
    """
    new_matches_found = []
    for transfer in transfers:
        transfer_filepath = transfer.filepath

        restore = session.query(Restore).filter(Restore.filepath == transfer_filepath).one_or_none()

        if restore:
            if transfer not in restore.transfers:
                restore.transfers.append(transfer)
                transfer.restore = restore
                new_matches_found.append(f"{transfer}:{restore}")

    return new_matches_found
