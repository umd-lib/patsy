from .database import Session
from .model import Restore, Accession
from .utils import get_accessions
import csv
import io
from contextlib import closing

unmatched_accessions_fields = ['batch', 'relpath']


def unmatched_accessions_command(batch=None, output=None, delete_unmatched=False):
    """
    Called by the CLI to perform the "unmatched_accessions" command.
    :param batch: The name of the batch to limit the search to. A batch name
                  must be provided.
    :param output: A filepath to write output to. Defaults to returning a string.
    :param delete_unmatched: True if unmatched accessions should be deleted.
                             Defaults to False.
    :return: a String containing information about the unmatched accessions
    """
    session = Session()

    unmatched_accessions_found = unmatched_accessions(session, batch)

    if output is None:
        handle = io.StringIO()
    else:
        handle = open(output, mode='w')

    stdout_output = None
    with closing(handle) as file_stream:
        unmatched_accessions_output(file_stream, unmatched_accessions_found)

        if delete_unmatched and (len(unmatched_accessions_found) > 0):
            delete_accessions(session, unmatched_accessions_found, file_stream)

        if isinstance(handle, io.StringIO):
            stdout_output = handle.getvalue()

    session.commit()
    session.close()

    if output is None:
        return stdout_output
    else:
        return f"Unmatched accessions output to file: {output}"


def unmatched_accessions(session, batch=None):
    """
    Finds unmatched accessions in the given batch. An accession is unmatched
    if it has no perfect matches.

    :param session: the Session in which to perform the query
    :param batch: The name of the batch to limit the search to. A batch name
                  must be provided.
    :return: a (possibly empty) array of unmatched accessions.
    """
    if batch is None:
        raise ValueError("Batch is required.")

    accessions = get_accessions(session, batch)
    return accessions.filter(Accession.perfect_matches == None).all()


def delete_accessions(session, unmatched_accessions_found, file_stream):
    """
    Deletes all the accessions in the provided array.

    :param session: the Session in which to perform the deletion
    :param unmatched_accessions_found: an array of accessions to delete.
    :param file_stream: an I/O stream to write output information to.
    """
    file_stream.write("\n---- Deleting accessions ----\n")
    for entry in unmatched_accessions_found:
        accession_output(file_stream, entry)
        session.delete(entry)


def unmatched_accessions_output(file_stream, unmatched_accessions):
    """
    Outputs information about unmatched accessions.

    :param file_stream: the I/O stream to write the entries to
    :param unmatched_accessions: an array of unmatched accessions
    """
    if len(unmatched_accessions) == 0:
        file_stream.write("No unmatched accessions found!")
        return

    writer = csv.DictWriter(file_stream, fieldnames=unmatched_accessions_fields,
                            extrasaction='ignore')
    writer.writeheader()
    for entry in unmatched_accessions:
        accession_output(file_stream, entry)


def accession_output(file_stream, accession):
    """
    Outputs information about an individual accession to the given file_stream.

    :param file_stream: the I/O stream to write the entry to
    :param accession: the accession to output
    """
    writer = csv.DictWriter(file_stream, fieldnames=unmatched_accessions_fields,
                            extrasaction='ignore')
    writer.writerow(accession.__dict__)