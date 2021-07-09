import csv
from .database import Session
from .model import Accession, Restore
from sqlalchemy.orm import joinedload


def create_manifest_command(batch, output=None):
    """
    Called by the CLI to perform the "create_manifest_command" command.
    :param batch: The name of the batch to limit the search to.
    :param output: The filepath to write the manifest entries to.
    :return: a String describing success or failure.
    """
    session = Session()

    untransferred_accessions = find_untransferred_accessions(session, batch)
    manifest_entries = generate_manifest_entries(untransferred_accessions)

    if not manifest_entries:
        return f"No untransferred accessions with perfect matches found for '{batch}'. Skipping manifest output."

    with open(output, mode='w') as manifest_file_stream:
        output_manifest_entries(manifest_file_stream, manifest_entries)

    return f"{len(manifest_entries)} entries written to {output}"


def find_untransferred_accessions(session, batch):
    """
    Returns an array of accessions in the given batch that have perfect
    matches, but have never been transferred.

    :param session: the Session in which to perform the query
    :param batch: the name of the batch to limit the search to
    :return: an array of accessions in the given batch that have perfect
    matches, but have never been transferred.
    """
    batch_accessions = session.query(Accession)\
        .options(joinedload(Accession.perfect_matches, innerjoin=True).joinedload(Restore.transfers))\
        .filter(Accession.batch == batch)

    untransferred_accessions = []

    for accession in batch_accessions:
        needs_transfer = True

        perfect_matches = accession.perfect_matches
        for perfect_match in perfect_matches:
            if perfect_match.transfers:
                # If we have any transfers, don't transfer this accession
                needs_transfer = False
                break

        if needs_transfer:
            untransferred_accessions.append(accession)

    return untransferred_accessions


def generate_manifest_entries(accessions):
    """
    Returns an array of manifest entries, with each element represented by a
    Dictionary.

    :param accessions: the list of accessions to use in the manifest
    :return: an array of manifest entries, with each element represented by a
    Dictionary.
    """
    if not accessions:
        return []

    manifest_entries = []
    for accession in accessions:
        # Note: When there are multiple perfect matches, the actual restore
        # records used is likely indeterminate
        perfect_match = accession.perfect_matches[0]
        entry = {'md5': perfect_match.md5, 'filepath': perfect_match.filepath, 'relpath': accession.relpath}
        manifest_entries.append(entry)

    return manifest_entries


def output_manifest_entries(manifest_file_stream, manifest_entries):
    """
    Writes out the manifest entries to the given stream. The caller is
    responsible for opening and closing the stream.

    :param manifest_file_stream: the I/O stream to write the entries to
    :param manifest_entries: an array of Dictionary objects representing the
           entries for the manifest
    """
    manifest_fields = ['md5', 'filepath', 'relpath']
    writer = csv.DictWriter(manifest_file_stream, fieldnames=manifest_fields)
    writer.writeheader()
    for entry in manifest_entries:
        writer.writerow(entry)


