import csv
import os
import sys
from .database import Session
from datetime import datetime
from .utils import get_batch_names


def export_inventory_command(batch, output=None):
    """
    Called by the CLI to perform the "export_inventory" command.
    :param batch: The name of the batch to limit the search to.
    :param output: The filepath to write the CVS file to.
    """
    session = Session()

    if batch is None:
        batch_list = get_batch_names(session)
    else:
        batch_list = [batch]

    num_exported = export_inventory(session, batch_list, output)

    session.commit()
    session.close()

    return f"Exported {num_exported} accessions."


def export_inventory(session, batch_list, output=None):
    """
    Exports all the records in a format suitable for the patsy v2 "inventory"
    manifest.

    :param session: the Session in which to perform the query
    :param output: The filepath to write the inventory manifest to
    """
    num_exported = 0
    if output is None:
        out = sys.stdout
        num_exported = export_inventory_entries(session, out, batch_list)
        return num_exported
    else:
        with open(output, mode='w') as file_stream:
            num_exported = export_inventory_entries(session, file_stream, batch_list)
        return num_exported


def export_inventory_entries(session, file_stream, batch_list):
    """
    Appends CSV records for the given batch_list to the given file_stream

    :param session: the Session in which to perform the query
    :param file_stream: the I/O stream to write the entries to
    :param batch_list: the list of batch names to query
    :return: the number of exported entries
    """
    inventory_fields = [
        'BATCH', 'PATH', 'DIRECTORY', 'RELPATH', 'FILENAME', 'EXTENSION',
        'BYTES', 'MTIME', 'MODDATE', 'MD5', 'SHA1', 'SHA256',
        'storageprovider', 'storagepath']

    writer = csv.DictWriter(file_stream, fieldnames=inventory_fields, extrasaction='ignore')

    writer.writeheader()

    num_exported = 0
    for batch in batch_list:
        num_exported += export_transferred_accessions(session, writer, batch)
        num_exported += export_untransferred_accessions(session, writer, batch)

    return num_exported


def export_transferred_accessions(session, writer, batch):
    """
    Appends CSV records for accessions with transfers for the given batch using
    the provided writer.

    :param session: the Session in which to perform the query
    :param writer: the csv.DictWriter to write the entries to
    :param batch: The batch to export
    :return: the number of exported entries
    """
    num_exported = 0
    engine = session.get_bind()
    with engine.connect() as con:
        select_fields = [
            'BATCH', 'PATH', 'RELPATH', 'FILENAME', 'BYTES', 'timestamp', 'MD5', 'storagepath'
        ]
        select_phrase = ",".join(select_fields)
        rs = con.execute(f"SELECT {select_phrase} FROM transferred_inventory_records WHERE BATCH=?", batch)

        storage_provider = "AWS"
        for row in rs:
            export_row(writer, row, select_fields, storage_provider)
            num_exported += 1

    return num_exported


def export_untransferred_accessions(session, writer, batch):
    """
    Appends CSV records for accessions without transfers for the given batch
    using the provided writer.

    :param session: the Session in which to perform the query
    :param writer: the csv.DictWriter to write the entries to
    :param batch: The batch to export
    :return: the number of exported entries
    """
    num_exported = 0

    engine = session.get_bind()
    with engine.connect() as con:
        select_fields = [
            'BATCH', 'PATH', 'RELPATH', 'FILENAME', 'BYTES', 'timestamp', 'MD5', 'storagepath'
        ]
        select_phrase = ",".join(select_fields)
        rs = con.execute(f"SELECT {select_phrase} FROM untransferred_inventory_records WHERE BATCH=?", batch)

        storage_provider = ""  # No storage provider for untransferred records
        for row in rs:
            export_row(writer, row, select_fields, storage_provider)
            num_exported += 1

    return num_exported


def export_row(writer, row, fields, storage_provider):
    """
    Writes a single CSV line to the given cvs.DictWriter using the given fields
    with the information provided by the row.

    :param writer: the csv.DictWriter to write the entries to
    :param row: A Dictionary containing the information to export
    :fields: A list containing the fields to include in the CSV output
    :storage_provider: the string to use for the "storage_provider" field in
                       the output
    """
    row_dict = {}
    for index, field in enumerate(fields):
        row_dict[field] = row[index]
    path = row_dict['PATH']
    relpath = row_dict['RELPATH']
    # A few patsy-db v1 entries do not have a RELPATH. In these cases,
    # assign FILENAME to RELPATH
    if not relpath:
        row_dict['RELPATH'] = row_dict['FILENAME']
        relpath = row_dict['FILENAME']

    row_dict["DIRECTORY"] = os.path.dirname(path)
    # Using RELPATH for extension, instead of PATH, because PATH may not be
    # present, while RELPATH is required.
    row_dict["EXTENSION"] = os.path.splitext(relpath)[1].lstrip('.').upper()

    timestamp = row_dict["timestamp"]
    timestamp_dict = handle_timestamp(timestamp)
    row_dict.update(timestamp_dict)

    row_dict["SHA1"] = ""
    row_dict["SHA256"] = ""
    row_dict["storageprovider"] = storage_provider
    row_dict
    writer.writerow(row_dict)


def handle_timestamp(timestamp):
    timestamp_dict = {}
    timestamp_dict["MTIME"] = ""
    timestamp_dict["MODDATE"] = ""

    if not timestamp or timestamp == "":
        return timestamp_dict

    timestamp_formats = [
        "%a %b %d %H:%M:%S %Z %Y",  # "Tue Aug 18 15:20:38 EDT 2015"
        "%Y-%m-%dT%H:%M:%S",        # "2011-02-09T14:09:00"
        "%Y-%m-%d %H:%M:%S",        # "2013-04-16 02:22:33"
        "%m/%d/%Y %H:%M"            # "11/11/2009 17:33"
    ]

    modification_time = None
    for format in timestamp_formats:
        try:
            modification_time = datetime.strptime(timestamp, format)
            break
        except ValueError:
            continue

    if modification_time:
        seconds_since_epoch = modification_time.timestamp()
        timestamp_dict["MTIME"] = seconds_since_epoch
        mod_time = modification_time.strftime('%Y-%m-%dT%H:%M:%S')
        timestamp_dict["MODDATE"] = mod_time
    else:
        print(f'WARNING -- Timestamp in unknown format: "{timestamp}"')

    return timestamp_dict
