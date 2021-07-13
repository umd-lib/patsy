# patsy-db

Command-line client for preservation asset tracking system (PATSy)

## Project Branches

This project uses the "GitHub Flow" branching model (see
<https://confluence.umd.edu/display/LIB/GitHub+Flow+Model+for+Kubernetes+configuration+%28k8s-%29+repositories>)

The "main" branch represents version 2 (v2) of the "patsy-db" codebase, which
is incompatible with the "legacy" version 1 (v1) codebase.

Any work on the legacy patsy-db v1 application should be performed on the
"main-v1" branch.

## Prerequisites

* Python 3.7
* pip
* virtualenv

## Getting Started

You are free to use any Python virtual environment tool to set up Python. The
following instructions use "virtualenv":

1) Checkout the project, and switch to the project root directory.

2) Create a virtualenv called venv using Python 3:

```
> virtualenv venv
```

3) Activate the virtual environment:

```
> source venv/bin/activate
```

4) Install the requirements

```
> pip install -r requirements.txt
```

## Running Patsy

Patsy is run as follows:

```
> python3 -m patsy <command options>
```

### Getting help

The following lists the known commands:

```
> python3 -m patsy --help
```

### Creating a new (empty) database

```
> python3 -m patsy --database <DATABASE> schema
```

where `<DATABASE>` is one of:

* ":memory:" to use a transient in-memory database
* a PostgreSQL database connection string of the form "postgresql://username:password@host:port/database"
* a path to an SQLite database file

### Loading accession records

Load accession records from CSV files:

```
> python3 -m patsy --database <DATABASE> accessions --source <ACCESSION_PATH>
```

where <DATABASE> is the path to the SQLite database. If
<ACCESSION_PATH> is a file, only that file will be loaded. If <ACCESSION_PATH>
is a directory, every file in that directory will be loaded.

### Loading restored file records

Load "restored file" information from CSV files:

```
> python3 -m patsy --database <DATABASE> restores --source <RESTORES_PATH>
```

where <DATABASE> is the path to the SQLite database. If
<RESTORES_PATH> is a file, only that file will be loaded. If <RESTORES_PATH>
is a directory, every file in that directory will be loaded.

### Loading transfer file records

Load transfer records from CSV files:

```
> python3 -m patsy --database <DATABASE> transfers --source <TRANSFERS_PATH>
```

where <DATABASE> is the path to the SQLite database. If
<TRANSFERS_PATH> is a file, only that file will be loaded. If <TRANSFERS_PATH>
is a directory, every file in that directory will be loaded.

### Finding perfect matches

A perfect match between an accession and a restore is where:

* The MD5 values match (accession.md5 == restore.md5)
* The filenames match (accession.filename == restore.filename)
* The file sizes match (accession.bytes == restore.bytes)

To find new perfect matches for a particular batch of accessions:

```
> python3 -m patsy --database <DATABASE> find_perfect_matches --batch <BATCH>
```

where <DATABASE> is the path to the SQLite database, and \<BATCH> is
an (optional) batch name (corresponding to the "batch" field in the accession).

If the "--batch" parameter is not provided, all accessions will be searched.

### Finding altered MD5 matches

An altered MD5 match between an accession and a restore is where:

* The MD5 values do not match (accession.md5 != restore.md5)
* The filenames match (accession.filename == restore.filename)
* The file sizes match (accession.bytes == restore.bytes)

An altered MD5 match indicates a *possible* match between an accession to a
restore, where there might be data corruption.

To find new altered MD5 matches for a particular batch of accessions:

```
> python3 -m patsy --database <DATABASE> find_altered_md5_matches --batch <BATCH>
```

where <DATABASE> is the path to the SQLite database, and \<BATCH> is
an (optional) batch name (corresponding to the "batch" field in the accession).

If the "--batch" parameter is not provided, all accessions will be searched.

### Finding filename only matches

A filename only match between an accession and a restore is where:

* The MD5 values do not match (accession.md5 != restore.md5)
* The file sizes do not match (accession.bytes != restore.bytes)
* The filenames match (accession.filename == restore.filename)

A filename match indicates a *possible* match between an accession to a
restore, where there might be data corruption, or just a simple coincidence
of filename.

To find new filename only matches for a particular batch of accessions:

```
> python3 -m patsy --database <DATABASE> find_filename_only_matches --batch <BATCH>
```

where <DATABASE> is the path to the SQLite database, and \<BATCH> is
an (optional) batch name (corresponding to the "batch" field in the accession).

If the "--batch" parameter is not provided, all accessions will be searched.

### Finding transfer matches

A transfer match links a restore record to a storage location (such as AWS).

To find new transfer matches:

```
> python3 -m patsy --database <DATABASE> find_transfer_matches
```

where <DATABASE> is the path to the SQLite database. By default,
only transfer records that do not have a match to a restore records will be
searched.

### Creating manifest files

A manifest file for use with the "aws-archiver" application is created with
the following command:

```
> python3 -m patsy --database <DATABASE> create_manifest --batch <BATCH> --output <OUTPUT_FILE>
```

where <DATABASE> is the path to the SQLite database, and \<BATCH> is
a batch name (corresponding to the "batch" field in the accession), and
<OUTPUT_FILE> is the output filename for the manifest file. The batch name is
required.

The manifest file contains entries for accession records that have at least
one perfect match to a restore record, but which has not been transferred.
If an accession record has multiple perfect matches, and at least one of
the restore records has been transferred, it will *not* be included in the
list.

### Batch Statistics

Statistics about all batches (or a particular batch) can be generated with the
following command:

```
> python3 -m patsy --database <DATABASE> batch_stats --batch <BATCH> --output <OUTPUT_FILE>
```

where <DATABASE> is the path to the SQLite database, and \<BATCH> is
a batch name (corresponding to the "batch" field in the accession) and
<OUTPUT_FILE> is the output filename for the CSV file containing the results.
Both \<BATCH> and <OUTPUT_FILE> are optional. If \<BATCH> is not provided,
information about all the batches will be output. If <OUTPUT_FILE> is not
provided, the output will be printed to standard out.

The following fields will be output:

* batch: the name of tha batch
* num_accessions: The number of accessions in the batch
* num_accessions_with_perfect_matches,: The number of accessions with at least
  one perfect match. Accessions with multiple perfect matches are only counted
  once.
* num_accessions_transferred: The number of accessions that have been
  transferred. Accessions that match multiple restore records which have been
  transferred will only be counted once.

### Unmatched Accessions

A list of unmatched accessions can be generated with the following command:

```
> python3 -m patsy --database <DATABASE> unmatched_accessions --batch <BATCH>
```

where <DATABASE> is the path to the SQLite database, and \<BATCH> is
a batch name (corresponding to the "batch" field in the accession). The \<BATCH>
parameter is required. A list of accessions without perfect matches will be
printed to the console.

**Note:**: Only perfect matches are considered. If an accession has an altered
MD5 match, or a filename only match, it **will** be considered unmatched and
included in the output.

The following fields will be output:

* batch: the name of tha batch
* relpath: The relative path of the accession record

There is an optional "--delete" flag which will **DELETE** the unmatched
accessions from the database. Only use this flag if you want to delete the
unmatched accession records:

```
> python3 -m patsy --database <DATABASE> unmatched_accessions --batch <BATCH> --delete
```

### Deleting all accessions in a batch

----
#### SQLite and Foreign Key Constraints

By default, SQLite does not enable foreign key constraints. When deleting
accessions, the "sqlite3" CLI client will *not* delete related matches in the
"perfect_matches", "altered_md5_matches", or "filename_only_matches" tables,
unless foreign key constraints are enabled, using the following command
(which must be entered in every session, as it is not persisted):

```
PRAGMA foreign_keys=ON;
```

The "DB Browser for SQLite" GUI client (https://sqlitebrowser.org/) has
foreign key constraints enabled by default, and so should be safe to use.

When using either of these clients to delete entries using SQL, be sure that
foreign key constraints are enabled, and also verify that the expected records
in related tables are deleted.
----

An entire batch of accessions can be deleted using the following command, which
will properly remove related entries in the "perfect_matches",
"altered_md5_matches", and "filename_only_matches" tables:

```
> python3 -m patsy --database <DATABASE> delete_accessions --batch <BATCH>
```

where <DATABASE> is the path to the SQLite database, and \<BATCH> is
a batch name (corresponding to the "batch" field in the accession). The \<BATCH>
parameter is required.

### Exporting Inventory Records for Patsy v2

A "inventory" CSV file can be created for loading into a patsy v2 database
with the following command:

```
> python3 -m patsy --database <DATABASE> export_inventory --batch <BATCH> --output <OUTPUT_FILE>
```

where \<DATABASE> is the path to the SQLite database, \<BATCH> is
a batch name (corresponding to the "batch" field in the accession) and
<OUTPUT_FILE> is the output filename for the CSV file containing the results.
Both \<BATCH> and <OUTPUT_FILE> are optional. If \<BATCH> is not provided,
information about all the batches will be output. If <OUTPUT_FILE> is not
provided, the output will be printed to standard out.

## Accession Records

Accession records represent the "canonical" information about an asset. These
records represent the items that need to be preserved.

Accession records are imported from CSV files with the following format:

* batch - the "batch" the records is associated with
* sourcefile - The filename of the file containing the batch
* sourceline - The line number of entry in the batch source file
* filename - The simple filename (without the path) of the file
* bytes - The total number of bytes in the file
* timestamp - A human-readable timestamp for the file
* md5 - The MD5 checksum of the file
* relpath - the relative path of the file, including the filename

All fields are required, except the timestamp.

An accession record can be uniquely identified by the following combination
of fields:

* batch
* relpath

## Restore Records

Restore records represent the information from the DIV IT backups, that need
to be matched to the accession records.

Restore records are imported from CSV files with the following format:

* md5 - the MD5 checksum of the file
* filepath - the full path to the restore file
* filename - the simple filename (without the path) of the file
* bytes - The total number of bytes in the file

A restore record is uniquely identified by the "filepath" field.

## Transfer Records

Transfer records represent which restore records have been uploaded to a storage
location, such as AWS storage.

Transfer records are imported from CSV files with the following format:

* filepath - the full path to the restore file
* storage_path - the storage path for the file (typically, an AWS storage path)

A transfer record is uniquely identified by the combination of "filepath" and
"storage_path" fields.

## Manifest File

The manifest file generated by the "create_manifest" command is a CSV file
with the following fields:

* md5 - the MD5 checksum of the restore record
* filepath - the "filepath" field of the restore record
* relpath - the "relpath" field of the accession record

## Data Files

Contact Joshua Westgrad for access to the GitHub repository containing the
accession and restores CSV files.

Some sample data is provided in this repository for testing. See
[sample_data/README.md][2].

## Running the Tests

The unit tests for the application can be run using "pytest", i.e.:

```
> pytest
```

## License

See the [LICENSE](LICENSE) file for license rights and limitations.

---

[1]: https://docs.python-guide.org/dev/virtualenvs/#virtualenvwrapper
[2]: sample_data/README.md
