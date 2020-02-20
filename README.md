# patsy-db

Command-line client for preservation asset tracking system (PATSy)

## Prerequsites

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
> python3 -m patsy --database <SQLITE_DATABASE_FILE> schema 
```

where <SQLITE_DATABASE_FILE> is the path where the SQLite database should be
created.

### Loading accession records

Load accession records from CSV files:

```
> python3 -m patsy --database <SQLITE_DATABASE_FILE> accessions --source <ACCESSION_PATH>
```

where <SQLITE_DATABASE_FILE> is the path to the SQLite database. If
<ACCESSION_PATH> is a file, only that file will be loaded. If <ACCESSION_PATH>
is a directory, every file in that directory will be loaded.

### Loading restored file records

Load "restored file" information from CSV files:

```
> python3 -m patsy --database <SQLITE_DATABASE_FILE> restores --source <RESTORES_PATH>
```

where <SQLITE_DATABASE_FILE> is the path to the SQLite database. If
<RESTORES_PATH> is a file, only that file will be loaded. If <RESTORES_PATH>
is a directory, every file in that directory will be loaded.

### Finding perfect matches

To find new perfect matches for a particular batch of accessions:

```
> python3 -m patsy --database <SQLITE_DATABASE_FILE> find_perfect_matches --batch <BATCH>

where <SQLITE_DATABASE_FILE> is the path to the SQLite database, and <BATCH> is
an (optional) batch name (corresponding to the "batch" field in the accession).

If the "--batch" parameter is not provides, all accessions will be searched.


```
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
