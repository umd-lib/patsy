# Development Setup

## Introduction

This page describes how to set up a local development environment, and other
information useful for developers to be aware of.

An alternate setup, using VS Code "Dev Container" functionality, is described
in [VsCodeDevContainerSetup.md](./VsCodeDevContainerSetup.md).

## Prerequisites

The following instructions assume that "pyenv" and venv" are installed to
enable the setup of an isolated Python environment.

These instructions follow the guidance in the SSDR "Python" page in Confluence:
<https://confluence.umd.edu/display/LIB/Python>.

The "psycopg2" package requires that Postgres is installed on the local
workstation, either via the "Postgres.app" <https://postgresapp.com/>,
or via Brew:

```bash
$ brew install postgresql
```

## Local Development Setup

1) Clone the "patsy" GitHub repository:

    ```bash
    $ git clone git@github.com:umd-lib/patsy.git
    ```

2) Switch to the "patsy" directory:

    ```bash
    $ cd patsy
    ```

3) Set up the virtual environment:

    ```bash
    $ pyenv install --skip-existing $(cat .python-version)
    $ python -m venv .venv --prompt patsy
    $ source .venv/bin/activate
    ```

4) Run "pip install", including the "dev" and "test" dependencies:

    ```bash
    $ pip install -e .[dev,test]
    ```

## Database Setup

The application supports development using either a SQLite database,
or Postgres (production instances of the application always use
Postgres).

### SQLite Database

A empty SQLite database can be created simply by running the Alembic upgrade
command with the appropriate "database" argument (or "PATSY_DATABASE"
environment variable). For example, to create a SQLite database file named
"patsy-db.sqlite":

```bash
$ alembic -x database=patsy-db.sqlite upgrade head
```

## Postgres Server Setup

In cases where development against an actual Postgres database is
desirable, a local Docker container can be created.

The following command creates a "patsy-db" container, based on Postgres 14,
with a user named "postgres", and a password of "password":

```bash
$ docker run --rm -d -p 5432:5432 --name patsy-db \
  -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=password \
  -e POSTGRES_DB=patsy postgres:14
```

**Note:** All data in the container will be **LOST** when the container is
stopped.

The schema can then be set up using the following Alembic command:

```bash
$ alembic -x database=postgresql+psycopg2://postgres:password@localhost:5432/patsy upgrade head
```

For convenience, the "PATSY_DATABASE" environment variable can be set up:

```bash
$ export PATSY_DATABASE=postgresql+psycopg2://postgres:password@localhost:5432/patsy
```


## Sample Data - Postgres

### Set up a Docker Container running Postgres

1) Follow the instructions in the "Postgres Server Setup" to create a local
Docker container running Postgres.

### Dump/Restore Data from Kubernetes

The following procedure describes how to retrieve a Postgres database dump from
the "patsy-db-0" pod running in the Kubernetes "test" namespace, and using it
to populate a local Postgres database.

1) Switch to the Kubernetes "test" namespace:

    ```bash
    $ kubectl config use-context test
    ```

2) Create a Postgres database dump in the "/pgdate/patsy-db.sql" file:

    ```bash
    $ kubectl exec -it patsy-db-0 -- \
        bash -c 'pg_dump --username postgres --format=c --create --clean --if-exists --verbose patsy > /pgdata/dump-patsy.custom'
    ```

3) Download the "/pgdata/patsy-db.custom" to the local directory:

    ```bash
    $ kubectl cp patsy-db-0:/pgdata/dump-patsy.custom dump-patsy.custom
    ```

4) Delete the dump file in "/pgdata/patsy-db.custom" file in the "patsy-db-0"
   pod to recover the disk space:

    ```bash
    $ kubectl exec patsy-db-0 -- rm /pgdata/dump-patsy.custom
    ```

5) Populate the sample database with the Postgres dump, using a command of the
   form:

    ```bash
    $ pg_restore --host=<HOST> --port=<PORT> --username=<USERNAME> --dbname=<DBNAME> \
        --format=c --verbose --clean --if-exists --no-owner --no-privileges dump-patsy.custom
    ```

    where:

    * \<HOST> - the hostname or IP address of the local Postgres server to update
    * \<PORT> - the port of the local Postgres server to update
    * \<USERNAME> - the username of the local Postgres server to update
    * \<DBNAME> - the name of the database, typicontainer, based on Postgrescally "patsy"

    For example, to populate the local "patsy-db" Docker container:

    ```bash
    $ pg_restore --host=localhost --port=5432 --username=postgres --dbname=patsy \
        --format=c --verbose --clean --if-exists --no-owner --no-privileges dump-patsy.custom
    ```

## Running The Tests

```bash
$ pytest
```

By default, running pytest will run the tests in an SQLite in-memory database.
To run the test against a Postgres database, first create a docker container with
Postgres (see "Postgres Server Setup" section above).

Then run pytest with this additional parameter:

```bash
$ pytest --base-url="postgresql+psycopg2://postgres:password@localhost:5432/postgres"
```

## Test Coverage Report

A test coverage report can be generated using "pytest-cov":
Note: create a docker container first as mentioned previously

```bash
$ pytest --cov=patsy tests/
```

To generate an HTML report:

```bash
$ pytest --cov-report html --cov=patsy tests/
```

The report will be written to the "htmlcov/" directory.

## Code Style

Application code style should generally conform to the guidelines in
[PEP 8](https://www.python.org/dev/peps/pep-0008/). The "pycodestyle" tool
to check compliance with the guidelines can be run using:

```bash
$ pycodestyle .
```

## Python Type Hinting

The application uses Python type hinting (see
[PEP 484](https://www.python.org/dev/peps/pep-0484/)) to document class and
method signatures.

The "mypy" tool can be used to assess issues with the type hinting:

```bash
$ mypy patsy --strict --show-error-codes
```
