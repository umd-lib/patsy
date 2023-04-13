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

## Sample Data - Postgres

### Docker Postgres Container Setup

To set up a local Docker container running Postgres:

```bash
$ docker run --rm -d -p 5432:5432 --name patsy-sample-data -e POSTGRES_PASSWORD=password -e POSTGRES_DB=patsy postgres
```

The database connection URL would then be:

```text
postgresql+psycopg2://postgres:password@localhost:5432/patsy
```

If using the "PATSY_DATABASE" environment variable:

```bash
$ export PATSY_DATABASE=postgresql+psycopg2://postgres:password@localhost:5432/patsy
```

**Note:** All data in the container will be **LOST** when the container is
stopped.

### Dump/Restore Data from Kubernetes

The following procedure describes how to retrieve a Postgres database dump from
the "patsy-db-0" pod running in the Kubernetes "test" namespace, and using it
to populate a local Postgres database.

1) Switch to the Kubernetes "test" namespace:

    ```bash
    $ kubectl config use-context test
    ```

2) Create a Bash shell in the "patsy-db-0" pod:

    ```bash
    $ kubectl exec -it patsy-db-0 -- /bin/bash
    ```

3) In the "patsy-db-0" pod, create a Postgres database dump in the
   "/tmp/patsy-db.sql" file:

    ```bash
    patsy-db-0$ pg_dump --username postgres -Fc -v patsy > /pgdata/patsy-db.custom
    ```

4) Exit the "patsy-db-0" Bash shell:

    ```bash
    patsy-db-0$ exit
    ```

5) Download the "/pgdata/patsy-db.custom" to the local directory:

    ```bash
    $ kubectl cp --namespace=test patsy-db-0:/pgdata/patsy-db.custom patsy-db.custom
    ```

6) Delete the dump file in "/pgdata/patsy-db.custom" file in the "patsy-db-0"
   pod to recover the disk space:

    ```bash
    $ kubectl exec --namespace=test patsy-db-0 -- rm /pgdata/patsy-db.custom
    ```

7) Populate the sample database with the Postgres dump, using a command of the
   form:

    ```bash
    $ pg_restore --host=<HOST> --port=<PORT> --username=<USERNAME> --dbname=<DBNAME> -Fc -v patsy-db.custom
    ```

    where:

    * \<HOST> - the hostname or IP address of the local Postgres server to update
    * \<PORT> - the port of the local Postgres server to update
    * \<USERNAME> - the username of the local Postgres server to update
    * \<DBNAME> - the name of the database, typically "patsy"

    For example, to populate the local "patsy-sample-data" Docker container:

    ```bash
    $ pg_restore --host=localhost --port=5432 --username=postgres --dbname=patsy -Fc -v patsy-db.custom
    ```

## Running The Tests

```bash
$ pytest
```

By default, running pytest will run the tests in an SQLite in-memory database.
To run the test against a Postgres database, first create a docker container with
Postgres.

```bash
$ docker run -d -p 5432:5432 --name test -e POSTGRES_PASSWORD=password postgres
```

Then run pytest with this additional parameter

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
