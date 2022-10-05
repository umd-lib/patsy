# Development Setup

## Introduction

This page describes how to set up a development environment, and other
information useful for developers to be aware of.

## Prerequisites

The following instructions assume that "pyenv" and "pyenv-virtualenv" are
installed to enable the setup of an isolated Python environment.

See the following for setup instructions:

* <https://github.com/pyenv/pyenv>
* <https://github.com/pyenv/pyenv-virtualenv>

Once "pyenv" and "pyenv-virtualenv" have been installed, install Python 3.7.10:

```
> pyenv install 3.7.10 --skip-existing
```

## Installation for Development

1) Clone the "patsy-db" Git repository:

```
> git clone git@github.com:umd-lib/patsy-db.git
```

2) Switch to the "patsy-db" directory:

```
> cd patsy-db
```

3) Set up the virtual environment:

```
> pyenv virtualenv 3.7.10 patsy-db
> pyenv shell patsy-db
```

4) Run "pip install", including the "dev" and "test" dependencies:

```
> pip install -e .[dev,test]
```

## Running The Tests

```
> pytest
```

By default, running pytest will run the test in an Sqlite in memory database.
To run the test against a Postgres database, first create a docker container with
Postgres.

```
> docker run -d -p 5432:5432 --name test -e POSTGRES_PASSWORD=password postgres
```

Then run pytest with this additional parameter

```
> pytest --base-url="postgresql+psycopg2://postgres:password@localhost:5432/postgres"
```

## Test Coverage Report

A test coverage report can be generated using "pytest-cov":
Note: create a docker container first as mentioned previously

```
> pytest --cov=patsy tests/
```

To generate an HTML report:

```
> pytest --cov-report html --cov=patsy tests/
```

The report will be written to the "htmlcov/" directory.

## Code Style

Application code style should generally conform to the guidelines in
[PEP 8](https://www.python.org/dev/peps/pep-0008/). The "pycodestyle" tool
to check compliance with the guidelines can be run using:

```
> pycodestyle .
```

## Python Type Hinting

The application uses Python type hinting (see
[PEP 484](https://www.python.org/dev/peps/pep-0484/)) to document class and
method signatures.

The "mypy" tool can be used to assess issues with the type hinting:

```
> mypy patsy --strict --show-error-codes
```
