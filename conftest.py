import pytest
from argparse import Namespace
import pytest_alembic
from pytest_alembic.config import Config
from patsy.core.db_gateway import DbGateway


def pytest_addoption(parser):
    """
    Adds an (optional) "--base-url" command-line option to the "pytest" command
    so that a SQLite database file, or database connection URL can be specified
    to use for testing.
    """

    parser.addoption(
        '--base-url',
        action='store',
        default=':memory:',
        help='Base Database URL for the tests'
    )


@pytest.fixture
def addr(request):
    """
    Returns the (optional) "--base-url" command-line option
    """
    return request.config.getoption('--base-url')


@pytest.fixture
def db_gateway(addr):
    """
    Sets up the database using Alembic migrations and returns the
    "DbGateway" object to use to connect to the database.
    """
    args = Namespace()
    args.database = addr
    gateway = DbGateway(args)
    engine = gateway.session.get_bind()
    with pytest_alembic.runner(config=Config(), engine=engine) as runner:
        runner.migrate_up_to('head')
        yield gateway

    # Drop the "alembic_versions" table Alembic so that running the migrations
    # will update the database
    gateway.session.execute("DROP TABLE IF EXISTS alembic_version;")
    gateway.session.commit()
