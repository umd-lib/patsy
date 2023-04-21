import tempfile
import pytest
import os

from patsy.commands.schema import Command as CommandSchema
from patsy.commands.checksum import Command, get_checksum
from sqlalchemy.ext.compiler import compiles
from patsy.core.db_gateway import DbGateway
from sqlalchemy.schema import DropTable
from patsy.core.load import Load
from argparse import Namespace
from patsy.model import Base
from patsy.database import DatabaseNotSetError, get_database_connection_url, use_database_file, Session


@pytest.fixture(autouse=True)
def clear_env():
    # Remove the "PATSY_DATABASE" environment variable, if defined
    os.environ.pop('PATSY_DATABASE', None)


class TestDatabase:
    def test_get_database_connection_url_throws_DatabaseNotSetError_when_no_value_found(self):
        with pytest.raises(DatabaseNotSetError):
            get_database_connection_url(None)

    def test_get_database_connection_url_uses_argument_is_when_given(self):
        assert get_database_connection_url('CLI_TEST') == 'sqlite:///CLI_TEST'

    def test_get_database_connection_url_uses_environment_variable_when_argument_is_None(self):
        os.environ['PATSY_DATABASE'] = 'ENV_VAR'
        assert get_database_connection_url(None) == 'sqlite:///ENV_VAR'

    def test_get_database_connection_url_use_argument_is_when_both_argument_and_environment_variable_aregiven(self):
        os.environ['PATSY_DATABASE'] = 'ENV_VAR'
        assert get_database_connection_url('CLI_TEST') == 'sqlite:///CLI_TEST'

    def test_get_database_connection_converts_simple_string_into_sqlite_urlL(self):
        assert get_database_connection_url('SQLLITE_DB.sqlite') == 'sqlite:///SQLLITE_DB.sqlite'

    def test_get_database_connection_passes_postgres_url_untouched(self):
        result = get_database_connection_url('postgresql+psycopg2://postgres:password@localhost:5432/patsy')
        assert result == 'postgresql+psycopg2://postgres:password@localhost:5432/patsy'

    def test_use_database_file_properly_configures_session(self):
        use_database_file('SQLLITE_DB.sqlite')
        session = Session()
        assert str(session.bind.url) == 'sqlite:///SQLLITE_DB.sqlite'
