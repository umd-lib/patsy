from sqlalchemy.orm import Session
from sqlalchemy.ext.compiler import compiles
from patsy.core.db_gateway import DbGateway
from sqlalchemy.schema import DropTable
from patsy.core.load import Load
from argparse import Namespace
from patsy.model import Base
from pytest_alembic.plugin.fixtures import alembic_runner


# Needed when running tests against Postgres, so that dependent tables/views
# such as "patsy_record_view" don't prevent a table from being dropped.
@compiles(DropTable, "postgresql")
def _compile_drop_table(element, compiler, **kwargs):
    return compiler.visit_drop_table(element) + " CASCADE"


def clear_database(obj):
    """Used in test "teardown" to drop the database after tests."""
    obj.gateway.close()
    Base.metadata.drop_all(obj.gateway.session.get_bind())
    drop_patsy_records_view(obj.gateway.session)


def drop_patsy_records_view(session: Session):
    session.execute("DROP VIEW IF EXISTS patsy_records;")
