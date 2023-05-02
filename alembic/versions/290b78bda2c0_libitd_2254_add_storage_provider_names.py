"""LIBITD-2254. Add storage_provider names

Retrieves the unique list of storage provider names from the
"locations" table, and adds them to the "storage_providers" table.

Revision ID: 290b78bda2c0
Revises: 1e2e3bd85d6d
Create Date: 2023-04-21 18:09:03.342153

"""
from alembic import op
from sqlalchemy import text
from sqlalchemy.orm import Session
from patsy.model import StorageProvider


# revision identifiers, used by Alembic.
revision = '290b78bda2c0'
down_revision = '1e2e3bd85d6d'
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    session = Session(bind)

    sp_query = text(
      "SELECT DISTINCT storage_provider FROM locations"
    )
    results = session.execute(sp_query)

    sp_insert = text("INSERT INTO storage_providers (name) VALUES (:name)")
    for result in results:
        name = result[0]
        sp_insert = sp_insert.bindparams(name=name)
        session.execute(sp_insert)


def downgrade() -> None:
    # Delete all rows in the StorageProvider table
    session = Session(bind=op.get_bind())
    session.query(StorageProvider).delete()
    session.commit()
