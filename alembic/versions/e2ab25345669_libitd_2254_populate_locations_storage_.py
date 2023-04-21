"""LIBITD-2254. Populate locations.storage_provider_id

Populates the "locations.storage_provider_id" with the corresponding
entry in the "storage_provider" table, skipping any records with
existing values.

Note: Downgrading this migration will result in DATA LOSS if the
"storage_provider_id" column contained data prior to the upgrade.

Revision ID: e2ab25345669
Revises: 290b78bda2c0
Create Date: 2023-04-21 18:20:05.403846

"""
from alembic import op
from sqlalchemy import text
from sqlalchemy.orm import Session
from patsy.model import StorageProvider


# revision identifiers, used by Alembic.
revision = 'e2ab25345669'
down_revision = '290b78bda2c0'
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    session = Session(bind)

    storage_providers = session.query(StorageProvider)
    for storage_provider in storage_providers:
        sp_id = storage_provider.id
        sp_name = storage_provider.name

        update_query = text(
            """
            UPDATE locations
            SET storage_provider_id = :storage_provider_id
            WHERE storage_provider_id is NULL
            AND storage_provider = :storage_provider
            """
        )
        update_query = update_query.bindparams(
            storage_provider_id=f"{sp_id}",
            storage_provider=f"{sp_name}"
        )
        session.execute(update_query)


def downgrade() -> None:
    # Clear "storage_provider_id" column in all rows of the "locations" table.
    # Note: This will result in DATA LOSS if the "storage_provider_id" column
    # contained data prior to the upgrade.
    session = Session(bind=op.get_bind())
    update_query = text(
      """
     UPDATE locations SET storage_provider_id = NULL;
     """
    )
    session.execute(update_query)
