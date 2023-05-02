"""LIBITD-2254. Replace locations.storage_provider with relationship

Revision ID: 1d4f8fc4dfd8
Revises: e2ab25345669
Create Date: 2023-04-21 18:35:31.169175

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.engine.reflection import Inspector
from patsy.alembic.helpers.replaceable_objects import ReplaceableObject


# revision identifiers, used by Alembic.
revision = '1d4f8fc4dfd8'
down_revision = 'e2ab25345669'
branch_labels = None
depends_on = None

patsy_records_view = ReplaceableObject(
    "patsy_records",
    '''
    SELECT
        batches.id as "batch_id",
        batches.name as "batch_name",
        accessions.id as "accession_id",
        accessions.relpath,
        accessions.filename,
        accessions.extension,
        accessions.bytes,
        accessions.timestamp,
        accessions.md5,
        accessions.sha1,
        accessions.sha256,
        locations.id as "location_id",
        storage_providers.name as "storage_provider",
        locations.storage_location
        FROM batches
        LEFT JOIN accessions ON batches.id = accessions.batch_id
        LEFT JOIN accession_locations ON accessions.id = accession_locations.accession_id
        LEFT JOIN locations ON accession_locations.location_id = locations.id
        LEFT JOIN storage_providers ON locations.storage_provider_id = storage_providers.id
        ORDER BY batches.id
    '''
)


def upgrade() -> None:
    # Drop the "patsy_records" view.
    op.execute("DROP VIEW IF EXISTS patsy_records")

    with op.batch_alter_table('locations', schema=None) as batch_op:
        batch_op.drop_index('location_storage')
        batch_op.create_index('location_storage', ['storage_provider_id', 'storage_location'], unique=True)
        batch_op.drop_column('storage_provider')

    # Create the updated view
    op.create_view(patsy_records_view)


def downgrade() -> None:
    # Drop the "patsy_records" view.
    op.execute("DROP VIEW IF EXISTS patsy_records")

    with op.batch_alter_table('locations', schema=None) as batch_op:
        batch_op.add_column(sa.Column('storage_provider', sa.VARCHAR(), autoincrement=False, nullable=True))
        batch_op.drop_index('location_storage')
        batch_op.create_index('location_storage', ['storage_provider', 'storage_location'], unique=False)

    # Restore to the prior view
    prior_view_module = op.get_context().script.get_revision('f693a44bd7fe').module
    obj = getattr(prior_view_module, 'patsy_records_view')
    batch_op.create_view(obj)
