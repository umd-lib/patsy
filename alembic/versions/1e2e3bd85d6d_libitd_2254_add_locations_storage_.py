"""LIBITD-2254. Add locations.storage_provider_id foreign key column

Revision ID: 1e2e3bd85d6d
Revises: 672338e8ced9
Create Date: 2023-04-21 17:33:58.794147

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '1e2e3bd85d6d'
down_revision = '672338e8ced9'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Drop the "patsy_records" view. This is necessary because SQLite is
    # dropping and recreating the "locations" table, which the view uses.
    op.execute("DROP VIEW IF EXISTS patsy_records")

    with op.batch_alter_table('locations', schema=None) as batch_op:
        batch_op.add_column(sa.Column('storage_provider_id', sa.Integer(), nullable=True))
        batch_op.create_foreign_key(
            batch_op.f('fk_locations_storage_provider_id_storage_providers'),
            'storage_providers', ['storage_provider_id'], ['id']
        )

    # Restore the "patsy_records" view
    prior_view_module = op.get_context().script.get_revision('f693a44bd7fe').module
    obj = getattr(prior_view_module, 'patsy_records_view')
    batch_op.create_view(obj)


def downgrade() -> None:
    # Drop the "patsy_records" view. This is necessary because SQLite is
    # dropping and recreating the "locations" table, which the view uses.
    op.execute("DROP VIEW IF EXISTS patsy_records")

    with op.batch_alter_table('locations', schema=None) as batch_op:
        batch_op.drop_constraint(batch_op.f('fk_locations_storage_provider_id_storage_providers'), type_='foreignkey')
        batch_op.drop_column('storage_provider_id')

    # Restore the "patsy_records" view
    prior_view_module = op.get_context().script.get_revision('f693a44bd7fe').module
    obj = getattr(prior_view_module, 'patsy_records_view')
    batch_op.create_view(obj)
