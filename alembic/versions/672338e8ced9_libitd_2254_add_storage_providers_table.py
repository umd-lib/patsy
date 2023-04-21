"""LIBITD-2254. Add storage_providers table

Revision ID: 672338e8ced9
Revises: f693a44bd7fe
Create Date: 2023-04-21 15:47:17.763566

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '672338e8ced9'
down_revision = 'f693a44bd7fe'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table('storage_providers',
                    sa.Column('id', sa.Integer(), nullable=False),
                    sa.Column('name', sa.String(), nullable=False),
                    sa.PrimaryKeyConstraint('id', name=op.f('pk_storage_providers')),
                    sa.UniqueConstraint('name', name=op.f('uq_storage_providers_name'))
                    )


def downgrade() -> None:
    op.drop_table('storage_providers')
