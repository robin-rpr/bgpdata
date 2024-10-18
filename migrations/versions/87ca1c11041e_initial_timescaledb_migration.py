"""Initial TimescaleDB Migration

Revision ID: 87ca1c11041e
Revises: d7a9ac4b8e5c
Create Date: 2024-10-18 17:43:10.895735

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '87ca1c11041e'
down_revision: Union[str, None] = 'd7a9ac4b8e5c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'bgp_updates',
        sa.Column('id', sa.Integer(), primary_key=True, nullable=False),
        sa.Column('timestamp', sa.TIMESTAMP(), nullable=False),
        sa.Column('peer_asn', sa.BigInteger(), nullable=True),
        sa.Column('prefix', sa.String(), nullable=True),
        sa.Column('ip_version', sa.String(length=4), nullable=True),
        sa.Column('update_type', sa.String(length=10), nullable=True),
        sa.Column('resource', sa.String(), nullable=True),
    )

    # Convert the table to a TimescaleDB hypertable
    op.execute('SELECT create_hypertable(\'bgp_updates\', \'timestamp\');')


def downgrade() -> None:
    op.drop_table('bgp_updates')
