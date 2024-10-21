"""Initial PostgreSQL Migration

Revision ID: 190d3606e33d
Revises: 
Create Date: 2024-10-19 01:44:21.526153

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '190d3606e33d'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Users Table
    op.create_table(
        'users',
        sa.Column('id', sa.Integer(), primary_key=True, nullable=False),
        sa.Column('name', sa.String(), nullable=False),
        sa.Column('email', sa.String(), nullable=False, unique=True),
        sa.Column('created_at', sa.TIMESTAMP(), nullable=True),
    )

    # BGP Updates Table
    op.create_table(
        'ris',
        sa.Column('id', sa.Integer, primary_key=True, autoincrement=True),
        sa.Column('type', sa.String(1), nullable=False),  # F: Full (R)oute, U: (A)nnouncement, W: Withdrawal
        sa.Column('timestamp', sa.DateTime, nullable=False),
        sa.Column('collector', sa.String(50), nullable=False),
        sa.Column('peer_as', sa.Integer, nullable=False),
        sa.Column('peer_ip', sa.String(50), nullable=False),
        sa.Column('prefix', sa.String(50), nullable=False),
        sa.Column('origins', sa.Text, nullable=True),
        sa.Column('as_path', sa.Text, nullable=True),
        sa.PrimaryKeyConstraint('id', 'timestamp')
    )

    # RIB Lite Table
    op.create_table(
        'ris_lite',
        sa.Column('id', sa.Integer, primary_key=True, autoincrement=True),
        sa.Column('timestamp', sa.DateTime, nullable=False),
        sa.Column('collector', sa.String(50), nullable=False),
        sa.Column('prefix', sa.String(50), nullable=False),
        sa.Column('full_peer_count', sa.Integer, nullable=False),
        sa.Column('partial_peer_count', sa.Integer, nullable=False),
        sa.Column('segment', sa.Text, nullable=True),
    )

    # Convert the table to a TimescaleDB hypertable
    op.execute("SELECT create_hypertable('bgp_updates', 'timestamp', if_not_exists => TRUE);")

    # Add a retention policy to automatically drop data older than 48 hours
    op.execute("SELECT add_retention_policy('bgp_updates', INTERVAL '48 hours');")


def downgrade() -> None:
    op.drop_table('users')
    op.drop_table('bgp_updates')
    op.drop_table('ris_lite')
