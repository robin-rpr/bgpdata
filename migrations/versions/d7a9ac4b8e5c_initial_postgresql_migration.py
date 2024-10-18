"""Initial PostgreSQL Migration

Revision ID: d7a9ac4b8e5c
Revises: 
Create Date: 2024-10-18 17:42:50.141403

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd7a9ac4b8e5c'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'users',
        sa.Column('id', sa.Integer(), primary_key=True, nullable=False),
        sa.Column('name', sa.String(), nullable=False),
        sa.Column('email', sa.String(), nullable=False, unique=True),
        sa.Column('created_at', sa.TIMESTAMP(), nullable=True),
    )


def downgrade() -> None:
    op.drop_table('users')
