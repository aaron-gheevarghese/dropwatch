"""rename price_history checked_at to observed_at, add volume_24h

Revision ID: 8e82480fecfc
Revises: f2e3923a2a0a
Create Date: 2026-08-13 19:11:00.341825

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8e82480fecfc'
down_revision: Union[str, Sequence[str], None] = 'f2e3923a2a0a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.alter_column("price_history", "checked_at", new_column_name="observed_at")
    op.add_column("price_history", sa.Column("volume_24h", sa.Numeric(24, 8), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("price_history", "volume_24h")
    op.alter_column("price_history", "observed_at", new_column_name="checked_at")
