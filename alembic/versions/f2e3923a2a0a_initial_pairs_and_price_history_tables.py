"""initial pairs and price_history tables

Revision ID: f2e3923a2a0a
Revises: 
Create Date: 2026-08-13 17:14:57.492554

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'f2e3923a2a0a'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "pairs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("kraken_pair_name", sa.String(), nullable=False),
        sa.Column("display_name", sa.String(), nullable=False),
        sa.Column("base_currency", sa.String(), nullable=False),
        sa.Column("quote_currency", sa.String(), nullable=False),
        sa.Column("poll_interval_seconds", sa.Integer(), nullable=False, server_default="60"),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("notional_24h", sa.Numeric(24, 2), nullable=True),
        sa.Column("current_last_price", sa.Numeric(24, 8), nullable=True),
        sa.Column("current_bid_price", sa.Numeric(24, 8), nullable=True),
        sa.Column("current_ask_price", sa.Numeric(24, 8), nullable=True),
        sa.Column("last_checked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_pairs_kraken_pair_name", "pairs", ["kraken_pair_name"], unique=True)

    op.create_table(
        "price_history",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "pair_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("pairs.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("last_price", sa.Numeric(24, 8), nullable=False),
        sa.Column("bid_price", sa.Numeric(24, 8), nullable=False),
        sa.Column("ask_price", sa.Numeric(24, 8), nullable=False),
        sa.Column("checked_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_price_history_pair_id", "price_history", ["pair_id"])


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ix_price_history_pair_id", table_name="price_history")
    op.drop_table("price_history")
    op.drop_index("ix_pairs_kraken_pair_name", table_name="pairs")
    op.drop_table("pairs")
