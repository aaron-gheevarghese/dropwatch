"""add users table and FK from alert_rules.user_id

Revision ID: 953cea0aefbd
Revises: 33524157127e
Create Date: 2026-08-14 15:50:14.567366

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '953cea0aefbd'
down_revision: Union[str, Sequence[str], None] = '33524157127e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "users",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("contact", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )

    # No alert_rules rows exist yet (POST /rules could never succeed without this table),
    # so there's no orphaned user_id data to reconcile before adding the constraint.
    op.create_foreign_key(
        "fk_alert_rules_user_id_users",
        "alert_rules",
        "users",
        ["user_id"],
        ["id"],
        ondelete="CASCADE",
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint("fk_alert_rules_user_id_users", "alert_rules", type_="foreignkey")
    op.drop_table("users")
