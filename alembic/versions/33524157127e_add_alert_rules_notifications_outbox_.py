"""add alert_rules, notifications, outbox_events

Revision ID: 33524157127e
Revises: 8e82480fecfc
Create Date: 2026-08-14 15:31:21.302078

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '33524157127e'
down_revision: Union[str, Sequence[str], None] = '8e82480fecfc'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "alert_rules",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "pair_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("pairs.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("rule_type", sa.String(), nullable=False),
        sa.Column("threshold", sa.Numeric(24, 8), nullable=True),
        sa.Column("percent", sa.Numeric(8, 4), nullable=True),
        sa.Column("window_seconds", sa.Integer(), nullable=True),
        sa.Column("sigma", sa.Numeric(6, 3), nullable=True),
        sa.Column("direction", sa.String(), nullable=True),
        sa.Column("cooldown_seconds", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("is_enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("last_fired_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_alert_rules_pair_id", "alert_rules", ["pair_id"])

    op.create_table(
        "notifications",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "rule_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("alert_rules.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "pair_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("pairs.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("type", sa.String(), nullable=False),
        sa.Column("detected_price", sa.Numeric(24, 8), nullable=False),
        sa.Column("detected_state_hash", sa.String(), nullable=False),
        sa.Column("idempotency_key", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False, server_default="pending"),
        sa.Column("triggered_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_notifications_rule_id", "notifications", ["rule_id"])
    op.create_index("ix_notifications_pair_id", "notifications", ["pair_id"])
    op.create_index("ix_notifications_idempotency_key", "notifications", ["idempotency_key"], unique=True)

    op.create_table(
        "outbox_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "notification_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("notifications.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("payload", sa.String(), nullable=False),
        sa.Column("publish_attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.String(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_outbox_events_notification_id", "outbox_events", ["notification_id"], unique=True)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ix_outbox_events_notification_id", table_name="outbox_events")
    op.drop_table("outbox_events")
    op.drop_index("ix_notifications_idempotency_key", table_name="notifications")
    op.drop_index("ix_notifications_pair_id", table_name="notifications")
    op.drop_index("ix_notifications_rule_id", table_name="notifications")
    op.drop_table("notifications")
    op.drop_index("ix_alert_rules_pair_id", table_name="alert_rules")
    op.drop_table("alert_rules")
