import uuid
from datetime import datetime
from decimal import Decimal

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, Numeric, String, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Pair(Base):
    __tablename__ = "pairs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    # Kraken's canonical name, e.g. "XXBTZUSD" — resolved once via AssetPairs and never re-resolved.
    kraken_pair_name: Mapped[str] = mapped_column(String, nullable=False, unique=True, index=True)
    # The symbol as originally submitted (POST /pairs) or Kraken's wsname (discovery), e.g. "XBT/USD".
    display_name: Mapped[str] = mapped_column(String, nullable=False)
    base_currency: Mapped[str] = mapped_column(String, nullable=False)
    quote_currency: Mapped[str] = mapped_column(String, nullable=False)

    poll_interval_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=60)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    # Set by the daily discovery job; null until the first discovery run has seen this pair.
    notional_24h: Mapped[Decimal | None] = mapped_column(Numeric(24, 2), nullable=True)

    # Denormalized latest observation, updated by the poller on every successful poll.
    current_last_price: Mapped[Decimal | None] = mapped_column(Numeric(24, 8), nullable=True)
    current_bid_price: Mapped[Decimal | None] = mapped_column(Numeric(24, 8), nullable=True)
    current_ask_price: Mapped[Decimal | None] = mapped_column(Numeric(24, 8), nullable=True)
    last_checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    price_history: Mapped[list["PriceHistory"]] = relationship(
        back_populates="pair", cascade="all, delete-orphan"
    )


class PriceHistory(Base):
    __tablename__ = "price_history"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pair_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("pairs.id", ondelete="CASCADE"), nullable=False, index=True
    )

    last_price: Mapped[Decimal] = mapped_column(Numeric(24, 8), nullable=False)
    bid_price: Mapped[Decimal] = mapped_column(Numeric(24, 8), nullable=False)
    ask_price: Mapped[Decimal] = mapped_column(Numeric(24, 8), nullable=False)
    # 24h volume at observation time (Kraken's v[1]). Nullable: rows written before this
    # column existed have no value and can't be backfilled accurately; every new poll writes it.
    volume_24h: Mapped[Decimal | None] = mapped_column(Numeric(24, 8), nullable=True)

    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    pair: Mapped["Pair"] = relationship(back_populates="price_history")


class AlertRule(Base):
    __tablename__ = "alert_rules"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    # No users table exists yet anywhere in this project — plain UUID, no FK. Delivery
    # is a single shared SNS email subscription for now, so this isn't consulted for
    # per-user routing yet; it's a forward-compatible identifying field.
    user_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    pair_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("pairs.id", ondelete="CASCADE"), nullable=False, index=True
    )

    # "absolute_below" | "absolute_above" implemented now; "percent_change" |
    # "zscore_move" | "spread_widen" are Step 7 — the columns they need already exist
    # below so the table doesn't need reshaping when they're wired up.
    rule_type: Mapped[str] = mapped_column(String, nullable=False)

    threshold: Mapped[Decimal | None] = mapped_column(Numeric(24, 8), nullable=True)
    percent: Mapped[Decimal | None] = mapped_column(Numeric(8, 4), nullable=True)
    window_seconds: Mapped[int | None] = mapped_column(Integer, nullable=True)
    sigma: Mapped[Decimal | None] = mapped_column(Numeric(6, 3), nullable=True)
    direction: Mapped[str | None] = mapped_column(String, nullable=True)

    # Not consulted for suppression yet (Step 4) — a rule can fire on every qualifying
    # observation for now. last_fired_at is still updated on every fire so Step 4 has
    # accurate data to work with once cooldown logic lands.
    cooldown_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    is_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    last_fired_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    pair: Mapped["Pair"] = relationship()
    notifications: Mapped[list["Notification"]] = relationship(back_populates="rule", cascade="all, delete-orphan")


class Notification(Base):
    __tablename__ = "notifications"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    rule_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("alert_rules.id", ondelete="CASCADE"), nullable=False, index=True
    )
    # Denormalized off the rule for fast queries without a join, same pattern as
    # PriceHistory.pair_id.
    pair_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("pairs.id", ondelete="CASCADE"), nullable=False, index=True
    )

    type: Mapped[str] = mapped_column(String, nullable=False)
    detected_price: Mapped[Decimal] = mapped_column(Numeric(24, 8), nullable=False)
    detected_state_hash: Mapped[str] = mapped_column(String, nullable=False)
    # sha256(f"{rule_id}:{pair_id}:{detected_state_hash}") — the authoritative dedup
    # guard. A duplicate poll of the same minute-bucketed price collides on this and is
    # rejected rather than creating a second notification.
    idempotency_key: Mapped[str] = mapped_column(String, nullable=False, unique=True, index=True)

    status: Mapped[str] = mapped_column(String, nullable=False, default="pending")
    triggered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    rule: Mapped["AlertRule"] = relationship(back_populates="notifications")
    outbox_event: Mapped["OutboxEvent"] = relationship(back_populates="notification", cascade="all, delete-orphan")


class OutboxEvent(Base):
    __tablename__ = "outbox_events"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    notification_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("notifications.id", ondelete="CASCADE"), nullable=False, unique=True
    )

    payload: Mapped[str] = mapped_column(String, nullable=False)
    publish_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # Additive beyond the PRD's listed columns, same precedent as PriceHistory.volume_24h:
    # nullable, doesn't touch existing fields. NULL = eligible immediately; set on each
    # failed publish to now() + exponential backoff so a failing row doesn't retry on
    # the very next publisher tick and doesn't block healthy rows behind it.
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[str | None] = mapped_column(String, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    notification: Mapped["Notification"] = relationship(back_populates="outbox_event")
