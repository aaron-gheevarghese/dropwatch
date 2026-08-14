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
