from datetime import datetime
from decimal import Decimal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.client import get_session
from db.models import Pair
from providers.base import MarketDataProvider

router = APIRouter(prefix="/pairs", tags=["pairs"])


async def get_provider(request: Request) -> MarketDataProvider:
    return request.app.state.provider


class CreatePairRequest(BaseModel):
    symbol: str
    poll_interval_seconds: int = Field(default=60, gt=0)


class PairResponse(BaseModel):
    id: UUID
    kraken_pair_name: str
    display_name: str
    base_currency: str
    quote_currency: str
    poll_interval_seconds: int
    is_active: bool
    notional_24h: Decimal | None
    current_last_price: Decimal | None
    current_bid_price: Decimal | None
    current_ask_price: Decimal | None
    last_checked_at: datetime | None
    created_at: datetime

    model_config = {"from_attributes": True}


@router.post("", response_model=PairResponse, status_code=status.HTTP_201_CREATED)
async def create_pair(
    body: CreatePairRequest,
    session: AsyncSession = Depends(get_session),
    provider: MarketDataProvider = Depends(get_provider),
) -> Pair:
    try:
        resolved = await provider.resolve_pair(body.symbol)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc

    existing = await session.scalar(select(Pair).where(Pair.kraken_pair_name == resolved.canonical_name))
    if existing is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"{resolved.canonical_name} is already tracked",
        )

    pair = Pair(
        kraken_pair_name=resolved.canonical_name,
        display_name=resolved.display_name,
        base_currency=resolved.base_currency,
        quote_currency=resolved.quote_currency,
        poll_interval_seconds=body.poll_interval_seconds,
        is_active=True,
    )
    session.add(pair)
    await session.commit()
    await session.refresh(pair)
    return pair


@router.get("", response_model=list[PairResponse])
async def list_pairs(session: AsyncSession = Depends(get_session)) -> list[Pair]:
    result = await session.execute(select(Pair).order_by(Pair.created_at.desc()))
    return list(result.scalars())
