from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI

from api.routes import alerts, pairs, rules
from config.settings import settings
from providers.kraken import KrakenProvider


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    async with httpx.AsyncClient(timeout=10.0) as client:
        app.state.provider = KrakenProvider(
            client,
            base_url=settings.kraken_api_base_url,
            requests_per_second=settings.kraken_requests_per_second,
        )
        yield


app = FastAPI(title="dropwatch", lifespan=lifespan)
app.include_router(pairs.router)
app.include_router(rules.router)
app.include_router(alerts.router)
