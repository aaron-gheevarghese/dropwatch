import asyncio
import time
from decimal import Decimal
from typing import Any

import httpx

from providers.base import MarketDataProvider, MarketSnapshot, Quote, ResolvedPair

USD_QUOTE_CODES = ("ZUSD", "USD")


class KrakenAPIError(RuntimeError):
    """Raised when Kraken's response envelope contains a non-empty `error` list."""


class KrakenProvider(MarketDataProvider):
    def __init__(
        self,
        client: httpx.AsyncClient,
        base_url: str = "https://api.kraken.com/0/public",
        requests_per_second: float = 1.0,
    ) -> None:
        self._client = client
        self._base_url = base_url.rstrip("/")
        self._min_interval = 1.0 / requests_per_second
        self._throttle_lock = asyncio.Lock()
        self._last_request_at = 0.0
        self._asset_pairs_cache: dict[str, Any] | None = None
        self._asset_pairs_lock = asyncio.Lock()

    async def _throttle(self) -> None:
        async with self._throttle_lock:
            now = time.monotonic()
            wait = self._last_request_at + self._min_interval - now
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_request_at = time.monotonic()

    async def _get(self, path: str, params: dict[str, str] | None = None) -> dict[str, Any]:
        await self._throttle()
        resp = await self._client.get(f"{self._base_url}{path}", params=params)
        resp.raise_for_status()
        data = resp.json()

        if data["error"]:
            raise KrakenAPIError(f"{path} returned error: {data['error']}")

        return data["result"]

    async def _get_asset_pairs(self) -> dict[str, Any]:
        # Cached for the provider's lifetime — AssetPairs metadata doesn't change within a run.
        async with self._asset_pairs_lock:
            if self._asset_pairs_cache is None:
                self._asset_pairs_cache = await self._get("/AssetPairs")
            return self._asset_pairs_cache

    async def resolve_pair(self, symbol: str) -> ResolvedPair:
        asset_pairs = await self._get_asset_pairs()

        by_canonical = {name.upper(): (name, info) for name, info in asset_pairs.items()}
        by_wsname = {
            info["wsname"].upper(): (name, info) for name, info in asset_pairs.items() if info.get("wsname")
        }

        cleaned = symbol.strip().upper()

        match = by_canonical.get(cleaned) or by_wsname.get(cleaned)

        if match is None and "/" not in cleaned:
            quote_codes = sorted({ws.split("/")[1] for ws in by_wsname if "/" in ws}, key=len, reverse=True)
            for quote in quote_codes:
                if cleaned.endswith(quote) and len(cleaned) > len(quote):
                    candidate = f"{cleaned[: -len(quote)]}/{quote}"
                    if candidate in by_wsname:
                        match = by_wsname[candidate]
                        break

        if match is None:
            raise ValueError(f"Could not resolve pair for {symbol!r}")

        canonical_name, info = match
        base, quote = info["wsname"].split("/")

        return ResolvedPair(
            canonical_name=canonical_name,
            display_name=symbol.strip(),
            base_currency=base,
            quote_currency=quote,
        )

    async def get_quotes(self, canonical_names: list[str]) -> dict[str, Quote]:
        result = await self._get("/Ticker", {"pair": ",".join(canonical_names)})

        return {
            pair: Quote(
                canonical_name=pair,
                last=Decimal(info["c"][0]),
                bid=Decimal(info["b"][0]),
                ask=Decimal(info["a"][0]),
                volume_24h=Decimal(info["v"][1]),
            )
            for pair, info in result.items()
        }

    async def get_usd_market_snapshot(self) -> dict[str, MarketSnapshot]:
        asset_pairs = await self._get_asset_pairs()
        usd_pairs = {
            name: info for name, info in asset_pairs.items() if info.get("quote") in USD_QUOTE_CODES
        }

        result = await self._get("/Ticker")

        snapshot: dict[str, MarketSnapshot] = {}
        for pair, info in result.items():
            pair_info = usd_pairs.get(pair)
            if pair_info is None:
                continue

            base, quote = pair_info["wsname"].split("/")
            snapshot[pair] = MarketSnapshot(
                canonical_name=pair,
                display_name=pair_info["wsname"],
                base_currency=base,
                quote_currency=quote,
                last=Decimal(info["c"][0]),
                bid=Decimal(info["b"][0]),
                ask=Decimal(info["a"][0]),
                volume_24h=Decimal(info["v"][1]),
                vwap_24h=Decimal(info["p"][1]),
            )

        return snapshot
