from abc import ABC, abstractmethod
from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class ResolvedPair:
    """Result of resolving a user-supplied symbol to a provider's canonical pair."""

    canonical_name: str
    display_name: str
    base_currency: str
    quote_currency: str


@dataclass(frozen=True)
class Quote:
    """A single point-in-time last/bid/ask observation for one pair."""

    canonical_name: str
    last: Decimal
    bid: Decimal
    ask: Decimal


@dataclass(frozen=True)
class MarketSnapshot:
    """A quote plus enough metadata and 24h stats to drive discovery."""

    canonical_name: str
    display_name: str
    base_currency: str
    quote_currency: str
    last: Decimal
    bid: Decimal
    ask: Decimal
    volume_24h: Decimal
    vwap_24h: Decimal

    @property
    def notional_24h(self) -> Decimal:
        return self.volume_24h * self.vwap_24h


class MarketDataProvider(ABC):
    """Adapter interface for a market data source (Kraken, later others)."""

    @abstractmethod
    async def resolve_pair(self, symbol: str) -> ResolvedPair:
        """Resolve a user-supplied symbol (e.g. "XBTUSD") to the provider's canonical pair."""

    @abstractmethod
    async def get_quotes(self, canonical_names: list[str]) -> dict[str, Quote]:
        """Fetch last/bid/ask for a batch of canonical pair names. Used by the poller."""

    @abstractmethod
    async def get_usd_market_snapshot(self) -> dict[str, MarketSnapshot]:
        """Fetch quote + 24h volume/vwap for every USD-quoted pair. Used by discovery."""
