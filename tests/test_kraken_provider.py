import httpx
import respx

from providers.kraken import KrakenAPIError, KrakenProvider

BASE_URL = "https://api.kraken.com/0/public"

ASSET_PAIRS_RESPONSE = {
    "error": [],
    "result": {
        # Legacy pair: Z-prefixed fiat quote code, X-prefixed crypto base code.
        "XXBTZUSD": {"wsname": "XBT/USD", "base": "XXBT", "quote": "ZUSD"},
        "XETHZUSD": {"wsname": "ETH/USD", "base": "XETH", "quote": "ZUSD"},
        # Modern pair: plain quote code, no legacy prefixing.
        "SOLUSD": {"wsname": "SOL/USD", "base": "SOL", "quote": "USD"},
    },
}


def make_provider(client: httpx.AsyncClient) -> KrakenProvider:
    return KrakenProvider(client, base_url=BASE_URL, requests_per_second=1000)


@respx.mock
async def test_error_envelope_raises_before_touching_result() -> None:
    respx.get(f"{BASE_URL}/AssetPairs").mock(
        return_value=httpx.Response(200, json={"error": ["EGeneral:Invalid arguments"], "result": {}})
    )

    async with httpx.AsyncClient() as client:
        provider = make_provider(client)
        try:
            await provider.resolve_pair("XBTUSD")
        except KrakenAPIError as exc:
            assert "EGeneral:Invalid arguments" in str(exc)
        else:
            raise AssertionError("expected KrakenAPIError")


@respx.mock
async def test_resolve_pair_canonical_direct_match() -> None:
    respx.get(f"{BASE_URL}/AssetPairs").mock(return_value=httpx.Response(200, json=ASSET_PAIRS_RESPONSE))

    async with httpx.AsyncClient() as client:
        provider = make_provider(client)
        resolved = await provider.resolve_pair("XXBTZUSD")

    assert resolved.canonical_name == "XXBTZUSD"
    assert resolved.base_currency == "XBT"
    assert resolved.quote_currency == "USD"
    assert resolved.display_name == "XXBTZUSD"


@respx.mock
async def test_resolve_pair_wsname_direct_match() -> None:
    respx.get(f"{BASE_URL}/AssetPairs").mock(return_value=httpx.Response(200, json=ASSET_PAIRS_RESPONSE))

    async with httpx.AsyncClient() as client:
        provider = make_provider(client)
        resolved = await provider.resolve_pair("XBT/USD")

    assert resolved.canonical_name == "XXBTZUSD"


@respx.mock
async def test_resolve_pair_no_slash_falls_back_to_quote_suffix_split() -> None:
    respx.get(f"{BASE_URL}/AssetPairs").mock(return_value=httpx.Response(200, json=ASSET_PAIRS_RESPONSE))

    async with httpx.AsyncClient() as client:
        provider = make_provider(client)
        resolved = await provider.resolve_pair("XBTUSD")

    assert resolved.canonical_name == "XXBTZUSD"
    assert resolved.display_name == "XBTUSD"


@respx.mock
async def test_resolve_pair_no_slash_falls_back_for_modern_pair() -> None:
    respx.get(f"{BASE_URL}/AssetPairs").mock(return_value=httpx.Response(200, json=ASSET_PAIRS_RESPONSE))

    async with httpx.AsyncClient() as client:
        provider = make_provider(client)
        resolved = await provider.resolve_pair("SOLUSD")

    assert resolved.canonical_name == "SOLUSD"
    assert resolved.base_currency == "SOL"
    assert resolved.quote_currency == "USD"


@respx.mock
async def test_resolve_pair_unresolvable_symbol_raises_value_error() -> None:
    respx.get(f"{BASE_URL}/AssetPairs").mock(return_value=httpx.Response(200, json=ASSET_PAIRS_RESPONSE))

    async with httpx.AsyncClient() as client:
        provider = make_provider(client)
        try:
            await provider.resolve_pair("NOTAREALPAIR")
        except ValueError:
            pass
        else:
            raise AssertionError("expected ValueError")
