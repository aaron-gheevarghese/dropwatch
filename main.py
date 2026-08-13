"""Throwaway spike: prove we can talk to Kraken's public API before building anything else."""

from decimal import Decimal

import httpx

BASE_URL = "https://api.kraken.com/0/public"


def resolve_pair(client: httpx.Client, wsname: str) -> str:
    resp = client.get(f"{BASE_URL}/AssetPairs")
    resp.raise_for_status()
    data = resp.json()

    if data["error"]:
        raise RuntimeError(f"AssetPairs error: {data['error']}")

    for canonical_name, info in data["result"].items():
        if info.get("wsname") == wsname or canonical_name == wsname:
            return canonical_name

    raise ValueError(f"Could not resolve pair for {wsname!r}")


def get_ticker(client: httpx.Client, pairs: list[str]) -> dict:
    resp = client.get(f"{BASE_URL}/Ticker", params={"pair": ",".join(pairs)})
    resp.raise_for_status()
    data = resp.json()

    if data["error"]:
        raise RuntimeError(f"Ticker error: {data['error']}")

    return data["result"]


def usd_pairs(client: httpx.Client) -> set[str]:
    resp = client.get(f"{BASE_URL}/AssetPairs")
    resp.raise_for_status()
    data = resp.json()

    if data["error"]:
        raise RuntimeError(f"AssetPairs error: {data['error']}")

    return {name for name, info in data["result"].items() if info.get("quote") in ("ZUSD", "USD")}


def survey_usd_volume(client: httpx.Client) -> None:
    usd = usd_pairs(client)

    resp = client.get(f"{BASE_URL}/Ticker")
    resp.raise_for_status()
    data = resp.json()

    if data["error"]:
        raise RuntimeError(f"Ticker error: {data['error']}")

    # v[1] is 24h volume in base currency; p[1] is 24h VWAP. Their product is
    # USD notional, which is what "top by liquidity" should actually mean.
    rows = [
        (pair, Decimal(info["v"][1]) * Decimal(info["p"][1]))
        for pair, info in data["result"].items()
        if pair in usd
    ]
    rows.sort(key=lambda row: row[1], reverse=True)

    print(f"\nUSD-quoted pairs: {len(rows)}")
    print("Ranked by 24h USD notional volume (v[1] * p[1]):")
    for rank, (pair, notional) in enumerate(rows[:50], start=1):
        print(f"  {rank:>3}  {pair:<14} ${notional:,.0f}")

    print("\nPairs at or above notional floor:")
    for threshold in (1_000_000, 500_000, 100_000, 50_000, 10_000, 1_000, 100):
        count = sum(1 for _, notional in rows if notional >= threshold)
        print(f"  >= ${threshold:>10,}: {count:>3} pairs")


def main() -> None:
    with httpx.Client(timeout=10.0) as client:
        btc_pair = resolve_pair(client, "XBT/USD")
        print(f"XBTUSD resolved to canonical name: {btc_pair}")

        eth_pair = resolve_pair(client, "ETH/USD")
        print(f"ETHUSD resolved to canonical name: {eth_pair}")

        ticker = get_ticker(client, [btc_pair, eth_pair])

        for pair, info in ticker.items():
            last = Decimal(info["c"][0])
            bid = Decimal(info["b"][0])
            ask = Decimal(info["a"][0])
            print(f"{pair}: last={last} bid={bid} ask={ask}")

        survey_usd_volume(client)


if __name__ == "__main__":
    main()
