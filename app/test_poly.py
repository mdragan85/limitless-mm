from datetime import datetime, timezone
import json
import time

from venues.polymarket.client import PolymarketClient


RULES = [
    {
        "name": "crypto_intraday_smoke",
        "queries": [
            "Bitcoin",
        ],
        "min_minutes_to_expiry": -10e9,
        "max_minutes_to_expiry": +10e9,  # keep tiny for smoke test
        "must_contain": [],
        "must_not_contain": [],
    },
]


def main():
    c = PolymarketClient(timeout=20.0)  # longer timeout for dev

    t0 = time.time()
    markets = c.discover_markets(RULES)
    dt = time.time() - t0

    print(f"\nFetched {len(markets)} markets in {dt:.2f}s\n")

    # show top 10 soonest-expiring
    def mins_left(m):
        now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        return (m["end_ms"] - now_ms) / 60000.0

    markets = sorted(markets, key=mins_left)

    for m in markets[:10]:
        print(
            f"- {m['market_id']} | {mins_left(m):6.1f} min | {m['question']}"
        )
        print(f"  YES={m['token_yes']}  NO={m['token_no']}")
        print(f"  rule={m['rule']}")
        print()

    # optional: dump first raw for inspection
    if markets:
        print("First raw keys:", list(markets[0]["raw"].keys()))
        # print(json.dumps(markets[0]["raw"], indent=2)[:1500])


if __name__ == "__main__":
    
    #main()

    c = PolymarketClient(timeout=20.0)  # longer timeout for dev
    blob = c.public_search("Bitcoin Up or Down")

    d = c.get_market_by_slug("btc-updown-15m-1766611800")
    print(d.keys())
    print("clobTokenIds:", d.get("clobTokenIds"))
    print("tokens:", d.get("tokens"))
    print("question:", d.get("question"))
    print("endDate:", d.get("endDate"))

    for e0 in blob["events"]:
        print("event title:", e0["title"])
        print("first market:", e0["markets"][0]["question"], e0["markets"][0]["slug"])