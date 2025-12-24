import json
from collections import defaultdict
from datetime import datetime, timezone

from venues.polymarket.client import PolymarketClient

# Use your real rules import if you want:
# from config.settings import POLYMARKET_RULES

RULES = [
    {
        "name": "crypto_intraday",
        "queries": ["Bitcoin Up or Down"],
        "min_minutes_to_expiry": 1,
        "max_minutes_to_expiry": 24 * 60,
        "must_contain": [],
        "must_not_contain": [],
    },
]


def utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


if __name__ == "__main__":
    c = PolymarketClient(timeout=20.0)

    instruments = c.discover_instruments(RULES)
    print(f"[{utc_now_iso()}] discovered instruments: {len(instruments)}")

    # Group by market (slug). Each slug should have 2 instruments (Up/Down or Yes/No).
    by_slug = defaultdict(list)
    for inst in instruments:
        by_slug[inst.get("slug", "<?>")].append(inst)

    # Sort markets by time-to-expiry (soonest first)
    def slug_sort_key(item):
        slug, insts = item
        # minutes_to_expiry should be the same for both instruments; take min just in case.
        mtes = [x.get("minutes_to_expiry") for x in insts if x.get("minutes_to_expiry") is not None]
        return min(mtes) if mtes else 1e18

    markets = sorted(by_slug.items(), key=slug_sort_key)

    print(f"unique markets (slugs): {len(markets)}\n")

    # Print a clean summary per market
    for slug, insts in markets:
        insts_sorted = sorted(insts, key=lambda x: str(x.get("outcome", "")))
        any_inst = insts_sorted[0]

        market_id = any_inst.get("market_id")
        question = any_inst.get("question", "")
        exp_ms = any_inst.get("expiration")
        mte = any_inst.get("minutes_to_expiry")
        rule = any_inst.get("rule")

        outcomes = [(x.get("outcome"), x.get("instrument_id")[:12] + "...", x.get("outcome_price")) for x in insts_sorted]

        print(f"- {slug}")
        print(f"    question: {question}")
        print(f"    market_id: {market_id} | rule: {rule} | minutes_to_expiry: {mte:.1f}" if isinstance(mte, (int, float)) else
              f"    market_id: {market_id} | rule: {rule} | minutes_to_expiry: {mte}")
        print(f"    expiration_ms: {exp_ms}")
        print(f"    instruments ({len(insts)}): {outcomes}")

    # Optional: show a small "top N soonest" view
    N = 20
    print(f"\n=== TOP {N} SOONEST MARKETS ===")
    for slug, insts in markets[:N]:
        any_inst = insts[0]
        mte = any_inst.get("minutes_to_expiry")
        market_id = any_inst.get("market_id")
        print(f"{mte:8.1f}m  {market_id:8s}  {slug}" if isinstance(mte, (int, float)) else f"{mte}  {market_id}  {slug}")

