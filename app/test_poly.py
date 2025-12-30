"""
Polymarket discovery diagnostic harness.

Goals:
- Use the real POLYMARKET_RULES from config (no drift).
- Show (1) candidate slug pool from Gamma public-search per rule/query
- Show (2) final discovered instruments from PolymarketClient.discover_instruments
- Summarize by rule / underlying / market families (15m/4h/hourly/daily-ish)
- Print a concise "active set" of unique markets

This script should contain diagnostics only, not production logic.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone

from config.settings import POLYMARKET_RULES
from venues.polymarket.client import PolymarketClient


def now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def guess_underlying_from_slug(slug: str) -> str:
    s = (slug or "").lower()
    if s.startswith("btc-") or s.startswith("bitcoin-"):
        return "BTC"
    if s.startswith("eth-") or s.startswith("ethereum-"):
        return "ETH"
    if s.startswith("sol-") or s.startswith("solana-"):
        return "SOL"
    if s.startswith("xrp-"):
        return "XRP"
    return "UNK"


def classify_family_from_slug(slug: str) -> str:
    s = (slug or "").lower()
    if "-updown-15m-" in s:
        return "15m_structured"
    if "-updown-4h-" in s:
        return "4h_structured"
    if "-updown-60m-" in s or "-updown-1h-" in s:
        return "60m_structured"
    if s.startswith("bitcoin-up-or-down-december-") and (s.endswith("-am-et") or s.endswith("-pm-et")):
        return "hourly_human_slug"
    if s.startswith("bitcoin-up-or-down-on-"):
        return "daily_human_slug"
    return "other"


def print_header(title: str):
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def main():
    poly = PolymarketClient()

    # ---------------------------------------------------------------------
    # (A) Candidate slug pool per rule/query (direct Gamma public-search)
    # ---------------------------------------------------------------------
    print_header("A) Candidate slug pool from Gamma public-search (per rule/query)")

    all_candidate_slugs: set[str] = set()
    candidates_by_rule: dict[str, set[str]] = defaultdict(set)

    for rule in POLYMARKET_RULES:
        rname = rule.get("name", "<unnamed>")
        queries = rule.get("queries", []) or []
        print(f"\nRule: {rname} | queries={len(queries)}")

        for q in queries:
            blob = poly.public_search(q)
            slugs: set[str] = set()
            poly._collect_market_slugs(blob, slugs)  # uses your existing helper
            print(f"  query='{q}' -> slugs={len(slugs)}")

            candidates_by_rule[rname].update(slugs)
            all_candidate_slugs.update(slugs)

    print(f"\nTotal unique candidate slugs across all rules: {len(all_candidate_slugs)}")

    # Quick peek at candidate families/underlyings
    fam_counts = Counter(classify_family_from_slug(s) for s in all_candidate_slugs)
    und_counts = Counter(guess_underlying_from_slug(s) for s in all_candidate_slugs)

    print("\nCandidate slugs by family:")
    for k, v in fam_counts.most_common():
        print(f"  {k:20s} {v}")

    print("\nCandidate slugs by underlying (slug-guess):")
    for k, v in und_counts.most_common():
        print(f"  {k:6s} {v}")

    # ---------------------------------------------------------------------
    # (B) Final discovery output (production discover_instruments)
    # ---------------------------------------------------------------------
    print_header("B) Final discovery output from discover_instruments(POLYMARKET_RULES)")

    instruments = poly.discover_instruments(POLYMARKET_RULES)
    print(f"Discovered instruments: {len(instruments)}")

    markets_by_slug: dict[str, list[dict]] = defaultdict(list)
    for inst in instruments:
        markets_by_slug[inst["slug"]].append(inst)

    print(f"Unique markets (slugs): {len(markets_by_slug)}")

    by_rule = Counter((i.get("rule") or "None") for i in instruments)
    by_underlying = Counter(guess_underlying_from_slug(i.get("slug", "")) for i in instruments)
    by_family = Counter(classify_family_from_slug(i.get("slug", "")) for i in instruments)

    print("\nInstruments by rule:")
    for k, v in by_rule.most_common():
        print(f"  {k:24s} {v}")

    print("\nInstruments by underlying (slug-guess):")
    for k, v in by_underlying.most_common():
        print(f"  {k:6s} {v}")

    print("\nInstruments by family:")
    for k, v in by_family.most_common():
        print(f"  {k:20s} {v}")

    # ---------------------------------------------------------------------
    # (C) Print "active set" of markets (unique slugs) with key timing fields
    # ---------------------------------------------------------------------
    print_header("C) Markets (unique slugs) summary (sorted by minutes_to_expiry)")

    # Each slug should have 2 instruments (Yes/No or Up/Down). Use the first instrument as representative.
    rows = []
    for slug, insts in markets_by_slug.items():
        rep = insts[0]
        rm = rep.get("raw_market") or {}
        rows.append({
            "slug": slug,
            "rule": rep.get("rule"),
            "underlying": guess_underlying_from_slug(slug),
            "family": classify_family_from_slug(slug),
            "minutes_to_expiry": rep.get("minutes_to_expiry"),
            "expiration_ms": rep.get("expiration"),
            "eventStartTime": rm.get("eventStartTime"),
            "startDateIso": rm.get("startDateIso"),
            "startDate": rm.get("startDate"),
            "acceptingOrders": rm.get("acceptingOrders"),
            "enableOrderBook": rm.get("enableOrderBook"),
            "active": rm.get("active"),
            "closed": rm.get("closed"),
        })

    def safe_float(x):
        try:
            return float(x)
        except Exception:
            return 1e18

    rows.sort(key=lambda r: safe_float(r["minutes_to_expiry"]))

    for r in rows:
        mte = safe_float(r["minutes_to_expiry"])
        print(f"- {r['slug']}")
        print(f"    rule={r['rule']} | underlying={r['underlying']} | family={r['family']} | mte={mte:.1f}")
        print(f"    eventStartTime={r['eventStartTime']} | startDateIso={r['startDateIso']} | startDate={r['startDate']}")
        print(f"    active={r['active']} | closed={r['closed']} | enableOrderBook={r['enableOrderBook']} | acceptingOrders={r['acceptingOrders']}")

    # ---------------------------------------------------------------------
    # (D) Optional: Print candidate slugs missing from final discovery, per rule
    # ---------------------------------------------------------------------
    print_header("D) Candidate slugs missing from final discovery (per rule)")

    final_slugs = set(markets_by_slug.keys())
    for rule in POLYMARKET_RULES:
        rname = rule.get("name", "<unnamed>")
        cand = candidates_by_rule.get(rname, set())
        missing = sorted(cand - final_slugs)
        print(f"\nRule: {rname} | candidate={len(cand)} | final={len(cand & final_slugs)} | missing={len(missing)}")
        for s in missing[:30]:
            print(f"  - {s}")
        if len(missing) > 30:
            print(f"  ... ({len(missing) - 30} more)")


    print_header("E) Probe one missing 15m market (start-time fields)")

    probe_slug = "btc-updown-15m-1767054600"  # pick any from the missing list
    d = poly.get_market_by_slug(probe_slug)
    if not d:
        print("Probe market not found:", probe_slug)
    else:
        print("slug:", probe_slug)
        print("endDate:", d.get("endDate"))
        print("eventStartTime:", d.get("eventStartTime"))
        print("startDateIso:", d.get("startDateIso"))
        print("startDate:", d.get("startDate"))
        print("active:", d.get("active"), "acceptingOrders:", d.get("acceptingOrders"),
            "enableOrderBook:", d.get("enableOrderBook"), "closed:", d.get("closed"))



    print("\nDone.")


if __name__ == "__main__":
    main()
