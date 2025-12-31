"""
Limitless venue client wrapper.

This is a thin adapter around the existing LimitlessAPI and LimitlessMarket
logic so the collector no longer depends on Limitless directly.

IMPORTANT:
- This file must not change behavior.
- It forwards calls verbatim to existing code.
"""

from typing import Any, Dict, List 
from venues.limitless.api import LimitlessAPI
from venues.limitless.market import LimitlessMarket


class LimitlessVenueClient:
    venue = "limitless"

    def __init__(self, *args, **kwargs):
        """
        Initialize the underlying Limitless API client.

        Accepts the same args/kwargs as LimitlessAPI so existing
        construction logic remains unchanged.
        """
        self.api = LimitlessAPI(*args, **kwargs)

    # -------------------------
    # Market discovery
    # -------------------------
    def discover_markets(self, underlying: str):
        """
        Discover markets for a given underlying.

        Returns:
            list[LimitlessMarket]
        """
        return self.api.discover_markets(underlying)

    def discover_instruments(self, rules: list[str]) -> list[dict]:
        """
        Discover loggable Limitless instruments across a list of underlyings.

        Output contract (shared across venues):
        - expiration is epoch milliseconds (int)
        - one instrument per market (Limitless YES/NO share an orderbook)

        NOTE: This method intentionally matches existing behavior from
        app/run_discovery.py (do not change filters yet).
        
        rules: currently a list of underlyings; intentionally shaped to match
        Polymarket's discovery interface.
        """
        instruments: list[dict] = []

        for u in rules:
            markets = self.discover_markets(u)

            for m in markets:
                raw = m.raw or {}

                # Only include markets that actually have an orderbook
                if raw.get("tradeType") != "clob":
                    continue
                if not raw.get("tokens"):
                    continue
                if raw.get("expired") is True:
                    continue
                if raw.get("status") not in ("FUNDED", "ACTIVE"):  # keep FUNDED at least
                    continue

                instruments.append(
                    {
                        "venue": "limitless",
                        "market_id": m.market_id,
                        "instrument_id": "BOOK",
                        "poll_key": m.slug,
                        "slug": m.slug,
                        "underlying": m.underlying,
                        "expiration": raw.get("expirationTimestamp"),
                        "title": getattr(m, "title", None),
                        "raw": raw,
                    }
                )

        return instruments

    # -------------------------
    # Orderbook snapshot
    # -------------------------
    def get_orderbook(self, slug: str):
        """
        Fetch a raw orderbook snapshot for a given market slug.

        Returns:
            raw orderbook payload (dict)
        """
        return self.api.get_orderbook(slug)
