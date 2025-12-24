"""
Limitless venue client wrapper.

This is a thin adapter around the existing LimitlessAPI and LimitlessMarket
logic so the collector no longer depends on Limitless directly.

IMPORTANT:
- This file must not change behavior.
- It forwards calls verbatim to existing code.
"""

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
