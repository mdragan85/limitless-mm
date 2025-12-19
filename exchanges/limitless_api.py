"""
Limitless REST API client.
Provides market discovery and orderbook snapshot retrieval.
"""

import requests
from typing import Any, Dict, List, Optional
import httpx
from .limitless_market import LimitlessMarket

from config.settings import settings

TIMEOUT = 10

class LimitlessAPI:
    """
    Lightweight wrapper around the Limitless REST API.
    Focused on:
    - Listing available markets
    - Fetching orderbook snapshots
    """

    def __init__(self):
        self.base_url = "https://api.limitless.exchange"
        self.session = requests.Session()          # <-- REQUIRED

        # Build headers dynamically based on whether API key exists
        headers = {
            "accept": "application/json",
        }
        if getattr(settings, "LIMITLESS_API_KEY", None):
            headers["Authorization"] = f"Bearer {settings.LIMITLESS_API_KEY}"

        self._headers = headers

    # -------------------------
    # Low-level request helper
    # -------------------------
    def _get(self, path: str, params: dict | None = None):
        url = f"{self.base_url}/{path.lstrip('/')}"
        resp = self.session.get(url, headers=self._headers, params=params, timeout=TIMEOUT)

        try:
            resp.raise_for_status()
        except requests.exceptions.HTTPError as exc:
            # Attach status code if we have a response
            status = exc.response.status_code if exc.response is not None else "N/A"
            raise RuntimeError(
                f"Limitless API request failed [{status}] for URL: {url}"
            ) from exc

        return resp.json()

    # -------------------------
    # Market endpoints
    # -------------------------
    def list_markets(self, underlying: str | None = None) -> list[dict]:
        """Return a list of active markets, optionally filtered by underlying ticker."""
        payload = self._get("markets/active")
        markets = payload.get("data", []) if isinstance(payload, dict) else payload or []

        if not underlying:
            return markets

        symbol = underlying.upper()
        filtered: list[dict] = []
        for market in markets:
            if not isinstance(market, dict):
                continue

            ticker = (market.get("ticker") or "").upper()
            title = (market.get("title") or "").upper()
            if symbol in ticker or symbol in title:
                filtered.append(market)

        return filtered

    def discover_markets(self, underlying: str) -> list[LimitlessMarket]:
        """
        Fetch and normalize markets for one underlying.
        Returns a filtered list of loggable markets capped by settings.
        """
        raw_markets = self.list_markets(underlying)
        markets = [
            LimitlessMarket.from_api({**m, "underlying": underlying})
            for m in raw_markets
            if isinstance(m, dict)
        ]

        loggable = [m for m in markets if m.is_loggable()]
        if settings.MAX_MARKETS_PER_UNDERLYING:
            return loggable[: settings.MAX_MARKETS_PER_UNDERLYING]
        return loggable



    def get_market(self, market_id: str) -> Dict[str, Any]:
        """
        Returns detailed metadata for a single market.
        """
        return self._get(f"markets/{market_id}")

    # -------------------------
    # Orderbook endpoints
    # -------------------------
    
    def get_orderbook(self, slug: str) -> dict:
        """
        Fetch the current orderbook for a market.

        The Limitless API returns a single orderbook per market when
        you hit /markets/{slug}/orderbook.
         NOTE: Limitless orderbooks are fetched by *slug*, not market_id.
         Endpoint: /markets/{slug}/orderbook
        """
        if slug.isdigit():
            raise ValueError(
                f"get_orderbook expects slug, got numeric market_id: {slug}"
            )
        return self._get(f"markets/{slug}/orderbook")


    # -------------------------
    # Cleanup
    # -------------------------
    def close(self):
        self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
