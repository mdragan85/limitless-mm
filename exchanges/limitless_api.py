"""
Limitless REST API client.
Provides market discovery and orderbook snapshot retrieval.
"""

import requests
from typing import Any, Dict, List, Optional
import httpx

from config.settings import settings


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
        resp = self.session.get(url, headers=self._headers, params=params)

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
        payload = self._get("markets/active")  # returns BrowseActiveMarketsResponseDto

        # Unwrap the actual list of markets from the response
        if isinstance(payload, dict):
            raw = payload.get("data", []) or []
        else:
            raw = payload or []

        if underlying:
            u = underlying.upper()
            filtered: list[dict] = []
            for m in raw:
                if not isinstance(m, dict):
                    continue
                ticker = (m.get("ticker") or "").upper()
                title = (m.get("title") or "").upper()
                if ticker.startswith(u) or u in title:
                    filtered.append(m)
            raw = filtered

        return raw



    def get_market(self, market_id: str) -> Dict[str, Any]:
        """
        Returns detailed metadata for a single market.
        """
        return self._get(f"markets/{market_id}")

    # -------------------------
    # Orderbook endpoints
    # -------------------------

    def get_orderbook(self, slug: str, token_id: str) -> dict:
        """
        Fetch orderbook for a specific market outcome (YES or NO).
        Limitless requires a tokenId query parameter to choose the outcome.
        """
        params = {"tokenId": token_id}
        return self._get(f"markets/{slug}/orderbook", params=params)

    # -------------------------
    # Cleanup
    # -------------------------
    def close(self):
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
