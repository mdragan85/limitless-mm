"""
Limitless REST API client.
Provides market discovery and orderbook snapshot retrieval.
"""

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

    def __init__(self, base_url: Optional[str] = None, timeout: float = 10.0):
        self.base_url = base_url or settings.BASE_URL
        self.client = httpx.Client(timeout=timeout)

    # -------------------------
    # Low-level request helper
    # -------------------------
    def _get(self, endpoint: str, params: Optional[dict] = None) -> Any:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        response = self.client.get(url, params=params)

        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise RuntimeError(
                f"Limitless API request failed [{exc.response.status_code}] "
                f"for URL: {url}"
            ) from exc

        return response.json()

    # -------------------------
    # Market endpoints
    # -------------------------
    def list_markets(self, underlying: str) -> List[Dict[str, Any]]:
        """
        Returns a list of markets for a given underlying (BTC, ETH, SOL, etc.).
        """
        return self._get("markets", params={"underlying": underlying})

    def get_market(self, market_id: str) -> Dict[str, Any]:
        """
        Returns detailed metadata for a single market.
        """
        return self._get(f"markets/{market_id}")

    # -------------------------
    # Orderbook endpoints
    # -------------------------
    def get_orderbook(self, market_id: str) -> Dict[str, Any]:
        """
        Returns the best bids/asks and depth for the given market.
        """
        return self._get(f"markets/{market_id}/orderbook")

    # -------------------------
    # Cleanup
    # -------------------------
    def close(self):
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
