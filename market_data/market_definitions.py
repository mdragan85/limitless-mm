"""
Market model for Limitless markets.
Normalizes raw API output and provides useful accessors.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional


def parse_datetime(maybe_ts: Any) -> Optional[datetime]:
    """
    Convert a timestamp or ISO string to a datetime object.
    Returns None if parsing fails.
    """
    if maybe_ts is None:
        return None

    try:
        # If it's numeric, assume epoch seconds
        if isinstance(maybe_ts, (int, float)):
            return datetime.utcfromtimestamp(maybe_ts)
        # If it's a string, try ISO format
        if isinstance(maybe_ts, str):
            return datetime.fromisoformat(maybe_ts.replace("Z", "+00:00"))
    except Exception:
        return None

    return None


@dataclass
class LimitlessMarket:
    """
    Strongly typed representation of a Limitless market.
    """

    market_id: str
    underlying: str
    title: str
    strike: Optional[float]
    expiry: Optional[datetime]
    active: bool
    market_type: str  # e.g. "binary", "range", "prediction", etc.
    raw: Dict[str, Any]  # Keep full payload for debugging

    # -------------------------
    # Construct from API JSON
    # -------------------------
    @classmethod
    def from_api(cls, payload: Dict[str, Any]) -> "LimitlessMarket":
        """
        Build a LimitlessMarket object from raw API JSON.
        Normalizes missing fields.
        """

        return cls(
            market_id=str(payload.get("id", "")),
            underlying=str(payload.get("underlying", "")).upper(),
            title=payload.get("title", ""),
            strike=(float(payload["strike"]) if "strike" in payload else None),
            expiry=parse_datetime(payload.get("expiry")),
            active=bool(payload.get("active", True)),
            market_type=payload.get("type", "unknown"),
            raw=payload,
        )

    # -------------------------
    # Convenience methods
    # -------------------------
    def is_expired(self) -> bool:
        if self.expiry is None:
            return False
        return datetime.utcnow() > self.expiry

    def is_loggable(self) -> bool:
        """
        Determines whether this market should be logged.
        Filters out expired or improperly defined markets.
        """
        if not self.active:
            return False
        if self.is_expired():
            return False
        return True
