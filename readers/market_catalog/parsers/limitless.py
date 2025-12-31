# readers/market_catalog/parsers/limitless.py
"""
Limitless venue parser.

Important characteristics of Limitless logs:
- Some JSONL lines are *instrument-capable* (contain poll_key + expiration).
- Some JSONL lines are *market-only summaries* (no poll_key / expiration).
  Those must be skipped.

Limitless has exactly ONE orderbook stream per market, so:
- parse_line() returns either [] or [InstrumentDraft].
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from ..models import InstrumentDraft, make_instrument_id
from ..utils import parse_iso_to_ms, pick_seen_ms, require


# Mapping from Limitless category/tag → normalized cadence bucket.
# This is intentionally conservative; unknown categories yield cadence=None.
_LIMITLESS_CATEGORY_TO_CADENCE = {
    "Hourly": "1h",
    "Daily": "1d",
    "Weekly": "1w",
    "Monthly": "1mo",
}


def _derive_limitless_cadence(raw: Dict[str, Any]) -> Optional[str]:
    """
    Infer cadence from Limitless market metadata.

    Priority:
    1) raw.categories (preferred)
    2) raw.tags (fallback)

    Returns normalized cadence string or None.
    """
    cats = raw.get("categories") or []
    for c in cats:
        if c in _LIMITLESS_CATEGORY_TO_CADENCE:
            return _LIMITLESS_CATEGORY_TO_CADENCE[c]

    tags = raw.get("tags") or []
    for t in tags:
        if t in _LIMITLESS_CATEGORY_TO_CADENCE:
            return _LIMITLESS_CATEGORY_TO_CADENCE[t]

    return None


def _limitless_extra_subset(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract a small, stable subset of venue-specific metadata.

    Rationale:
    - These fields are useful for analysis/debugging.
    - Storing the full raw blob would bloat memory and complicate merges.
    """
    metadata = raw.get("metadata") or {}
    settings = raw.get("settings") or {}

    return {
        "status": raw.get("status"),
        "tags": raw.get("tags"),
        "tradeType": raw.get("tradeType"),
        "marketType": raw.get("marketType"),
        "conditionId": raw.get("conditionId"),
        "volume": raw.get("volumeFormatted") or raw.get("volume"),
        "priorityIndex": raw.get("priorityIndex"),
        "isRewardable": raw.get("isRewardable"),
        "metadata": {
            "shouldMarketMake": metadata.get("shouldMarketMake"),
            "isPolyArbitrage": metadata.get("isPolyArbitrage"),
            "isBannered": metadata.get("isBannered"),
            "fee": metadata.get("fee"),
        },
        "settings": {
            "maxSpread": settings.get("maxSpread"),
            "dailyReward": settings.get("dailyReward"),
            "minSize": settings.get("minSize"),
        },
    }


class LimitlessParser:
    """
    VenueParser implementation for Limitless.
    """

    venue = "limitless"

    def parse_line(self, rec: Dict[str, Any]) -> List[InstrumentDraft]:
        """
        Parse one Limitless markets JSONL record.

        Returns:
        - [] if this record does not describe a pollable instrument
          (market-only summary line).
        - [InstrumentDraft] for instrument-capable records.
        """

        # Skip market-only summary records (no orderbook stream).
        poll_key = rec.get("poll_key")
        expiration = rec.get("expiration")
        if not poll_key or expiration is None:
            return []

        # Fail fast if a supposedly instrument-capable record is malformed.
        require(rec, ["poll_key", "market_id", "expiration"], self.venue)

        market_id = str(rec["market_id"])
        instrument_id = make_instrument_id(self.venue, poll_key)

        raw = rec.get("raw") or {}

        # Use freshest available timestamp to represent "seen at".
        seen_ms = pick_seen_ms(
            parse_iso_to_ms(raw.get("updatedAt")),
            parse_iso_to_ms(raw.get("createdAt")),
        )

        draft = InstrumentDraft(
            instrument_id=instrument_id,
            venue=self.venue,
            poll_key=poll_key,
            market_id=market_id,
            slug=rec.get("slug"),
            expiration_ms=int(expiration),
            title=rec.get("title"),
            underlying=rec.get("underlying"),
            outcome=None,              # Limitless markets are binary but not outcome-split
            rule=rec.get("rule"),      # may be None (by design)
            cadence=_derive_limitless_cadence(raw),
            seen_ms=seen_ms,
            extra=_limitless_extra_subset(raw),
        )

        return [draft]
