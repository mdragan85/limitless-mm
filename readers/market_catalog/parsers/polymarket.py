# readers/market_catalog/parsers/polymarket.py
"""
Polymarket venue parser.

Important characteristics of Polymarket logs:
- JSONL is *one line per instrument*, not one line per market.
- Each market typically has TWO instruments (one per outcome),
  which share the same market_id.
- Market-level metadata is duplicated across instrument lines.

parse_line() therefore always returns exactly ONE InstrumentDraft
for instrument-capable records.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from ..models import InstrumentDraft, make_instrument_id
from ..utils import parse_iso_to_ms, pick_seen_ms, require


def _derive_poly_cadence(rec: Dict[str, Any]) -> Optional[str]:
    """
    Infer cadence for Polymarket instruments.

    Priority order:
    1) Structured series recurrence (raw_market.events[0].series[0].recurrence)
       This is the most semantically correct source when present.
    2) Fallback: infer from slug tokens (e.g. '-15m-', '-1h-') if encoded.

    Returns normalized cadence string (e.g. '15m', '1h') or None.
    """

    raw = rec.get("raw_market") or {}

    # 1) Preferred: structured recurrence from series metadata
    try:
        events = raw.get("events") or []
        if events:
            series = events[0].get("series") or []
            if series:
                recurrence = series[0].get("recurrence")
                if isinstance(recurrence, str):
                    return recurrence
    except Exception:
        # Defensive: schema drift should degrade gracefully here
        pass

    # 2) Fallback: infer cadence from slug if encoded
    slug = rec.get("slug") or ""
    for token in ("15m", "30m", "1h", "4h", "1d", "1w"):
        if f"-{token}-" in slug:
            return token

    return None


def _poly_extra_subset(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract a stable subset of Polymarket-specific metadata.

    These fields are useful for analysis and filtering, but are not required
    for core identity or grouping logic.
    """
    return {
        "conditionId": raw.get("conditionId"),
        "active": raw.get("active"),
        "closed": raw.get("closed"),
        "archived": raw.get("archived"),
        "restricted": raw.get("restricted"),
        "acceptingOrders": raw.get("acceptingOrders"),
        "spread": raw.get("spread"),
        "bestBid": raw.get("bestBid"),
        "bestAsk": raw.get("bestAsk"),
        "liquidityNum": raw.get("liquidityNum"),
        "volumeNum": raw.get("volumeNum"),
        "orderMinSize": raw.get("orderMinSize"),
        "orderPriceMinTickSize": raw.get("orderPriceMinTickSize"),
    }


class PolymarketParser:
    """
    VenueParser implementation for Polymarket.
    """

    venue = "polymarket"

    def parse_line(self, rec: Dict[str, Any]) -> List[InstrumentDraft]:
        """
        Parse one Polymarket markets JSONL record.

        Polymarket logs are already instrument-level, so this method
        returns exactly ONE InstrumentDraft for valid records.
        """

        # Fail fast if an instrument-capable record is malformed.
        require(rec, ["poll_key", "market_id", "expiration"], self.venue)

        poll_key = rec["poll_key"]
        market_id = str(rec["market_id"])
        instrument_id = make_instrument_id(self.venue, poll_key)

        raw = rec.get("raw_market") or {}

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
            expiration_ms=int(rec["expiration"]),
            title=rec.get("question"),
            underlying=None,               # can be derived later if desired
            outcome=rec.get("outcome"),    # e.g. "Up"/"Down"
            rule=rec.get("rule"),
            cadence=_derive_poly_cadence(rec),
            seen_ms=seen_ms,
            extra=_poly_extra_subset(raw),
        )

        return [draft]
