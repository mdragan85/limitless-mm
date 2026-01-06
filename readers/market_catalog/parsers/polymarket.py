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

import re


# --- cadence helpers ---------------------------------------------------------
_CADENCE_WORD_MAP = {
    "hourly": "1h",
    "daily": "1d",
    "weekly": "1w",
}

_TOKEN_RE = re.compile(r"(?:^|[-_\s])(\d+)\s*([mhdw])(?:$|[-_\s])", re.IGNORECASE)
_HR_RE = re.compile(r"(?:^|[-_\s])(\d+)\s*(?:hr|hrs|hour|hours)(?:$|[-_\s])", re.IGNORECASE)
_MIN_RE = re.compile(r"(?:^|[-_\s])(\d+)\s*(?:min|mins|minute|minutes)(?:$|[-_\s])", re.IGNORECASE)

_TIME_RANGE_RE = re.compile(
    r"(\d{1,2})(?::(\d{2}))?\s*(am|pm)\s*-\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm)",
    re.IGNORECASE,
)

# --- underlying inference ----------------------------------------------------

# Only allow the small approved set. Everything else -> None.
_ALLOWED_UNDERLYINGS = ("BTC", "ETH", "SOL", "XRP")

# Match tokens in slugs/titles with safe boundaries (spaces, start/end, or separators).
# Also support full names as a fallback.
_UNDERLYING_PATTERNS = [
    ("BTC", re.compile(r"(?:^|[\s\-_./])btc(?:$|[\s\-_./])|\bbitcoin\b", re.IGNORECASE)),
    ("ETH", re.compile(r"(?:^|[\s\-_./])eth(?:$|[\s\-_./])|\bethereum\b", re.IGNORECASE)),
    ("SOL", re.compile(r"(?:^|[\s\-_./])sol(?:$|[\s\-_./])|\bsolana\b", re.IGNORECASE)),
    ("XRP", re.compile(r"(?:^|[\s\-_./])xrp(?:$|[\s\-_./])|\bripple\b", re.IGNORECASE)),
]

def _infer_underlying_polymarket(rec: Dict[str, Any]) -> Optional[str]:
    """
    Infer underlying from stable text fields (slug/title).
    Strict: returns only BTC/ETH/SOL/XRP; otherwise None.
    """
    raw = rec.get("raw_market") or {}

    candidates = [
        str(rec.get("slug") or ""),
        str(rec.get("question") or ""),
        str(raw.get("question") or ""),
    ]

    # Optional extra signal: series slug/ticker/title sometimes encodes the asset
    try:
        events = raw.get("events") or []
        if events:
            series = events[0].get("series") or []
            if series:
                s0 = series[0]
                candidates.extend([
                    str(s0.get("slug") or ""),
                    str(s0.get("ticker") or ""),
                    str(s0.get("title") or ""),
                ])
    except Exception:
        pass

    blob = " | ".join([c for c in candidates if c]).strip()
    if not blob:
        return None

    for sym, pat in _UNDERLYING_PATTERNS:
        if pat.search(blob):
            return sym

    return None


def _norm_cadence_from_text(text: str) -> Optional[str]:
    if not text:
        return None
    s = text.strip().lower()

    # explicit tokens like 15m / 4h / 1d in slugs/tickers
    m = _TOKEN_RE.search(s)
    if m:
        n = int(m.group(1))
        unit = m.group(2).lower()
        return f"{n}{unit}"

    # "Hourly"/"Daily" words
    for k, v in _CADENCE_WORD_MAP.items():
        if k in s:
            return v

    # "4H" with no separator (common in titles)
    m = re.search(r"(\d+)\s*h\b", s)
    if m:
        return f"{int(m.group(1))}h"

    # "4hr", "15min"
    m = _HR_RE.search(s)
    if m:
        return f"{int(m.group(1))}h"
    m = _MIN_RE.search(s)
    if m:
        return f"{int(m.group(1))}m"

    return None

def _cadence_from_title_timerange(title: str) -> Optional[str]:
    """
    Fallback: infer window length from "8:00AM-12:00PM" patterns in the title.
    Only returns common buckets (15m/30m/1h/4h/24h).
    """
    if not title:
        return None

    m = _TIME_RANGE_RE.search(title)
    if not m:
        return None

    sh, sm, sap, eh, em, eap = m.groups()
    sh = int(sh); eh = int(eh)
    sm = int(sm or 0); em = int(em or 0)
    sap = sap.lower(); eap = eap.lower()

    def to_minutes(h: int, minute: int, ap: str) -> int:
        # 12-hour → 24-hour minutes
        if h == 12:
            h = 0
        if ap == "pm":
            h += 12
        return h * 60 + minute

    start = to_minutes(sh, sm, sap)
    end = to_minutes(eh, em, eap)
    if end <= start:
        end += 24 * 60  # cross-midnight safety

    delta = end - start  # minutes

    # map to your canonical buckets
    if delta in (15, 30, 60, 240, 1440):
        if delta < 60:
            return f"{delta}m"
        if delta == 60:
            return "1h"
        if delta == 240:
            return "4h"
        if delta == 1440:
            return "1d"

    return None

# --- main cadence derivation ----------------------------------------
def _derive_poly_cadence(rec: Dict[str, Any]) -> Optional[str]:
    """
    Cadence = contract window length (15m/30m/1h/4h/1d...), NOT recurrence.
    Polymarket series recurrence can be 'daily' even for 4h products.
    """
    raw = rec.get("raw_market") or {}

    # 1) Strongest signals: instrument slug + series slug/title/ticker (often encode 4h/15m/etc)
    slug = rec.get("slug") or ""
    c = _norm_cadence_from_text(slug)
    if c:
        return c

    try:
        events = raw.get("events") or []
        if events:
            series = events[0].get("series") or []
            if series:
                s0 = series[0]
                for field in ("slug", "ticker", "title"):
                    c = _norm_cadence_from_text(str(s0.get(field) or ""))
                    if c:
                        return c
    except Exception:
        pass

    # 2) Title time-range fallback (e.g. "8:00AM-12:00PM ET" => 4h)
    title = rec.get("question") or raw.get("question") or ""
    c = _cadence_from_title_timerange(title)
    if c:
        return c

    # 3) Last resort: recurrence mapping (better than nothing, but semantically weaker)
    try:
        events = raw.get("events") or []
        if events:
            series = events[0].get("series") or []
            if series:
                recurrence = series[0].get("recurrence")
                if isinstance(recurrence, str):
                    return _CADENCE_WORD_MAP.get(recurrence.lower())
    except Exception:
        pass

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
            underlying=_infer_underlying_polymarket(rec),
            outcome=rec.get("outcome"),    # e.g. "Up"/"Down"
            rule=rec.get("rule"),
            cadence=_derive_poly_cadence(rec),
            seen_ms=seen_ms,
            extra=_poly_extra_subset(raw),
        )

        return [draft]
