# readers/market_catalog/models.py
"""
Core *data shapes* for the MarketCatalog pipeline.

Key idea:
- The MarketCatalog is a metadata catalog, NOT an orderbook index.
- This module defines the smallest "common language" that all venues must be
  convertible into.

We start with **Draft** objects because ingestion is messy:
- JSONL logs can contain duplicates, partial records, and evolving schemas.
- Drafts are mutable/merge-friendly; later stages can freeze into immutable metas.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional


def make_instrument_id(venue: str, poll_key: str) -> str:
    """
    Canonical instrument identity across the whole system.

    Invariant:
      instrument_id == "<venue>:<poll_key>"

    Why:
    - poll_key is the stable "stream key" used by the poller (slug for Limitless,
      token_id for Polymarket, etc.)
    - prefixing with venue prevents collisions across venues.
    """
    return f"{venue}:{poll_key}"


@dataclass
class InstrumentDraft:
    """
    A merge-friendly representation of ONE orderbook stream.

    Drafts are produced by VenueParsers from market metadata JSONL.
    They are NOT guaranteed to be complete; the catalog's merge step will
    consolidate drafts across many files/days.

    This object intentionally contains both:
    - normalized fields the catalog depends on for querying/sampling, and
    - a small 'extra' dict for useful venue-specific attributes (kept small).
    """

    # ---- Identity / grouping ----
    instrument_id: str           # "<venue>:<poll_key>"
    venue: str                   # "limitless", "polymarket", etc.
    poll_key: str                # stream key for polling orderbooks
    market_id: str               # groups instruments -> market

    # ---- Common metadata ----
    slug: Optional[str] = None
    expiration_ms: int = 0       # epoch milliseconds (unified across venues)

    # ---- Queryable descriptors (may be None depending on venue/log schema) ----
    title: Optional[str] = None
    underlying: Optional[str] = None
    outcome: Optional[str] = None   # e.g. Polymarket "Up"/"Down" or "YES"/"NO"
    rule: Optional[str] = None      # venue-specific rule tag; may be None
    cadence: Optional[str] = None   # derived ("15m", "1h", "1d", ...), may be None

    # ---- Observation timestamp ----
    # This is not the market expiration; it's "when we saw this record".
    # It is used to build first_seen_ms / last_seen_ms ranges.
    seen_ms: int = 0

    # ---- Venue-specific extras ----
    # Keep this intentionally small + stable (avoid dumping entire raw blobs).
    extra: dict[str, Any] = field(default_factory=dict)
