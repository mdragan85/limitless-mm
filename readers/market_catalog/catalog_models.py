# readers/market_catalog/catalog_models.py
"""
Accumulator-layer models for MarketCatalog.

Why this layer exists:
- Market metadata logs are noisy and repetitive.
- The same instrument appears across many JSONL files/days.
- We need a place to *merge* observations before freezing immutable metadata.

These classes are intentionally mutable and strict about invariants.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Set


@dataclass
class InstrumentAccum:
    """
    Accumulator for one canonical instrument (orderbook stream).

    Invariants:
    - instrument_id is stable across all merges.
    - market_id must NOT change for a given instrument_id.
    - expiration_ms must be consistent; mismatch is treated as corruption.
    """

    instrument_id: str
    venue: str
    poll_key: str
    market_id: str

    slug: Optional[str] = None
    expiration_ms: int = 0

    title: Optional[str] = None
    underlying: Optional[str] = None
    outcome: Optional[str] = None
    rule: Optional[str] = None
    cadence: Optional[str] = None

    first_seen_ms: int = 0
    last_seen_ms: int = 0

    extra: Dict[str, Any] = field(default_factory=dict)

    def merge(self, d) -> None:
        """
        Merge one InstrumentDraft into this accumulator.

        Strategy:
        - Prefer non-null fields from newer drafts.
        - Expand first_seen / last_seen window.
        - Fail loudly if core invariants are violated.
        """

        if d.instrument_id != self.instrument_id:
            raise ValueError("instrument_id mismatch during merge")

        if d.market_id != self.market_id:
            raise ValueError(
                f"Instrument {self.instrument_id} moved markets: "
                f"{self.market_id} -> {d.market_id}"
            )

        # Prefer non-null metadata updates
        self.slug = d.slug or self.slug
        self.title = d.title or self.title
        self.underlying = d.underlying or self.underlying
        self.outcome = d.outcome or self.outcome
        self.rule = d.rule or self.rule
        self.cadence = d.cadence or self.cadence

        # Expiration must be consistent across sightings
        if self.expiration_ms == 0:
            self.expiration_ms = d.expiration_ms
        elif d.expiration_ms and d.expiration_ms != self.expiration_ms:
            raise ValueError(
                f"Expiration mismatch for {self.instrument_id}: "
                f"{self.expiration_ms} vs {d.expiration_ms}"
            )

        # Expand observation window
        if self.first_seen_ms == 0:
            self.first_seen_ms = d.seen_ms
            self.last_seen_ms = d.seen_ms
        elif d.seen_ms:
            self.first_seen_ms = min(self.first_seen_ms, d.seen_ms)
            self.last_seen_ms = max(self.last_seen_ms, d.seen_ms)

        # Venue-specific extras: shallow merge, newer wins
        self.extra.update(d.extra)


@dataclass
class MarketAccum:
    """
    Accumulator for one canonical market (group of instruments).

    Markets group instruments by (venue, market_id).

    Notes:
    - A market may have 1 instrument (Limitless) or many (Polymarket).
    - Expiration mismatches across instruments are tolerated but recorded.
    """

    venue: str
    market_id: str

    slug: Optional[str] = None
    title: Optional[str] = None
    underlying: Optional[str] = None
    rule: Optional[str] = None
    cadence: Optional[str] = None

    expiration_ms: int = 0
    first_seen_ms: int = 0
    last_seen_ms: int = 0

    instruments: Set[str] = field(default_factory=set)
    extra: Dict[str, Any] = field(default_factory=dict)

    def absorb_instrument(self, inst: InstrumentAccum) -> None:
        """
        Merge an InstrumentAccum into this MarketAccum.

        Strategy:
        - Aggregate instrument_ids.
        - Prefer non-null market-level descriptors.
        - Track expiration inconsistencies without failing hard.
        """

        self.instruments.add(inst.instrument_id)

        self.slug = inst.slug or self.slug
        self.title = inst.title or self.title
        self.underlying = inst.underlying or self.underlying
        self.rule = inst.rule or self.rule
        self.cadence = inst.cadence or self.cadence

        if self.expiration_ms == 0:
            self.expiration_ms = inst.expiration_ms
        elif inst.expiration_ms and inst.expiration_ms != self.expiration_ms:
            # Preserve truth rather than lying
            mn = min(self.expiration_ms, inst.expiration_ms)
            mx = max(self.expiration_ms, inst.expiration_ms)
            self.extra["expiration_min_ms"] = mn
            self.extra["expiration_max_ms"] = mx
            self.expiration_ms = mn

        if self.first_seen_ms == 0:
            self.first_seen_ms = inst.first_seen_ms
            self.last_seen_ms = inst.last_seen_ms
        else:
            self.first_seen_ms = min(self.first_seen_ms, inst.first_seen_ms)
            self.last_seen_ms = max(self.last_seen_ms, inst.last_seen_ms)
