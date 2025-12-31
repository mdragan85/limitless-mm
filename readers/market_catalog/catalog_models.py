from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Set, Tuple

from .models import InstrumentDraft


@dataclass
class InstrumentAccum:
    """
    Accumulator for merging many InstrumentDraft sightings into one canonical instrument.
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

    def merge(self, d: InstrumentDraft) -> None:
        # Identity sanity
        if d.instrument_id != self.instrument_id:
            raise ValueError("instrument_id mismatch in merge")
        if d.market_id != self.market_id:
            raise ValueError(
                f"Instrument {self.instrument_id} moved markets? "
                f"{self.market_id} -> {d.market_id}"
            )

        # Prefer non-null updates
        self.slug = d.slug or self.slug
        self.title = d.title or self.title
        self.underlying = d.underlying or self.underlying
        self.outcome = d.outcome or self.outcome
        self.rule = d.rule or self.rule
        self.cadence = d.cadence or self.cadence

        if self.expiration_ms == 0:
            self.expiration_ms = d.expiration_ms
        elif d.expiration_ms and d.expiration_ms != self.expiration_ms:
            # Don’t silently lie
            raise ValueError(
                f"Expiration mismatch for {self.instrument_id}: "
                f"{self.expiration_ms} vs {d.expiration_ms}"
            )

        # Seen range
        if self.first_seen_ms == 0:
            self.first_seen_ms = d.seen_ms
            self.last_seen_ms = d.seen_ms
        else:
            if d.seen_ms:
                self.first_seen_ms = min(self.first_seen_ms, d.seen_ms)
                self.last_seen_ms = max(self.last_seen_ms, d.seen_ms)

        # Extra: shallow merge, newer wins
        self.extra.update(d.extra)


@dataclass
class MarketAccum:
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
        self.instruments.add(inst.instrument_id)

        self.slug = inst.slug or self.slug
        self.title = inst.title or self.title
        self.underlying = inst.underlying or self.underlying
        self.rule = inst.rule or self.rule
        self.cadence = inst.cadence or self.cadence

        if self.expiration_ms == 0:
            self.expiration_ms = inst.expiration_ms
        elif inst.expiration_ms and inst.expiration_ms != self.expiration_ms:
            # Market-level mismatch: keep min/max in extra rather than crash
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
