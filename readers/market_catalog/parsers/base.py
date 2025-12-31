# readers/market_catalog/parsers/base.py
"""
Parser contract for converting venue-specific market metadata logs into Drafts.

A VenueParser is the ONLY place where venue-specific schema knowledge should live.

Contract:
- Input: one JSONL record already parsed into a dict.
- Output: zero or more InstrumentDraft objects.
  - zero drafts means: "this record is not instrument-capable" (e.g., market-only summary)
  - one draft means: one orderbook stream
  - (rare) multiple drafts if a single record encodes multiple streams

MarketCatalog depends on this contract to remain venue-agnostic.
"""

from __future__ import annotations

from typing import Any, Dict, List, Protocol

from ..models import InstrumentDraft


class VenueParser(Protocol):
    """
    Protocol for venue-specific parsers.

    Invariant:
    - instrument_id must equal "<venue>:<poll_key>" for every produced draft.
    - market_id must be stable for a given instrument_id across all time.
    """

    venue: str

    def parse_line(self, rec: Dict[str, Any]) -> List[InstrumentDraft]:
        """
        Convert a single JSONL record into InstrumentDraft(s).

        Return [] to skip records that do not represent a pollable instrument stream.
        """
        ...
