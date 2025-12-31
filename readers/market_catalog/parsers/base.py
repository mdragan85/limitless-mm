from __future__ import annotations
from typing import Any, Dict, List, Protocol
from ..models import InstrumentDraft


class VenueParser(Protocol):
    venue: str

    def parse_line(self, rec: Dict[str, Any]) -> List[InstrumentDraft]:
        ...
