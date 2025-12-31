from __future__ import annotations

from typing import Any, Dict, List, Optional

from ..models import InstrumentDraft, make_instrument_id
from ..utils import parse_iso_to_ms, pick_seen_ms, require


def _derive_poly_cadence(rec: Dict[str, Any]) -> Optional[str]:
    # 1) slug is the most stable indicator for these recurring markets
    slug = rec.get("slug") or ""
    for token in ("15m", "30m", "1h", "4h", "1d", "1w"):
        if f"-{token}-" in slug:
            return token

    # 2) next best: events[0].series[0].recurrence
    raw = rec.get("raw_market") or {}
    try:
        events = raw.get("events") or []
        if events:
            series = (events[0].get("series") or [])
            if series:
                r = series[0].get("recurrence")
                if isinstance(r, str):
                    return r
    except Exception:
        pass

    return None


def _poly_extra_subset(raw: Dict[str, Any]) -> Dict[str, Any]:
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
    venue = "polymarket"

    def parse_line(self, rec: Dict[str, Any]) -> List[InstrumentDraft]:
        require(rec, ["poll_key", "market_id", "expiration"], self.venue)

        poll_key = rec["poll_key"]
        market_id = str(rec["market_id"])
        instrument_id = make_instrument_id(self.venue, poll_key)

        raw = rec.get("raw_market") or {}
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
            underlying=None,
            outcome=rec.get("outcome"),
            rule=rec.get("rule"),
            cadence=_derive_poly_cadence(rec),
            seen_ms=seen_ms,
            extra=_poly_extra_subset(raw),
        )
        return [draft]
