from __future__ import annotations

from typing import Any, Dict, List, Optional

from ..models import InstrumentDraft, make_instrument_id
from ..utils import parse_iso_to_ms, pick_seen_ms, require


_LIMITLESS_CATEGORY_TO_CADENCE = {
    "Hourly": "1h",
    "Daily": "1d",
    "Weekly": "1w",
    "Monthly": "1mo",
}


def _derive_limitless_cadence(raw: Dict[str, Any]) -> Optional[str]:
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
    venue = "limitless"

    def parse_line(self, rec: Dict[str, Any]) -> List[InstrumentDraft]:
        # Some lines are market-only summaries (no poll_key / expiration). Skip them.
        if not rec.get("poll_key") or not rec.get("expiration"):
            return []

        require(rec, ["poll_key", "market_id", "expiration"], self.venue)

        poll_key = rec["poll_key"]
        market_id = str(rec["market_id"])
        instrument_id = make_instrument_id(self.venue, poll_key)

        raw = rec.get("raw") or {}
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
            title=rec.get("title"),
            underlying=rec.get("underlying"),
            outcome=None,
            rule=rec.get("rule"),
            cadence=_derive_limitless_cadence(raw),
            seen_ms=seen_ms,
            extra=_limitless_extra_subset(raw),
        )
        return [draft]
