from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class LimitlessMarket:
    
    market_id: str
    slug: str
    underlying: str
    title: str
    yes_token: str
    no_token: str
    raw: Dict[str, Any]

    @classmethod
    def from_api(cls, d: Dict[str, Any]) -> "LimitlessMarket":
        """
        Parse a market dictionary from Limitless /markets/active API.
        This expects the API to return fields like id, slug, title,
        and a tokens object containing yesTokenId and noTokenId.
        """
        tokens = d.get("tokens", {})
        return cls(
            market_id=str(d.get("id")),
            slug=d.get("slug") or "",
            underlying=d.get("underlying") or "",
            title=d.get("title") or "",
            yes_token=tokens.get("yesTokenId") or "",
            no_token=tokens.get("noTokenId") or "",
            raw=d,
        )

    def is_loggable(self) -> bool:
        """
        Placeholder filter for now.
        Later we can exclude resolved/expired/illiquid markets.
        For now, accept everything that has a slug.
        """
        return bool(self.slug)