from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class LimitlessMarket:
    """
    Canonical domain representation of a Limitless market.

    This is a thin, mostly-passive data model whose primary purpose is to:
    - Provide stable, typed access to commonly used market fields
    - Preserve the full raw API payload for forward compatibility

    Design notes:
    - This class intentionally avoids embedding business logic.
    - Any field not explicitly modeled here should be assumed to live in `raw`.
    - The model is designed to be constructed directly from the Limitless API
      and passed through collectors, loggers, and downstream consumers.
    """

    # Stable identifiers and commonly used metadata
    market_id: str
    slug: str
    underlying: str
    title: str

    # Token identifiers used for trading / settlement
    yes_token: str
    no_token: str

    # Full raw API payload for schema evolution and future parsing
    raw: Dict[str, Any]

    @classmethod
    def from_api(cls, d: Dict[str, Any]) -> "LimitlessMarket":
        """
        Construct a LimitlessMarket from a raw dictionary returned by the
        /markets/active API endpoint.

        Expected (but not strictly enforced) structure:
        - d["id"]              -> market identifier
        - d["slug"]            -> human-readable market slug
        - d["underlying"]      -> underlying symbol (e.g. BTC, ETH)
        - d["title"]           -> market title/description
        - d["tokens"]          -> object containing yesTokenId / noTokenId

        Any additional fields are preserved verbatim in `raw`.
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
        Determine whether this market should be considered for logging.

        This is intentionally minimal for now:
        - A market is loggable if it has a valid slug.

        Future extensions may filter out:
        - Resolved or expired markets
        - Illiquid markets
        - Markets failing venue-specific health checks
        """
        return bool(self.slug)
