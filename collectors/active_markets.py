import json
import time
from pathlib import Path


class ActiveMarkets:
    """
    Lightweight persistence + pruning for the set of "currently active" markets.

    Responsibilities:
    - Maintain a dict keyed by market_id -> minimal metadata needed for polling
    - Persist this dict to disk so the logger can restart without re-discovering immediately
    - Prune expired markets, with a configurable grace window

    Non-responsibilities:
    - Fetching markets from the exchange (done elsewhere)
    - Fetching orderbooks (done elsewhere)
    - Any strategy / selection logic beyond simple expiry-based pruning
    """

    def __init__(self, path: Path, grace_seconds: int):
        # Path to a JSON file used to persist active market state between runs
        self.path = path

        # Grace window after expiration during which we still consider a market "active"
        # Stored in milliseconds to match the exchange "expirationTimestamp" convention.
        self.grace = grace_seconds * 1000

        # Dict[market_id, {"slug": str, "underlying": str, "expiration": Optional[int]}]
        self.active = {}

        # Load persisted state on startup (if present)
        self._load()

    def _load(self):
        """Load persisted active markets from disk if available."""
        if self.path.exists():
            # Stored as raw JSON dict; we keep it untyped for simplicity.
            self.active = json.loads(self.path.read_text())
        else:
            self.active = {}

    def save(self):
        """
        Persist current active market map to disk.

        NOTE: This writes a single JSON file (no atomic write). If you later care about
        crash-safety during write, you can implement a temp-file + rename strategy.
        """
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self.active))

    def refresh(self, markets):
        """
        Update/insert active-market entries from a freshly discovered market list.

        We intentionally store only the fields needed for orderbook polling and pruning:
        - slug: used to query orderbooks
        - underlying: useful for downstream partitioning/analytics
        - expirationTimestamp: used for pruning (may be absent)
        """
        for m in markets:
            self.active[m.market_id] = {
                "slug": m.slug,
                "underlying": m.underlying,
                "expiration": m.raw.get("expirationTimestamp"),
            }

    def prune(self):
        """
        Drop markets that are expired beyond the grace window.

        Rationale:
        - Some venues can have slight discrepancies around expiration time.
        - The grace window avoids thrashing due to clock skew or delayed updates.
        """
        now = int(time.time() * 1000)  # milliseconds since epoch
        self.active = {
            k: v
            for k, v in self.active.items()
            # Keep markets that have no known expiration, or that haven't expired (plus grace)
            if not v["expiration"] or now < v["expiration"] + self.grace
        }
