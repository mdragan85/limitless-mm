import json
import time
from pathlib import Path


class ActiveInstruments:
    """
    Persistence + pruning for the set of "currently active" instruments.

    Keyed by instrument_key = "<venue>:<market_id>:<instrument_id>"

    For Limitless in Phase B:
      venue="limitless", instrument_id="BOOK"
    """

    def __init__(self, path: Path, grace_seconds: int):
        self.path = path
        self.grace = grace_seconds * 1000  # ms
        self.active = {}
        self._load()

    def _load(self):
        if self.path.exists():
            self.active = json.loads(self.path.read_text())
        else:
            self.active = {}

    def save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self.active))

    @staticmethod
    def make_key(*, venue: str, market_id: str, instrument_id: str) -> str:
        return f"{venue}:{market_id}:{instrument_id}"

    def refresh_from_instruments(self, instruments):
        """
        Unified ingestion: venues provide instrument dicts that already include poll_key.

        Required fields per instrument dict:
          - venue, market_id, instrument_id, poll_key
        Optional:
          - slug, underlying, expiration, outcome, question, rule, etc.
        """
        for inst in instruments:
            venue = inst["venue"]
            market_id = str(inst["market_id"])
            instrument_id = str(inst["instrument_id"])
            poll_key = inst["poll_key"]

            key = self.make_key(venue=venue, market_id=market_id, instrument_id=instrument_id)

            # Keep existing fields if present; overwrite with latest discovery.
            prev = self.active.get(key, {})
            merged = {**prev, **inst}

            # Ensure required fields exist and are canonical types
            merged["venue"] = venue
            merged["market_id"] = market_id
            merged["instrument_id"] = instrument_id
            merged["poll_key"] = poll_key

            self.active[key] = merged


    # FROM OLD CODE -> WILL BE DEPRECATED (REPLACED BY refresh_from_instruments)
    def refresh_from_markets(self, *, venue: str, markets):
        """
        Phase-B bridge: take the old discovered market objects and expand them
        into instruments. For Limitless: one instrument per market ("BOOK").
        """
        for m in markets:
            key = self.make_key(venue=venue, market_id=m.market_id, instrument_id="BOOK")
            self.active[key] = {
                "venue": venue,
                "market_id": m.market_id,
                "instrument_id": "BOOK",
                "slug": m.slug,
                "poll_key": m.slug,  
                "underlying": m.underlying,
                "expiration": m.raw.get("expirationTimestamp"),
            }

    def prune(self):
        now = int(time.time() * 1000)
        self.active = {
            k: v
            for k, v in self.active.items()
            if not v.get("expiration") or now < v["expiration"] + self.grace
        }

