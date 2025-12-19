import json
import time
from pathlib import Path


class ActiveMarkets:
    def __init__(self, path: Path, grace_seconds: int):
        self.path = path
        self.grace = grace_seconds * 1000
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

    def refresh(self, markets):
        for m in markets:
            self.active[m.market_id] = {
                "slug": m.slug,
                "underlying": m.underlying,
                "expiration": m.raw.get("expirationTimestamp"),
            }

    def prune(self):
        now = int(time.time() * 1000)
        self.active = {
            k: v for k, v in self.active.items()
            if not v["expiration"] or now < v["expiration"] + self.grace
        }
