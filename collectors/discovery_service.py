# collectors/discovery_service.py

import json
import os
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

from config.settings import settings
from collectors.active_instruments import ActiveInstruments
from collectors.venue_runtime import VenueRuntime


def _atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    """
    Atomic-ish JSON write (POSIX): write temp file then rename.
    Ensures poller never reads a partially written snapshot.
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    tmp_path = path.with_suffix(path.suffix + ".tmp")
    data = json.dumps(payload, ensure_ascii=False)

    with tmp_path.open("w", encoding="utf-8") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())

    tmp_path.replace(path)


@dataclass
class DiscoveryService:
    """
    Periodically discovers instruments for each venue and writes a snapshot file
    that other processes can consume (e.g., poller).

    Keeps using ActiveInstruments for persistence/pruning across restarts.
    """
    venues: List[VenueRuntime]
    snapshot_name: str = "active_instruments.snapshot.json"

    def run_once(self) -> None:
        now_iso = datetime.utcnow().isoformat()

        for v in self.venues:
            v.out_dir.mkdir(parents=True, exist_ok=True)

            active_path = v.out_dir / "state" / "active_instruments.json"
            snap_path = v.out_dir / "state" / self.snapshot_name

            active = ActiveInstruments(active_path, settings.EXPIRE_GRACE_SECONDS)

            instruments = v.discover_fn()
            active.refresh_from_instruments(instruments)
            active.prune()
            active.save()

            snapshot = {
                "asof_ts_utc": now_iso,
                "venue": v.name,
                "count": len(active.active),
                "instruments": active.active,
            }

            _atomic_write_json(snap_path, snapshot)

            print(
                f"[DISCOVERY] venue={v.name} instruments={len(active.active)} "
                f"snapshot={snap_path}"
            )

    def run_forever(self) -> None:
        while True:
            start = time.time()
            try:
                self.run_once()
            except Exception as exc:
                # discovery should never kill the process because one venue hiccuped
                print(f"[DISCOVERY][WARN] run_once failed: {type(exc).__name__}: {exc}")

            elapsed = time.time() - start
            sleep_for = max(1.0, settings.DISCOVER_EVERY_SECONDS - elapsed)
            time.sleep(sleep_for)
