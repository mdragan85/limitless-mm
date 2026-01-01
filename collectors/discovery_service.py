# collectors/discovery_service.py

import json
import os
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

from config.settings import settings
from collectors.venue_runtime import VenueRuntime
from storage.jsonl_writer import JsonlRotatingWriter


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
            current_date = datetime.utcnow().strftime("%Y-%m-%d")

            markets_writer = JsonlRotatingWriter(
                v.out_dir / "markets" / f"date={current_date}",
                "markets",
                settings.ROTATE_MINUTES,
                settings.FSYNC_SECONDS,
            )

            try:
                snap_path = v.out_dir / "state" / self.snapshot_name

                instruments = v.discover_fn()

                # Build active dict directly from discovered instruments (no persistence)
                active: dict[str, dict] = {}
                for inst in instruments:
                    ikey = inst.get("instrument_key") or inst.get("poll_key") or inst.get("instrument_id")
                    if not ikey:
                        continue
                    active[str(ikey)] = inst

                # Write market metadata here (discovery-owned)
                for inst in instruments:
                    # -------------------------------------------------------------
                    # Write-boundary record contract (markets / metadata)
                    # Add envelope fields so readers can route and tolerate evolution.
                    # -------------------------------------------------------------
                    inst.setdefault("record_type", "market")
                    inst.setdefault("schema_version", settings.SCHEMA_VERSION_MARKETS)

                    # Optional (recommended): enforce canonical identity invariants
                    # only if the fields exist / can be derived safely.
                    venue = inst.get("venue") or v.name
                    inst.setdefault("venue", venue)

                    pk = inst.get("poll_key") or inst.get("slug") or inst.get("asset_id") or inst.get("instrument_key")
                    if pk is not None:
                        inst.setdefault("poll_key", str(pk))
                        canonical_id = f"{venue}:{str(pk)}"
                        if inst.get("instrument_id") != canonical_id:
                            inst["instrument_id"] = canonical_id

                    markets_writer.write(inst)

                snapshot = {
                    "asof_ts_utc": now_iso,
                    "venue": v.name,
                    "count": len(active),
                    "instruments": active,
                }

                _atomic_write_json(snap_path, snapshot)

                print(f"<DiscoveryApp>: venue={v.name} instruments={len(active)} snapshot={snap_path}")

            finally:
                # Always close to avoid leaking file handles across long-running loops
                try:
                    markets_writer.close()
                except Exception:
                    pass


    def run_forever(self) -> None:
        while True:
            start = time.time()
            try:
                self.run_once()
            except Exception as exc:
                # discovery should never kill the process because one venue hiccuped
                print(f"<DiscoveryApp|Warning>: run_once failed: {type(exc).__name__}: {exc}")

            elapsed = time.time() - start
            sleep_for = max(1.0, settings.DISCOVER_EVERY_SECONDS - elapsed)
            time.sleep(sleep_for)
