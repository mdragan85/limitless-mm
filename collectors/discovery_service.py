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


def _load_snapshot_instruments(path: Path) -> Dict[str, Any]:
    """
    Best-effort load of prior snapshot instruments dict.
    Returns {} if missing/malformed/unreadable.
    """
    try:
        if not path.exists():
            return {}
        payload = json.loads(path.read_text(encoding="utf-8"))
        inst = payload.get("instruments")
        return inst if isinstance(inst, dict) else {}
    except Exception:
        return {}


@dataclass
class DiscoveryService:
    """
    Periodically discovers instruments for each venue and writes a snapshot file
    that other processes can consume (e.g., poller).

    Option A behavior: only write snapshot + markets jsonl when membership changes.
    """
    venues: List[VenueRuntime]
    snapshot_name: str = "active_instruments.snapshot.json"

    def run_once(self) -> None:
        for v in self.venues:
            v.out_dir.mkdir(parents=True, exist_ok=True)

            snap_path = v.out_dir / "state" / self.snapshot_name

            # --- run venue discovery ---
            instruments = v.discover_fn() or []

            # --- build active dict from discovered instruments ---
            active: dict[str, dict] = {}
            for inst in instruments:
                # Prefer explicit instrument_key if the venue provides it
                ikey = inst.get("instrument_key")

                # Otherwise derive from poll_key (preferred) or instrument_id fallback
                if not ikey:
                    pk = inst.get("poll_key") or inst.get("slug") or inst.get("asset_id") or inst.get("instrument_id")
                    if pk is not None:
                        ikey = f"{v.name}:{str(pk)}"
                        inst["instrument_key"] = ikey

                if not ikey:
                    continue

                # ensure venue is set
                inst.setdefault("venue", v.name)

                active[str(ikey)] = inst

            # --- compare against prior snapshot membership ---
            old_instruments = _load_snapshot_instruments(snap_path)

            old_keys = set(old_instruments.keys())
            new_keys = set(active.keys())

            added_keys = new_keys - old_keys
            removed_keys = old_keys - new_keys

            changed = bool(added_keys or removed_keys)

            if not changed:
                # No file churn: no snapshot rewrite, no markets jsonl write
                print(f"<DiscoveryApp>: venue={v.name} no change (count={len(active)})")
                continue

            # --- only now: open writer (avoid creating a new jsonl unless changed) ---
            current_date = datetime.utcnow().strftime("%Y-%m-%d")
            markets_writer = JsonlRotatingWriter(
                v.out_dir / "markets" / f"date={current_date}",
                "markets",
                settings.ROTATE_MINUTES,
                settings.FSYNC_SECONDS,
            )

            try:
                now_iso = datetime.utcnow().isoformat()

                # Write market/instrument metadata (discovery-owned)
                for inst in instruments:
                    # envelope fields for readers
                    inst.setdefault("record_type", "market")  # (you can rename to "instrument" later if you want)
                    inst.setdefault("schema_version", settings.SCHEMA_VERSION_MARKETS)

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

                print(
                    f"<DiscoveryApp>: venue={v.name} instruments={len(active)} "
                    f"added={len(added_keys)} removed={len(removed_keys)} snapshot={snap_path}"
                )

            finally:
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
                print(f"<DiscoveryApp|Warning>: run_once failed: {type(exc).__name__}: {exc}")

            elapsed = time.time() - start
            sleep_for = max(1.0, settings.DISCOVER_EVERY_SECONDS - elapsed)
            time.sleep(sleep_for)
