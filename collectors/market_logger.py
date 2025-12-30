"""
Order book polling service.

Consumes per-venue discovery snapshots (active_instruments.snapshot.json),
polls order books for the active set, and writes JSONL logs with rotation.

This module does NOT perform discovery and does NOT write market-metadata logs.
Discovery should run in a separate process and write the snapshot files.
"""

import json
import os
import time
from datetime import datetime
from pathlib import Path

from config.settings import settings
from collectors.active_instruments import ActiveInstruments
from collectors.venue_runtime import VenueRuntime
from storage.jsonl_writer import JsonlRotatingWriter


class MarketLogger:
    """
    Multi-venue order book poller.

    Responsibilities:
    - Read per-venue active set snapshots from discovery (atomic JSON file writes)
    - Maintain in-memory per-instrument backoff state and per-venue cooldown
    - Poll order book snapshots and write normalized records to JSONL with rotation

    Non-responsibilities:
    - Market discovery / selection / filtering
    - Market metadata logging
    - Trading / strategy / execution
    """

    def __init__(self, venues: list[VenueRuntime]):
        self.venues = venues
        for v in self.venues:
            v.out_dir.mkdir(parents=True, exist_ok=True)

    # -------------------------
    # Helpers
    # -------------------------
    def _init_venue_state(self) -> dict:
        """
        One-time setup: writers, snapshot path tracking, and per-venue counters.
        """
        venue_state: dict[str, dict] = {}

        for v in self.venues:
            current_date = datetime.utcnow().strftime("%Y-%m-%d")

            books_writer = JsonlRotatingWriter(
                v.out_dir / "orderbooks" / f"date={current_date}",
                "orderbooks",
                settings.ROTATE_MINUTES,
                settings.FSYNC_SECONDS,
            )

            # Kept for now as a small wrapper around a dict; poller treats this as in-memory only.
            active = ActiveInstruments(
                v.out_dir / "state" / "active_instruments.json",
                settings.EXPIRE_GRACE_SECONDS,
            )

            snapshot_path = v.out_dir / "state" / "active_instruments.snapshot.json"

            venue_state[v.name] = {
                "venue": v,
                "current_date": current_date,
                "books_writer": books_writer,

                # Active instruments (in-memory only for poller; discovery owns persistence)
                "active": active,

                # Snapshot reload state
                "snapshot_path": snapshot_path,
                "snapshot_mtime": 0.0,
                "snapshot_asof": None,

                # Failure/backoff + cooldown (monotonic time)
                "fail_state": {},
                "cooldown_until": 0.0,
            }

        return venue_state

    def _rollover_if_needed(self, vs: dict) -> None:
        """
        Midnight UTC rollover for one venue: closes writers and opens new ones.
        """
        v = vs["venue"]
        old_date = vs["current_date"]
        new_date = datetime.utcnow().strftime("%Y-%m-%d")

        if new_date == old_date:
            return

        books_writer = vs.get("books_writer")

        try:
            if books_writer is not None:
                books_writer.close()
        finally:
            vs["current_date"] = new_date
            vs["books_writer"] = JsonlRotatingWriter(
                v.out_dir / "orderbooks" / f"date={new_date}",
                "orderbooks",
                settings.ROTATE_MINUTES,
                settings.FSYNC_SECONDS,
            )
            print(f"[POLLER] rollover venue={v.name} {old_date} -> {new_date}")

    def _maybe_reload_snapshot(self, vs: dict) -> None:
        """
        If the discovery snapshot has changed, load it and replace active instruments
        for this venue.

        This keeps polling independent of discovery latency.

        IMPORTANT: poller treats snapshots as read-only; it does NOT write active state.
        """
        snap_path: Path = vs["snapshot_path"]

        try:
            if not snap_path.exists():
                return

            st = os.stat(snap_path)
            mtime = st.st_mtime
            if mtime <= vs["snapshot_mtime"]:
                return

            payload = json.loads(snap_path.read_text(encoding="utf-8"))
            instruments = payload.get("instruments")
            if not isinstance(instruments, dict):
                print(f"[POLLER][WARN] snapshot malformed venue={vs['venue'].name}: no instruments dict")
                return

            active: ActiveInstruments = vs["active"]

            old_keys = set(active.active.keys())
            new_keys = set(instruments.keys())

            # Replace in-memory active set with snapshot
            active.active = instruments

            # Prune fail_state so it doesn't grow forever
            fail_state = vs["fail_state"]
            for k in list(fail_state.keys()):
                if k not in active.active:
                    del fail_state[k]

            vs["snapshot_mtime"] = mtime
            vs["snapshot_asof"] = payload.get("asof_ts_utc")

            added = len(new_keys - old_keys)
            removed = len(old_keys - new_keys)

            print(
                f"[POLLER] loaded snapshot venue={vs['venue'].name} "
                f"count={len(active.active)} added={added} removed={removed} "
                f"asof={vs['snapshot_asof']}"
            )

        except Exception as exc:
            # Poller should never die because snapshot read hiccupped
            print(f"[POLLER][WARN] failed to reload snapshot venue={vs['venue'].name}: {type(exc).__name__}: {exc}")

    def _poll_once(self, vs: dict, now_mono: float) -> tuple[int, int]:
        """
        Poll all active instruments once for one venue.
        Returns (successes, failures).

        Uses monotonic time for backoff/cooldown to avoid issues with system clock jumps.
        """
        # Honor per-venue cooldown without blocking other venues
        if now_mono < vs["cooldown_until"]:
            return (0, 0)

        v = vs["venue"]
        active = vs["active"]
        fail_state = vs["fail_state"]
        books_writer = vs["books_writer"]

        loop_failures = 0
        loop_successes = 0

        for ikey, info in active.active.items():
            st = fail_state.get(ikey, {"count": 0, "next_ok": 0.0, "last_log": 0.0})
            if now_mono < st["next_ok"]:
                continue

            slug = info.get("slug")
            mid = info.get("market_id")

            try:
                poll_key = info["poll_key"]
                raw_ob = v.client.get_orderbook(poll_key)

                fail_state[ikey] = {"count": 0, "next_ok": 0.0, "last_log": 0.0}
                loop_successes += 1

            except Exception as exc:
                loop_failures += 1
                st["count"] += 1

                backoff = min(60, 2 ** min(st["count"], 6))
                st["next_ok"] = now_mono + backoff

                if st["count"] in (1, 3, 5) or (now_mono - st["last_log"] > 60):
                    print(
                        f"[WARN] get_orderbook failed "
                        f"venue={v.name} ikey={ikey} mid={mid} slug={slug} "
                        f"count={st['count']} backoff={backoff}s "
                        f"err={type(exc).__name__}: {exc}"
                    )
                    st["last_log"] = now_mono

                fail_state[ikey] = st
                continue

            snap = {
                "timestamp": datetime.utcnow().isoformat(),
                "snapshot_asof": vs.get("snapshot_asof"),

                "market_id": mid,
                "slug": slug,
                "underlying": info.get("underlying"),
                "orderbook": raw_ob,

                "instrument_key": ikey,
                "instrument_id": info.get("instrument_id"),
                "venue": v.name,
                "poll_key": info.get("poll_key"),
            }

            rec = v.normalizer(snap, full_orderbook=settings.FULL_ORDERBOOK) or snap
            books_writer.write(rec)

        return (loop_successes, loop_failures)

    def _maybe_apply_cooldown(self, vs: dict, successes: int, failures: int, now_mono: float) -> None:
        """
        If many instruments are failing, cool down this venue only (non-blocking).
        """
        active = vs["active"]
        v = vs["venue"]

        if failures >= max(3, len(active.active) // 2):
            cooldown = 10
            vs["cooldown_until"] = now_mono + cooldown
            print(
                f"[WARN] high failure rate this loop for venue={v.name} "
                f"(failures={failures}, successes={successes}). "
                f"Cooling down {cooldown}s."
            )

    # -------------------------
    # Main loop (orchestrator)
    # -------------------------
    def run(self) -> None:
        venue_state = self._init_venue_state()

        try:
            while True:
                now_mono = time.monotonic()

                # deterministic order for predictable logs
                for vname in sorted(venue_state.keys()):
                    vs = venue_state[vname]

                    self._rollover_if_needed(vs)
                    self._maybe_reload_snapshot(vs)

                    successes, failures = self._poll_once(vs, now_mono=now_mono)
                    print(
                        f"[POLLER] venue={vs['venue'].name} "
                        f"saved={successes} failed={failures} total={successes + failures}"
                    )

                    self._maybe_apply_cooldown(vs, successes=successes, failures=failures, now_mono=now_mono)

                time.sleep(settings.POLL_INTERVAL)

        except KeyboardInterrupt:
            print("[POLLER] shutdown requested (KeyboardInterrupt)")
        finally:
            # Best-effort cleanup
            for vname, vs in venue_state.items():
                try:
                    bw = vs.get("books_writer")
                    if bw is not None:
                        bw.close()
                except Exception:
                    pass
