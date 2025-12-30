"""
Market logging engine for Limitless.
Discovers markets, filters them, polls snapshots, and writes logs.
"""

import json
import time
import os

from datetime import datetime
from pathlib import Path

from config.settings import settings
from venues.limitless.client import LimitlessVenueClient
from venues.limitless.market import LimitlessMarket

from storage.jsonl_writer import JsonlRotatingWriter
from collectors.active_instruments import ActiveInstruments
from collectors.venue_runtime import VenueRuntime

from venues.limitless.normalizer import normalize_orderbook


class MarketLogger:
    """
    Market data collection service for Limitless.

    Responsibilities:
    - Periodically discover markets for configured underlyings (settings.UNDERLYINGS)
    - Maintain a persisted set of "active" markets across restarts (ActiveMarkets)
    - Poll orderbook snapshots for active markets in a tight loop
    - Apply per-market exponential backoff and a global cooldown during outages
    - Persist both market metadata and normalized orderbook snapshots to disk
      using time-based file rotation (JsonlRotatingWriter)

    Output conventions:
    - Market metadata logs are written under:  <OUTPUT_DIR>/markets/date=YYYY-MM-DD/
    - Orderbook snapshot logs are written under: <OUTPUT_DIR>/orderbooks/date=YYYY-MM-DD/
    - Active market state is written under:     <OUTPUT_DIR>/state/active_markets.json

    Notes / sharp edges:
    - This class currently includes an older "single snapshot -> append JSONL" pathway
      (log_snapshot / log_markets) that writes per-underlying files directly.
      The main production pipeline is `run()`, which uses rotating writers and the
      ActiveMarkets state machine. If you standardize outputs, consider removing the
      direct-write pathway to avoid having two logging formats.
    - This module is intentionally limited to data collection and persistence; it should
      not contain strategy, pricing, or execution logic.
    """

    def __init__(self, venues: list[VenueRuntime]):
        self.venues = venues
        for v in self.venues:
            v.out_dir.mkdir(parents=True, exist_ok=True)

    # -------------------------
    # Logging a single snapshot
    # -------------------------
    def log_snapshot(self, market: LimitlessMarket) -> None:
        """
        Fetch a single orderbook snapshot for a Limitless market and
        append it as one JSON line to the per-underlying log file.
        """
        try:
            orderbook = self.client.get_orderbook(market.slug)
        except Exception as exc:
            print(
                f"[WARN] Failed to fetch orderbook for "
                f"{market.market_id}/{market.slug}: {exc}"
            )
            return

        record = {
            "timestamp": datetime.utcnow().isoformat(),
            "market_id": market.market_id,
            "slug": market.slug,
            "underlying": market.underlying,
            "title": market.title,
            "orderbook": orderbook,
        }

        # Use underlying symbol in filename; fall back to UNKNOWN if empty
        underlying = market.underlying or "UNKNOWN"
        file_path = self.out_dir / f"{underlying}_orderbooks.jsonl"

        with file_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

    # -------------------------
    # Logging helpers
    # -------------------------
    def log_markets(self, markets: list[LimitlessMarket]) -> None:
        for market in markets:
            self.log_snapshot(market)

    # -------------------------
    # Helpers
    # -------------------------
    def _init_venue_state(self) -> dict:
        """
        One-time setup: writers, ActiveInstruments, and per-venue counters.
        """
        venue_state = {}

        for v in self.venues:
            current_date = datetime.utcnow().strftime("%Y-%m-%d")

            markets_writer = JsonlRotatingWriter(
                v.out_dir / "markets" / f"date={current_date}",
                "markets",
                settings.ROTATE_MINUTES,
                settings.FSYNC_SECONDS,
            )

            books_writer = JsonlRotatingWriter(
                v.out_dir / "orderbooks" / f"date={current_date}",
                "orderbooks",
                settings.ROTATE_MINUTES,
                settings.FSYNC_SECONDS,
            )

            active = ActiveInstruments(
                v.out_dir / "state" / "active_instruments.json",
                settings.EXPIRE_GRACE_SECONDS,
            )

            snapshot_path = v.out_dir / "state" / "active_instruments.snapshot.json"

            venue_state[v.name] = {
                "venue": v,
                "current_date": current_date,
                "markets_writer": markets_writer,
                "books_writer": books_writer,
                "active": active,

                # snapshot reload state
                "snapshot_path": snapshot_path,
                "snapshot_mtime": 0.0,

                "fail_state": {},
                "cooldown_until": 0.0,
            }

        return venue_state

    def _rollover_if_needed(self, vs: dict) -> None:
        """
        Midnight UTC rollover for one venue: closes writers and opens new ones.
        """
        v = vs["venue"]
        current_date = vs["current_date"]
        new_date = datetime.utcnow().strftime("%Y-%m-%d")

        if new_date == current_date:
            return

        markets_writer = vs["markets_writer"]
        books_writer = vs["books_writer"]

        try:
            markets_writer.close()
            books_writer.close()
        finally:
            vs["current_date"] = new_date
            vs["markets_writer"] = JsonlRotatingWriter(
                v.out_dir / "markets" / f"date={new_date}",
                "markets",
                settings.ROTATE_MINUTES,
                settings.FSYNC_SECONDS,
            )
            vs["books_writer"] = JsonlRotatingWriter(
                v.out_dir / "orderbooks" / f"date={new_date}",
                "orderbooks",
                settings.ROTATE_MINUTES,
                settings.FSYNC_SECONDS,
            )

    def _maybe_reload_snapshot(self, vs: dict) -> None:
        """
        If the discovery snapshot has changed, load it and replace active instruments
        for this venue.

        This keeps polling independent of discovery latency.
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
                print(f"[POLL][WARN] snapshot malformed for venue={vs['venue'].name}: no instruments dict")
                return

            active: ActiveInstruments = vs["active"]

            old_keys = set(active.active.keys())
            new_keys = set(instruments.keys())

            active.active = instruments
            # optional: extra safety in case discovery didn't prune for some reason
            active.prune()
            active.save()

            # prune fail_state so it doesn't grow forever
            fail_state = vs["fail_state"]
            for k in list(fail_state.keys()):
                if k not in active.active:
                    del fail_state[k]

            added = len(new_keys - old_keys)
            removed = len(old_keys - new_keys)

            vs["snapshot_mtime"] = mtime
            print(
                f"[POLL] loaded snapshot venue={vs['venue'].name} "
                f"count={len(active.active)} added={added} removed={removed} "
                f"asof={payload.get('asof_ts_utc')}"
            )

        except Exception as exc:
            # Poller should never die because snapshot read hiccupped
            print(f"[POLL][WARN] failed to reload snapshot venue={vs['venue'].name}: {type(exc).__name__}: {exc}")

    def _maybe_discover(self, vs: dict, now: float) -> None:
        """
        Periodic discovery + persistence for one venue.
        """
        # NEW: honor per-venue cooldown without blocking other venues
        if now < vs["cooldown_until"]:
            return

        last_discover = vs["last_discover"]
        if now - last_discover <= settings.DISCOVER_EVERY_SECONDS:
            return

        v = vs["venue"]
        active = vs["active"]
        markets_writer = vs["markets_writer"]

        vs["last_discover"] = now

        instruments = v.discover_fn()
        active.refresh_from_instruments(instruments)

        for inst in instruments:
            markets_writer.write({
                "asof_ts_utc": datetime.utcnow().isoformat(),
                **inst,
            })

        active.prune()
        active.save()

    def _poll_once(self, vs: dict, now_ts: float) -> tuple[int, int]:
        """
        Poll all active instruments once for one venue.
        Returns (successes, failures).
        """
        # NEW: honor per-venue cooldown without blocking other venues
        if now_ts < vs["cooldown_until"]:
            return (0, 0)

        v = vs["venue"]
        active = vs["active"]
        fail_state = vs["fail_state"]
        books_writer = vs["books_writer"]

        loop_failures = 0
        loop_successes = 0

        for ikey, info in active.active.items():
            st = fail_state.get(ikey, {"count": 0, "next_ok": 0.0, "last_log": 0.0})
            if now_ts < st["next_ok"]:
                continue

            slug = info.get("slug")
            mid = info["market_id"]

            try:
                poll_key = info["poll_key"]
                raw_ob = v.client.get_orderbook(poll_key)

                fail_state[ikey] = {"count": 0, "next_ok": 0.0, "last_log": 0.0}
                loop_successes += 1

            except Exception as exc:
                loop_failures += 1
                st["count"] += 1

                backoff = min(60, 2 ** min(st["count"], 6))
                st["next_ok"] = now_ts + backoff

                if st["count"] in (1, 3, 5) or (now_ts - st["last_log"] > 60):
                    print(
                        f"[WARN] get_orderbook failed "
                        f"venue={v.name} ikey={ikey} mid={mid} slug={slug} "
                        f"count={st['count']} backoff={backoff}s "
                        f"err={type(exc).__name__}: {exc}"
                    )
                    st["last_log"] = now_ts

                fail_state[ikey] = st
                continue

            snap = {
                "timestamp": datetime.utcnow().isoformat(),
                "market_id": mid,
                "slug": slug,
                "underlying": info.get("underlying"),
                "orderbook": raw_ob,

                "instrument_key": ikey,
                "instrument_id": info.get("instrument_id"),
                "venue": info.get("venue"),
                "poll_key": info.get("poll_key"),
            }

            rec = v.normalizer(snap, full_orderbook=settings.FULL_ORDERBOOK) or snap
            books_writer.write(rec)

        return (loop_successes, loop_failures)

    def _maybe_apply_cooldown(self, vs: dict, successes: int, failures: int, now: float) -> None:
        """
        If many instruments are failing, cool down this venue only (non-blocking).
        """
        active = vs["active"]
        v = vs["venue"]

        if failures >= max(3, len(active.active) // 2):
            cooldown = 10  # was GLOBAL_COOLDOWN_SECONDS
            vs["cooldown_until"] = now + cooldown  # NEW: non-blocking cooldown
            print(
                f"[WARN] high failure rate this loop for venue={v.name} "
                f"(failures={failures}, successes={successes}). "
                f"Cooling down {cooldown}s."
            )

    # -------------------------
    # Main loop (orchestrator)
    # -------------------------
    def run(self):

        venue_state = self._init_venue_state()

        while True:
            now = time.time()

            for vs in venue_state.values():
                self._rollover_if_needed(vs)
                self._maybe_reload_snapshot(vs)

                successes, failures = self._poll_once(vs, now_ts=now)
                self._maybe_apply_cooldown(vs, successes=successes, failures=failures, now=now)

            time.sleep(settings.POLL_INTERVAL)