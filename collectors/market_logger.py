"""
Market logging engine for Limitless.
Discovers markets, filters them, polls snapshots, and writes logs.
"""

import json
import time
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
    # Main loop
    # -------------------------
    def run(self):

        venue_state = {}  # NEW: per-venue runtime state lives here

        GLOBAL_COOLDOWN_SECONDS = 10  # unchanged concept; used per-venue loop below

        # NEW: Initialize per-venue writers/state/backoff
        for v in self.venues:
            current_date = datetime.utcnow().strftime("%Y-%m-%d")  # NEW: per-venue current date

            markets_writer = JsonlRotatingWriter(
                v.out_dir / "markets" / f"date={current_date}",  # NEW: venue-scoped out_dir
                "markets",
                settings.ROTATE_MINUTES,
                settings.FSYNC_SECONDS,
            )

            books_writer = JsonlRotatingWriter(
                v.out_dir / "orderbooks" / f"date={current_date}",  # NEW: venue-scoped out_dir
                "orderbooks",
                settings.ROTATE_MINUTES,
                settings.FSYNC_SECONDS,
            )

            active = ActiveInstruments(
                v.out_dir / "state" / "active_instruments.json",  # NEW: venue-scoped state file
                settings.EXPIRE_GRACE_SECONDS,
            )

            venue_state[v.name] = {  # NEW: bundle all per-venue variables
                "venue": v,
                "current_date": current_date,
                "markets_writer": markets_writer,
                "books_writer": books_writer,
                "active": active,
                "last_discover": 0,
                "fail_state": {},  # key = instrument_key
            }

        while True:
            now = time.time()

            # NEW: run the same logic for each venue, independently
            for vs in venue_state.values():
                v = vs["venue"]  # NEW: the VenueRuntime
                active = vs["active"]
                fail_state = vs["fail_state"]
                markets_writer = vs["markets_writer"]
                books_writer = vs["books_writer"]
                current_date = vs["current_date"]
                last_discover = vs["last_discover"]

                # ----- Midnight UTC rollover (per venue) -----
                new_date = datetime.utcnow().strftime("%Y-%m-%d")
                if new_date != current_date:
                    try:
                        markets_writer.close()
                        books_writer.close()
                    finally:
                        current_date = new_date
                        # NEW: recreate writers with venue-scoped out_dir
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
                        # NEW: write back updated per-venue state
                        vs["current_date"] = current_date
                        vs["markets_writer"] = markets_writer
                        vs["books_writer"] = books_writer
                # --------------------------------------------

                # ----- Periodic market discovery (per venue) -----
                if now - last_discover > settings.DISCOVER_EVERY_SECONDS:
                    last_discover = now
                    vs["last_discover"] = last_discover  # NEW: persist per-venue last_discover

                    # NOTE: Limitless uses settings.UNDERLYINGS; Polymarket will not.
                    # We'll keep this as-is for now; Polymarket integration will replace
                    # discovery via v.discover_fn later.
                    for u in settings.UNDERLYINGS:
                        markets = v.client.discover_markets(u)  # CHANGED: self.client -> v.client
                        active.refresh_from_markets(venue=v.client.venue, markets=markets)  # CHANGED: per-venue client

                        for m in markets:
                            markets_writer.write({
                                "asof_ts_utc": datetime.utcnow().isoformat(),
                                "market_id": m.market_id,
                                "slug": m.slug,
                                "underlying": m.underlying,
                                "raw": m.raw,
                            })

                    active.prune()
                    active.save()
                # ------------------------------------------------

                loop_failures = 0
                loop_successes = 0
                now_ts = time.time()

                # ----- Orderbook polling (per venue) -----
                for ikey, info in active.active.items():
                    st = fail_state.get(ikey, {"count": 0, "next_ok": 0.0, "last_log": 0.0})
                    if now_ts < st["next_ok"]:
                        continue

                    slug = info.get("slug")  # CHANGED: safer (Polymarket won't have slug)
                    mid = info["market_id"]

                    try:
                        poll_key = info["poll_key"]
                        raw_ob = v.client.get_orderbook(poll_key)  # CHANGED: self.client -> v.client

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

                    rec = v.normalizer(snap, full_orderbook=settings.FULL_ORDERBOOK)  # CHANGED: normalize via venue runtime
                    books_writer.write(rec)
                # ---------------------------------------

                # ----- Global health check (per venue) -----
                if loop_failures >= max(3, len(active.active) // 2):
                    print(
                        f"[WARN] high failure rate this loop for venue={v.name} "
                        f"(failures={loop_failures}, successes={loop_successes}). "
                        f"Cooling down {GLOBAL_COOLDOWN_SECONDS}s."
                    )
                    time.sleep(GLOBAL_COOLDOWN_SECONDS)
                # ------------------------------------------

            # Normal polling interval (shared)
            time.sleep(settings.POLL_INTERVAL)
