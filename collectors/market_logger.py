"""
Market logging engine for Limitless.
Discovers markets, filters them, polls snapshots, and writes logs.
"""

import json
import time
from datetime import datetime
from pathlib import Path

from config.settings import settings
from venues.limitless_api import LimitlessAPI
from venues.limitless_market import LimitlessMarket

from storage.jsonl_writer import JsonlRotatingWriter
from collectors.active_markets import ActiveMarkets

from .normalize_orderbook import normalize_orderbook


class MarketLogger:
    """
    Polls multiple markets across multiple underlyings and writes snapshot logs.
    """

    def __init__(self, api: LimitlessAPI):
        self.api = api
        self.out_dir = Path(settings.OUTPUT_DIR)
        self.out_dir.mkdir(parents=True, exist_ok=True)


    # -------------------------
    # Logging a single snapshot
    # -------------------------
    def log_snapshot(self, market: LimitlessMarket) -> None:
        """
        Fetch a single orderbook snapshot for a Limitless market and
        append it as one JSON line to the per-underlying log file.
        """
        try:
            orderbook = self.api.get_orderbook(market.slug)
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
        # Track the current UTC date so we can roll log folders at midnight
        current_date = datetime.utcnow().strftime("%Y-%m-%d")

        def make_writers(date_str: str):
            """
            Create fresh rotating writers for a given UTC date.
            Called once at startup and again on midnight rollover.
            """
            markets_writer = JsonlRotatingWriter(
                self.out_dir / "markets" / f"date={date_str}",
                "markets",
                settings.ROTATE_MINUTES,
                settings.FSYNC_SECONDS,
            )

            books_writer = JsonlRotatingWriter(
                self.out_dir / "orderbooks" / f"date={date_str}",
                "orderbooks",
                settings.ROTATE_MINUTES,
                settings.FSYNC_SECONDS,
            )
            return markets_writer, books_writer

        # Initial writers
        markets_writer, books_writer = make_writers(current_date)

        # Tracks which markets are currently active / stale
        active = ActiveMarkets(
            self.out_dir / "state" / "active_markets.json",
            settings.EXPIRE_GRACE_SECONDS,
        )

        # Timestamp of last market discovery run
        last_discover = 0

        # Per-market failure state:
        # - count: consecutive failures
        # - next_ok: unix timestamp after which we try again
        # - last_log: last time we logged a warning for this market
        fail_state = {}  # market_id -> dict

        # If a large fraction of markets are failing, pause globally
        GLOBAL_COOLDOWN_SECONDS = 10

        while True:
            now = time.time()

            # ----- Midnight UTC rollover -----
            # If the date has changed, close current writers and open new ones
            new_date = datetime.utcnow().strftime("%Y-%m-%d")
            if new_date != current_date:
                try:
                    markets_writer.close()
                    books_writer.close()
                finally:
                    current_date = new_date
                    markets_writer, books_writer = make_writers(current_date)
            # ---------------------------------

            # ----- Periodic market discovery -----
            # Refresh the list of tradable markets every DISCOVER_EVERY_SECONDS
            if now - last_discover > settings.DISCOVER_EVERY_SECONDS:
                last_discover = now

                for u in settings.UNDERLYINGS:
                    markets = self.api.discover_markets(u)
                    active.refresh(markets)

                    # Persist raw market metadata for audit / research
                    for m in markets:
                        markets_writer.write({
                            "asof_ts_utc": datetime.utcnow().isoformat(),
                            "market_id": m.market_id,
                            "slug": m.slug,
                            "underlying": m.underlying,
                            "raw": m.raw,
                        })

                # Remove expired markets and persist state
                active.prune()
                active.save()
            # ------------------------------------

            loop_failures = 0
            loop_successes = 0
            now_ts = time.time()

            # ----- Orderbook polling -----
            for mid, info in active.active.items():
                # Initialize or fetch failure state for this market
                st = fail_state.get(mid, {"count": 0, "next_ok": 0.0, "last_log": 0.0})

                # Skip markets that are currently cooling down
                if now_ts < st["next_ok"]:
                    continue

                slug = info["slug"]

                try:
                    raw_ob = self.api.get_orderbook(slug)

                    # Success: reset failure state
                    fail_state[mid] = {"count": 0, "next_ok": 0.0, "last_log": 0.0}
                    loop_successes += 1

                except Exception as exc:
                    # Failure: increment backoff state
                    loop_failures += 1
                    st["count"] += 1

                    # Exponential backoff capped at 60 seconds
                    # 2, 4, 8, 16, 32, 60...
                    backoff = min(60, 2 ** min(st["count"], 6))
                    st["next_ok"] = now_ts + backoff

                    # Log sparingly to avoid console spam
                    if st["count"] in (1, 3, 5) or (now_ts - st["last_log"] > 60):
                        print(
                            f"[WARN] get_orderbook failed "
                            f"mid={mid} slug={slug} "
                            f"count={st['count']} backoff={backoff}s "
                            f"err={type(exc).__name__}: {exc}"
                        )
                        st["last_log"] = now_ts

                    fail_state[mid] = st
                    continue

                # Normalize and persist the orderbook snapshot
                snap = {
                    "timestamp": datetime.utcnow().isoformat(),
                    "market_id": mid,
                    "slug": slug,
                    "underlying": info.get("underlying"),
                    "orderbook": raw_ob,
                }

                rec = normalize_orderbook(
                    snap, full_orderbook=settings.FULL_ORDERBOOK
                )
                books_writer.write(rec)
            # -----------------------------

            # ----- Global health check -----
            # If many markets failed in this loop, pause briefly to avoid hammering
            if loop_failures >= max(3, len(active.active) // 2):
                print(
                    f"[WARN] high failure rate this loop "
                    f"(failures={loop_failures}, successes={loop_successes}). "
                    f"Cooling down {GLOBAL_COOLDOWN_SECONDS}s."
                )
                time.sleep(GLOBAL_COOLDOWN_SECONDS)
            # --------------------------------

            # Normal polling interval
            time.sleep(settings.POLL_INTERVAL)
