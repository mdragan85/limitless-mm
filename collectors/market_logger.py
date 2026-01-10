"""
Order book polling service.

Consumes per-venue discovery snapshots (active_instruments.snapshot.json),
polls order books for the active set, and writes JSONL logs with rotation.

This module does NOT perform discovery and does NOT write market-metadata logs.
Discovery should run in a separate process and write the snapshot files.
"""

import re
import json
import os
import time
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque


from pathlib import Path

from config.settings import settings
from collectors.venue_runtime import VenueRuntime
from storage.jsonl_writer import JsonlRotatingWriter



def _print_instrument_list(prefix: str, instruments: dict[str, dict], keys: set[str]):
    if not keys:
        return

    # Compute max slug width for alignment
    slugs = [instruments[k].get("slug", "") for k in keys if k in instruments]
    max_slug = max((len(s) for s in slugs), default=0)

    for k in sorted(keys):
        inst = instruments.get(k)
        if not inst:
            continue
        slug = inst.get("slug", "")
        title = inst.get("question") or inst.get("title") or ""
        print(f"  {prefix} slug={slug:<{max_slug}} | {title}")


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
        One-time setup: writers, snapshot path tracking, per-venue counters,
        and per-venue concurrency limits.
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

            stats_writer = JsonlRotatingWriter(
                v.out_dir / "poll_stats" / f"date={current_date}",
                "poll_stats",
                settings.ROTATE_MINUTES,
                settings.FSYNC_SECONDS,
            )

            errors_writer = JsonlRotatingWriter(
                v.out_dir / "poll_errors" / f"date={current_date}",
                "poll_errors",
                settings.ROTATE_MINUTES,
                settings.FSYNC_SECONDS,
            )

            # Kept for now as a small wrapper around a dict; poller treats this as in-memory only.
            active: dict[str, dict] = {}

            snapshot_path = v.out_dir / "state" / "active_instruments.snapshot.json"

            # -------------------------------
            # Per-venue concurrency settings
            # -------------------------------
            if v.name == "polymarket":
                max_workers = getattr(settings, "POLL_MAX_WORKERS_POLY", 32)
                max_inflight = getattr(settings, "POLL_MAX_INFLIGHT_POLY", max_workers)
            elif v.name == "limitless":
                max_workers = getattr(settings, "POLL_MAX_WORKERS_LIMITLESS", 8)
                max_inflight = getattr(settings, "POLL_MAX_INFLIGHT_LIMITLESS", min(2, max_workers))
            else:
                # Safe defaults for any future venue
                max_workers = getattr(settings, "POLL_MAX_WORKERS_DEFAULT", 8)
                max_inflight = getattr(settings, "POLL_MAX_INFLIGHT_DEFAULT", max_workers)

            # Keep inflight <= workers to avoid bursty queued submissions
            max_inflight = min(max_inflight, max_workers)

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

                # Per-venue thread pool for orderbook fetch
                "executor": ThreadPoolExecutor(max_workers=max_workers),
                "max_inflight": max_inflight,

                # Debug visibility
                "max_workers": max_workers,

                # stats/errors
                "stats_writer": stats_writer,
                "stats_last_mono": 0.0,
                "errors_writer": errors_writer,
            }

            print(f"<PollApp>: venue={v.name} concurrency workers={max_workers} inflight={max_inflight}")

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
            print(f"<PollApp>: rollover venue={v.name} {old_date} -> {new_date}")

    def _maybe_reload_snapshot(self, vs: dict) -> None:
        """
        If the discovery snapshot has changed, load it and update active instruments
        for this venue.

        Sticky rule:
        - If an instrument was previously active but disappears from the snapshot,
        KEEP it until expiration passes (expiration <= now => drop).
        - Always drop expired instruments (even if discovery still includes them).

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
                print(f"<PollApp|Warning>: snapshot malformed venue={vs['venue'].name}: no instruments dict")
                return

            active: dict[str, dict] = vs["active"]
            old_active = dict(active)  # snapshot of current active set (for stickiness + diffs)
            old_keys = set(old_active.keys())

            now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)

            def _parse_exp_ms(inst: dict) -> int | None:
                exp = inst.get("expiration")
                if exp is None:
                    return None
                try:
                    return int(exp)
                except Exception:
                    return None

            def _not_expired(inst: dict) -> bool:
                exp = _parse_exp_ms(inst)
                return exp is not None and exp > now_ms

            # 1) Start from new snapshot instruments
            merged: dict[str, dict] = dict(instruments)

            # 2) Sticky keep: if something existed before but is missing now,
            #    keep it until expiration passes.
            for ikey, inst in old_active.items():
                if ikey in merged:
                    continue
                if _not_expired(inst):
                    merged[ikey] = inst

            # 3) Hard prune: drop anything expired (even if discovery included it)
            for ikey in list(merged.keys()):
                inst = merged.get(ikey) or {}
                exp = _parse_exp_ms(inst)
                if exp is not None and exp <= now_ms:
                    del merged[ikey]

            new_keys = set(merged.keys())

            # Replace in-memory active set with merged view (in-place)
            active.clear()
            active.update(merged)

            # Prune fail_state so it doesn't grow forever
            fail_state = vs["fail_state"]
            for k in list(fail_state.keys()):
                if k not in active:
                    del fail_state[k]

            vs["snapshot_mtime"] = mtime
            vs["snapshot_asof"] = payload.get("asof_ts_utc")

            added_keys = new_keys - old_keys
            removed_keys = old_keys - new_keys

            print(
                f"<PollApp>: loaded snapshot venue={vs['venue'].name} "
                f"count={len(active)} added={len(added_keys)} removed={len(removed_keys)} "
                f"asof={vs['snapshot_asof']}"
            )

            # Print added/removed lists using stable dicts
            if added_keys:
                print(f"<PollApp>: added instruments venue={vs['venue'].name}")
                _print_instrument_list("+", merged, added_keys)

            if removed_keys:
                print(f"<PollApp>: removed instruments venue={vs['venue'].name}")
                _print_instrument_list("-", old_active, removed_keys)

        except Exception as exc:
            # Poller should never die because snapshot read hiccupped
            print(
                f"<PollApp|Warning>: failed to reload snapshot venue={vs['venue'].name}: "
                f"{type(exc).__name__}: {exc}"
            )

    def _poll_once(self, vs: dict, now_mono: float) -> tuple[int, int]:
        """
        Poll all active instruments once for one venue, in parallel.

        Additions:
        - rate-limit / error telemetry (aggregated)
        - fetch latency metrics (ms)
        - immediate cooldown on HTTP 429
        - optional sampled error logging for later review

        Still true:
        - only network fetch is parallel
        - backoff/cooldown state mutation happens on main thread
        - file writes happen on main thread
        """
        # Honor per-venue cooldown without blocking other venues
        if now_mono < vs["cooldown_until"]:
            return (0, 0)

        v = vs["venue"]
        active = vs["active"]
        fail_state = vs["fail_state"]
        books_writer = vs["books_writer"]
        executor = vs["executor"]
        max_inflight = vs["max_inflight"]

        # --- NEW: telemetry accumulators for this loop ---
        submitted = 0
        loop_failures = 0
        loop_successes = 0

        http_429 = 0
        http_4xx = 0
        http_5xx = 0
        timeouts = 0
        other_errs = 0

        # Keep a small rolling window of latencies so we can compute p50/p95 cheaply
        # Store in vs so you can see longer-term trends without external tooling.
        lat_buf: deque[int] = vs.setdefault("lat_ms_buf", deque(maxlen=5000))

        # -------------------------------
        # helpers: status + timeout detect
        # -------------------------------
        _status_re = re.compile(r"\[(\d{3})\]")

        def _extract_status_code(exc: Exception) -> int | None:
            """
            Best-effort status extractor across:
            - httpx.HTTPStatusError (Polymarket)
            - RuntimeError("... [429] ...") (Limitless wrapper)
            - requests exceptions (if they leak through)
            """
            # httpx HTTPStatusError (has response)
            resp = getattr(exc, "response", None)
            if resp is not None:
                sc = getattr(resp, "status_code", None)
                if isinstance(sc, int):
                    return sc

            # Limitless wraps requests.HTTPError into RuntimeError with "[status]"
            m = _status_re.search(str(exc))
            if m:
                try:
                    return int(m.group(1))
                except Exception:
                    return None

            return None

        def _is_timeout(exc: Exception) -> bool:
            """
            Conservative timeout detection without importing httpx/requests exception types.
            """
            name = type(exc).__name__.lower()
            if "timeout" in name:
                return True
            msg = str(exc).lower()
            return ("timed out" in msg) or ("timeout" in msg)

        # -------------------------------
        # 1) Select eligible instruments
        # -------------------------------
        eligible = []
        for ikey, info in active.items():
            st = fail_state.get(ikey, {"count": 0, "next_ok": 0.0, "last_log": 0.0})
            if now_mono < st["next_ok"]:
                continue
            eligible.append((ikey, info.get("poll_key"), info, st))

        # Cap inflight work so we don't overwhelm the venue
        eligible = eligible[:max_inflight]

        # -------------------------------
        # 2) Submit fetch jobs
        # -------------------------------
        futures = {}

        def _fetch(poll_key: str):
            """
            Worker function: returns (ok, payload_or_exc, latency_ms, status_code)
            """
            t0 = time.perf_counter()
            try:
                ob = v.client.get_orderbook(poll_key)
                ms = int((time.perf_counter() - t0) * 1000)
                return (True, ob, ms, None)
            except Exception as exc:
                ms = int((time.perf_counter() - t0) * 1000)
                sc = _extract_status_code(exc)
                return (False, exc, ms, sc)

        for ikey, poll_key, info, st in eligible:
            submitted += 1
            fut = executor.submit(_fetch, poll_key)
            futures[fut] = (ikey, poll_key, info, st)

        # -------------------------------
        # 3) Collect results
        # -------------------------------
        for fut in as_completed(futures):
            ikey, poll_key, info, st = futures[fut]
            slug = info.get("slug")
            mid = info.get("market_id")

            ok, payload, lat_ms, status_code = fut.result()
            lat_buf.append(lat_ms)

            if ok:
                raw_ob = payload
                # success: reset failure state
                fail_state[ikey] = {"count": 0, "next_ok": 0.0, "last_log": 0.0}
                loop_successes += 1
            else:
                exc = payload
                loop_failures += 1
                st["count"] += 1

                # --- NEW: classify failures ---
                if status_code == 429:
                    http_429 += 1
                elif isinstance(status_code, int) and 400 <= status_code <= 499:
                    http_4xx += 1
                elif isinstance(status_code, int) and 500 <= status_code <= 599:
                    http_5xx += 1
                elif _is_timeout(exc):
                    timeouts += 1
                else:
                    other_errs += 1

                # --- NEW: immediate cooldown on 429 (prevents hammering into bans) ---
                if status_code == 429:
                    # Let this be configurable; default is conservative.
                    cd = getattr(settings, "RATE_LIMIT_COOLDOWN_SECONDS", 30)
                    vs["cooldown_until"] = max(vs["cooldown_until"], now_mono + cd)

                # Backoff stays exactly as before
                backoff = min(60, 2 ** min(st["count"], 6))
                st["next_ok"] = now_mono + backoff

                if st["count"] in (1, 3, 5) or (now_mono - st["last_log"] > 60):
                    print(
                        f"[WARN] get_orderbook failed "
                        f"venue={v.name} ikey={ikey} mid={mid} slug={slug} "
                        f"count={st['count']} backoff={backoff}s "
                        f"status={status_code} latency_ms={lat_ms} "
                        f"err={type(exc).__name__}: {exc}"
                    )
                    st["last_log"] = now_mono

                fail_state[ikey] = st

                # --- OPTIONAL: sampled error log for later review ---
                errw = vs.get("errors_writer")
                sample_every = getattr(settings, "POLL_ERROR_SAMPLE_EVERY", 0)  # 0 disables
                if errw is not None and sample_every and (st["count"] % sample_every == 0):
                    errw.write({
                        "ts_utc": datetime.utcnow().isoformat(),
                        "ts_ms": int(datetime.now(tz=timezone.utc).timestamp() * 1000),
                        "venue": v.name,
                        "market_id": mid,
                        "slug": slug,
                        "instrument_key": ikey,
                        "poll_key": poll_key,
                        "status": status_code,
                        "latency_ms": lat_ms,
                        "error_type": type(exc).__name__,
                        "error": str(exc)[:500],
                    })

                continue

            # -------------------------------
            # 4) Build + write record (main thread)
            # -------------------------------
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
                "poll_key": poll_key,
            }

            rec = v.normalizer(snap, full_orderbook=settings.FULL_ORDERBOOK) or snap

            # --- Enforce join-safe invariants at write boundary ---
            rec.setdefault("venue", v.name)

            pk = rec.get("poll_key") or poll_key or slug
            if pk is not None:
                rec.setdefault("poll_key", pk)
                canonical_id = f"{v.name}:{pk}"
                if rec.get("instrument_id") != canonical_id:
                    rec["instrument_id"] = canonical_id

            if "ts_ms" not in rec:
                iso = rec.get("ts_utc") or rec.get("timestamp") or snap.get("timestamp")
                if iso:
                    try:
                        s = iso.replace("Z", "+00:00")
                        dt = datetime.fromisoformat(s)
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)
                        rec["ts_ms"] = int(dt.timestamp() * 1000)
                    except Exception:
                        pass

            if "ob_ts_ms" not in rec:
                ob = rec.get("orderbook")
                if isinstance(ob, dict):
                    ots = ob.get("timestamp")
                    if ots is not None:
                        try:
                            rec["ob_ts_ms"] = int(ots)
                        except Exception:
                            pass

            rec.setdefault("record_type", "orderbook")
            rec.setdefault("schema_version", settings.SCHEMA_VERSION_ORDERBOOK)

            books_writer.write(rec)

        # -------------------------------
        # 5) Periodic poll stats logging
        # -------------------------------
        stats_writer = vs.get("stats_writer")
        every = getattr(settings, "POLL_STATS_EVERY_SECONDS", 10)
        last = vs.get("stats_last_mono", 0.0)

        if stats_writer is not None and (now_mono - last) >= every:
            # compute p50 / p95 from recent buffer slice
            lat_list = list(lat_buf)[-min(len(lat_buf), 500):]  # last 500 samples
            lat_list.sort()
            def _pct(p: float) -> int | None:
                if not lat_list:
                    return None
                idx = int(p * (len(lat_list) - 1))
                return lat_list[idx]

            stats_writer.write({
                "ts_utc": datetime.utcnow().isoformat(),
                "ts_ms": int(datetime.now(tz=timezone.utc).timestamp() * 1000),
                "venue": v.name,
                "active_count": len(active),
                "submitted": submitted,
                "successes": loop_successes,
                "failures": loop_failures,
                "http_429": http_429,
                "http_4xx": http_4xx,
                "http_5xx": http_5xx,
                "timeouts": timeouts,
                "other_errs": other_errs,
                "lat_p50_ms": _pct(0.50),
                "lat_p95_ms": _pct(0.95),
                "cooldown_remaining_s": max(0.0, vs["cooldown_until"] - now_mono),
                "max_inflight": max_inflight,
                "max_workers": vs.get("max_workers"),
            })
            vs["stats_last_mono"] = now_mono

        return (loop_successes, loop_failures)

    def _maybe_apply_cooldown(self, vs: dict, successes: int, failures: int, now_mono: float) -> None:
        """
        If many instruments are failing, cool down this venue only (non-blocking).
        """
        active = vs["active"]
        v = vs["venue"]

        if failures >= max(3, len(active) // 2):
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
                        f"<PollApp>: venue={vs['venue'].name} "
                        f"saved={successes} failed={failures} total={successes + failures}"
                    )

                    self._maybe_apply_cooldown(vs, successes=successes, failures=failures, now_mono=now_mono)

                time.sleep(settings.POLL_INTERVAL)

        except KeyboardInterrupt:
            print("<PollApp>: shutdown requested (KeyboardInterrupt)")
        finally:
            # Best-effort cleanup
            for vname, vs in venue_state.items():
                try:
                    ex = vs.get("executor")
                    if ex is not None:
                        ex.shutdown(wait=False)
                except Exception:
                    pass

                try:
                    bw = vs.get("books_writer")
                    if bw is not None:
                        bw.close()
                except Exception:
                    pass
