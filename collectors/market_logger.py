"""
collectors/market_logger.py

Order book polling service.

Consumes per-venue discovery snapshots (active_instruments.snapshot.json),
polls order books for the active set, and writes JSONL logs with rotation.

This module does NOT perform discovery and does NOT write market-metadata logs.
Discovery should run in a separate process and write the snapshot files.

2026-01 refactor notes:
- Keep architecture/semantics identical
- Make the code readable and future-proof:
  - replace "vs dict junk drawer" with typed dataclasses
  - split _poll_once into small helpers
  - add proper rollover for *all* writers (books + stats + errors)
  - add proper shutdown for executors
  - keep concurrency boundary strict: only network fetch is parallel

2026-01 adaptive notes (optional, per-venue):
- Add AIMD inflight controller (additive increase / multiplicative decrease)
- Goal: maximum sustainable throughput with near-zero 429s
- Independent per venue (Polymarket and Limitless adapt separately)
- Only affects how many requests are in-flight per loop; does not change discovery.
"""

from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed, Future

from config.settings import settings
from collectors.venue_runtime import VenueRuntime
from storage.jsonl_writer import JsonlRotatingWriter


# -------------------------
# Small helpers (printing)
# -------------------------
def _print_instrument_list(prefix: str, instruments: dict[str, dict], keys: set[str]) -> None:
    if not keys:
        return
    slugs = [instruments[k].get("slug", "") for k in keys if k in instruments]
    max_slug = max((len(s) for s in slugs), default=0)

    for k in sorted(keys):
        inst = instruments.get(k)
        if not inst:
            continue
        slug = inst.get("slug", "")
        title = inst.get("question") or inst.get("title") or ""
        print(f"  {prefix} slug={slug:<{max_slug}} | {title}")


# -------------------------
# Typed state containers
# -------------------------
@dataclass(frozen=True)
class VenueLimits:
    """Per-venue concurrency limits."""
    max_workers: int
    max_inflight: int


@dataclass
class WorkItem:
    """A single polling unit of work derived from the active snapshot."""
    ikey: str
    poll_key: str
    info: dict
    st: dict  # per-instrument failure state (count/next_ok/last_log)


@dataclass
class PollCounters:
    """Aggregated telemetry for one _poll_once loop."""
    submitted: int = 0
    successes: int = 0
    failures: int = 0

    http_429: int = 0
    http_4xx: int = 0
    http_5xx: int = 0
    timeouts: int = 0
    other_errs: int = 0


@dataclass
class AimdState:
    """Per-venue AIMD inflight control state (poller-owned)."""
    inflight: int
    ceiling: int

    stable_since_mono: float = 0.0
    last_adjust_mono: float = 0.0


@dataclass
class VenueState:
    """All poller-owned runtime state for a venue."""
    venue: VenueRuntime
    current_date: str

    # writers
    books_writer: JsonlRotatingWriter
    stats_writer: Optional[JsonlRotatingWriter] = None
    errors_writer: Optional[JsonlRotatingWriter] = None

    # discovery snapshot tracking
    snapshot_path: Path = Path()
    snapshot_mtime: float = 0.0
    snapshot_asof: Optional[str] = None

    # active instruments (poller in-memory view)
    active: dict[str, dict] = field(default_factory=dict)

    # failure/backoff + cooldown (monotonic time)
    fail_state: dict[str, dict] = field(default_factory=dict)
    cooldown_until: float = 0.0

    # concurrency
    executor: Optional[ThreadPoolExecutor] = None
    limits: VenueLimits = field(default_factory=lambda: VenueLimits(max_workers=8, max_inflight=8))

    # telemetry rolling window
    lat_ms_buf: deque[int] = field(default_factory=lambda: deque(maxlen=5000))
    stats_last_mono: float = 0.0

    # adaptive inflight control (optional)
    aimd: Optional[AimdState] = None


# -------------------------
# Error classification helpers
# -------------------------
_STATUS_RE = re.compile(r"\[(\d{3})\]")


def _extract_status_code(exc: Exception) -> Optional[int]:
    """Best-effort status extractor across httpx/requests/wrapped errors."""
    resp = getattr(exc, "response", None)
    if resp is not None:
        sc = getattr(resp, "status_code", None)
        if isinstance(sc, int):
            return sc

    m = _STATUS_RE.search(str(exc))
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None

    return None


def _is_timeout(exc: Exception) -> bool:
    """Conservative timeout detection without importing httpx/requests types."""
    name = type(exc).__name__.lower()
    if "timeout" in name:
        return True
    msg = str(exc).lower()
    return ("timed out" in msg) or ("timeout" in msg)


def _pct_from_sorted(values: list[int], p: float) -> Optional[int]:
    """Return percentile from a sorted list."""
    if not values:
        return None
    idx = int(p * (len(values) - 1))
    return values[idx]


def _p95_from_deque(dq: deque[int]) -> Optional[int]:
    """Compute p95 from the last ~500 latency samples in a deque."""
    if not dq:
        return None
    xs = list(dq)[-min(len(dq), 500):]
    xs.sort()
    return xs[int(0.95 * (len(xs) - 1))]


# -------------------------
# MarketLogger
# -------------------------
class MarketLogger:
    """Multi-venue order book poller."""

    def __init__(self, venues: list[VenueRuntime]):
        self.venues = venues
        for v in self.venues:
            v.out_dir.mkdir(parents=True, exist_ok=True)

    # -------------------------
    # Venue init & lifecycle
    # -------------------------
    def _venue_limits(self, venue_name: str) -> VenueLimits:
        """Get per-venue concurrency limits from settings with safe defaults."""
        if venue_name == "polymarket":
            w = getattr(settings, "POLL_MAX_WORKERS_POLY", getattr(settings, "POLL_MAX_WORKERS", 32))
            i = getattr(settings, "POLL_MAX_INFLIGHT_POLY", getattr(settings, "POLL_MAX_INFLIGHT", w))
        elif venue_name == "limitless":
            w = getattr(settings, "POLL_MAX_WORKERS_LIMITLESS", getattr(settings, "POLL_MAX_WORKERS", 8))
            i = getattr(settings, "POLL_MAX_INFLIGHT_LIMITLESS", getattr(settings, "POLL_MAX_INFLIGHT", min(2, w)))
        else:
            w = getattr(settings, "POLL_MAX_WORKERS_DEFAULT", getattr(settings, "POLL_MAX_WORKERS", 8))
            i = getattr(settings, "POLL_MAX_INFLIGHT_DEFAULT", getattr(settings, "POLL_MAX_INFLIGHT", w))

        w = max(1, int(w))
        i = max(1, min(int(i), w))
        return VenueLimits(max_workers=w, max_inflight=i)

    def _init_aimd(self, venue_name: str, limits: VenueLimits) -> Optional[AimdState]:
        """Create per-venue AIMD state from settings. Returns None if disabled."""
        enabled = bool(getattr(settings, "AIMD_ENABLED", False))
        if not enabled:
            return None

        # ceilings (cap by workers to avoid queued bursts)
        if venue_name == "polymarket":
            ceiling = int(getattr(settings, "AIMD_INFLIGHT_CEILING_POLY", limits.max_inflight))
        elif venue_name == "limitless":
            ceiling = int(getattr(settings, "AIMD_INFLIGHT_CEILING_LIMITLESS", limits.max_inflight))
        else:
            ceiling = int(getattr(settings, "AIMD_INFLIGHT_CEILING_DEFAULT", limits.max_inflight))

        ceiling = max(1, min(ceiling, limits.max_workers))

        start = getattr(settings, "AIMD_START_INFLIGHT", None)
        if start is None:
            start = limits.max_inflight

        inflight = max(1, min(int(start), ceiling))
        return AimdState(inflight=inflight, ceiling=ceiling)

    def _open_writers(self, v: VenueRuntime, date_str: str) -> tuple[JsonlRotatingWriter, JsonlRotatingWriter, JsonlRotatingWriter]:
        """Open all writers for a venue for a given UTC date."""
        books_writer = JsonlRotatingWriter(
            v.out_dir / "orderbooks" / f"date={date_str}",
            "orderbooks",
            settings.ROTATE_MINUTES,
            settings.FSYNC_SECONDS,
        )
        stats_writer = JsonlRotatingWriter(
            v.out_dir / "poll_stats" / f"date={date_str}",
            "poll_stats",
            settings.ROTATE_MINUTES,
            settings.FSYNC_SECONDS,
        )
        errors_writer = JsonlRotatingWriter(
            v.out_dir / "poll_errors" / f"date={date_str}",
            "poll_errors",
            settings.ROTATE_MINUTES,
            settings.FSYNC_SECONDS,
        )
        return books_writer, stats_writer, errors_writer

    def _init_venue_state(self) -> dict[str, VenueState]:
        """One-time setup: writers, snapshot tracking, executor, and limits."""
        venue_state: dict[str, VenueState] = {}

        for v in self.venues:
            date_str = datetime.utcnow().strftime("%Y-%m-%d")
            books_writer, stats_writer, errors_writer = self._open_writers(v, date_str)

            snap_path = v.out_dir / "state" / "active_instruments.snapshot.json"

            limits = self._venue_limits(v.name)
            executor = ThreadPoolExecutor(max_workers=limits.max_workers)

            vs = VenueState(
                venue=v,
                current_date=date_str,
                books_writer=books_writer,
                stats_writer=stats_writer,
                errors_writer=errors_writer,
                snapshot_path=snap_path,
                executor=executor,
                limits=limits,
                aimd=self._init_aimd(v.name, limits),
            )

            venue_state[v.name] = vs

            if vs.aimd is None:
                print(f"<PollApp>: venue={v.name} concurrency workers={limits.max_workers} inflight={limits.max_inflight}")
            else:
                print(
                    f"<PollApp>: venue={v.name} concurrency workers={limits.max_workers} "
                    f"inflight={limits.max_inflight} aimd_inflight={vs.aimd.inflight} aimd_ceiling={vs.aimd.ceiling}"
                )

        return venue_state

    def _close_venue_state(self, vs: VenueState) -> None:
        """Best-effort cleanup of writers and executor."""
        for w in (vs.books_writer, vs.stats_writer, vs.errors_writer):
            if w is None:
                continue
            try:
                w.close()
            except Exception:
                pass

        if vs.executor is not None:
            try:
                vs.executor.shutdown(wait=True, cancel_futures=True)
            except TypeError:
                vs.executor.shutdown(wait=True)
            except Exception:
                pass

    def _rollover_if_needed(self, vs: VenueState) -> None:
        """Midnight UTC rollover: close all writers and open new date writers."""
        v = vs.venue
        old_date = vs.current_date
        new_date = datetime.utcnow().strftime("%Y-%m-%d")
        if new_date == old_date:
            return

        try:
            for w in (vs.books_writer, vs.stats_writer, vs.errors_writer):
                if w is not None:
                    w.close()
        finally:
            books_writer, stats_writer, errors_writer = self._open_writers(v, new_date)
            vs.current_date = new_date
            vs.books_writer = books_writer
            vs.stats_writer = stats_writer
            vs.errors_writer = errors_writer
            print(f"<PollApp>: rollover venue={v.name} {old_date} -> {new_date}")

    # -------------------------
    # Snapshot reload (sticky semantics)
    # -------------------------
    def _maybe_reload_snapshot(self, vs: VenueState) -> None:
        """Reload snapshot if changed; keep instruments sticky until expiration."""
        snap_path = vs.snapshot_path

        try:
            if not snap_path.exists():
                return

            st = os.stat(snap_path)
            mtime = st.st_mtime
            if mtime <= vs.snapshot_mtime:
                return

            payload = json.loads(snap_path.read_text(encoding="utf-8"))
            instruments = payload.get("instruments")
            if not isinstance(instruments, dict):
                print(f"<PollApp|Warning>: snapshot malformed venue={vs.venue.name}: no instruments dict")
                return

            old_active = dict(vs.active)
            old_keys = set(old_active.keys())

            now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)

            def _parse_exp_ms(inst: dict) -> Optional[int]:
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

            merged: dict[str, dict] = dict(instruments)

            for ikey, inst in old_active.items():
                if ikey in merged:
                    continue
                if _not_expired(inst):
                    merged[ikey] = inst

            for ikey in list(merged.keys()):
                inst = merged.get(ikey) or {}
                exp = _parse_exp_ms(inst)
                if exp is not None and exp <= now_ms:
                    del merged[ikey]

            new_keys = set(merged.keys())

            vs.active.clear()
            vs.active.update(merged)

            for k in list(vs.fail_state.keys()):
                if k not in vs.active:
                    del vs.fail_state[k]

            vs.snapshot_mtime = mtime
            vs.snapshot_asof = payload.get("asof_ts_utc")

            added_keys = new_keys - old_keys
            removed_keys = old_keys - new_keys

            print(
                f"<PollApp>: loaded snapshot venue={vs.venue.name} "
                f"count={len(vs.active)} added={len(added_keys)} removed={len(removed_keys)} "
                f"asof={vs.snapshot_asof}"
            )

            if added_keys:
                print(f"<PollApp>: added instruments venue={vs.venue.name}")
                _print_instrument_list("+", merged, added_keys)

            if removed_keys:
                print(f"<PollApp>: removed instruments venue={vs.venue.name}")
                _print_instrument_list("-", old_active, removed_keys)

        except Exception as exc:
            print(
                f"<PollApp|Warning>: failed to reload snapshot venue={vs.venue.name}: "
                f"{type(exc).__name__}: {exc}"
            )

    # -------------------------
    # Cooldown policy
    # -------------------------
    def _maybe_apply_cooldown(self, vs: VenueState, successes: int, failures: int, now_mono: float) -> None:
        """If many instruments are failing, cool down this venue only (non-blocking)."""
        if failures >= max(3, len(vs.active) // 2):
            cooldown = 10
            vs.cooldown_until = now_mono + cooldown
            print(
                f"[WARN] high failure rate this loop for venue={vs.venue.name} "
                f"(failures={failures}, successes={successes}). Cooling down {cooldown}s."
            )

    def _cooldown_on_429(self, vs: VenueState, now_mono: float) -> None:
        """Immediate cooldown on 429 to avoid hammering into bans."""
        cd = float(getattr(settings, "RATE_LIMIT_COOLDOWN_SECONDS", 30))
        vs.cooldown_until = max(vs.cooldown_until, now_mono + cd)

    # -------------------------
    # Inflight control (AIMD-aware)
    # -------------------------
    def _current_inflight_limit(self, vs: VenueState) -> int:
        """Return current inflight cap for this venue (AIMD if enabled)."""
        if vs.aimd is None:
            return vs.limits.max_inflight
        # Never exceed workers; never exceed ceiling; never exceed configured max_inflight (safety)
        return max(1, min(vs.aimd.inflight, vs.aimd.ceiling, vs.limits.max_workers, vs.limits.max_inflight))

    def _aimd_params(self, venue_name: str) -> dict[str, float | int]:
        """Per-venue AIMD thresholds (kept simple; pull from settings)."""
        if venue_name == "polymarket":
            return {
                "stable_s": float(getattr(settings, "AIMD_STABLE_SECONDS_POLY", 300)),
                "adjust_min_s": float(getattr(settings, "AIMD_ADJUST_MIN_SECONDS_POLY", 60)),
                "p95_hi": int(getattr(settings, "AIMD_LAT_P95_HIGH_MS_POLY", 1500)),
                "p95_lo": int(getattr(settings, "AIMD_LAT_P95_LOW_MS_POLY", 800)),
                "fail_hi": float(getattr(settings, "AIMD_FAIL_RATE_HIGH_POLY", 0.25)),
            }
        if venue_name == "limitless":
            return {
                "stable_s": float(getattr(settings, "AIMD_STABLE_SECONDS_LIMITLESS", 600)),
                "adjust_min_s": float(getattr(settings, "AIMD_ADJUST_MIN_SECONDS_LIMITLESS", 120)),
                "p95_hi": int(getattr(settings, "AIMD_LAT_P95_HIGH_MS_LIMITLESS", 2000)),
                "p95_lo": int(getattr(settings, "AIMD_LAT_P95_LOW_MS_LIMITLESS", 1000)),
                "fail_hi": float(getattr(settings, "AIMD_FAIL_RATE_HIGH_LIMITLESS", 0.20)),
            }
        return {
            "stable_s": float(getattr(settings, "AIMD_STABLE_SECONDS_DEFAULT", 600)),
            "adjust_min_s": float(getattr(settings, "AIMD_ADJUST_MIN_SECONDS_DEFAULT", 120)),
            "p95_hi": int(getattr(settings, "AIMD_LAT_P95_HIGH_MS_DEFAULT", 2000)),
            "p95_lo": int(getattr(settings, "AIMD_LAT_P95_LOW_MS_DEFAULT", 1000)),
            "fail_hi": float(getattr(settings, "AIMD_FAIL_RATE_HIGH_DEFAULT", 0.25)),
        }

    def _maybe_adjust_aimd(self, vs: VenueState, counters: PollCounters, now_mono: float) -> None:
        """Adjust AIMD inflight cap based on 429s, failure rate, and latency."""
        if vs.aimd is None:
            return

        # Nothing to learn from empty loops
        submitted = max(1, counters.submitted)
        fail_rate = counters.failures / submitted
        p95 = _p95_from_deque(vs.lat_ms_buf)

        params = self._aimd_params(vs.venue.name)

        if vs.aimd.stable_since_mono <= 0.0:
            vs.aimd.stable_since_mono = now_mono
            vs.aimd.last_adjust_mono = now_mono
            return

        # Multiplicative decrease on any 429
        if counters.http_429 > 0:
            old = vs.aimd.inflight
            vs.aimd.inflight = max(1, vs.aimd.inflight // 2)
            vs.aimd.inflight = min(vs.aimd.inflight, vs.aimd.ceiling, vs.limits.max_workers, vs.limits.max_inflight)
            vs.aimd.stable_since_mono = now_mono
            vs.aimd.last_adjust_mono = now_mono
            print(f"<AIMD>: venue={vs.venue.name} 429_seen old_inflight={old} new_inflight={vs.aimd.inflight}")
            return

        # Gentle decrease on stress (high fail or high latency)
        stressed = (fail_rate >= params["fail_hi"]) or (p95 is not None and p95 >= params["p95_hi"])
        if stressed:
            old = vs.aimd.inflight
            vs.aimd.inflight = max(1, vs.aimd.inflight - 1)
            vs.aimd.inflight = min(vs.aimd.inflight, vs.aimd.ceiling, vs.limits.max_workers, vs.limits.max_inflight)
            vs.aimd.stable_since_mono = now_mono
            vs.aimd.last_adjust_mono = now_mono
            reason = "fail_rate" if fail_rate >= params["fail_hi"] else "p95_latency"
            print(f"<AIMD>: venue={vs.venue.name} decrease reason={reason} old_inflight={old} new_inflight={vs.aimd.inflight} fail_rate={fail_rate:.2f} p95={p95}")
            return

        # Additive increase only after long stable window + minimum adjust interval
        if (now_mono - vs.aimd.last_adjust_mono) < params["adjust_min_s"]:
            return

        stable_for = now_mono - vs.aimd.stable_since_mono
        clean_latency = (p95 is None) or (p95 <= params["p95_lo"])
        clean_fail = fail_rate < (float(params["fail_hi"]) / 2.0)

        if stable_for >= params["stable_s"] and clean_latency and clean_fail:
            old = vs.aimd.inflight
            vs.aimd.inflight = min(vs.aimd.ceiling, vs.aimd.inflight + 1, vs.limits.max_workers, vs.limits.max_inflight)
            vs.aimd.last_adjust_mono = now_mono
            if vs.aimd.inflight != old:
                print(f"<AIMD>: venue={vs.venue.name} increase old_inflight={old} new_inflight={vs.aimd.inflight} stable_for={stable_for:.0f}s p95={p95} fail_rate={fail_rate:.2f}")

    # -------------------------
    # Polling helpers
    # -------------------------
    def _select_eligible(self, vs: VenueState, now_mono: float) -> list[WorkItem]:
        """Select instruments eligible to poll (honors per-instrument next_ok backoff)."""
        eligible: list[WorkItem] = []

        for ikey, info in vs.active.items():
            st = vs.fail_state.get(ikey, {"count": 0, "next_ok": 0.0, "last_log": 0.0})
            if now_mono < st["next_ok"]:
                continue

            poll_key = info.get("poll_key")
            if poll_key is None:
                continue

            eligible.append(WorkItem(ikey=ikey, poll_key=str(poll_key), info=info, st=st))

        return eligible[: self._current_inflight_limit(vs)]

    def _worker_fetch(self, client: Any, poll_key: str) -> tuple[bool, Any, int, Optional[int]]:
        """Worker: returns (ok, payload_or_exc, latency_ms, status_code)."""
        t0 = time.perf_counter()
        try:
            ob = client.get_orderbook(poll_key)
            ms = int((time.perf_counter() - t0) * 1000)
            return (True, ob, ms, None)
        except Exception as exc:
            ms = int((time.perf_counter() - t0) * 1000)
            sc = _extract_status_code(exc)
            return (False, exc, ms, sc)

    def _submit_fetches(self, vs: VenueState, eligible: list[WorkItem], counters: PollCounters) -> dict[Future, WorkItem]:
        """Submit network fetch jobs to the per-venue executor."""
        if vs.executor is None:
            return {}

        futures: dict[Future, WorkItem] = {}
        client = vs.venue.client

        for w in eligible:
            counters.submitted += 1
            fut = vs.executor.submit(self._worker_fetch, client, w.poll_key)
            futures[fut] = w

        return futures

    def _classify_failure(self, exc: Exception, status_code: Optional[int], counters: PollCounters) -> None:
        """Increment appropriate counters for a failure."""
        if status_code == 429:
            counters.http_429 += 1
        elif isinstance(status_code, int) and 400 <= status_code <= 499:
            counters.http_4xx += 1
        elif isinstance(status_code, int) and 500 <= status_code <= 599:
            counters.http_5xx += 1
        elif _is_timeout(exc):
            counters.timeouts += 1
        else:
            counters.other_errs += 1

    def _apply_backoff(self, st: dict, now_mono: float) -> int:
        """Apply exponential backoff with a 60s cap. Returns backoff seconds."""
        backoff = min(60, 2 ** min(int(st["count"]), 6))
        st["next_ok"] = now_mono + backoff
        return int(backoff)

    def _maybe_log_failure(
        self,
        vs: VenueState,
        w: WorkItem,
        exc: Exception,
        status_code: Optional[int],
        lat_ms: int,
        backoff: int,
        now_mono: float,
    ) -> None:
        """Sparse console logs + optional sampled JSONL error stream."""
        vname = vs.venue.name
        slug = w.info.get("slug")
        mid = w.info.get("market_id")

        last_log = float(w.st.get("last_log", 0.0))
        if w.st["count"] in (1, 3, 5) or (now_mono - last_log > 60):
            print(
                f"[WARN] get_orderbook failed "
                f"venue={vname} ikey={w.ikey} mid={mid} slug={slug} "
                f"count={w.st['count']} backoff={backoff}s status={status_code} latency_ms={lat_ms} "
                f"err={type(exc).__name__}: {exc}"
            )
            w.st["last_log"] = now_mono

        sample_every = int(getattr(settings, "POLL_ERROR_SAMPLE_EVERY", 0) or 0)
        if vs.errors_writer is not None and sample_every > 0 and (w.st["count"] % sample_every == 0):
            vs.errors_writer.write({
                "ts_utc": datetime.utcnow().isoformat(),
                "ts_ms": int(datetime.now(tz=timezone.utc).timestamp() * 1000),
                "venue": vname,
                "market_id": mid,
                "slug": slug,
                "instrument_key": w.ikey,
                "poll_key": w.poll_key,
                "status": status_code,
                "latency_ms": lat_ms,
                "error_type": type(exc).__name__,
                "error": str(exc)[:500],
            })

    def _build_record(self, vs: VenueState, w: WorkItem, raw_ob: dict) -> dict:
        """Build record and enforce join-safe invariants at the write boundary."""
        v = vs.venue
        slug = w.info.get("slug")
        mid = w.info.get("market_id")

        snap = {
            "timestamp": datetime.utcnow().isoformat(),
            "snapshot_asof": vs.snapshot_asof,

            "market_id": mid,
            "slug": slug,
            "underlying": w.info.get("underlying"),
            "orderbook": raw_ob,

            "instrument_key": w.ikey,
            "instrument_id": w.info.get("instrument_id"),
            "venue": v.name,
            "poll_key": w.poll_key,
        }

        rec = v.normalizer(snap, full_orderbook=settings.FULL_ORDERBOOK) or snap

        rec.setdefault("venue", v.name)

        pk = rec.get("poll_key") or w.poll_key or slug
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
        return rec

    def _write_stats_if_due(self, vs: VenueState, counters: PollCounters, now_mono: float) -> Optional[int]:
        """Write periodic per-venue polling stats to JSONL. Returns p95 latency if written."""
        if vs.stats_writer is None:
            return None

        every = int(getattr(settings, "POLL_STATS_EVERY_SECONDS", 10) or 10)
        if every <= 0:
            return None

        if (now_mono - vs.stats_last_mono) < every:
            return None

        lat_list = list(vs.lat_ms_buf)[-min(len(vs.lat_ms_buf), 500):]
        lat_list.sort()

        p50 = _pct_from_sorted(lat_list, 0.50)
        p95 = _pct_from_sorted(lat_list, 0.95)

        vs.stats_writer.write({
            "ts_utc": datetime.utcnow().isoformat(),
            "ts_ms": int(datetime.now(tz=timezone.utc).timestamp() * 1000),
            "venue": vs.venue.name,
            "active_count": len(vs.active),

            "submitted": counters.submitted,
            "successes": counters.successes,
            "failures": counters.failures,

            "http_429": counters.http_429,
            "http_4xx": counters.http_4xx,
            "http_5xx": counters.http_5xx,
            "timeouts": counters.timeouts,
            "other_errs": counters.other_errs,

            "lat_p50_ms": p50,
            "lat_p95_ms": p95,

            "cooldown_remaining_s": max(0.0, vs.cooldown_until - now_mono),
            "max_inflight": self._current_inflight_limit(vs),
            "max_workers": vs.limits.max_workers,

            "aimd_enabled": bool(vs.aimd is not None),
            "aimd_inflight": (vs.aimd.inflight if vs.aimd else None),
            "aimd_ceiling": (vs.aimd.ceiling if vs.aimd else None),
        })

        vs.stats_last_mono = now_mono
        return p95

    # -------------------------
    # The poller loop (refactored, same semantics)
    # -------------------------
    def _poll_once(self, vs: VenueState, now_mono: float) -> tuple[int, int]:
        """Poll all active instruments once for one venue, in parallel (network only)."""
        if now_mono < vs.cooldown_until:
            return (0, 0)

        counters = PollCounters()

        eligible = self._select_eligible(vs, now_mono=now_mono)
        futures = self._submit_fetches(vs, eligible, counters=counters)

        for fut in as_completed(futures):
            w = futures[fut]
            ok, payload, lat_ms, status_code = fut.result()
            vs.lat_ms_buf.append(lat_ms)

            if ok:
                raw_ob = payload

                vs.fail_state[w.ikey] = {"count": 0, "next_ok": 0.0, "last_log": 0.0}
                counters.successes += 1

                rec = self._build_record(vs, w, raw_ob)
                vs.books_writer.write(rec)

            else:
                exc: Exception = payload
                counters.failures += 1

                w.st["count"] = int(w.st.get("count", 0)) + 1
                self._classify_failure(exc, status_code, counters)

                if status_code == 429:
                    self._cooldown_on_429(vs, now_mono)

                backoff = self._apply_backoff(w.st, now_mono)
                self._maybe_log_failure(vs, w, exc, status_code, lat_ms, backoff, now_mono)
                vs.fail_state[w.ikey] = w.st

        p95 = self._write_stats_if_due(vs, counters, now_mono=now_mono)
        self._maybe_adjust_aimd(vs, counters, now_mono=now_mono)

        return (counters.successes, counters.failures)

    # -------------------------
    # Main loop (orchestrator)
    # -------------------------
    def run(self) -> None:
        venue_state = self._init_venue_state()

        try:
            while True:
                now_mono = time.monotonic()

                for vname in sorted(venue_state.keys()):
                    vs = venue_state[vname]

                    self._rollover_if_needed(vs)
                    self._maybe_reload_snapshot(vs)

                    successes, failures = self._poll_once(vs, now_mono=now_mono)
                    print(
                        f"<PollApp>: venue={vs.venue.name} "
                        f"saved={successes} failed={failures} total={successes + failures} "
                        f"inflight={self._current_inflight_limit(vs)}"
                    )

                    self._maybe_apply_cooldown(vs, successes=successes, failures=failures, now_mono=now_mono)

                time.sleep(settings.POLL_INTERVAL)

        except KeyboardInterrupt:
            print("<PollApp>: shutdown requested (KeyboardInterrupt)")
        finally:
            for vs in venue_state.values():
                self._close_venue_state(vs)
