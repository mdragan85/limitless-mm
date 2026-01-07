import json
from datetime import datetime, timezone
import httpx


PUBIC_SEARCH_LIMIT = 1000
GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"


class PolymarketClient:
    venue = "polymarket"

    def __init__(self, timeout: float = 10.0):
        self.http = httpx.Client(timeout=timeout)

    # ---------- Low-level API ----------
    def _gamma_get(self, path: str, params: dict | None = None):
        resp = self.http.get(f"{GAMMA_BASE}{path}", params=params or {})
        resp.raise_for_status()
        return resp.json()

    def list_markets_paginated(
        self,
        *,
        limit: int = 200,
        offset: int = 0,
        max_pages: int | None = None,
        gamma_params: dict | None = None,
    ):
        """
        Yield markets from Gamma GET /markets using limit/offset pagination.
        """
        params = dict(gamma_params or {})
        params.setdefault("order", "id")
        params.setdefault("ascending", False)

        page = 0
        while True:
            params["limit"] = limit
            params["offset"] = offset

            data = self._gamma_get("/markets", params=params)

            if not data:
                break

            for m in data:
                yield m

            offset += limit
            page += 1
            if max_pages is not None and page >= max_pages:
                break

    def public_search(self, query: str) -> dict:
        resp = self.http.get(
            f"{GAMMA_BASE}/public-search",
            params={
                "q": query,
                "limit_per_type": PUBIC_SEARCH_LIMIT,
                "search_tags": False,
                "search_profiles": False,
                "optimized": True,
            },
        )
        resp.raise_for_status()
        return resp.json()

    def get_market_details(self, market_id: str) -> dict:
        resp = self.http.get(f"{GAMMA_BASE}/markets", params={"id": market_id})
        resp.raise_for_status()
        data = resp.json()
        return data[0] if isinstance(data, list) and data else data

    def get_market_by_slug(self, slug: str) -> dict | None:
        resp = self.http.get(f"{GAMMA_BASE}/markets", params={"slug": slug})
        resp.raise_for_status()
        data = resp.json()
        # endpoint returns a list
        return data[0] if isinstance(data, list) and data else None

    # ---------- Helpers ----------
    @staticmethod
    def _parse_end_ms(details: dict) -> int | None:
        end = details.get("endDate")
        if not end:
            return None
        try:
            dt = datetime.fromisoformat(end.replace("Z", "+00:00"))
            return int(dt.timestamp() * 1000)
        except Exception:
            return None

    @staticmethod
    def _minutes_to_expiry(end_ms: int) -> float:
        now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        return (end_ms - now_ms) / 60000.0

    # ---------- Discovery ----------
    @staticmethod
    def _parse_json_list_field(x):
        """
        Gamma often returns list-ish fields as JSON-encoded strings.
        Normalize to list.
        """
        if x is None:
            return []
        if isinstance(x, list):
            return x
        if isinstance(x, str):
            s = x.strip()
            try:
                v = json.loads(s)
                return v if isinstance(v, list) else []
            except Exception:
                return []
        return []

    def discover_crypto_markets(self, rules: list[dict]) -> list[dict]:
        """
        Enumerate crypto markets via GET /markets pagination (no public-search).

        Emits same instrument schema as discover_instruments (1 per token/outcome).
        """
        # --- tiny local helpers (copy from your discover_instruments to stay consistent) ---
        def _iso_to_ms(s: str) -> int | None:
            try:
                s = s.replace("Z", "+00:00")
                dt = datetime.fromisoformat(s)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return int(dt.timestamp() * 1000)
            except Exception:
                return None

        def _get_start_ms(raw_market: dict, fields: list[str]) -> int | None:
            for k in fields:
                v = raw_market.get(k)
                if not v:
                    continue
                if isinstance(v, (int, float)):
                    vv = int(v)
                    return vv * 1000 if vv < 10_000_000_000 else vv
                if isinstance(v, str):
                    ms = _iso_to_ms(v)
                    if ms is not None:
                        return ms
            return None

        def _first_series(raw_market: dict) -> dict:
            events = raw_market.get("events") or []
            if not events:
                return {}
            series = events[0].get("series") or []
            return series[0] if series else {}

        now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)

        out: list[dict] = []

        # Optional: server-side narrowing to reduce scan volume
        # closed=false is safe for “active snapshot” discovery.
        gamma_params = {"closed": False}

        for d in self.list_markets_paginated(limit=200, gamma_params=gamma_params):
            # ----- hard filters -----
            # hard filters: tradable now
            if not d.get("enableOrderBook", False):
                continue
            if d.get("archived", False):
                continue
            if d.get("closed", False):
                continue
            if d.get("acceptingOrders") is not True:
                continue

            slug = d.get("slug") or ""
            if not slug:
                continue

            series = _first_series(d)
            series_slug = (series.get("slug") or "")
            recurrence = (series.get("recurrence") or "")

            # parse end/start
            end_ms = self._parse_end_ms(d)
            if end_ms is None:
                continue
            minutes = self._minutes_to_expiry(end_ms)

            # match a rule (crypto-style rule shape)
            for rule in rules:
                if rule.get("mode") != "crypto_markets":
                    continue

                prefixes = rule.get("series_slug_prefixes") or []
                allowed = set(rule.get("allowed_recurrence") or [])

                if prefixes and not any(series_slug.startswith(p) for p in prefixes):
                    continue
                if allowed and recurrence not in allowed:
                    continue

                # expiry window
                min_m = rule.get("min_minutes_to_expiry", float("-inf"))
                max_m = rule.get("max_minutes_to_expiry", float("inf"))
                if minutes < min_m or minutes > max_m:
                    continue

                # active window check (optional but you already use it)
                lead_ms = int(rule.get("lead_ms", 60_000))
                start_fields = rule.get("start_time_fields", ["eventStartTime", "startTime"])
                start_ms = _get_start_ms(d, start_fields)
                if start_ms is None:
                    continue
                if not (start_ms - lead_ms <= now_ms < end_ms):
                    continue

                # outcomes/tokens
                token_ids = self._parse_json_list_field(d.get("clobTokenIds"))
                outcomes = self._parse_json_list_field(d.get("outcomes"))
                outcome_prices = self._parse_json_list_field(d.get("outcomePrices"))

                if len(token_ids) < 2:
                    break  # this market isn't usable

                if not outcomes or len(outcomes) != len(token_ids):
                    outcomes = [f"OUTCOME_{i}" for i in range(len(token_ids))]

                if not outcome_prices or len(outcome_prices) != len(token_ids):
                    outcome_prices = [None for _ in range(len(token_ids))]

                question = d.get("question") or d.get("title") or ""
                market_id = str(d.get("id"))

                for i, token_id in enumerate(token_ids):
                    out.append({
                        "venue": self.venue,
                        "market_id": market_id,
                        "instrument_id": str(token_id),
                        "poll_key": str(token_id),
                        "slug": slug,
                        "question": question,
                        "expiration": end_ms,
                        "minutes_to_expiry": minutes,
                        "outcome": outcomes[i],
                        "outcome_price": outcome_prices[i],
                        "rule": rule.get("name"),
                        "raw_market": d,
                    })

                break  # don't let multiple rules double-emit same market unless you want that
        
        debug_print(out)

        return out

    def discover_instruments(self, rules: list[dict], mode: str | None = None) -> list[dict]:
        """
        Dispatcher:
          - mode="crypto_markets": enumerate via /markets
          - mode="search": public-search -> hydrate slugs
        If mode is None, auto-select based on rules.
        """
        if mode is None:
            mode = "crypto_markets" if any(r.get("mode") == "crypto_markets" for r in rules) else "search"

        if mode == "crypto_markets":
            return self.discover_crypto_markets(rules)

        return self._discover_search(rules)

    def _discover_search(self, rules: list[dict]) -> list[dict]:
        """
        Polymarket discovery v2.

        Key fixes vs v1:
        - Preserve slug -> rule association (no more BTC rule claiming XRP slugs)
        - Add active-window filter using explicit start time fields from raw_market
        (eventStartTime / startDateIso / startDate), with configurable lead_ms
        """

        # ---------- helpers (local; keep method self-contained) ----------
        def _iso_to_ms(s: str) -> int | None:
            try:
                s = s.replace("Z", "+00:00")
                dt = datetime.fromisoformat(s)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return int(dt.timestamp() * 1000)
            except Exception:
                return None

        def _get_start_ms(raw_market: dict, fields: list[str]) -> int | None:
            for k in fields:
                v = raw_market.get(k)
                if not v:
                    continue
                if isinstance(v, (int, float)):
                    vv = int(v)
                    # seconds vs ms heuristic
                    return vv * 1000 if vv < 10_000_000_000 else vv
                if isinstance(v, str):
                    ms = _iso_to_ms(v)
                    if ms is not None:
                        return ms
            return None

        now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)

        # -----------------------------------------------------------------
        # 1) Search per rule -> build mapping slug -> list of rule indices
        # -----------------------------------------------------------------
        slug_to_rule_idxs: dict[str, set[int]] = {}

        for ridx, rule in enumerate(rules):
            for q in rule.get("queries", []) or []:
                blob = self.public_search(q)

                found: set[str] = set()
                self._collect_market_slugs(blob, found)
                for slug in found:
                    slug_to_rule_idxs.setdefault(slug, set()).add(ridx)

        out: list[dict] = []

        # -----------------------------------------------------------------
        # 2) Hydrate each unique slug once -> apply only its associated rules
        # -----------------------------------------------------------------
        for slug in sorted(slug_to_rule_idxs.keys()):
            d = self.get_market_by_slug(slug)
            if not d:
                continue

            # Market-level hard filters (source-of-truth)
            if d.get("closed", False):
                continue
            if d.get("archived", False):
                continue
            if not d.get("enableOrderBook", False):
                continue

            end_ms = self._parse_end_ms(d)
            if end_ms is None:
                continue
            minutes = self._minutes_to_expiry(end_ms)

            # Build a stable "title" string for must_contain checks
            title = f"{d.get('question') or d.get('title') or ''} {slug}".lower()

            # Parse token ids and outcomes
            token_ids = self._parse_json_list_field(d.get("clobTokenIds"))
            outcomes = self._parse_json_list_field(d.get("outcomes"))
            outcome_prices = self._parse_json_list_field(d.get("outcomePrices"))

            if len(token_ids) < 2:
                continue

            if not outcomes or len(outcomes) != len(token_ids):
                outcomes = [f"OUTCOME_{i}" for i in range(len(token_ids))]

            if not outcome_prices or len(outcome_prices) != len(token_ids):
                outcome_prices = [None for _ in range(len(token_ids))]

            question = d.get("question") or d.get("title") or ""
            market_id = str(d.get("id"))

            # Apply only rules that actually found this slug
            for ridx in sorted(slug_to_rule_idxs[slug]):
                rule = rules[ridx]

                # Expiry window
                min_m = rule.get("min_minutes_to_expiry", float("-inf"))
                max_m = rule.get("max_minutes_to_expiry", float("inf"))
                if minutes < min_m or minutes > max_m:
                    continue

                # Optional contains filters
                must_contain = rule.get("must_contain", []) or []
                must_not = rule.get("must_not_contain", []) or []
                if must_contain and not any(k.lower() in title for k in must_contain):
                    continue
                if must_not and any(k.lower() in title for k in must_not):
                    continue

                # Active window filter using explicit start time fields
                # (no inference, fully configurable per rule)
                lead_ms = int(rule.get("lead_ms", 60_000))
                start_fields = rule.get("start_time_fields", ["eventStartTime", "startDateIso", "startDate"])
                start_ms = _get_start_ms(d, start_fields)

                if start_ms is None:
                    continue
                if not (start_ms - lead_ms <= now_ms < end_ms):
                    continue

                # Emit 1 instrument per token/outcome for this rule
                for i, token_id in enumerate(token_ids):
                    out.append({
                        "venue": self.venue,
                        "market_id": market_id,
                        "instrument_id": str(token_id),
                        "poll_key": str(token_id),
                        "slug": slug,
                        "question": question,
                        "expiration": end_ms,
                        "minutes_to_expiry": minutes,
                        "outcome": outcomes[i],
                        "outcome_price": outcome_prices[i],
                        "rule": rule.get("name"),
                        "raw_market": d,  # keep for debugging; can drop later
                    })

        return out

    @staticmethod
    def _collect_market_slugs(blob, out: set[str]):
        # public-search returns {"events":[{"markets":[{"slug":...}, ...]}, ...]}
        if not isinstance(blob, dict):
            return
        for e in blob.get("events", []) or []:
            for m in (e.get("markets") or []):
                slug = m.get("slug")
                if slug:
                    out.add(slug)

    def get_orderbook(self, token_id: str) -> dict:
        """
        Fetch CLOB orderbook for a specific token_id (poll_key for Polymarket).
        """
        resp = self.http.get(f"{CLOB_BASE}/book", params={"token_id": token_id})
        resp.raise_for_status()
        return resp.json()



# -------------------------------------------------------------
# DEBUG: dump discovered instruments (market-level view)
# -------------------------------------------------------------
def debug_print(out):

    rows = {}
    for inst in out:
        key = (
            inst.get("venue"),
            inst.get("slug"),
            inst.get("expiration"),
        )
        rows[key] = inst  # collapse YES/NO tokens

    def _fmt_utc(ms):
        if not ms:
            return "None"
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()

    debug_rows = sorted(
        rows.values(),
        key=lambda x: (
            x.get("venue") or "",
            (x.get("question") or x.get("slug") or ""),
            x.get("expiration") or 0,
        ),
    )

    print("\n=== DISCOVERED MARKETS (DEBUG) ===")
    for r in debug_rows:
        print(
            f"{r.get('venue'):10s} | "
            f"slug={r.get('slug'):40s} | "
            f"{(r.get('question') or '')[:70]:70s} | "
            f"expires={_fmt_utc(r.get('expiration'))}"
        )
    print(f"=== TOTAL MARKETS: {len(debug_rows)} ===\n")
# -------------------------------------------------------------
# END: dump discovered instruments (market-level view)
# -------------------------------------------------------------