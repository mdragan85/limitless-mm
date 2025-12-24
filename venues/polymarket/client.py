import json
from datetime import datetime, timezone
import httpx


GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"


class PolymarketClient:
    venue = "polymarket"

    def __init__(self, timeout: float = 10.0):
        self.http = httpx.Client(timeout=timeout)

    # ---------- Low-level API ----------
    def public_search(self, query: str) -> dict:
        resp = self.http.get(
            f"{GAMMA_BASE}/public-search",
            params={
                "q": query,
                "limit_per_type": 50,
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

    def discover_instruments(self, rules: list[dict]) -> list[dict]:
        """
        Polymarket discovery v1.

        Returns a list of instrument dicts (one per CLOB token), e.g.
        {
          "venue": "polymarket",
          "market_id": "...",
          "instrument_id": "<token_id>",
          "poll_key": "<token_id>",                 # simplest form for now
          "slug": "...",
          "question": "...",
          "expiration": <end_ms>,
          "minutes_to_expiry": <float>,
          "outcome": "Up"/"Down"/"Yes"/"No"/...,
          "outcome_price": "0.495" (optional),
          "rule": "<rule_name>",
          "raw_market": {...}                       # optional but handy for debugging
        }
        """
        # 1) Search -> collect slugs
        slugs: set[str] = set()
        for rule in rules:
            for q in rule.get("queries", []):
                blob = self.public_search(q)
                self._collect_market_slugs(blob, slugs)

        out: list[dict] = []

        # 2) Hydrate -> filter -> emit instruments
        for slug in sorted(slugs):
            d = self.get_market_by_slug(slug)
            if not d:
                continue

            # Hard filters from hydrated market (source-of-truth)
            if not d.get("active", False):
                continue
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

            title = f"{d.get('question') or d.get('title') or ''} {slug}".lower()

            # Parse token ids and outcomes (both are JSON strings in your example)
            token_ids = self._parse_json_list_field(d.get("clobTokenIds"))
            outcomes = self._parse_json_list_field(d.get("outcomes"))
            outcome_prices = self._parse_json_list_field(d.get("outcomePrices"))

            # Need at least token ids
            if len(token_ids) < 2:
                continue

            # If outcomes missing, fabricate stable labels
            if not outcomes or len(outcomes) != len(token_ids):
                outcomes = [f"OUTCOME_{i}" for i in range(len(token_ids))]

            # outcomePrices may be missing or length mismatch; handle gracefully
            if not outcome_prices or len(outcome_prices) != len(token_ids):
                outcome_prices = [None for _ in range(len(token_ids))]

            # Apply rule-based filters (expiry window + contains rules)
            for rule in rules:
                min_m = rule.get("min_minutes_to_expiry", float("-inf"))
                max_m = rule.get("max_minutes_to_expiry", float("inf"))

                if minutes < min_m or minutes > max_m:
                    continue

                must_contain = rule.get("must_contain", []) or []
                must_not = rule.get("must_not_contain", []) or []

                if must_contain:
                    if not any(k.lower() in title for k in must_contain):
                        continue
                if must_not:
                    if any(k.lower() in title for k in must_not):
                        continue

                # Emit 1 instrument per token/outcome
                market_id = str(d.get("id"))
                question = d.get("question") or d.get("title") or ""
                for i, token_id in enumerate(token_ids):
                    out.append({
                        "venue": self.venue,
                        "market_id": market_id,
                        "instrument_id": str(token_id),      # token_id is the identity
                        "poll_key": str(token_id),           # simplest for now
                        "slug": slug,
                        "question": question,
                        "expiration": end_ms,
                        "minutes_to_expiry": minutes,
                        "outcome": outcomes[i],
                        "outcome_price": outcome_prices[i],
                        "rule": rule.get("name"),
                        "raw_market": d,                     # keep for debugging; can drop later
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
