import json
from datetime import datetime, timezone
import httpx


GAMMA_BASE = "https://gamma-api.polymarket.com"


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
    def discover_markets(self, rules: list[dict]) -> list[dict]:
        """
        Returns a list of market dicts with:
        market_id, question, end_ms, raw, token_yes, token_no, rule_name
        """
        market_slugs = set()

        for rule in rules:
            for q in rule["queries"]:
                blob = self.public_search(q)
                self._collect_market_slugs(blob, market_slugs)

        for slug in market_slugs:
            d = self.get_market_by_slug(slug)

        out = []

        # 2) Fetch + filter
        for mid in market_ids:
            d = self.get_market_details(mid)
            if not d or not d.get("active") or d.get("closed"):
                continue

            end_ms = self._parse_end_ms(d)
            if end_ms is None:
                continue

            minutes = self._minutes_to_expiry(end_ms)

            for rule in rules:
                if minutes < rule["min_minutes_to_expiry"]:
                    continue
                if minutes > rule["max_minutes_to_expiry"]:
                    continue

                title = (d.get("question") or d.get("title") or "").lower()
                if rule["must_contain"]:
                    if not any(k.lower() in title for k in rule["must_contain"]):
                        continue
                if rule["must_not_contain"]:
                    if any(k.lower() in title for k in rule["must_not_contain"]):
                        continue

                # parse CLOB tokens
                try:
                    toks = json.loads(d.get("clobTokenIds", "[]"))
                    token_yes, token_no = toks[0], toks[1]
                except Exception:
                    continue

                out.append({
                    "market_id": str(d["id"]),
                    "question": d.get("question") or d.get("title"),
                    "end_ms": end_ms,
                    "token_yes": token_yes,
                    "token_no": token_no,
                    "raw": d,
                    "rule": rule["name"],
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

    def get_market_details_by_slug(self, slug: str) -> dict | None:
        # ONE of these will be true in your client:
        # 1) /markets?slug=<slug>
        # 2) /markets/<slug>
        # 3) /markets?search=<slug>  (then exact-match)
        # 4) a different “public markets” endpoint
        pass