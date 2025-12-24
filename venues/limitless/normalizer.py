def normalize_orderbook(snapshot: dict, *, full_orderbook: bool):
    """
    Normalize a raw orderbook snapshot into a stable, disk-friendly schema.

    Responsibilities:
    - Extract bids/asks from the raw exchange payload
    - Compute basic derived quantities (best bid/ask, mid, spread)
    - Optionally truncate the orderbook to top-of-book only
    - Coerce numeric fields into consistent Python types

    Non-responsibilities:
    - Validation of exchange data correctness
    - Any pricing, modeling, or strategy logic
    - Persistence or file layout decisions

    Notes:
    - This function assumes `snapshot["orderbook"]` contains the raw exchange
      payload and that price/size fields are present and numeric (or castable).
    - The returned structure is designed to be append-only and stable over time,
      even if the upstream API evolves.
    """
    ob = snapshot["orderbook"]

    # Raw bid/ask ladders as provided by the exchange
    bids = ob.get("bids", [])
    asks = ob.get("asks", [])

    def best_bid():
        """Return the highest-priced bid level, or None if no bids exist."""
        return max(bids, key=lambda x: x["price"], default=None)

    def best_ask():
        """Return the lowest-priced ask level, or None if no asks exist."""
        return min(asks, key=lambda x: x["price"], default=None)

    bb = best_bid()
    ba = best_ask()

    # Basic derived quantities; None if one side of the book is missing
    mid = (bb["price"] + ba["price"]) / 2 if bb and ba else None
    spread = (ba["price"] - bb["price"]) if bb and ba else None

    def strip(levels):
        """
        Convert raw price levels into a minimal, typed representation.

        We intentionally drop all non-essential fields here and coerce:
        - price -> float
        - size  -> int

        This keeps on-disk snapshots compact and schema-stable.
        """
        return [{"price": float(l["price"]), "size": int(l["size"])} for l in levels]

    return {
        # Snapshot metadata
        "ts_utc": snapshot["timestamp"],
        "market_id": snapshot["market_id"],
        "slug": snapshot["slug"],
        "underlying": snapshot["underlying"],

        # Exchange-specific identifiers and passthrough fields
        "tokenId": ob.get("tokenId"),

        # Orderbook ladders:
        # - full_orderbook=True  -> store entire ladder
        # - full_orderbook=False -> store top-of-book only
        "bids": strip(bids if full_orderbook else ([bb] if bb else [])),
        "asks": strip(asks if full_orderbook else ([ba] if ba else [])),

        # Convenience fields duplicated for easy access during analysis
        "best_bid": bb,
        "best_ask": ba,
        "mid": mid,
        "spread": spread,

        # Additional exchange metadata preserved verbatim
        "adjustedMidpoint": ob.get("adjustedMidpoint"),
        "lastTradePrice": ob.get("lastTradePrice"),
        "minSize": int(ob.get("minSize")),
        "maxSpread": float(ob.get("maxSpread")),
    }
