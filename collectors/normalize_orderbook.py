

def normalize_orderbook(snapshot: dict, *, full_orderbook: bool):
    ob = snapshot["orderbook"]

    bids = ob.get("bids", [])
    asks = ob.get("asks", [])

    def best_bid():
        return max(bids, key=lambda x: x["price"], default=None)

    def best_ask():
        return min(asks, key=lambda x: x["price"], default=None)

    bb = best_bid()
    ba = best_ask()

    mid = (bb["price"] + ba["price"]) / 2 if bb and ba else None
    spread = (ba["price"] - bb["price"]) if bb and ba else None

    def strip(levels):
        return [{"price": float(l["price"]), "size": int(l["size"])} for l in levels]

    return {
        "ts_utc": snapshot["timestamp"],
        "market_id": snapshot["market_id"],
        "slug": snapshot["slug"],
        "underlying": snapshot["underlying"],
        "tokenId": ob.get("tokenId"),
        "bids": strip(bids if full_orderbook else ([bb] if bb else [])),
        "asks": strip(asks if full_orderbook else ([ba] if ba else [])),
        "best_bid": bb,
        "best_ask": ba,
        "mid": mid,
        "spread": spread,
        "adjustedMidpoint": ob.get("adjustedMidpoint"),
        "lastTradePrice": ob.get("lastTradePrice"),
        "minSize": int(ob.get("minSize")),
        "maxSpread": float(ob.get("maxSpread")),
    }



