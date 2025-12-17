from exchanges.limitless_api import LimitlessAPI

def try_path(api, path: str):
    try:
        data = api._get(path)
        print("OK ", path, "keys:", list(data.keys())[:8])
        return True
    except Exception as e:
        print("BAD", path, "->", repr(e))
        return False

def main():
    api = LimitlessAPI()
    m = api.discover_markets("BTC")[0]
    print("market_id:", m.market_id)
    print("slug     :", m.slug)

    ok_slug = try_path(api, f"markets/{m.slug}/orderbook")
    ok_id   = try_path(api, f"markets/{m.market_id}/orderbook")

    print("\nRESULT:", "slug" if ok_slug and not ok_id else "id" if ok_id and not ok_slug else "ambiguous")

if __name__ == "__main__":
    main()