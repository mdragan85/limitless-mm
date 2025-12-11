from market_data.market_logger import MarketLogger

logger = MarketLogger()
markets = logger.discover_markets("BTC")
print(markets)