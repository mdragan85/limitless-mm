from market_data.market_logger import MarketLogger

logger = MarketLogger()
markets = logger.discover_markets("BTC")
m = markets[0]
logger.log_snapshot(m)