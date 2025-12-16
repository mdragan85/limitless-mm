"""
Entrypoint for running the Limitless market logger.
"""

import sys
import signal
import time

from config.settings import settings
from exchanges.limitless_api import LimitlessAPI
from market_data.market_logger import MarketLogger


def main():
    api = LimitlessAPI()
    logger = MarketLogger(api)

    def handle_sigint(sig, frame):
        print("\nReceived interrupt. Shutting down cleanly...")
        logger.api.close()
        sys.exit(0)

    # Graceful shutdown on Ctrl+C
    signal.signal(signal.SIGINT, handle_sigint)

    print(f"Starting Limitless market logger. Output -> {logger.out_dir}")

    while True:
        for underlying in settings.UNDERLYINGS:
            try:
                markets = api.discover_markets(underlying)
            except Exception as exc:
                print(f"[WARN] Market discovery failed for {underlying}: {exc}")
                continue

            logger.log_markets(markets)

        time.sleep(settings.POLL_INTERVAL)


if __name__ == "__main__":
    main()
