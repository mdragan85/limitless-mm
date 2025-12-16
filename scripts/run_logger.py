"""
Entrypoint for running the Limitless market logger.
"""

import signal
import time

from config.settings import settings
from exchanges.limitless_api import LimitlessAPI
from market_data.market_logger import MarketLogger


def main():
    api = LimitlessAPI()
    logger = MarketLogger(api)

    stop = False

    def handle_stop(sig, frame):
        nonlocal stop
        print("\nReceived interrupt. Shutting down cleanly...")
        stop = True

    # Graceful shutdown on Ctrl+C and SIGTERM
    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)

    print(f"Starting Limitless market logger. Output -> {logger.out_dir}")

    try:
        while not stop:
            for underlying in settings.UNDERLYINGS:
                if stop:
                    break

                try:
                    markets = api.discover_markets(underlying)
                except Exception as exc:
                    print(f"[WARN] Market discovery failed for {underlying}: {exc}")
                    continue

                logger.log_markets(markets)

            if stop:
                break

            print(f"sleeping for {settings.POLL_INTERVAL}...")
            time.sleep(settings.POLL_INTERVAL)
            if stop:
                break
    except KeyboardInterrupt:
        print("\nKeyboard interrupt received. Exiting...")
    finally:
        api.close()


if __name__ == "__main__":
    main()
