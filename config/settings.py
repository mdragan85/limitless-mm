"""
Application configuration for the Limitless data logger.
Reads settings from environment variables and provides defaults.
"""

import os
from pathlib import Path
from dataclasses import dataclass, field
from dotenv import load_dotenv

# Load .env file if present (explicit path avoids python-dotenv auto-discovery issues on newer Python)
ENV_PATH = Path(__file__).resolve().parents[1] / ".env"  # repo_root/.env if config/ is one level down
load_dotenv(dotenv_path=ENV_PATH, override=False)



# -------------------------
# Polymarket discovery rules
# -------------------------

POLYMARKET_RULES = [
    {
        "name": "crypto_intraday",
        "queries": [
            "Bitcoin up or down",
            "Ethereum up or down",
            "Solana up or down",
            "XRP up or down",
        ],
        "min_minutes_to_expiry": 5,
        "max_minutes_to_expiry": 1440,  # < 24h
        # optional guardrails (can be empty)
        "must_contain": [],
        "must_not_contain": [],
    },
]



@dataclass
class LimitlessConfig:
    """
    Settings for connecting to the Limitless API and logging behavior.
    """

    # Logging behavior
    FULL_ORDERBOOK = True
    ROTATE_MINUTES = 10
    FSYNC_SECONDS = 5
    DISCOVER_EVERY_SECONDS = 60
    EXPIRE_GRACE_SECONDS = 120

    # API base URL
    BASE_URL: str = os.getenv("LIMITLESS_BASE_URL", "https://api.limitless.exchange")

    # How often (in seconds) to poll orderbooks
    POLL_INTERVAL: float = float(os.getenv("LIMITLESS_POLL_INTERVAL", "2.0"))

    # Which underlyings to log
    UNDERLYINGS: list[str] = field(
        default_factory=lambda: os.getenv(
            "LIMITLESS_UNDERLYINGS", "BTC,ETH,SOL,XRP"
        ).split(",")
    )

    # Max number of markets per underlying to log
    MAX_MARKETS_PER_UNDERLYING: int = int(
        os.getenv("LIMITLESS_MAX_MARKETS_PER_UNDERLYING", "10")
    )
	
    # Where to write logs
    OUTPUT_DIR: Path = Path(os.getenv("LIMITLESS_OUTPUT_DIR", ".outputs/logs"))


    def __post_init__(self):
        # Normalize underlying symbols to uppercase
        self.UNDERLYINGS = [u.strip().upper() for u in self.UNDERLYINGS if u.strip()]


# Create a single config instance for import
settings = LimitlessConfig()
