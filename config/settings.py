"""
Application configuration for the Limitless data logger.
Reads settings from environment variables and provides defaults.
"""

import os
from pathlib import Path
from dataclasses import dataclass, field
from dotenv import load_dotenv

from config.polymarket_rules import POLYMARKET_RULES
from config.limitless_rules import LIMITLESS_RULES

# Load .env file if present (explicit path avoids python-dotenv auto-discovery issues on newer Python)
ENV_PATH = Path(__file__).resolve().parents[1] / ".env"  # repo_root/.env if config/ is one level down
load_dotenv(dotenv_path=ENV_PATH, override=False)


@dataclass
class AppSettings:
    """
    Settings for connecting to the Limitless API and logging behavior.
    """

    # Logging behavior
    FULL_ORDERBOOK = True
    ROTATE_MINUTES = 10
    FSYNC_SECONDS = 5
    DISCOVER_EVERY_SECONDS = 60
    EXPIRE_GRACE_SECONDS = 120

    # How often (in seconds) to poll orderbooks
    POLL_INTERVAL: float = float(os.getenv("LIMITLESS_POLL_INTERVAL", "2.0"))

    # Max number of markets per underlying to log
    MAX_MARKETS_PER_UNDERLYING: int = int(
        os.getenv("LIMITLESS_MAX_MARKETS_PER_UNDERLYING", "10")
    )
	
    # Where to write logs
    OUTPUT_DIR: Path = Path(os.getenv("LIMITLESS_OUTPUT_DIR", ".outputs/logs"))



# Create a single config instance for import
settings = AppSettings()
