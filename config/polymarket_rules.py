# -------------------------
# Polymarket discovery rules
# -------------------------
# Note: for start_time_fields there is a field called startTime which is the field for most
# markets, and eventStartTime does not exist. These crypto specific markets,  have a startTime corresponding
# to when the market is created but not yet traddeable. it bcomes tradeable at eventStartTime


POLYMARKET_RULES = [
    {
        "name": "crypto_intraday_btc",
        "mode": "crypto_markets",
        "series_slug_prefixes": ["btc-up-or-down-"],
        "allowed_recurrence": ["15m", "hourly", "daily"],  # add "4h" only if recurrence ever equals "4h"
        "min_minutes_to_expiry": 0,
        "max_minutes_to_expiry": 1440,
    },
    {
        "name": "crypto_intraday_eth",
        "mode": "crypto_markets",
        "series_slug_prefixes": ["eth-up-or-down-"],
        "allowed_recurrence": ["15m", "hourly", "daily"],
        "min_minutes_to_expiry": 0,
        "max_minutes_to_expiry": 1440,
    },
    {
        "name": "crypto_intraday_sol",
        "mode": "crypto_markets",
        "series_slug_prefixes": ["sol-up-or-down-"],
        "allowed_recurrence": ["15m", "hourly", "daily"],
        "min_minutes_to_expiry": 0,
        "max_minutes_to_expiry": 1440,
    },
    {
        "name": "crypto_intraday_xrp",
        "mode": "crypto_markets",
        "series_slug_prefixes": ["xrp-up-or-down-"],
        "allowed_recurrence": ["15m", "hourly", "daily"],
        "min_minutes_to_expiry": 0,
        "max_minutes_to_expiry": 1440,
    },
]



POLYMARKET_RULES_ = [
    {
        "name": "crypto_intraday_btc",
        "queries": ["Bitcoin up or down"],
        "min_minutes_to_expiry": 0,
        "max_minutes_to_expiry": 1440,  # < 24h
        "lead_ms": 120_000,
        "start_time_fields": ["eventStartTime", "startTime"],
        "must_contain": [],
        "must_not_contain": [],
    },
    {
        "name": "crypto_intraday_eth",
        "queries": ["Ethereum up or down"],
        "min_minutes_to_expiry": 0,
        "max_minutes_to_expiry": 1440,
        "lead_ms": 120_000,
        "start_time_fields": ["eventStartTime", "startTime"],
        "must_contain": [],
        "must_not_contain": [],
    },
    {
        "name": "crypto_intraday_sol",
        "queries": ["Solana up or down"],
        "min_minutes_to_expiry": 0,
        "max_minutes_to_expiry": 1440,
        "lead_ms": 120_000,
        "start_time_fields": ["eventStartTime", "startTime"],
        "must_contain": [],
        "must_not_contain": [],
    },
    {
        "name": "crypto_intraday_xrp",
        "queries": ["XRP up or down"],
        "min_minutes_to_expiry": 0,
        "max_minutes_to_expiry": 1440,
        "lead_ms": 120_000,
        "start_time_fields": ["eventStartTime", "startTime"],
        "must_contain": [],
        "must_not_contain": [],
    },
]