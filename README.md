# limitless-mm

A modular framework for market making and data collection across prediction markets (Limitless, Polymarket, etc.).

## Overview

This repository contains the Phase 0 data ingestion scaffold.  
It connects to the Limitless public REST API, retrieves market metadata, and logs order book snapshots for research and market-making purposes.

## Installation

pip install -r requirements.txt


## Running the Logger

python scripts/run_logger.py


## Project Structure

- **config/** — environment variables and settings  
- **data/** — log storage and historical outputs  
- **exchanges/** — API clients for external exchanges (starting with Limitless)  
- **limitless_mm/** — core package initialization  
- **market_data/** — market definitions and logging utilities  
- **scripts/** — user-facing scripts (entrypoints)
