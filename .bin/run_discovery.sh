#!/usr/bin/env bash
set -euo pipefail

cd /Users/maciejdragan/code/limitless-mm

source .venv/bin/activate

set -a
source .env
set +a

mkdir -p .logs

python -m app.run_discovery 2>&1 | tee -a .logs/discovery.log

