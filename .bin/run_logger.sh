#!/usr/bin/env bash
set -euo pipefail

cd /Users/maciejdragan/code/limitless-mm

source .venv/bin/activate

set -a
source .env
set +a

mkdir -p .logs

python -u -m app.run_logger

