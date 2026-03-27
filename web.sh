#!/usr/bin/env bash
set -euo pipefail

PORT="${PORT:-3000}"
export PORT

echo "http://127.0.0.1:${PORT}"
exec python3 web/server.py