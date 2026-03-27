#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: $0 --block <blk.dat> <rev.dat> <xor.dat>" >&2
  exit 1
}

if [[ $# -lt 4 ]] || [[ "$1" != "--block" ]]; then
  echo '{"ok":false,"error":{"code":"INVALID_ARGS","message":"Usage: cli.sh --block <blk.dat> <rev.dat> <xor.dat>"}}' 
  exit 1
fi

BLK_FILE="$2"
REV_FILE="$3"
XOR_FILE="$4"

if [ ! -f "$BLK_FILE" ]; then
  echo "{\"ok\":false,\"error\":{\"code\":\"FILE_NOT_FOUND\",\"message\":\"Block file not found: $BLK_FILE\"}}"
  exit 1
fi

mkdir -p out

python3 src/cli.py --block "$BLK_FILE" "$REV_FILE" "$XOR_FILE"