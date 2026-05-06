#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

service="${1:-${SERVICE:-distributor}}"

case "$service" in
  distributor)
    exec cargo run --bin distributor
    ;;
  loader)
    exec cargo run --bin loader
    ;;
  *)
    echo "Usage: $0 [distributor|loader]"
    exit 1
    ;;
esac
