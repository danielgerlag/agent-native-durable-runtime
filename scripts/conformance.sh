#!/usr/bin/env bash
# Cross-language checkpoint load. Exit 1 on the first mismatch.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

cargo test --test checkpoint --offline

if [[ -d python ]]; then
  (cd python && python3 -m pytest -q)
fi

if [[ -d typescript ]]; then
  (cd typescript && npm test)
fi
