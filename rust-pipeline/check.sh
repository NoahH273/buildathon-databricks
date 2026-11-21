#!/usr/bin/env bash
# Local correctness + timing loop: fixtures, dev-answer, seed-3 cross-check, then a timed big run.
# Usage: rust-pipeline/check.sh [BIN] (env: S = folder holding data/ and outputs)
set -u
ROOT=$(cd "$(dirname "$0")/.." && pwd)
S=${S:?set S to a scratch folder holding data/dev, data/s3, out-s3-duck, data/p20}
BIN=${1:-$S/target/release/shopping-sessions.exe}
export PYTHONIOENCODING=utf8
python "$ROOT/starter-duckdb/scripts/run_fixtures.py" "$ROOT/starter-duckdb/fixtures" -- "$BIN" 2>&1 | grep -v "^ok"
rm -rf "$S/out-dev"; DATA_DIR="$S/data/dev" OUTPUT_DIR="$S/out-dev" "$BIN" 2>/dev/null
(cd "$ROOT/starter-duckdb/scripts" && echo "dev: $(python compare.py "$S/out-dev" "$ROOT/dev-answer" | tail -1)")
if [ -d "$S/out-s3-duck" ]; then
  rm -rf "$S/out-s3"; DATA_DIR="$S/data/s3" OUTPUT_DIR="$S/out-s3" "$BIN" 2>/dev/null
  (cd "$ROOT/starter-duckdb/scripts" && echo "seed3: $(python compare.py "$S/out-s3" "$S/out-s3-duck" | tail -1)")
fi
if [ -d "$S/data/p20" ]; then
  for i in 1 2; do
    rm -rf "$S/out-p20"; DATA_DIR="$S/data/p20" OUTPUT_DIR="$S/out-p20" NCPU=8 "$BIN" 2>&1 | grep -vE "thread:"
  done
fi
