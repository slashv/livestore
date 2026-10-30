#!/usr/bin/env bash
# Runs the snapshot boot benchmark suite. Every measurement is its own Node process; results append to
# results/*.jsonl next to this script (override with SNAPSHOT_BENCH_RESULTS). Databases and cached snapshots go to
# SNAPSHOT_BENCH_DIR (default: $TMPDIR/livestore-snapshot-bench, ~15 GB for the 10M runs).
#
#   ./run-all.sh micro prepare snapshot replay breakdown checkpointer export-limits leak-probe   # phases
#   ./run-all.sh prepare10m                                              # ~2-3 h per mix
set -euo pipefail
cd "$(dirname "$0")"
export NODE_ENV=production

phase() {
  case "$1" in
    micro)
      for mix in append update; do node micro.ts --mix "$mix" --events 50000; done
      ;;
    prepare)
      for mix in append update; do
        node boot.ts prepare --mix "$mix" --events 100000 --tails 0,1000,10000
        node segmented.ts prepare --mix "$mix" --events 1000000 --segment 250000 --tails 0,1000,10000,100000
      done
      ;;
    prepare10m)
      for mix in update append; do
        node segmented.ts prepare --mix "$mix" --events 10000000 --segment 250000 --tails 0,1000,10000,100000
      done
      ;;
    replay)
      for mix in append update; do
        for rep in 1 2 3; do node boot.ts replay --mix "$mix" --events 100000 --rep "$rep"; done
        # 1M+ full replays come from `prepare` (segmented.ts records them in results/prepare-replay.jsonl). More reps:
        # node segmented.ts replay --mix "$mix" --events 1000000 --segment 250000 --rep 1
      done
      ;;
    snapshot)
      for mix in append update; do
        for tail in 0 1000 10000; do
          for rep in 1 2 3 4 5; do node boot.ts snapshot --mix "$mix" --events 100000 --tail "$tail" --rep "$rep"; done
        done
        for tail in 0 1000 10000 100000; do
          for rep in 1 2 3; do node boot.ts snapshot --mix "$mix" --events 1000000 --tail "$tail" --rep "$rep"; done
        done
      done
      ;;
    snapshot10m)
      for mix in update append; do
        for tail in 0 1000 10000 100000; do
          node boot.ts snapshot --mix "$mix" --events 10000000 --tail "$tail" --rep 1 || true
        done
      done
      ;;
    breakdown)
      for variant in full noop nosession; do
        for rep in 1 2 3; do
          node boot.ts replay --mix append --events 100000 --storage memory --breakdown "$variant" --rep "$rep"
        done
      done
      for rep in 1 2 3; do node boot.ts replay --mix append --events 100000 --storage fs --breakdown full --rep "$rep"; done
      ;;
    leak-probe)
      node --expose-gc leak-probe.ts --calls 200000
      ;;
    export-limits)
      for mib in 256 512 1024 1536 2048; do node export-limits.ts --mib "$mib" || true; done
      ;;
    checkpointer)
      # Wall clock: events arrive in real time (needs a machine that grants the process a full core).
      for base in 100000 1000000; do
        for rate in 100 1000 10000; do
          node checkpointer.ts --mix append --base "$base" --rate "$rate" --seconds 120
        done
      done
      # Event time: CPU per virtual second = core share needed for the rate, independent of machine load.
      calibrated='{"replayCostPerEventMs":0.5}'
      node checkpointer.ts --clock virtual --mix append --base 100000 --rate 100 --seconds 120 --label default
      node checkpointer.ts --clock virtual --mix append --base 100000 --rate 1000 --seconds 60 --label default
      node checkpointer.ts --clock virtual --mix append --base 100000 --rate 1000 --seconds 60 --label calibrated --trigger "$calibrated"
      node checkpointer.ts --clock virtual --mix append --base 100000 --rate 10000 --seconds 10 --label default
      node checkpointer.ts --clock virtual --mix append --base 1000000 --rate 1000 --seconds 30 --label calibrated --trigger "$calibrated"
      ;;
    quiet)
      # Re-run after the core leak fixes, on a quiet machine: single processes, strictly sequential.
      # Writes to results/quiet (set SNAPSHOT_BENCH_RESULTS). Checkpointer leaders are headless.
      for mix in append update; do node boot.ts prepare --mix "$mix" --events 100000 --tails 0,1000,10000 --headless; done
      for mix in append update; do node boot.ts prepare --mix "$mix" --events 1000000 --tails 0,1000,10000,100000 --headless; done
      for mix in append update; do
        for tail in 0 1000 10000; do
          for rep in 1 2 3 4 5; do node boot.ts snapshot --mix "$mix" --events 100000 --tail "$tail" --rep "$rep"; done
        done
        for tail in 0 1000 10000 100000; do
          for rep in 1 2 3; do node boot.ts snapshot --mix "$mix" --events 1000000 --tail "$tail" --rep "$rep"; done
        done
      done
      for mix in append update; do
        for rep in 1 2 3 4 5; do node --expose-gc boot.ts replay --mix "$mix" --events 100000 --rep "$rep"; done
        for rep in 1 2 3; do node --expose-gc boot.ts replay --mix "$mix" --events 1000000 --rep "$rep"; done
      done
      node --expose-gc leak-probe.ts --calls 200000
      node export-limits.ts --mib 2048 || true
      node --expose-gc checkpointer.ts --mix append --base 100000 --rate 1000 --seconds 1800 --headless --label sustained
      ;;
    quiet10m)
      node --expose-gc boot.ts prepare --mix update --events 10000000 --tails 0,1000,10000,100000 --headless
      for tail in 0 1000 10000 100000; do node boot.ts snapshot --mix update --events 10000000 --tail "$tail" --rep 1; done
      ;;
    *)
      echo "unknown phase $1" >&2
      exit 1
      ;;
  esac
}

for p in "$@"; do phase "$p"; done
