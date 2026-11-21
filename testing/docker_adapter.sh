#!/usr/bin/env bash
# Generic no-network, read-only container adapter. Pass IMAGE [COMMAND ARGS...].
set -euo pipefail
: "${DATA_DIR:?}" "${OUTPUT_DIR:?}" "${SCRATCH_DIR:?}"
: "${NCPU:=4}" "${MEM_GB:=8}"
image="$1"
shift
container="shopping-test-${BASHPID}-${RANDOM}"
monitor_pid=""
cleanup() {
    if [[ -n "$monitor_pid" ]]; then kill "$monitor_pid" >/dev/null 2>&1 || true; fi
    docker rm -f "$container" >/dev/null 2>&1 || true
}
trap cleanup EXIT
trap 'exit 143' TERM
trap 'exit 130' INT
profile_args=(-e CHECKPOINTS_ENABLED=0)
if [[ "${CHECKPOINTS_ENABLED:-0}" == 1 && -n "${CHECKPOINTS_FILE:-}" ]]; then
    profile_args=(-e CHECKPOINTS_ENABLED=1 -e CHECKPOINTS_FILE=/profiling/checkpoints.jsonl
                  -v "$(dirname "$CHECKPOINTS_FILE"):/profiling")
fi
docker run --name "$container" --rm --platform linux/amd64 \
    --network none --read-only --tmpfs /tmp:rw,size=64m,mode=1777 \
    --cap-drop ALL --security-opt no-new-privileges --pids-limit 4096 \
    --cpus "$NCPU" --memory "${MEM_GB}g" --memory-swap "${MEM_GB}g" \
    --user "$(id -u):$(id -g)" \
    -v "$DATA_DIR:/data:ro" -v "$OUTPUT_DIR:/output" -v "$SCRATCH_DIR:/scratch" \
    -e DATA_DIR=/data -e OUTPUT_DIR=/output -e SCRATCH_DIR=/scratch -e HOME=/scratch \
    -e NCPU="$NCPU" -e MEM_GB="$MEM_GB" "${profile_args[@]}" "$image" "$@" &
runner_pid=$!
if [[ "${CHECKPOINTS_ENABLED:-0}" == 1 && -n "${CHECKPOINTS_FILE:-}" ]]; then
    # Diagnostic samples only. Never run a monitor in official measurements.
    (
        for attempt in {1..100}; do
            if docker inspect "$container" >/dev/null 2>&1; then
                # --no-stream avoids ANSI screen-refresh bytes in JSONL files.
                while docker stats --no-stream --no-trunc --format '{{json .}}' "$container"; do :; done
                exit 0
            fi
            sleep 0.02
        done
    ) >"$(dirname "$CHECKPOINTS_FILE")/container-stats.jsonl" 2>"$(dirname "$CHECKPOINTS_FILE")/container-stats.stderr.log" &
    monitor_pid=$!
fi
wait "$runner_pid"
