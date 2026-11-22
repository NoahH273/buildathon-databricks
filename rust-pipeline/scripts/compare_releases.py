#!/usr/bin/env python3
"""Alternate two native release binaries. Print JSON samples after exact validation."""
import argparse
import json
import multiprocessing
import os
from pathlib import Path
import sys
from tempfile import TemporaryDirectory

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "testing"))
from harness import run_process
from validation import validate


def check_output(output, expected):
    comparison = validate(output, expected)
    if not comparison["passed"]:
        raise ValueError(f"exact validation failed: {comparison}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--benchmark", nargs=3, action="append", required=True,
                        metavar=("NAME", "DATA", "EXPECTED"))
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--cpus", type=int, required=True)
    parser.add_argument("--mem-gb", type=int, required=True)
    parser.add_argument("--timeout", type=float, default=300)
    args = parser.parse_args()
    if min(args.repetitions, args.cpus, args.mem_gb) < 1:
        parser.error("repetitions, cpus and mem-gb must be positive")
    binaries = [("baseline", args.baseline.resolve()), ("candidate", args.candidate.resolve())]
    for name, data, expected in args.benchmark:
        for iteration in range(args.repetitions + 1):
            for label, binary in (binaries if iteration % 2 == 0 else reversed(binaries)):
                with TemporaryDirectory() as path:
                    directory = Path(path)
                    scratch, output = directory / "scratch", directory / "out"
                    scratch.mkdir()
                    output.mkdir()
                    env = dict(os.environ, DATA_DIR=str(Path(data).resolve()),
                               OUTPUT_DIR=str(output), SCRATCH_DIR=str(scratch),
                               NCPU=str(args.cpus), MEM_GB=str(args.mem_gb),
                               CHECKPOINTS_ENABLED="0")
                    result = run_process([str(binary)], ROOT, env, directory, args.timeout, "process")
                    if result.get("exit_code") != 0 or result["timed_out"]:
                        sys.exit(f"{binary} failed: {result}\n{(directory / 'stderr.log').read_text()}")
                    # Fresh validation processes keep Polars allocations out of
                    # the parent that launches the next measured executable.
                    check = multiprocessing.get_context("spawn").Process(
                        target=check_output, args=(output, Path(expected)))
                    check.start()
                    check.join()
                    if check.exitcode:
                        sys.exit(f"{name}/{label} failed exact validation")
                    print(json.dumps(dict(dataset=name, binary=label, iteration=iteration,
                                          warmup=iteration == 0, **result)), flush=True)


if __name__ == "__main__":
    main()
