#!/usr/bin/env python3
"""Black-box correctness and performance harness; implementations can use any language."""
import argparse
from collections import defaultdict
from datetime import datetime, timezone
import csv
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import shlex
import shutil
import signal
import statistics
import subprocess
import sys
import time
import uuid

import polars as pl

from validation import SCHEMAS, load_expected, validate

REPO = Path(__file__).resolve().parents[1]


def write_json(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, default=str, allow_nan=False) + "\n")
    temporary.replace(path)


def fingerprint(folder, input_data=False):
    """Fingerprint exact bytes outside the timed region, including all input parts."""
    files, digest, counts = [], hashlib.sha256(), defaultdict(int)
    for path in sorted(p for p in folder.rglob("*") if p.is_file() and not p.is_symlink()):
        relative = path.relative_to(folder).as_posix()
        if input_data and not (relative == "manifest.json" or relative in ("products.csv", "campaigns.csv")
                               or (path.parent.name in ("events", "clicks", "links") and path.suffix == ".csv")):
            continue
        file_digest, lines, last_byte = hashlib.sha256(), 0, b""
        with path.open("rb") as source:
            for block in iter(lambda: source.read(1024 * 1024), b""):
                file_digest.update(block)
                if input_data and path.parent.name in ("events", "clicks", "links"):
                    lines += block.count(b"\n")
                    last_byte = block[-1:]
        if last_byte and last_byte != b"\n":
            lines += 1
        if input_data and path.parent.name in ("events", "clicks", "links"):
            counts[path.parent.name] += max(0, lines - 1)
        item = {"path": relative, "bytes": path.stat().st_size, "sha256": file_digest.hexdigest()}
        files.append(item)
        digest.update((relative + "\0" + item["sha256"] + "\n").encode())
    return {"path": str(folder), "sha256": digest.hexdigest(), "files": files,
            "bytes": sum(f["bytes"] for f in files), "physical_data_lines": dict(counts)}


def checkpoint_results(path):
    """Checkpoints are completed stages, with elapsed seconds from implementation start."""
    result = {"stages": [], "warnings": []}
    if not path.exists():
        return result
    if path.is_symlink() or not path.is_file():
        result["warnings"].append("checkpoint path must be a regular file")
        return result
    previous = 0.0
    with path.open(encoding="utf-8", errors="replace") as source:
        for number, line in enumerate(source, 1):
            try:
                event = json.loads(line)
                # Reject NaN/Infinity anywhere, including optional metrics, so
                # bad instrumentation cannot break serialization of reports.
                json.dumps(event, allow_nan=False)
                if not isinstance(event, dict) or not isinstance(event.get("name"), str) or not event["name"]:
                    raise ValueError("name must be a nonempty string")
                elapsed = event.get("elapsed_seconds")
                if isinstance(elapsed, bool) or not isinstance(elapsed, (int, float)) or not math.isfinite(elapsed):
                    raise ValueError("elapsed_seconds must be a finite number")
                if elapsed < previous:
                    raise ValueError("elapsed_seconds must be nonnegative and nondecreasing")
                result["stages"].append({"name": event["name"], "elapsed_seconds": elapsed,
                                         "duration_seconds": elapsed - previous,
                                         "metrics": event.get("metrics", {})})
                previous = elapsed
            except (ValueError, TypeError) as error:
                result["warnings"].append(f"line {number}: {error}")
    return result


def kill_group(pid, sig):
    try:
        os.killpg(pid, sig)
    except ProcessLookupError:
        pass


def terminate(process):
    """Let launcher traps clean up containers before forcing termination."""
    kill_group(process.pid, signal.SIGTERM)
    deadline = time.perf_counter() + 1.0
    while time.perf_counter() < deadline:
        pid, status, usage = os.wait4(process.pid, os.WNOHANG)
        if pid:
            return status, usage
        time.sleep(0.005)
    kill_group(process.pid, signal.SIGKILL)
    _, status, usage = os.wait4(process.pid, 0)
    return status, usage


def run_process(command, cwd, env, directory, timeout, resource_mode):
    """wait4 returns per-run usage, avoiding cumulative RUSAGE_CHILDREN peaks."""
    started = time.perf_counter()
    with (directory / "stdout.log").open("wb") as stdout, (directory / "stderr.log").open("wb") as stderr:
        try:
            process = subprocess.Popen(command, cwd=cwd, env=env, stdout=stdout, stderr=stderr,
                                       start_new_session=True)
        except OSError as error:
            return {"exit_code": None, "timed_out": False, "error": str(error),
                    "wall_seconds": time.perf_counter() - started}
        timed_out, usage, status = False, None, None
        try:
            while True:
                pid, next_status, next_usage = os.wait4(process.pid, os.WNOHANG)
                if pid:
                    status, usage = next_status, next_usage
                    break
                if time.perf_counter() - started >= timeout:
                    timed_out = True
                    status, usage = terminate(process)
                    break
                time.sleep(0.005)
        finally:
            # A launcher may exit before its workers. Never leave those running.
            if status is not None and usage is not None:
                process.returncode = os.waitstatus_to_exitcode(status)
            else:
                status, usage = terminate(process)
                process.returncode = os.waitstatus_to_exitcode(status)
            kill_group(process.pid, signal.SIGKILL)
    result = {"exit_code": process.returncode, "timed_out": timed_out,
              "wall_seconds": time.perf_counter() - started,
              "cpu_user_seconds": None, "cpu_system_seconds": None,
              "max_process_rss_bytes": None}
    if resource_mode == "process" and usage is not None:
        result.update(cpu_user_seconds=usage.ru_utime, cpu_system_seconds=usage.ru_stime,
                      max_process_rss_bytes=usage.ru_maxrss * (1 if sys.platform == "darwin" else 1024))
    return result


def percentile(values, fraction):
    values = sorted(values)
    position = (len(values) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    return values[lower] + (values[upper] - values[lower]) * (position - lower)


def summarize(records):
    benchmarks = {}
    for name in sorted({r["dataset"] for r in records if r["kind"] != "fixture"}):
        measured = [r for r in records if r["dataset"] == name and r["kind"] == "measurement"]
        successful = [r for r in measured if r["status"] == "passed"]
        times = [r["wall_seconds"] for r in successful]
        summary = {"successful_measurements": len(successful), "total_measurements": len(measured)}
        if times:
            summary.update(median_seconds=statistics.median(times), min_seconds=min(times),
                           max_seconds=max(times), p95_seconds=percentile(times, .95))
            memory = [r["max_process_rss_bytes"] for r in successful if r.get("max_process_rss_bytes") is not None]
            summary["max_process_rss_bytes"] = max(memory) if memory else None
            event_lines = successful[0]["input_event_lines"]
            summary["input_event_lines_per_second"] = event_lines / statistics.median(times)
        stages = defaultdict(list)
        profiles = [r for r in records if r["dataset"] == name and r["kind"] == "profile"]
        for run in profiles:
            if run["status"] != "passed":
                continue
            per_run = defaultdict(float)
            for stage in run["checkpoints"]["stages"]:
                per_run[stage["name"]] += stage["duration_seconds"]
            for stage, duration in per_run.items():
                stages[stage].append(duration)
        stage_summary = [{"name": name, "samples": len(values), "median_seconds": statistics.median(values)}
                         for name, values in stages.items()]
        total = sum(stage["median_seconds"] for stage in stage_summary)
        for stage in stage_summary:
            stage["share_of_profiled_stages_percent"] = stage["median_seconds"] / total * 100 if total else 0
        summary["profile_stages"] = sorted(stage_summary, key=lambda s: -s["median_seconds"])
        summary["profile_runs"] = len(profiles)
        summary["profile_warnings"] = [warning for r in profiles for warning in r["checkpoints"]["warnings"]]
        benchmarks[name] = summary
    fixtures = [r for r in records if r["kind"] == "fixture"]
    return {"fixtures_passed": sum(r["status"] == "passed" for r in fixtures),
            "fixtures_total": len(fixtures), "benchmarks": benchmarks,
            "failed_runs": sum(r["status"] != "passed" for r in records)}


def write_reports(root, metadata, records, complete):
    summary = summarize(records)
    payload = {"schema_version": 1, "complete": complete, "metadata": metadata,
               "summary": summary, "runs": records}
    write_json(root / "results.json", payload)
    columns = ["dataset", "kind", "iteration", "status", "wall_seconds", "cpu_user_seconds",
               "cpu_system_seconds", "max_process_rss_bytes", "exit_code", "timed_out", "directory"]
    with (root / "results.csv").open("w", newline="") as target:
        writer = csv.DictWriter(target, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)
    lines = [f"# {metadata['name']}: Shopping Sessions results", "",
             f"Run: `{metadata['started_at_utc']}`. Complete: **{complete}**.", "",
             f"Fixtures: **{summary['fixtures_passed']}/{summary['fixtures_total']} passed**. "
             f"Failed runs across all phases: **{summary['failed_runs']}**.", "",
             "Measured runs have checkpoint recording disabled. Warmups, profiling, input hashing, "
             "and output validation are excluded from benchmark statistics.", "",
             "| Dataset | Correct runs | Median (s) | P95 (s) | Min–max (s) | Max process RSS (MiB) | Input event lines/s |",
             "| --- | ---: | ---: | ---: | ---: | ---: | ---: |"]
    if metadata.get("harness_error"):
        lines[4:4] = [f"Harness error: {metadata['harness_error']}", ""]
    for name, row in summary["benchmarks"].items():
        if "median_seconds" in row:
            rss = row["max_process_rss_bytes"]
            memory = f"{rss / 1024**2:.1f}" if rss is not None else "unavailable"
            lines.append(f"| {name} | {row['successful_measurements']}/{row['total_measurements']} | "
                         f"{row['median_seconds']:.4f} | {row['p95_seconds']:.4f} | "
                         f"{row['min_seconds']:.4f}–{row['max_seconds']:.4f} | {memory} | "
                         f"{row['input_event_lines_per_second']:,.0f} |")
        else:
            lines.append(f"| {name} | 0/{row['total_measurements']} | — | — | — | — | — |")
    for name, row in summary["benchmarks"].items():
        if row["profile_stages"]:
            lines.extend(["", f"## {name}: separate profiling runs", "",
                          "These timings include instrumentation overhead; they are diagnostic and never used for ranking.", "",
                          "| Completed stage | Samples | Median (s) | Share of profiled stages |",
                          "| --- | ---: | ---: | ---: |"])
            for stage in row["profile_stages"]:
                lines.append(f"| {stage['name']} | {stage['samples']} | {stage['median_seconds']:.4f} | "
                             f"{stage['share_of_profiled_stages_percent']:.1f}% |")
        if row["profile_runs"] and not row["profile_stages"]:
            lines.extend(["", f"{name}: implementation did not supply valid checkpoints."])
        for warning in row["profile_warnings"]:
            lines.extend(["", f"Checkpoint warning: {warning}"])
    lines.extend(["", "## Measurement scope", "",
                  f"Command: `{shlex.join(metadata['command'])}`", "",
                  f"Working directory: `{metadata['cwd']}`. Resource mode: `{metadata['resource_mode']}`.", "",
                  "Wall time includes process startup and output writes. In process mode, CPU usage is child "
                  "resource usage returned by wait4; RSS is the largest individual process high-water mark, "
                  "including waited-for children, rather than summed process-tree memory. Detached workers "
                  "and container resources are not measured. Use wall-only mode for container/remote launchers.", "",
                  f"NCPU={metadata['cpus']}, MEM_GB={metadata['mem_gb']} are environment hints; "
                  "the command runner itself does not enforce CPU or memory limits.", "",
                  "Per-run outputs, validation details, checkpoints, and stdout/stderr are retained beside this report. "
                  "Dataset fingerprints and environment details are in results.json. These are local measurements, not scorer times."])
    failures = [r for r in records if r["status"] != "passed"]
    if failures:
        lines.extend(["", "## Failed runs", ""])
        for row in failures:
            lines.append(f"- {row['dataset']} ({row['kind']} #{row['iteration']}): "
                         f"{row['status']}; see `{row['directory']}/validation.json` and logs.")
    (root / "report.md").write_text("\n".join(lines) + "\n")


def safe_name(name):
    value = re.sub(r"[^A-Za-z0-9_.-]+", "-", name).strip(".-")
    if not value:
        raise ValueError("name must contain a letter or number")
    return value


def host_metadata():
    try:
        git_sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip()
        dirty = bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=REPO, text=True).strip())
    except (OSError, subprocess.CalledProcessError):
        git_sha, dirty = None, None
    return {"platform": platform.platform(), "machine": platform.machine(),
            "python": sys.version, "polars": pl.__version__, "logical_cpus": os.cpu_count(),
            "git_commit": git_sha, "git_dirty": dirty}


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--name", required=True, help="implementation/report name")
    parser.add_argument("--fixtures", type=Path, default=REPO / "starter-duckdb/fixtures")
    parser.add_argument("--skip-fixtures", action="store_true")
    parser.add_argument("--benchmark", nargs=3, action="append", default=[], metavar=("NAME", "INPUT", "EXPECTED"))
    parser.add_argument("--artifacts", type=Path, default=REPO / "artifacts")
    parser.add_argument("--cwd", type=Path, default=REPO)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--profile-runs", type=int, default=1,
                        help="separate diagnostic runs per benchmark; never enter timing statistics")
    parser.add_argument("--profile-command", help="optional shell-quoted argv for a separate instrumented binary; no shell is invoked")
    parser.add_argument("--timeout", type=float, default=120.0, help="seconds per invocation")
    parser.add_argument("--cpus", type=int, default=4, help="NCPU/CPUS environment hints")
    parser.add_argument("--mem-gb", type=int, default=8, help="MEM_GB/MEM environment hints")
    parser.add_argument("--resource-mode", choices=("process", "wall-only"), default="process")
    parser.add_argument("command", nargs=argparse.REMAINDER, help="-- executable args; optional {data}, {output}, {scratch} placeholders")
    args = parser.parse_args(argv)
    if args.command[:1] == ["--"]:
        args.command = args.command[1:]
    if not args.command:
        parser.error("provide an implementation command after --")
    if args.profile_command:
        try:
            if not shlex.split(args.profile_command):
                parser.error("profile-command must contain an executable")
        except ValueError as error:
            parser.error(str(error))
    try:
        safe_name(args.name)
    except ValueError as error:
        parser.error(str(error))
    if args.repetitions < 1 or min(args.warmups, args.profile_runs) < 0 or min(args.cpus, args.mem_gb) < 1:
        parser.error("repetitions/cpus/mem-gb must be positive; warmups/profile-runs must be nonnegative")
    if not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error("timeout must be a positive finite number")
    if not hasattr(os, "wait4"):
        parser.error("the harness requires a POSIX host with os.wait4 (Linux or macOS)")
    if args.skip_fixtures and not args.benchmark:
        parser.error("select fixtures or at least one benchmark")
    return args


def main(argv=None):
    args = parse_args(argv)
    started = datetime.now(timezone.utc)
    name = safe_name(args.name)
    root = args.artifacts.resolve() / f"{name}-{started.strftime('%Y%m%dT%H%M%S%fZ')}-{uuid.uuid4().hex[:6]}"
    root.mkdir(parents=True)
    records = []
    metadata = {"name": args.name, "started_at_utc": started.isoformat(),
                "command": args.command, "profile_command": shlex.split(args.profile_command) if args.profile_command else args.command,
                "cwd": str(args.cwd.resolve()), "resource_mode": args.resource_mode,
                "cpus": args.cpus, "mem_gb": args.mem_gb, "timeout_seconds": args.timeout,
                "repetitions": args.repetitions, "warmups": args.warmups, "profile_runs": args.profile_runs,
                "host": host_metadata(), "datasets": []}
    print(f"Artifacts: {root}", flush=True)
    try:
        datasets = []
        if not args.skip_fixtures:
            if not args.fixtures.is_dir():
                raise ValueError(f"fixtures directory not found: {args.fixtures}")
            cases = sorted(p for p in args.fixtures.iterdir() if p.is_dir())
            if not cases:
                raise ValueError("fixtures directory has no cases")
            for case in cases:
                data = case / ("input" if (case / "input").is_dir() else "data")
                datasets.append((case.name, data.resolve(), (case / "expected").resolve(), True))
        datasets.extend((name, Path(data).resolve(), Path(expected).resolve(), False)
                        for name, data, expected in args.benchmark)
        seen_names = set()
        for dataset, data, expected, fixture in datasets:
            slug = safe_name(dataset)
            if slug in seen_names:
                raise ValueError(f"duplicate dataset name: {dataset}")
            seen_names.add(slug)
            if not data.is_dir() or not (data / "manifest.json").is_file():
                raise ValueError(f"dataset requires a directory and manifest.json: {data}")
            for table in SCHEMAS:
                reference = load_expected(expected, table)
                if reference.height != reference.n_unique(subset=[c for c in ("user_id", "campaign_id", "day") if c in reference.columns]):
                    raise ValueError(f"duplicate keys in reference: {expected}/{table}")
            info = {"name": dataset, "fixture": fixture, "input": fingerprint(data, True),
                    "expected": fingerprint(expected)}
            metadata["datasets"].append(info)
            phases = [("fixture", 1)] if fixture else ([('warmup', i + 1) for i in range(args.warmups)]
                + [('measurement', i + 1) for i in range(args.repetitions)]
                + [('profile', i + 1) for i in range(args.profile_runs)])
            for kind, iteration in phases:
                directory = root / slug / f"{kind}-{iteration:03}"
                output, scratch = directory / "output", directory / "scratch"
                output.mkdir(parents=True)
                scratch.mkdir()
                env = dict(os.environ, DATA_DIR=str(data), OUTPUT_DIR=str(output), SCRATCH_DIR=str(scratch),
                           NCPU=str(args.cpus), MEM_GB=str(args.mem_gb), CPUS=str(args.cpus), MEM=f"{args.mem_gb}g",
                           PYTHONDONTWRITEBYTECODE="1", CHECKPOINTS_ENABLED="1" if kind == "profile" else "0")
                env.pop("CHECKPOINTS_FILE", None)
                checkpoint_file = directory / "profiling" / "checkpoints.jsonl"
                if kind == "profile":
                    checkpoint_file.parent.mkdir()
                    env["CHECKPOINTS_FILE"] = str(checkpoint_file)
                template = metadata["profile_command"] if kind == "profile" else args.command
                command = [argument.replace("{data}", str(data)).replace("{output}", str(output))
                           .replace("{scratch}", str(scratch)) for argument in template]
                record = {"dataset": dataset, "kind": kind, "iteration": iteration,
                          "directory": str(directory.relative_to(root)), "command": command,
                          "checkpoint_recording_enabled": kind == "profile",
                          "input_event_lines": info["input"]["physical_data_lines"].get("events", 0)}
                write_json(directory / "invocation.json", record)
                record.update(run_process(command, args.cwd.resolve(), env, directory, args.timeout, args.resource_mode))
                if record.get("error"):
                    record["status"] = "error"
                elif record["timed_out"]:
                    record["status"] = "timeout"
                elif record["exit_code"] != 0:
                    record["status"] = "error"
                else:
                    record["validation"] = validate(output, expected)
                    record["status"] = "passed" if record["validation"]["passed"] else "failed"
                record["checkpoints"] = checkpoint_results(checkpoint_file) if kind == "profile" else {"stages": [], "warnings": []}
                write_json(directory / "validation.json", record.get("validation", {"passed": False, "status": record["status"], "error": record.get("error")}))
                records.append(record)
                shutil.rmtree(scratch)
                write_reports(root, metadata, records, False)
                print(f"{record['status']:7} {dataset:20} {kind:11} #{iteration} {record['wall_seconds']:.4f}s", flush=True)
        write_reports(root, metadata, records, True)
        return 1 if any(r["status"] != "passed" for r in records) else 0
    except (ValueError, OSError, pl.exceptions.PolarsError) as error:
        metadata["harness_error"] = str(error)
        write_reports(root, metadata, records, False)
        print(f"Harness error: {error}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        metadata["harness_error"] = "interrupted"
        write_reports(root, metadata, records, False)
        return 130


if __name__ == "__main__":
    sys.exit(main())
