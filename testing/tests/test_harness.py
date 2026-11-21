"""Harness contract tests, independent of the DuckDB implementation."""
from datetime import date
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import polars as pl

TESTING = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(TESTING))
from harness import checkpoint_results, main, run_process, summarize
from validation import SCHEMAS, load_actual, validate


DAY = date(2026, 10, 3)


def write_tables(folder, duplicate=False, day_string=False, wrong_revenue=False):
    users = pl.DataFrame({"user_id": ["u1"], "day": [DAY], "sessions": [1], "events": [2],
                          "revenue_cents": [100], "top_category": ["books"]}).cast(SCHEMAS["user_daily"])
    campaigns = pl.DataFrame({"campaign_id": ["organic"], "channel": ["organic"], "day": [DAY],
                              "sessions": [1], "purchasing_sessions": [1], "revenue_cents": [100],
                              "assisted_sessions": [0]}).cast(SCHEMAS["campaign_daily"])
    if duplicate:
        users = pl.concat([users, users])
    if day_string:
        users = users.with_columns(pl.col("day").cast(pl.String))
    if wrong_revenue:
        campaigns = campaigns.with_columns(pl.lit(101).cast(pl.Int64).alias("revenue_cents"))
    for name, frame in (("user_daily", users), ("campaign_daily", campaigns)):
        directory = folder / name
        directory.mkdir(parents=True, exist_ok=True)
        # The contract accepts narrower integer types and any column order.
        frame = frame.with_columns(pl.col(c).cast(pl.Int32) for c, dtype in frame.schema.items() if dtype.is_integer())
        frame.select(list(reversed(frame.columns))).write_parquet(directory / "part.parquet")
    return users, campaigns


class ValidationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.expected = self.root / "expected"
        self.expected.mkdir()
        users, campaigns = write_tables(self.root / "seed")
        for name, frame in (("user_daily", users), ("campaign_daily", campaigns)):
            frame.write_csv(self.expected / f"{name}.csv")

    def test_valid_outputs_ignore_nonparquet_hidden_and_symlink_files(self):
        output = self.root / "output"
        write_tables(output)
        folder = output / "user_daily"
        (folder / "_bad.parquet").write_text("ignored")
        (folder / ".bad.parquet").write_text("ignored")
        (folder / "notes.txt").write_text("ignored")
        (folder / "linked.parquet").symlink_to(folder / "part.parquet")
        self.assertTrue(validate(output, self.expected)["passed"])

    def test_duplicates_fail_even_when_set_of_values_matches(self):
        output = self.root / "output"
        write_tables(output, duplicate=True)
        result = validate(output, self.expected)
        self.assertFalse(result["passed"])
        self.assertEqual(result["tables"]["user_daily"]["duplicate_keys"], 1)

    def test_date_strings_fail_the_parquet_schema(self):
        output = self.root / "output"
        write_tables(output, day_string=True)
        result = validate(output, self.expected)
        self.assertFalse(result["passed"])
        self.assertIn("expected Date", result["tables"]["user_daily"]["error"])

    def test_wrong_values_report_examples_and_invariant_failures(self):
        output = self.root / "output"
        write_tables(output, wrong_revenue=True)
        result = validate(output, self.expected)
        self.assertFalse(result["passed"])
        self.assertEqual(result["tables"]["campaign_daily"]["extra_rows"], 1)
        self.assertIn({"name": "equal_revenue_totals", "passed": False}, result["invariants"])

    def test_missing_and_corrupt_outputs_are_validation_failures(self):
        output = self.root / "output"
        self.assertFalse(validate(output, self.expected)["passed"])
        write_tables(output)
        (output / "user_daily" / "part.parquet").write_text("not parquet")
        self.assertFalse(validate(output, self.expected)["passed"])

    def test_multiple_parquet_parts_are_combined(self):
        output = self.root / "output"
        write_tables(output)
        frame = load_actual(output / "user_daily", SCHEMAS["user_daily"])
        frame.head(0).write_parquet(output / "user_daily" / "empty.parquet")
        self.assertTrue(validate(output, self.expected)["passed"])


class ProcessAndCheckpointTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_timeout_kills_process_group_and_retains_logs(self):
        child = self.root / "child.pid"
        script = ("import subprocess, sys, time; "
                  "p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(30)']); "
                  f"open({str(child)!r},'w').write(str(p.pid)); "
                  "print('started',flush=True); time.sleep(30)")
        result = run_process([sys.executable, "-c", script], self.root, os.environ,
                             self.root, .3, "process")
        self.assertTrue(result["timed_out"])
        self.assertLess(result["wall_seconds"], 2)
        self.assertIn("started", (self.root / "stdout.log").read_text())
        if child.exists():
            pid = int(child.read_text())
            status = Path(f"/proc/{pid}/stat")
            # An orphan can remain as a zombie until the host reaps it.
            if status.exists():
                self.assertEqual(status.read_text().rsplit(")", 1)[1].split()[0], "Z")

    def test_launch_failure_is_reported(self):
        result = run_process(["/definitely/not/an/executable"], self.root, os.environ,
                             self.root, 1, "process")
        self.assertIn("error", result)
        self.assertIsNone(result["exit_code"])

    def test_checkpoint_errors_warn_without_discarding_valid_events(self):
        path = self.root / "checkpoints.jsonl"
        path.write_text('\n'.join(['{"name":"parse","elapsed_seconds":1}',
                                   '{"name":"backwards","elapsed_seconds":0.5}',
                                   '{"name":"bad","elapsed_seconds":NaN}',
                                   '{"name":"badmetrics","elapsed_seconds":2,"metrics":{"rows":NaN}}',
                                   'not json', '{"name":"join","elapsed_seconds":3}']))
        result = checkpoint_results(path)
        self.assertEqual([s["duration_seconds"] for s in result["stages"]], [1, 2])
        self.assertEqual(len(result["warnings"]), 4)

    def test_only_correct_measurements_enter_performance_statistics(self):
        common = {"dataset": "dev", "input_event_lines": 100, "max_process_rss_bytes": None,
                  "checkpoints": {"stages": [], "warnings": []}}
        records = [{**common, "kind": kind, "status": status, "wall_seconds": seconds}
                   for kind, status, seconds in [("measurement", "passed", 2),
                                                ("measurement", "failed", .01),
                                                ("warmup", "passed", 10),
                                                ("profile", "passed", 100)]]
        result = summarize(records)["benchmarks"]["dev"]
        self.assertEqual(result["median_seconds"], 2)
        self.assertEqual(result["successful_measurements"], 1)
        self.assertEqual(result["input_event_lines_per_second"], 50)


class IntegrationTests(unittest.TestCase):
    def test_external_command_profiles_separately_and_writes_artifacts(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            data, expected, seed = root / "data", root / "expected", root / "seed"
            data.mkdir()
            expected.mkdir()
            (data / "manifest.json").write_text('{}')
            users, campaigns = write_tables(seed)
            users.write_csv(expected / "user_daily.csv")
            campaigns.write_csv(expected / "campaign_daily.csv")
            # This fixture generator is a standalone executable: the harness
            # knows no implementation functions, imports, or language details.
            implementation = root / "external.py"
            implementation.write_text('''import json, os, pathlib, shutil, time
started = time.perf_counter()
assert os.environ['NCPU'] == '2'
source = pathlib.Path(__file__).parent / 'seed'
output = pathlib.Path(os.environ['OUTPUT_DIR'])
for table in ('user_daily', 'campaign_daily'):
    shutil.copytree(source / table, output / table)
if os.environ['CHECKPOINTS_ENABLED'] == '1':
    time.sleep(.1)
    pathlib.Path(os.environ['CHECKPOINTS_FILE']).write_text(json.dumps(
        {'name':'copy','elapsed_seconds':time.perf_counter()-started})+'\\n')
else:
    assert 'CHECKPOINTS_FILE' not in os.environ
print(os.environ['CHECKPOINTS_ENABLED'])
''')
            # An inherited profiling path must never contaminate timed runs.
            with patch.dict(os.environ, {"CHECKPOINTS_FILE": str(root / "should-not-exist")}):
                code = main(["--name", "external", "--skip-fixtures", "--benchmark", "tiny", str(data),
                             str(expected), "--artifacts", str(root / "artifacts"), "--cpus", "2",
                             "--repetitions", "2", "--warmups", "1", "--profile-runs", "1", "--",
                             sys.executable, str(implementation)])
            self.assertEqual(code, 0)
            run_root = next((root / "artifacts").iterdir())
            report = json.loads((run_root / "results.json").read_text())
            self.assertTrue(report["complete"])
            self.assertEqual(report["summary"]["benchmarks"]["tiny"]["successful_measurements"], 2)
            self.assertEqual(len(report["runs"]), 4)
            for run in report["runs"]:
                directory = run_root / run["directory"]
                self.assertTrue((directory / "stdout.log").exists())
                self.assertTrue((directory / "validation.json").exists())
                self.assertEqual(run["checkpoint_recording_enabled"], run["kind"] == "profile")
                self.assertEqual(bool(run["checkpoints"]["stages"]), run["kind"] == "profile")
                self.assertFalse((directory / "scratch").exists())
            self.assertEqual(report["summary"]["benchmarks"]["tiny"]["profile_stages"][0]["name"], "copy")
            self.assertTrue((run_root / "report.md").exists())
            self.assertTrue((run_root / "results.csv").exists())
            self.assertFalse((root / "should-not-exist").exists())

    def test_nonzero_exit_fails_run_and_keeps_stderr(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            reference = TESTING.parent / "starter-duckdb/fixtures/E1"
            code = main(["--name", "failure", "--skip-fixtures", "--benchmark", "tiny",
                         str(reference / "input"), str(reference / "expected"),
                         "--artifacts", str(root), "--warmups", "0", "--repetitions", "1", "--profile-runs", "0",
                         "--", "sh", "-c", "echo broken >&2; exit 7"])
            self.assertEqual(code, 1)
            run_root = next(root.iterdir())
            report = json.loads((run_root / "results.json").read_text())
            self.assertEqual(report["runs"][0]["exit_code"], 7)
            self.assertIn("broken", (run_root / report["runs"][0]["directory"] / "stderr.log").read_text())


if __name__ == "__main__":
    unittest.main()
