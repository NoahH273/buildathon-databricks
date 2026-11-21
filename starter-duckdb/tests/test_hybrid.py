"""Hybrid-specific handoff, partition, deduplication and fallback regressions."""
import json
import os
from pathlib import Path
import subprocess
import unittest
from unittest.mock import patch

import duckdb
import test_pipeline as contract


class HybridTests(unittest.TestCase):
    def setUp(self):
        self.fixture = contract.PipelineTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.temp.cleanup)
        self.data = self.fixture.data
        self.root = self.fixture.root
        self.binary = os.environ.get("HYBRID_BIN", "/app/bin/shopping-hybrid")

    def populate(self):
        self.fixture.add("links", [[" a\r", "u2"], ["u2", "u1"]])
        self.fixture.add("events", [
            ["badwinner", "u1", "bad", "2026-10-03T10:00:01Z", "view", "p1", ""],
            ["badwinner", "other", "2026-10-03T10:00:00Z", "2026-10-03T10:00:02Z", "purchase", "p1", "999"],
            ["e1", "u1", "2026-10-03T10:00:00Z", "2026-10-03T10:01:00Z", "purchase", "p1", "0250"],
            ["e2", "u2", "2026-10-10T10:00:00Z", "2026-10-10T10:01:00Z", "view", "p1", ""],
            ["e3", "unlinked", "2026-10-03T10:00:00Z", "2026-10-03T10:01:00Z", "view", "p1", ""],
        ])
        self.fixture.add("clicks", [["é\r", "u2", "c_1", "2026-10-03T10:00:00Z"],
                                    [" a", "u1", "c_2", "2026-10-03T10:00:00Z"],
                                    [" a", "u1", "c_2", "2026-10-03T10:00:00Z"]])
        # A person can span multiple physical files and linked IDs.
        for folder in ("events", "clicks"):
            original = self.data / folder / "part-0.csv"
            lines = original.read_bytes().split(b"\n")
            (self.data / folder / "part-1.csv").write_bytes(lines[0]+b"\n"+b"\n".join(lines[2::2])+b"\n")
            original.write_bytes(lines[0]+b"\n"+b"\n".join(lines[1::2])+b"\n")

    def test_partition_counts_dedup_plans_and_forced_fallback_agree(self):
        self.populate()
        baseline = None
        for options in [dict(HYBRID_BUCKETS="1",HYBRID_DEDUP="arg_min"),
                        dict(HYBRID_BUCKETS="4",HYBRID_DEDUP="row_number"),
                        dict(HYBRID_BUCKETS="8",HYBRID_MEMORY_BYTES="65536")]:
            with self.subTest(options=options), patch.dict(os.environ, options):
                actual = self.fixture.run_pipeline()
                if baseline is None:
                    baseline = actual
                    self.assertEqual(sum(row[4] for row in actual["user_daily"]),250)
                    self.assertEqual(len(actual["user_daily"]),3)
                else:
                    self.assertEqual(actual,baseline)

    def test_parquet_preserves_invalid_candidates_nulls_and_byte_ids(self):
        self.populate()
        out = self.root / "ingest"
        subprocess.run([self.binary,"ingest","--data",str(self.data),"--out",str(out),"--threads","2"],check=True)
        with duckdb.connect() as con:
            candidates = con.execute("SELECT is_valid, person_id, event_ms FROM read_parquet(?) WHERE event_id='badwinner' ORDER BY ingest_ms",
                                     [str(out / "event_candidates/*.parquet")]).fetchall()
            self.assertEqual(candidates[0],(False,None,None))
            self.assertTrue(candidates[1][0])
            clicks = con.execute("SELECT DISTINCT person_id,click_id FROM read_parquet(?) ORDER BY click_id",
                                 [str(out / "click_candidates/*.parquet")]).fetchall()
            self.assertEqual(clicks,[(" a\r", b" a"),(" a\r","é\r".encode())])

    def test_missing_click_partition_and_typed_empty_outputs(self):
        with patch.dict(os.environ, {"HYBRID_BUCKETS":"4","HYBRID_MEMORY_BYTES":"65536"}):
            self.assertEqual(self.fixture.run_pipeline(), {"user_daily":[],"campaign_daily":[]})
            self.fixture.add("events", [["e1","u1","2026-10-03T10:00:00Z","2026-10-03T10:01:00Z","view","p1",""]])
            actual = self.fixture.run_pipeline()
            self.assertEqual(actual["campaign_daily"],[("organic","organic",contract.DAY,1,0,0,0)])

    def test_attribute_reads_every_file_in_a_bucket(self):
        self.populate()
        with patch.dict(os.environ,{"HYBRID_KEEP_WORK":"1","HYBRID_BUCKETS":"1"}):
            output = self.root / "output"
            env = dict(os.environ,DATA_DIR=str(self.data),OUTPUT_DIR=str(output),SCRATCH_DIR=str(self.root),NCPU="2",MEM_GB="2")
            subprocess.run([os.sys.executable,str(contract.PIPELINE)],env=env,check=True,capture_output=True)
        work = next(self.root.glob("duckdb-hybrid-*"))
        with duckdb.connect() as con:
            for relation in ("sessions","clicks"):
                folder = work / "handoff" / relation / "bucket=0"
                # Read before writing fresh shards, and keep the same explicit
                # columns/types. DuckDB supplies the interchange writer here.
                con.execute("CREATE OR REPLACE TABLE split_rows AS SELECT * EXCLUDE(bucket) FROM read_parquet(?)",[str(folder / "*.parquet")])
                for path in folder.glob("*.parquet"):
                    path.unlink()
                con.execute("COPY (SELECT * FROM split_rows LIMIT 1) TO ? (FORMAT PARQUET, COMPRESSION SNAPPY)",[str(folder / "a.parquet")])
                con.execute("COPY (SELECT * FROM split_rows OFFSET 1) TO ? (FORMAT PARQUET, COMPRESSION SNAPPY)",[str(folder / "b.parquet")])
        metrics = self.root / "metrics"
        subprocess.run([self.binary,"attribute","--input",str(work / "handoff"),"--out",str(metrics),"--threads","2","--memory-bytes",str(256*1024**2)],check=True)
        with duckdb.connect() as con:
            for source in (work / "metrics",metrics):
                values = con.execute("SELECT * FROM read_parquet(?) ORDER BY 1,2",[str(source / "bucket-*.parquet")]).fetchall()
                if source == work / "metrics":
                    expected = values
                else:
                    self.assertEqual(values,expected)
        self.assertEqual(json.loads((metrics / "deferred_buckets.json").read_text()),[])
