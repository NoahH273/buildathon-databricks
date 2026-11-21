#!/usr/bin/env python3
"""Rust ingestion/attribution with DuckDB deduplication, sessions and final output."""
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import time

import duckdb


class Checkpoints:
    def __init__(self, path, started):
        self.path, self.started = Path(path), started
        self.work, self.peak_scratch = None, 0

    def mark(self, name, metrics=None):
        event = {"name": name, "elapsed_seconds": time.monotonic() - self.started}
        event["metrics"] = dict(metrics or {})
        if self.work is not None:
            observed = sum(p.stat().st_size for p in self.work.rglob("*") if p.is_file())
            self.peak_scratch = max(self.peak_scratch, observed)
            event["metrics"].update(observed_scratch_bytes=observed,
                                    peak_observed_scratch_bytes=self.peak_scratch)
        with self.path.open("a") as target:
            target.write(json.dumps(event) + "\n")


def execute_stages(con, sql, checkpoints):
    if checkpoints is None:
        con.execute(sql)
        return
    position = 0
    for marker in re.finditer(r"^-- checkpoint: ([a-z_]+)[ \t]*$", sql, re.MULTILINE):
        if sql[position:marker.start()].strip():
            con.execute(sql[position:marker.start()])
        checkpoints.mark(marker.group(1))
        position = marker.end()
    if sql[position:].strip():
        con.execute(sql[position:])


def main():
    started = time.monotonic()
    checkpoint_file = os.environ.get("CHECKPOINTS_FILE")
    checkpoints = (Checkpoints(checkpoint_file, started)
                   if checkpoint_file and os.environ.get("CHECKPOINTS_ENABLED") != "0" else None)
    data = Path(os.environ.get("DATA_DIR", "/data")).resolve()
    output = Path(os.environ.get("OUTPUT_DIR", "/output")).resolve()
    scratch = Path(os.environ.get("SCRATCH_DIR", "/scratch")).resolve()
    binary = os.environ.get("HYBRID_BIN", "/app/bin/shopping-hybrid")
    threads = max(1, int(os.environ.get("NCPU", os.cpu_count() or 1)))
    mem_gb = max(1, int(os.environ.get("MEM_GB", "8")))
    budget = int(os.environ.get("HYBRID_MEMORY_BYTES", int(mem_gb * .65 * 1024**3)))
    if budget < 1:
        raise ValueError("HYBRID_MEMORY_BYTES must be positive")
    scratch.mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix="duckdb-hybrid-", dir=scratch))
    if checkpoints:
        checkpoints.work = work
    keep = os.environ.get("HYBRID_KEEP_WORK") == "1"
    ingest, handoff, metrics = (work / name for name in ("ingest", "handoff", "metrics"))
    source_bytes = sum(p.stat().st_size for folder in ("events", "clicks") for p in (data / folder).glob("*.csv"))
    buckets = int(os.environ.get("HYBRID_BUCKETS", 1 if source_bytes < 8*1024**2 else min(threads, 16)))
    if buckets < 1:
        raise ValueError("HYBRID_BUCKETS must be positive")

    def connect():
        con = duckdb.connect(str(work / "pipeline.duckdb"))
        con.execute("SET threads = ?", [threads])
        con.execute("SET memory_limit = ?", [f"{max(1, int(mem_gb * .8))}GB"])
        con.execute("SET temp_directory = ?", [str(work / "spill")])
        con.execute("SET preserve_insertion_order = false")
        con.execute("SET TimeZone = 'UTC'")
        return con

    def rust(*arguments):
        subprocess.run([binary, *map(str, arguments)], check=True)

    def read_view(con, name, path):
        con.read_parquet(str(path)).create_view(name, replace=True)

    try:
        rust("ingest", "--data", data, "--out", ingest, "--threads", threads)
        if checkpoints:
            diagnostics = json.loads((ingest / "diagnostics.json").read_text())
            diagnostics["staging_bytes"] = sum(p.stat().st_size for p in ingest.rglob("*.parquet"))
            checkpoints.mark("rust_ingest", diagnostics)
        with connect() as con:
            read_view(con, "candidates", ingest / "event_candidates/*.parquet")
            read_view(con, "click_candidates", ingest / "click_candidates/*.parquet")
            read_view(con, "categories", ingest / "categories.parquet")
            # Keep dimensions as persisted tables across the Rust phase.
            con.execute("CREATE TABLE campaigns AS SELECT * FROM read_parquet(?)", [str(ingest / "campaigns.parquet")])
            con.execute("CREATE TABLE categories_saved AS SELECT * FROM categories")
            mode = os.environ.get("HYBRID_DEDUP", "arg_min")
            if mode == "arg_min":
                con.execute("""CREATE TABLE events AS
                    SELECT event_id, winner.person_id AS person_id, winner.event_ms AS event_ms,
                           winner.purchase_cents AS purchase_cents, winner.category_code AS category_code
                    FROM (SELECT event_id, arg_min(struct_pack(is_valid := is_valid, person_id := person_id,
                          event_ms := event_ms, purchase_cents := purchase_cents, category_code := category_code), ingest_ms) AS winner
                          FROM candidates GROUP BY event_id) WHERE winner.is_valid""")
            elif mode == "row_number":
                con.execute("""CREATE TABLE events AS SELECT event_id, person_id, event_ms, purchase_cents, category_code
                    FROM (SELECT *, row_number() OVER (PARTITION BY event_id ORDER BY ingest_ms) AS rn
                          FROM candidates) WHERE rn = 1 AND is_valid""")
            else:
                raise ValueError("HYBRID_DEDUP must be arg_min or row_number")
            if checkpoints:
                checkpoints.mark("duckdb_deduplicate")
            execute_stages(con, Path(__file__).with_name("pipeline.sql").read_text(), checkpoints)
            handoff.mkdir()
            for relation, select in (
                ("sessions", f"SELECT person_key, start_ms, revenue_cents, has_purchase, person_key % {buckets} AS bucket FROM sessions JOIN people USING (person_id)"),
                ("clicks", f"SELECT person_key, click_ms, campaign_code, click_id, person_key % {buckets} AS bucket FROM click_candidates JOIN people USING (person_id) WHERE click_ms BETWEEN first_start - 604800000 AND last_start"),
            ):
                con.execute(f"COPY ({select}) TO ? (FORMAT PARQUET, COMPRESSION SNAPPY, PARTITION_BY (bucket))", [str(handoff / relation)])
            if checkpoints:
                diagnostics = {"buckets": buckets, "sessions": con.execute("SELECT count(*) FROM sessions").fetchone()[0],
                               "valid_winning_events": con.execute("SELECT sum(events) FROM sessions").fetchone()[0],
                               "handoff_bytes": sum(p.stat().st_size for p in handoff.rglob("*.parquet"))}
                checkpoints.mark("duckdb_export_handoff", diagnostics)
            con.execute("DROP VIEW candidates; DROP VIEW click_candidates; DROP VIEW categories")
            con.execute("DROP TABLE sessions; DROP TABLE people")
        shutil.copyfile(ingest / "campaigns.parquet", handoff / "campaigns.parquet")
        if not keep:
            shutil.rmtree(ingest)
        if checkpoints:
            checkpoints.mark("release_ingest_and_duckdb")
        # All DuckDB memory is released before the memory-intensive Rust phase.
        rust("attribute", "--input", handoff, "--out", metrics, "--threads", threads, "--memory-bytes", budget)
        deferred = json.loads((metrics / "deferred_buckets.json").read_text())
        if checkpoints:
            diagnostics = json.loads((metrics / "diagnostics.json").read_text())
            checkpoints.mark("rust_attribute", diagnostics)
        sorted_root = work / "sorted"
        for bucket in deferred:
            directory = sorted_root / f"bucket={bucket}"
            directory.mkdir(parents=True)
            with connect() as con:
                for relation, columns, order in (
                    ("sessions", "person_key, start_ms, revenue_cents, has_purchase", "person_key, start_ms"),
                    ("clicks", "person_key, click_ms, campaign_code, click_id", "person_key, click_ms"),
                ):
                    parts = list((handoff / relation / f"bucket={bucket}").glob("*.parquet"))
                    if parts:
                        con.read_parquet([str(p) for p in parts]).create_view("sort_input", replace=True)
                        query = f"SELECT {columns} FROM sort_input ORDER BY {order}"
                    else:
                        query = ("SELECT NULL::BIGINT AS person_key, NULL::BIGINT AS click_ms, "
                                 "NULL::INTEGER AS campaign_code, NULL::BLOB AS click_id WHERE false")
                    con.execute(f"COPY ({query}) TO ? (FORMAT PARQUET, COMPRESSION SNAPPY, PRESERVE_ORDER true)", [str(directory / f"{relation}.parquet")])
            shutil.copyfile(handoff / "campaigns.parquet", sorted_root / "campaigns.parquet")
            rust("attribute", "--input", sorted_root, "--out", metrics, "--threads", 1,
                 "--memory-bytes", budget, "--sorted-bucket", bucket)
        if checkpoints and deferred:
            checkpoints.mark("sorted_fallback", {"buckets": deferred})
        if not keep:
            shutil.rmtree(handoff)
            if sorted_root.exists():
                shutil.rmtree(sorted_root)
        with connect() as con:
            con.execute("""CREATE TABLE campaign_daily AS
                SELECT c.campaign_id, c.channel, DATE '1970-01-01' + m.epoch_day::INTEGER AS day,
                       sum(m.sessions)::BIGINT AS sessions, sum(m.purchasing_sessions)::BIGINT AS purchasing_sessions,
                       sum(m.revenue_cents)::BIGINT AS revenue_cents, sum(m.assisted_sessions)::BIGINT AS assisted_sessions
                FROM read_parquet(?) m JOIN campaigns c USING (campaign_code)
                GROUP BY c.campaign_id, c.channel, m.epoch_day""", [str(metrics / "bucket-*.parquet")])
            for table in ("user_daily", "campaign_daily"):
                folder = output / table
                folder.mkdir(parents=True, exist_ok=True)
                con.execute(f"COPY {table} TO ? (FORMAT PARQUET)", [str(folder / "part-0.parquet")])
            if checkpoints:
                checkpoints.mark("duckdb_write_outputs", {"peak_observed_scratch_bytes": sum(p.stat().st_size for p in work.rglob("*") if p.is_file())})
        print(f"wrote hybrid outputs in {time.monotonic() - started:.3f}s", flush=True)
    finally:
        if keep:
            print(f"preserved hybrid scratch: {work}", flush=True)
        else:
            shutil.rmtree(work)


if __name__ == "__main__":
    main()
