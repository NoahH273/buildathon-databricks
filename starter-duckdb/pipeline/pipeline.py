#!/usr/bin/env python3
"""Shopping Sessions baseline: strict input parsing and DuckDB transformations.

Reads DATA_DIR (/data), writes OUTPUT_DIR (/output), spills to SCRATCH_DIR
(/scratch). Only the Python standard library and DuckDB are needed at runtime.
"""
import json
import os
from pathlib import Path
import re
import tempfile
import time

import duckdb


FIELD = rb'(?:"[^",\n]*"|[^",\n]*)'


class Checkpoints:
    """Optional JSONL profiling; the harness enables this only in separate runs."""

    def __init__(self, path, started):
        self.path = Path(path)
        self.started = started

    def mark(self, name):
        event = {"name": name, "elapsed_seconds": time.monotonic() - self.started}
        with self.path.open("a") as target:
            target.write(json.dumps(event) + "\n")


def rows(paths, width):
    """Skip each header and accept exactly the spec's LF-only CSV dialect."""
    pattern = re.compile(b",".join([FIELD] * width))
    for path in sorted(paths):
        with path.open("rb") as source:
            next(source, None)
            for line in source:
                line = line.removesuffix(b"\n")
                if line and pattern.fullmatch(line):
                    yield line


def fields(line):
    return [value[1:-1] if value.startswith(b'"') else value
            for value in line.split(b",")]


def encode_transport(line):
    # DuckDB treats CR as a line ending even with new_line='\n'. Escape CR and
    # literal % first, then reverse the escapes in SQL after CSV parsing.
    return line.replace(b"%", b"%25").replace(b"\r", b"%0D")


def stage_csv(con, paths, columns, table, work):
    path = work / f"{table}.csv"
    with path.open("wb") as target:
        target.write((",".join(columns) + "\n").encode())
        for line in rows(paths, len(columns)):
            target.write(encode_transport(line) + b"\n")
    schema = "{" + ", ".join(f"'{column}': 'VARCHAR'" for column in columns) + "}"
    projections = ", ".join(
        f"replace(replace({column}, '%0D', chr(13)), '%25', '%') AS {column}"
        for column in columns)
    con.execute(f"""CREATE TABLE {table} AS SELECT {projections}
        FROM read_csv(?, header=true, columns={schema}, auto_detect=false,
                      delim=',', new_line='\\n', nullstr='')""", [str(path)])
    path.unlink()


def stage_people(con, data, work):
    # Union by size keeps long chains cheap; the component's byte-wise minimum
    # is tracked separately from its structural root, including unused IDs.
    parent, size, smallest = {}, {}, {}

    def find(value):
        if value not in parent:
            parent[value], size[value], smallest[value] = value, 1, value
        root = value
        while parent[root] != root:
            root = parent[root]
        while parent[value] != value:
            next_value = parent[value]
            parent[value] = root
            value = next_value
        return root

    for line in rows((data / "links").glob("*.csv"), 2):
        left, right = fields(line)
        if not left or not right:
            continue
        left, right = find(left), find(right)
        if left == right:
            continue
        if size[left] < size[right]:
            left, right = right, left
        parent[right] = left
        size[left] += size.pop(right)
        smallest[left] = min(smallest[left], smallest.pop(right))

    path = work / "people-source.csv"
    with path.open("wb") as target:
        target.write(b"user_id,person_id\n")
        for value in parent:
            target.write(b'"' + value + b'","' + smallest[find(value)] + b'"\n')
    stage_csv(con, [path], ["user_id", "person_id"], "people", work)
    path.unlink()


def main():
    started = time.monotonic()
    checkpoint_path = os.environ.get("CHECKPOINTS_FILE")
    checkpoints = (Checkpoints(checkpoint_path, started)
                   if checkpoint_path and os.environ.get("CHECKPOINTS_ENABLED") != "0" else None)
    data = Path(os.environ.get("DATA_DIR", "/data"))
    output = Path(os.environ.get("OUTPUT_DIR", "/output"))
    scratch = Path(os.environ.get("SCRATCH_DIR", "/scratch"))
    scratch.mkdir(parents=True, exist_ok=True)
    with (data / "manifest.json").open() as source:
        manifest = json.load(source)

    # Each invocation owns its staging and spill files, including fixture runs.
    with tempfile.TemporaryDirectory(prefix="duckdb-", dir=scratch) as temporary:
        work = Path(temporary)
        with duckdb.connect() as con:
            con.execute("SET threads = ?", [max(1, int(os.environ.get("NCPU", os.cpu_count() or 1)))])
            memory = max(1, int(int(os.environ.get("MEM_GB", "8")) * 0.8))
            con.execute("SET memory_limit = ?", [f"{memory}GB"])
            con.execute("SET temp_directory = ?", [str(work / "spill")])
            con.execute("SET preserve_insertion_order = false")
            con.execute("SET TimeZone = 'UTC'")
            con.execute("CREATE TABLE config AS SELECT epoch_ms(?::TIMESTAMPTZ) AS window_start, "
                        "epoch_ms(?::TIMESTAMPTZ) AS window_end",
                        [manifest["window_start"], manifest["window_end"]])
            if checkpoints:
                checkpoints.mark("initialize")
            stage_people(con, data, work)
            if checkpoints:
                checkpoints.mark("resolve_linked_ids")
            for table, columns, paths in [
                ("products", ["product_id", "category"], [data / "products.csv"]),
                ("campaigns", ["campaign_id", "channel"], [data / "campaigns.csv"]),
                ("raw_events", ["event_id", "user_id", "ts", "ingest_ts", "event_type",
                                "product_id", "price_cents"], (data / "events").glob("*.csv")),
                ("raw_clicks", ["click_id", "user_id", "campaign_id", "ts"],
                 (data / "clicks").glob("*.csv")),
            ]:
                stage_csv(con, paths, columns, table, work)
                if checkpoints:
                    checkpoints.mark("parse_" + table.removeprefix("raw_"))
            print(f"parsed inputs in {time.monotonic() - started:.2f}s", flush=True)
            sql = Path(__file__).with_name("pipeline.sql").read_text()
            if checkpoints:
                position = 0
                for marker in re.finditer(r"^-- checkpoint: ([a-z_]+)[ \t]*$", sql, flags=re.MULTILINE):
                    statement = sql[position:marker.start()].strip()
                    if statement:
                        con.execute(statement)
                    checkpoints.mark(marker.group(1))
                    position = marker.end()
                if sql[position:].strip():
                    con.execute(sql[position:])
            else:
                # Official measurements use the original single-execute path:
                # no marker parsing, checkpoint callbacks, or checkpoint I/O.
                con.execute(sql)
            for table in ("user_daily", "campaign_daily"):
                folder = output / table
                folder.mkdir(parents=True, exist_ok=True)
                con.execute(f"COPY {table} TO ? (FORMAT PARQUET)",
                            [str(folder / "part-0.parquet")])
            if checkpoints:
                checkpoints.mark("write_outputs")
            print(f"wrote user_daily and campaign_daily in {time.monotonic() - started:.2f}s",
                  flush=True)


if __name__ == "__main__":
    main()
