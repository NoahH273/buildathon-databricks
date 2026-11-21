"""Additional end-to-end regressions beyond the supplied rule fixtures."""
import csv
from datetime import date
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import duckdb


PIPELINE = Path(__file__).resolve().parents[1] / "pipeline" / "pipeline.py"
HEADERS = {
    "events": "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents",
    "clicks": "click_id,user_id,campaign_id,ts",
    "links": "user_id_a,user_id_b",
}
DAY = date(2026, 10, 3)


def csv_rows(values):
    # Quote all fields so embedded carriage returns stay inside a whole field.
    stream = io.StringIO(newline="")
    csv.writer(stream, lineterminator="\n", quoting=csv.QUOTE_ALL).writerows(values)
    return stream.getvalue().encode()


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.data = self.root / "data"
        self.data.mkdir()
        for folder, header in HEADERS.items():
            (self.data / folder).mkdir()
            # Exercise multiple header-only files as well as populated parts.
            for part in ("part-0.csv", "empty.csv"):
                (self.data / folder / part).write_bytes(header.encode() + b"\n")
        (self.data / "products.csv").write_text("product_id,category\np1,shoes\np2,books\n")
        (self.data / "campaigns.csv").write_text("campaign_id,channel\nc_1,search\nc_2,social\n")
        (self.data / "manifest.json").write_text(json.dumps({
            "window_start": "2026-10-01T00:00:00Z", "window_end": "2026-10-15T00:00:00Z"}))

    def add(self, folder, values):
        with (self.data / folder / "part-0.csv").open("ab") as target:
            target.write(csv_rows(values))

    def run_pipeline(self):
        output = self.root / "output"
        subprocess.run([sys.executable, str(PIPELINE)], check=True, capture_output=True,
                       env=dict(os.environ, DATA_DIR=str(self.data), OUTPUT_DIR=str(output),
                                SCRATCH_DIR=str(self.root), NCPU="2", MEM_GB="2"))
        with duckdb.connect() as con:
            result = {}
            for table in ("user_daily", "campaign_daily"):
                cursor = con.execute("SELECT * FROM read_parquet(?) ORDER BY 1, day",
                                     [str(output / table / "part-0.parquet")])
                self.assertEqual(dict((d[0], str(d[1])) for d in cursor.description)["day"], "DATE")
                result[table] = cursor.fetchall()
        self.assertFalse(list(self.root.glob("duckdb-*")), "temporary files must be cleaned up")
        return result

    def test_header_only_inputs_write_typed_empty_outputs(self):
        self.assertEqual(self.run_pipeline(), {"user_daily": [], "campaign_daily": []})

    def test_carriage_returns_and_percent_escapes_remain_distinct(self):
        self.add("events", [
            [f"e{i}", user, "2026-10-03T10:00:00Z", "2026-10-03T10:01:00Z",
             "view", "p1", ""]
            for i, user in enumerate(["u", "u\r", "u%0D", "u%25", " u", "u "])
        ])
        self.add("clicks", [["k1", "u\r", "c_1", "2026-10-03T09:00:00Z"],
                            ["k2", "u%0D", "c_2", "2026-10-03T09:00:00Z\r"]])
        result = self.run_pipeline()
        self.assertEqual(result["user_daily"], [
            (user, DAY, 1, 1, 0, None) for user in sorted(["u", "u\r", "u%0D", "u%25", " u", "u "])])
        self.assertEqual(result["campaign_daily"], [
            ("c_1", "search", DAY, 1, 0, 0, 1), ("organic", "organic", DAY, 5, 0, 0, 0)])

    def test_long_link_chain_and_overlapping_assist_intervals(self):
        self.add("links", [[f"u{i:04}", f"u{i - 1:04}"] for i in range(800, 0, -1)])
        self.add("links", [[" a", "u0000"], ["u0800", "u0800"], ["u0001", "u0000"]])
        self.add("events", [
            ["e1", "u0800", "2026-10-03T10:00:00Z", "2026-10-03T10:01:00Z", "view", "p1", ""],
            ["e2", "u0400", "2026-10-03T10:30:00Z", "2026-10-03T10:31:00Z", "purchase", "p2", "400"],
            ["e3", "u0001", "2026-10-03T11:00:00Z", "2026-10-03T11:01:00Z", "cart", "p1", ""],
            ["e4", "u0001", "2026-10-10T10:00:00Z", "2026-10-10T10:01:00Z", "view", "p1", ""],
            ["e5", "u0001", "1791628200001", "2026-10-10T11:01:00Z", "view", "p1", ""],
        ])
        self.add("clicks", [
            ["k1", "u0500", "c_1", "2026-10-02T10:00:00Z"],
            ["k2", "u0500", "c_1", "2026-10-03T10:00:00Z"],
            ["k0", "u0200", "c_2", "2026-10-03T10:00:00Z"],
        ])
        result = self.run_pipeline()
        self.assertEqual(result["user_daily"], [
            (" a", DAY, 1, 3, 400, "books"), (" a", date(2026, 10, 10), 2, 2, 0, None)])
        self.assertEqual(result["campaign_daily"], [
            ("c_1", "search", DAY, 0, 0, 0, 1),
            ("c_1", "search", date(2026, 10, 10), 0, 0, 0, 1),
            ("c_2", "social", DAY, 1, 1, 400, 1),
            ("c_2", "social", date(2026, 10, 10), 1, 0, 0, 1),
            ("organic", "organic", date(2026, 10, 10), 1, 0, 0, 0)])


if __name__ == "__main__":
    unittest.main()
