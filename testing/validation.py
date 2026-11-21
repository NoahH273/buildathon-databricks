"""Implementation-independent validation of the Shopping Sessions output contract."""
from pathlib import Path

import polars as pl

SCHEMAS = {
    "user_daily": {"user_id": pl.String, "day": pl.Date, "sessions": pl.Int128,
                   "events": pl.Int128, "revenue_cents": pl.Int128, "top_category": pl.String},
    "campaign_daily": {"campaign_id": pl.String, "channel": pl.String, "day": pl.Date,
                       "sessions": pl.Int128, "purchasing_sessions": pl.Int128,
                       "revenue_cents": pl.Int128, "assisted_sessions": pl.Int128},
}
KEYS = {"user_daily": ["user_id", "day"], "campaign_daily": ["campaign_id", "day"]}


def parquet_files(folder):
    folder = Path(folder)
    if folder.is_symlink() or not folder.is_dir():
        raise ValueError(f"expected an output directory: {folder}")
    files = sorted(p for p in folder.glob("*.parquet")
                   if not p.name.startswith((".", "_")) and not p.is_symlink() and p.is_file())
    if not files:
        raise ValueError(f"no visible regular Parquet files in {folder}")
    return files


def check_columns(frame, schema, source):
    if set(frame.columns) != set(schema):
        raise ValueError(f"{source}: expected columns {list(schema)}, got {frame.columns}")


def load_actual(folder, schema):
    frames = []
    for path in parquet_files(folder):
        frame = pl.read_parquet(path)
        check_columns(frame, schema, path)
        for column, expected in schema.items():
            actual = frame.schema[column]
            if expected == pl.Int128 and actual.is_integer():
                continue
            if expected == pl.String and actual.base_type() in (pl.String, pl.Categorical, pl.Enum):
                continue
            if actual != expected:
                raise ValueError(f"{path}: {column} has type {actual}, expected {expected}")
        frames.append(frame.select(list(schema)).cast(schema))
    return pl.concat(frames)


def load_expected(root, table):
    schema = SCHEMAS[table]
    path = Path(root) / f"{table}.csv"
    if not path.is_file():
        return load_actual(Path(root) / table, schema)
    frame = pl.read_csv(path, schema_overrides={k: pl.String if v == pl.Date else v
                                              for k, v in schema.items()})
    check_columns(frame, schema, path)
    return frame.select(list(schema)).with_columns(pl.col("day").str.to_date()).cast(schema)


def validate(output, expected):
    """Exact comparison, duplicate detection, schema checks, and rule invariants."""
    result = {"passed": True, "tables": {}, "invariants": []}
    frames = {}
    for table, schema in SCHEMAS.items():
        detail = {"passed": False}
        result["tables"][table] = detail
        try:
            reference = load_expected(expected, table)
            actual = load_actual(Path(output) / table, schema)
            frames[table] = actual
            duplicate_keys = actual.height - actual.n_unique(subset=KEYS[table])
            missing = reference.join(actual, on=list(schema), how="anti", nulls_equal=True)
            extra = actual.join(reference, on=list(schema), how="anti", nulls_equal=True)
            nulls = {c: actual[c].null_count() for c in schema if c != "top_category"
                     and actual[c].null_count()}
            files = parquet_files(Path(output) / table)
            detail.update(rows=actual.height, expected_rows=reference.height,
                          duplicate_keys=duplicate_keys, unexpected_nulls=nulls,
                          missing_rows=missing.height, extra_rows=extra.height,
                          missing_examples=missing.head(5).to_dicts(),
                          extra_examples=extra.head(5).to_dicts(), parquet_files=len(files),
                          output_bytes=sum(p.stat().st_size for p in files))
            detail["passed"] = (actual.height == reference.height and not duplicate_keys
                                and not nulls and missing.is_empty() and extra.is_empty())
        except (ValueError, OSError, pl.exceptions.PolarsError) as error:
            detail["error"] = str(error)
        result["passed"] &= detail["passed"]

    def invariant(name, passed):
        result["invariants"].append({"name": name, "passed": bool(passed)})
        result["passed"] &= bool(passed)

    if len(frames) == 2:
        users, campaigns = frames["user_daily"], frames["campaign_daily"]
        invariant("equal_session_totals", users["sessions"].sum() == campaigns["sessions"].sum())
        invariant("equal_revenue_totals", users["revenue_cents"].sum() == campaigns["revenue_cents"].sum())
        invariant("assists_cover_nonorganic_sessions", campaigns["assisted_sessions"].sum()
                  >= campaigns.filter(pl.col("campaign_id") != "organic")["sessions"].sum())
        invariant("valid_user_counts", users.filter(
            (pl.col("sessions") <= 0) | (pl.col("events") < pl.col("sessions"))
            | (pl.col("revenue_cents") < 0)).is_empty())
        invariant("top_category_matches_purchase_presence", users.filter(
            (pl.col("revenue_cents") == 0) != pl.col("top_category").is_null()).is_empty())
        invariant("valid_campaign_counts", campaigns.filter(
            (pl.col("sessions") < 0) | (pl.col("assisted_sessions") < 0)
            | (pl.col("purchasing_sessions") < 0)
            | (pl.col("purchasing_sessions") > pl.col("sessions"))
            | (pl.col("revenue_cents") < 0)
            | ((pl.col("sessions") == 0) & (pl.col("assisted_sessions") == 0))).is_empty())
        invariant("organic_has_no_assists", campaigns.filter(
            (pl.col("campaign_id") == "organic")
            & ((pl.col("channel") != "organic") | (pl.col("assisted_sessions") != 0))).is_empty())
    return result
