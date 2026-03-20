"""@bruin

name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default
depends:
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: append

columns:
  - name: trip_id
    type: integer
    description: Deterministic hash id for the trip row.
    checks:
      - name: not_null
  - name: taxi_type
    type: string
    description: NYC taxi type.
    checks:
      - name: accepted_values
        value: [yellow, green]
  - name: vendor_id
    type: integer
    description: TLC vendor id.
    checks:
      - name: not_null
  - name: pickup_datetime
    type: string
    description: Pickup timestamp (ISO string, cast downstream).
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: string
    description: Dropoff timestamp (ISO string, cast downstream).
    checks:
      - name: not_null
  - name: pickup_location_id
    type: integer
    description: Pickup location id.
    checks:
      - name: not_null
  - name: dropoff_location_id
    type: integer
    description: Dropoff location id.
    checks:
      - name: not_null
  - name: passenger_count
    type: integer
    description: Passenger count.
    checks:
      - name: non_negative
  - name: trip_distance_miles
    type: float
    description: Trip distance in miles.
    checks:
      - name: non_negative
  - name: payment_type_id
    type: integer
    description: TLC payment type id.
    checks:
      - name: not_null
  - name: fare_amount
    type: float
    description: Fare amount.
    checks:
      - name: non_negative
  - name: tip_amount
    type: float
    description: Tip amount.
    checks:
      - name: non_negative
  - name: tolls_amount
    type: float
    description: Tolls amount.
    checks:
      - name: non_negative
  - name: total_amount
    type: float
    description: Total amount.
    checks:
      - name: non_negative
  - name: source_month
    type: string
    description: Source file month (YYYY-MM).
    checks:
      - name: not_null
  - name: source_url
    type: string
    description: Source parquet URL.
    checks:
      - name: not_null
  - name: loaded_at
    type: string
    description: Ingestion load timestamp (ISO string).
    checks:
      - name: not_null

@bruin"""

import json
import os
from dataclasses import dataclass
from datetime import date, datetime
from typing import Iterable

import pandas as pd

BASE_TLC_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ALLOWED_TAXI_TYPES = ("yellow", "green")
PICKUP_COLUMNS = {
    "yellow": "tpep_pickup_datetime",
    "green": "lpep_pickup_datetime",
}
DROPOFF_COLUMNS = {
    "yellow": "tpep_dropoff_datetime",
    "green": "lpep_dropoff_datetime",
}


@dataclass(frozen=True)
class RunWindow:
    start_date: date
    end_date: date
    start_ts: pd.Timestamp
    end_ts_exclusive: pd.Timestamp


def _get_required_column(df: pd.DataFrame, candidates: list[str], default=None) -> pd.Series:
    for column in candidates:
        if column in df.columns:
            return df[column]

    if default is None:
        raise KeyError(f"Missing required columns: expected one of {candidates}")

    return pd.Series([default] * len(df), index=df.index)


def _parse_taxi_types_from_env() -> list[str]:
    raw_vars = os.getenv("BRUIN_VARS", "{}")
    try:
        vars_data = json.loads(raw_vars)
    except json.JSONDecodeError:
        vars_data = {}

    taxi_types = vars_data.get("taxi_types", ["yellow"])
    if isinstance(taxi_types, str):
        taxi_types = [taxi_types]

    normalized = []
    for value in taxi_types:
        taxi_type = str(value).strip().lower()
        if taxi_type in ALLOWED_TAXI_TYPES and taxi_type not in normalized:
            normalized.append(taxi_type)

    if not normalized:
        raise ValueError("No valid taxi types provided. Use 'yellow' and/or 'green'.")
    return normalized


def _parse_bruin_date(env_name: str) -> date:
    raw_value = os.getenv(env_name)
    if raw_value is None:
        raise ValueError(f"{env_name} is required.")

    cleaned = raw_value.strip().replace("Z", "+00:00")
    try:
        if "T" in cleaned or " " in cleaned:
            return datetime.fromisoformat(cleaned).date()
        return date.fromisoformat(cleaned)
    except ValueError as exc:
        raise ValueError(f"{env_name} must be in a valid ISO date/datetime format. Got: {raw_value}") from exc


def _parse_run_window() -> RunWindow:
    start_date = _parse_bruin_date("BRUIN_START_DATE")
    end_date = _parse_bruin_date("BRUIN_END_DATE")

    if end_date < start_date:
        raise ValueError("BRUIN_END_DATE must be greater than or equal to BRUIN_START_DATE.")

    return RunWindow(
        start_date=start_date,
        end_date=end_date,
        start_ts=pd.Timestamp(start_date),
        end_ts_exclusive=pd.Timestamp(end_date) + pd.Timedelta(days=1),
    )


def _to_naive_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", utc=True).dt.tz_localize(None)


def _month_starts_between(start: date, end: date) -> Iterable[date]:
    cursor = date(start.year, start.month, 1)
    last_month = date(end.year, end.month, 1)
    while cursor <= last_month:
        yield cursor
        if cursor.month == 12:
            cursor = date(cursor.year + 1, 1, 1)
        else:
            cursor = date(cursor.year, cursor.month + 1, 1)


def _read_month_file(source_url: str, taxi_type: str, run_window: RunWindow) -> pd.DataFrame:
    pickup_col = PICKUP_COLUMNS[taxi_type]
    parquet_filters = [
        (pickup_col, ">=", run_window.start_ts),
        (pickup_col, "<", run_window.end_ts_exclusive),
    ]

    try:
        return pd.read_parquet(source_url, filters=parquet_filters)
    except Exception as filter_exc:
        print(f"[warn] parquet filter read failed for {source_url}: {filter_exc}; retrying full read")
        return pd.read_parquet(source_url)


def _normalize_tlc_frame(
    df: pd.DataFrame,
    taxi_type: str,
    source_month: str,
    source_url: str,
    run_window: RunWindow,
) -> pd.DataFrame:
    pickup_col = PICKUP_COLUMNS[taxi_type]
    dropoff_col = DROPOFF_COLUMNS[taxi_type]

    normalized = pd.DataFrame(
        {
            "taxi_type": taxi_type,
            "vendor_id": pd.to_numeric(
                _get_required_column(df, ["VendorID", "vendorid", "vendor_id"], 0), errors="coerce"
            ),
            "pickup_datetime": _to_naive_datetime(_get_required_column(df, [pickup_col])),
            "dropoff_datetime": _to_naive_datetime(_get_required_column(df, [dropoff_col])),
            "pickup_location_id": pd.to_numeric(
                _get_required_column(df, ["PULocationID", "pu_location_id"], 0), errors="coerce"
            ),
            "dropoff_location_id": pd.to_numeric(
                _get_required_column(df, ["DOLocationID", "do_location_id"], 0), errors="coerce"
            ),
            "passenger_count": pd.to_numeric(_get_required_column(df, ["passenger_count"], 0), errors="coerce"),
            "trip_distance_miles": pd.to_numeric(
                _get_required_column(df, ["trip_distance"], 0), errors="coerce"
            ),
            "payment_type_id": pd.to_numeric(_get_required_column(df, ["payment_type"], 0), errors="coerce"),
            "fare_amount": pd.to_numeric(_get_required_column(df, ["fare_amount"], 0), errors="coerce"),
            "tip_amount": pd.to_numeric(_get_required_column(df, ["tip_amount"], 0), errors="coerce"),
            "tolls_amount": pd.to_numeric(_get_required_column(df, ["tolls_amount"], 0), errors="coerce"),
            "total_amount": pd.to_numeric(_get_required_column(df, ["total_amount"], 0), errors="coerce"),
        }
    )

    normalized = normalized.dropna(subset=["pickup_datetime", "dropoff_datetime"])
    clean_mask = (
        (normalized["pickup_datetime"] >= run_window.start_ts)
        & (normalized["pickup_datetime"] < run_window.end_ts_exclusive)
        & (normalized["dropoff_datetime"] > normalized["pickup_datetime"])
        & (normalized["vendor_id"] > 0)
        & (normalized["passenger_count"] > 0)
        & (normalized["trip_distance_miles"] >= 0)
        & (normalized["fare_amount"] >= 0)
        & (normalized["tip_amount"] >= 0)
        & (normalized["tolls_amount"] >= 0)
        & (normalized["total_amount"] >= 0)
    )
    normalized = normalized[clean_mask]

    key_columns = normalized[
        [
            "taxi_type",
            "vendor_id",
            "pickup_datetime",
            "dropoff_datetime",
            "pickup_location_id",
            "dropoff_location_id",
            "fare_amount",
            "total_amount",
        ]
    ].astype(str)
    hash_values = pd.util.hash_pandas_object(key_columns, index=False).astype("uint64")
    normalized["trip_id"] = (hash_values % 9_000_000_000_000_000).astype("int64")

    int_columns = ["vendor_id", "pickup_location_id", "dropoff_location_id", "passenger_count", "payment_type_id"]
    for column_name in int_columns:
        normalized[column_name] = normalized[column_name].fillna(0).astype("int64")

    float_columns = ["trip_distance_miles", "fare_amount", "tip_amount", "tolls_amount", "total_amount"]
    for column_name in float_columns:
        normalized[column_name] = normalized[column_name].fillna(0.0)

    normalized["pickup_datetime"] = normalized["pickup_datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")
    normalized["dropoff_datetime"] = normalized["dropoff_datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")

    normalized["source_month"] = source_month
    normalized["source_url"] = source_url
    normalized["loaded_at"] = pd.Timestamp.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    return normalized[
        [
            "trip_id",
            "taxi_type",
            "vendor_id",
            "pickup_datetime",
            "dropoff_datetime",
            "pickup_location_id",
            "dropoff_location_id",
            "passenger_count",
            "trip_distance_miles",
            "payment_type_id",
            "fare_amount",
            "tip_amount",
            "tolls_amount",
            "total_amount",
            "source_month",
            "source_url",
            "loaded_at",
        ]
    ]


def materialize():
    run_window = _parse_run_window()
    taxi_types = _parse_taxi_types_from_env()

    output_frames: list[pd.DataFrame] = []

    for taxi_type in taxi_types:
        for month_start in _month_starts_between(run_window.start_date, run_window.end_date):
            month_tag = month_start.strftime("%Y-%m")
            source_url = f"{BASE_TLC_URL}/{taxi_type}_tripdata_{month_tag}.parquet"
            try:
                raw_frame = _read_month_file(source_url, taxi_type, run_window)
            except Exception as exc:
                print(f"[skip] {source_url} ({exc})")
                continue

            prepared_frame = _normalize_tlc_frame(
                raw_frame,
                taxi_type,
                month_tag,
                source_url,
                run_window,
            )

            if prepared_frame.empty:
                print(f"[skip] {source_url} produced 0 usable rows")
                continue

            print(f"[ok] {source_url} -> {len(prepared_frame)} rows")
            output_frames.append(prepared_frame)

    if not output_frames:
        raise RuntimeError("No TLC files were loaded for the selected interval and taxi_types.")

    return pd.concat(output_frames, ignore_index=True)
