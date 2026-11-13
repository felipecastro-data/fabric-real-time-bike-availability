"""DQ gate for the warm path and `dim_station`. Each check is a `Check`; the notebook fails when a gating check fails.

* SCD2 invariants: unique version key, one current version per station, contiguous intervals, open end only on
  current versions
* Reconciliation: Bronze = Silver + dead-letter (every valid Bronze key is in Silver and nothing else is; every
  rejected Bronze row is in the dead-letter), over rows polled before a cutoff so in-flight batches don't count
* Dead-letter rate and Silver freshness thresholds
* Trips bridge coverage (reported, not gating): stations with a `short_name`, and the share of trips whose start
  station joins a current station
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from divvy_rti.availability import validate
from divvy_rti.dim_station import OPEN_END


@dataclass(frozen=True)
class Check:
    name: str
    passed: bool
    value: float | int | None
    detail: str
    gate: bool = True


def _zero(name: str, rows: int, detail: str) -> Check:
    return Check(name, rows == 0, rows, detail)


def scd2_checks(dim: DataFrame) -> list[Check]:
    prev_end = F.lag("valid_to").over(Window.partitionBy("station_key").orderBy("valid_from"))
    gap = F.col("_prev_end").isNotNull() & (F.col("_prev_end") != F.col("valid_from"))
    gaps = dim.withColumn("_prev_end", prev_end).where((F.col("valid_from") >= F.col("valid_to")) | gap)
    open_end = F.col("valid_to") == F.lit(OPEN_END)
    return [
        _zero(
            "scd2.unique_version_key",
            dim.groupBy("station_version_key").count().where("count > 1").count(),
            "station_version_key values used by more than one row",
        ),
        _zero(
            "scd2.one_current_per_station",
            dim.where("is_current").groupBy("station_key").count().where("count > 1").count(),
            "stations with more than one current version",
        ),
        _zero("scd2.contiguous_intervals", gaps.count(), "versions with a gap, overlap or empty interval"),
        _zero(
            "scd2.open_end_iff_current",
            dim.where(F.col("is_current") != open_end).count(),
            "current versions not open-ended, or closed versions still open-ended",
        ),
    ]


def reconciliation_checks(bronze: DataFrame, silver: DataFrame, deadletter: DataFrame, cutoff: datetime) -> list[Check]:
    checked = validate(bronze).where(F.col("polled_at") < F.lit(cutoff))
    keys = ["station_id", "last_reported"]
    bronze_keys = checked.where(F.col("reject_reason").isNull()).select(*keys).distinct()
    silver_keys = silver.where(F.col("polled_at") < F.lit(cutoff)).select(*keys)
    rejected = checked.where(F.col("reject_reason").isNotNull()).select("row_hash").distinct()
    return [
        _zero(
            "reconcile.bronze_keys_missing_in_silver",
            bronze_keys.join(silver.select(*keys), keys, "left_anti").count(),
            f"valid Bronze (station_id, last_reported) polled before {cutoff:%Y-%m-%d %H:%M} UTC not in Silver",
        ),
        _zero(
            "reconcile.silver_keys_not_in_bronze",
            silver_keys.join(bronze_keys, keys, "left_anti").count(),
            "Silver rows without a valid Bronze row",
        ),
        _zero(
            "reconcile.rejected_missing_in_deadletter",
            rejected.join(deadletter.select("row_hash"), "row_hash", "left_anti").count(),
            "rejected Bronze rows not in the dead-letter",
        ),
    ]


def deadletter_rate_check(bronze_rows: int, deadletter_rows: int, max_rate: float = 0.01) -> Check:
    rate = deadletter_rows / bronze_rows if bronze_rows else 0.0
    return Check("deadletter.rate", rate <= max_rate, rate, f"dead-letter rows / Bronze rows <= {max_rate:.0%}")


def freshness_check(silver: DataFrame, now: datetime, max_age: timedelta) -> Check:
    # In Spark: collected timestamps are naive local times, so Python-side arithmetic would depend on the machine TZ
    age_s = silver.agg(F.lit(now).cast("long") - F.max("polled_at").cast("long")).first()[0]
    age_min = None if age_s is None else age_s / 60
    return Check(
        "silver.freshness",
        age_min is not None and age_min <= max_age.total_seconds() / 60,
        age_min,
        f"minutes since the latest Silver poll <= {max_age.total_seconds() / 60:.0f}",
    )


def coverage_checks(dim: DataFrame, fact_trips: DataFrame) -> list[Check]:
    """Report how far the GBFS ↔ trips bridge reaches (not gating: the 901 lightweight stations have no key)."""
    current = dim.where("is_current")
    total = current.count()
    keyed = current.where(F.col("trips_station_key").isNotNull()).select("trips_station_key").distinct()
    latest_date_key = fact_trips.agg(F.max("date_key")).first()[0]
    month = fact_trips.where(  # latest month in fact_trips; dockless trips (key -1) excluded
        (F.col("date_key") >= (latest_date_key // 100) * 100) & (F.col("start_station_key") != -1)
    ) if latest_date_key else fact_trips.limit(0)  # fmt: skip
    trips = month.count()
    joined = month.join(keyed, month.start_station_key == keyed.trips_station_key, "left_semi").count()
    return [
        Check(
            "bridge.stations_with_short_name",
            True,
            keyed.count() / total if total else None,
            f"{keyed.count()} of {total} current stations can join trips by short_name",
            gate=False,
        ),
        Check(
            "bridge.trip_share",
            True,
            joined / trips if trips else None,
            f"{joined} of {trips} station trips in month {latest_date_key and latest_date_key // 100} "
            "start at a current GBFS station",
            gate=False,
        ),
    ]
