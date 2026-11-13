import copy
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

pytest.importorskip("delta")

from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql import types as T  # noqa: E402

from divvy_rti import dq  # noqa: E402
from divvy_rti.availability import write_silver_batch  # noqa: E402
from divvy_rti.dim_station import apply_snapshots, hash_key  # noqa: E402
from divvy_rti.gbfs import station_information_rows  # noqa: E402

INFO_SCHEMA = T.StructType(
    [
        T.StructField("station_id", T.StringType()),
        T.StructField("name", T.StringType()),
        T.StructField("short_name", T.StringType()),
        T.StructField("lat", T.DoubleType()),
        T.StructField("lon", T.DoubleType()),
        T.StructField("capacity", T.LongType()),
        T.StructField("region_id", T.StringType()),
        T.StructField("address", T.StringType()),
        T.StructField("is_charging", T.BooleanType()),
        T.StructField("raw", T.StringType()),
        T.StructField("feed_last_updated", T.TimestampType()),
        T.StructField("fetched_at", T.TimestampType()),
        T.StructField("snapshot_date", T.DateType()),
    ]
)
DAY1 = datetime(2026, 11, 12, 18, 0, tzinfo=UTC)


def payload():
    return json.loads((Path(__file__).parents[1] / "fixtures" / "station_information.json").read_text())


def land(spark, path, info, at):
    info = copy.deepcopy(info)
    info["last_updated"] = int(at.timestamp())
    _, rows = station_information_rows(info, at)
    for row in rows:
        row["snapshot_date"] = date.fromisoformat(row["snapshot_date"])
    spark.createDataFrame([tuple(r[f.name] for f in INFO_SCHEMA.fields) for r in rows], INFO_SCHEMA).write.format(
        "delta"
    ).mode("append").save(path)


def day2(info):
    """A rename, a capacity change, a 20 m drift (type 1), a 500 m move, a removal and a new station."""
    stations = info["data"]["stations"]
    stations[0]["name"] = "Renamed Park"
    stations[1]["capacity"] += 4
    stations[2]["lat"] += 0.00018  # ≈ 20 m
    stations[3]["lat"] += 0.0045  # ≈ 500 m
    removed = stations.pop(4)
    stations.append({**stations[5], "station_id": "new-1", "short_name": None, "name": "Brand New"})
    return info, removed["station_id"]


def dim(spark, path):
    return spark.read.format("delta").load(path)


def test_scd2_initial_changes_rerun_and_backfill(spark, tmp_path):
    bronze, gold = str(tmp_path / "bronze"), str(tmp_path / "dim_station")
    land(spark, bronze, payload(), DAY1)

    (first,) = apply_snapshots(spark, bronze, gold)
    d = dim(spark, gold)
    assert first["insert"] == 20 and d.count() == 20
    assert {r.change_reason for r in d.collect()} == {"initial"}
    assert d.where("station_type = 'classic' AND trips_station_key IS NOT NULL").count() == 10
    assert d.where("station_type = 'lightweight' AND trips_station_key IS NULL").count() == 10

    info, removed_id = day2(payload())
    land(spark, bronze, info, DAY1 + timedelta(days=1))
    (second,) = apply_snapshots(spark, bronze, gold)
    d = dim(spark, gold)
    assert second == {**second, "insert": 4, "close": 4, "type1": 1}
    assert d.count() == 24 and d.where("is_current").count() == 20
    opened = {r.name: r.change_reason for r in d.where("valid_from > '2026-11-13'").collect()}
    assert sorted(opened.values()) == ["capacity", "new", "relocated", "renamed"]
    (gone,) = d.where(F.col("station_id") == removed_id).collect()
    assert not gone.is_current and gone.end_reason == "removed"
    drifted = payload()["data"]["stations"][2]["station_id"]
    assert d.where(F.col("station_id") == drifted).count() == 1  # type 1: updated in place
    assert all(c.passed for c in dq.scd2_checks(d)), dq.scd2_checks(d)

    assert apply_snapshots(spark, bronze, gold) == []  # rerun: nothing newer
    land(spark, bronze, payload(), DAY1 - timedelta(days=1))  # late backfill of an older day
    assert apply_snapshots(spark, bronze, gold) == [] and dim(spark, gold).count() == 24


def test_scd2_checks_catch_two_current_versions_and_gaps(spark, tmp_path):
    bronze, gold = str(tmp_path / "bronze"), str(tmp_path / "dim_station")
    land(spark, bronze, payload(), DAY1)
    apply_snapshots(spark, bronze, gold)
    one = dim(spark, gold).limit(1).withColumn("station_version_key", F.lit(1))
    broken = dim(spark, gold).unionByName(one.withColumn("valid_from", F.lit(DAY1 + timedelta(hours=1))))
    failed = {c.name for c in dq.scd2_checks(broken) if not c.passed}
    assert failed == {"scd2.one_current_per_station", "scd2.contiguous_intervals"}


def test_reconciliation_rate_and_freshness(spark, tmp_path):
    from test_availability import df, row

    from divvy_rti.availability import DEADLETTER_SCHEMA, SILVER_SCHEMA, ensure_table

    silver, deadletter = str(tmp_path / "silver"), str(tmp_path / "dl")
    ensure_table(spark, silver, SILVER_SCHEMA, ("report_date",))
    ensure_table(spark, deadletter, DEADLETTER_SCHEMA)
    rows = [row("s1", m) for m in range(5)] + [row("s2", 1, bikes=-1)]
    bronze = df(spark, rows)
    write_silver_batch(bronze, 0, silver, deadletter)
    cutoff = datetime(2026, 11, 14, tzinfo=UTC)

    s, d = dim(spark, silver), dim(spark, deadletter)
    assert all(c.passed for c in dq.reconciliation_checks(bronze, s, d, cutoff))
    failed = {c.name for c in dq.reconciliation_checks(bronze, s.limit(4), d.limit(0), cutoff) if not c.passed}
    assert failed == {"reconcile.bronze_keys_missing_in_silver", "reconcile.rejected_missing_in_deadletter"}

    assert dq.deadletter_rate_check(1000, 10).passed and not dq.deadletter_rate_check(1000, 11).passed
    latest = datetime(2026, 11, 13, 12, 4, tzinfo=UTC)  # minute 4
    fresh = dq.freshness_check(s, latest + timedelta(minutes=10), timedelta(minutes=30))
    stale = dq.freshness_check(s, latest + timedelta(minutes=31), timedelta(minutes=30))
    assert fresh.passed and not stale.passed


def test_bridge_coverage(spark, tmp_path):
    bronze, gold = str(tmp_path / "bronze"), str(tmp_path / "dim_station")
    land(spark, bronze, payload(), DAY1)
    apply_snapshots(spark, bronze, gold)
    short_names = [s["short_name"] for s in payload()["data"]["stations"] if s.get("short_name")]
    trips = spark.createDataFrame(
        [(20261101, short_names[0]), (20261102, short_names[1]), (20261103, "CHI99999"), (20261001, short_names[2])]
        + [(20261103, None)],
        "date_key int, sid string",
    ).select(
        "date_key",
        F.when(F.col("sid").isNull(), F.lit(-1).cast("long"))
        .otherwise(hash_key(F.col("sid")))
        .alias("start_station_key"),
    )
    stations, share = dq.coverage_checks(dim(spark, gold), trips)
    assert stations.value == 0.5 and share.value == pytest.approx(2 / 3)  # Nov: 3 station trips, 2 join; dockless out
